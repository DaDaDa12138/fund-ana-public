from __future__ import annotations

from backend.lifetime_returns import load_platform_lifetime_returns

import csv
import hashlib
import sqlite3
import json
import os
import sys
import threading
import time
from collections import Counter
from copy import deepcopy
from datetime import date, datetime, timedelta
from io import BytesIO
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlparse
from uuid import uuid4

import pandas as pd
from core.utils.core_csv_storage import (
    CoreCsvStorageError, artifact_is_file, glob_artifacts, has_stored_csv,
    open_csv_artifact, original_artifact_stat, read_csv_artifact,
    read_original_artifact_bytes as read_core_artifact_bytes, stored_csv_cache_token,
)
import requests
import yaml
from flask import Flask, Response, g, has_request_context, jsonify, request, send_from_directory

from backend.api_routes.dashboard import register_dashboard_routes
from backend.api_routes.favorites import register_favorites_routes
from backend.api_routes.publication import (
    build_publication_registry_summary,
    register_publication_routes,
)
from backend.api_routes.recommendations import register_recommendations_routes
from backend.api_routes.reports import register_reports_routes
from backend.api_routes.scheduler import register_scheduler_routes
from backend.api_routes.system import register_system_routes
from backend.api_routes.tracker import register_tracker_routes
from backend.http_compression import register_response_compression
from backend.collection_count_correction import load_verified_correction as load_collection_count_correction
from backend.refresh_collection_correction import (
    annotate_scheduler_payload,
    load_verified_correction as load_refresh_collection_correction,
)
from backend.api_payloads import dashboard as dashboard_payloads
from backend.api_payloads import info as info_payloads
from backend.api_payloads import recommendations as recommendation_payloads
from backend.autostock_budget import plan_autostock_call_budget, read_autostock_quota_snapshot
from backend.config import build_database_source_audit, load_settings
from backend.publication_dependencies import configured_publication_blocker
from backend.database_freshness_summary import (
    build_database_freshness_summary,
    database_freshness_summary_path,
    load_valid_database_freshness_summary,
    summarize_database_freshness_payload,
    write_database_freshness_summary,
)
from backend import scheduler_diagnostics
from backend.scheduler_lineage import daily_screening_evidence
from backend.schema_bootstrap import ensure_database_schema
from backend.autostock_nav_tail_fill import (
    build_missing_response_observation_payload,
    build_missing_response_publish_impact_summary,
    load_autostock_nav_tail_fill_snapshot,
)
from backend.nav_history_maintenance import (
    build_nav_history_integrity_snapshot,
    load_latest_nav_history_maintenance_payload,
)
from backend.recommendations_sidecar import (
    attach_recommendations_detail_source_contract,
    build_recommendations_detail_cache_basis as build_run_local_recommendations_detail_cache_basis,
    build_recommendations_detail_response as build_run_local_recommendations_detail_response,
    overlay_recommendations_tracker_occurrences,
    recommendations_display_report,
)
from backend.long_term_governance import (
    STALE_SCOPE_NAV_BUCKET_SCHEMA_VERSION,
    build_long_term_governance_summary,
    build_long_term_maintenance_detail,
)
from backend.long_term_history_maintenance import (
    build_long_term_history_gap_audit,
    load_latest_long_term_history_maintenance_payload,
    _maintenance_state_path,
    _hydrate_failure_backoff_policy,
    _load_maintenance_state,
    _read_bond_universe,
    _resolve_empty_payload_retry_days,
    _resolve_empty_payload_upstream_limited_failures,
    _resolve_stale_refresh_retry_days,
    _resolve_stale_refresh_upstream_limited_failures,
)
from backend.annual_return_gate_shadow import load_latest_annual_return_gate_shadow_payload
from backend.low_vol_overlap_audit import low_vol_overlap_cache_token
from backend.nav_lag_audit import load_latest_nav_lag_audit_payload
from backend.risk_coverage_gap_audit import load_latest_risk_coverage_gap_audit_payload
from backend.risk_lag_audit import load_latest_risk_lag_audit_payload
from backend.risk_sync_audit import load_latest_risk_sync_audit_payload
from backend.risk_coverage_semantics import build_risk_coverage_threshold_semantics
from backend.effective_nav import (
    _latest_risk_rowid_predicate,
    _risk_metric_ready_predicate,
    load_effective_nav_snapshot,
)
from backend.fee_semantics import (
    evaluate_fee_semantics,
    fee_semantics_ready,
    load_sales_fee_validation_classification_map,
)
from backend.final_selection_governance_queue import load_final_selection_governance_queue
from backend.publication import (
    load_current_published_report_payload,
    load_latest_canonical_report_payload,
    load_publication_registry,
)
from backend.publication_status import build_publication_status_summary, build_publication_summary
from backend.project_publish_decisions import (
    HOLDING_PERIOD_GENERIC_PLACEHOLDER_STATUS,
    HOLDING_PERIOD_NAME_UNPARSED_STATUS,
    HOLDING_PERIOD_NO_SIGNAL_STATUS,
    combine_holding_period_names,
    resolve_holding_period_status,
    DATA_VALIDATION_COLUMNS,
    validation_status_value,
)
from backend.project_tracker import PROJECT_TRACKER_INTERNAL_TO_EXPORT_RENAME
from backend.scheduler import load_scheduler_status
from backend.scheduler import ProjectScheduler
from backend.weekly_warning_contract import (
    UNRESOLVED_STAGE_STATUS,
    unresolved_stage_detail_errors,
)
from backend.scheduler import _select_task_closure_source
from backend.scheduler_state import SchedulerStateStore
from backend.scheduler_maintenance_metrics import restore_weekly_maintenance_metrics
from backend.scheduler_status_support import summarize_task_closures_for_state
from backend.workflow import WorkflowManager
from backend.metric_units import metric_unit_metadata
from core.utils.drawdown_duration import (
    ANNUAL_DRAWDOWN_OBSERVATIONS,
    normalize_drawdown_duration_record,
)
from backend.report_workbook import _add_sheet_with_style
from backend.report_builder import (
    _build_annual_return_score_sharpe_coupling_audit,
    _build_score_cluster_explainability_audit,
    build_formal_score_contract,
)
from backend.high_score_friction import load_high_score_friction_gate_review_from_workbook
from backend.risk_adjusted_metric_stack import (
    load_risk_adjusted_metric_stack_review_from_workbook,
    normalize_positive_ratio_review_evidence,
)
from backend.api_payloads.positive_ratio import (
    build_positive_ratio_semantics_summary,
    normalize_low_vol_positive_ratio_shadow_watch,
)
try:
    from core.version import CHANGELOG, VERSION_INFO, __version__
except ModuleNotFoundError:  # pytest and some scripts put core/ itself on sys.path.
    from version import CHANGELOG, VERSION_INFO, __version__


settings = load_settings()
manager = WorkflowManager(settings)
# P0-01 / B1: 在任何 audit / workflow 路径读写风险指标表之前先把 schema 迁移跑完。
_schema_bootstrap_result = ensure_database_schema(settings)
FRONTEND_DIR = settings.project_root / "frontend"
MANUAL_OPERATION_MODE = os.environ.get("FUND_ANA_ENABLE_MANUAL_OPERATIONS", "").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}

app = Flask(__name__, static_folder=None)
register_response_compression(app)
_FRONTEND_PAGE_PATHS = frozenset(
    {
        "/",
        "/tracker",
        "/recommendations",
        "/favorites",
        "/reports",
        "/task-summary",
        "/operations",
        "/settings",
        "/api-settings",
        "/excel-viewer",
    }
)
_FRONTEND_ASSET_SUFFIXES = (".js", ".css", ".svg")
_scheduler_bootstrap_lock = threading.Lock()
_scheduler_bootstrap_inflight = False
_scheduler_bootstrap_retry_after = 0.0
_embedded_scheduler: ProjectScheduler | None = None
_excel_cache_lock = threading.Lock()
_excel_cache: dict[str, dict] = {}
_EXCEL_CACHE_MAX_ENTRIES = 16
_valuation_cache_lock = threading.Lock()
_valuation_cache: dict[str, dict] = {}
_VALUATION_CACHE_TTL_SECONDS = 300
_VALUATION_MISSING_CACHE_TTL_SECONDS = 30
_TRACKER_VALUATION_SAFE_MAX_REQUEST_CODES = 50
_TRACKER_VALUATION_AUTOMATION_RESERVE_CALLS = 0
_TRACKER_VALUATION_SOURCE_BASIS_SCHEMA_VERSION = 4
_TRACKER_VALUATION_SOURCE_CONTRACT_SCHEMA_VERSION = 2
_TRACKER_VALUATION_ENDPOINTS = tuple(
    dict.fromkeys(
        endpoint
        for endpoint in (
            settings.tiantian_valuation_primary_url,
            settings.tiantian_valuation_fallback_url,
        )
        if str(endpoint or "").strip()
    )
)
_TRACKER_VALUATION_FIELDS = "FCODE,SHORTNAME,GSZZL,GZTIME,GSZ,NAV,PDATE"
_TRACKER_VALUATION_HTTP_TIMEOUT_SECONDS = 12.0
_TRACKER_CURRENT_SOURCE_BASIS_SCHEMA_VERSION = 1
_TRACKER_CURRENT_SOURCE_CONTRACT_SCHEMA_VERSION = 1
_RECOMMENDATIONS_SUMMARY_SOURCE_BASIS_SCHEMA_VERSION = 1
_RECOMMENDATIONS_SUMMARY_SOURCE_CONTRACT_SCHEMA_VERSION = 1
_RECOMMENDATIONS_DETAIL_SOURCE_CONTRACT_SCHEMA_VERSION = 2
_LATEST_REPORT_SOURCE_BASIS_SCHEMA_VERSION = 1
_PUBLICATION_SUMMARY_SOURCE_BASIS_SCHEMA_VERSION = 1
_PUBLICATION_SUMMARY_SOURCE_CONTRACT_SCHEMA_VERSION = 1
_DASHBOARD_LATEST_SOURCE_BASIS_SCHEMA_VERSION = 2
_DASHBOARD_LATEST_SOURCE_CONTRACT_SCHEMA_VERSION = 2
_EXCEL_PREVIEW_SOURCE_BASIS_SCHEMA_VERSION = 1
_PROJECT_FILE_SOURCE_HEADER_SCHEMA_VERSION = 1
_SCHEDULER_DETAIL_SOURCE_BASIS_SCHEMA_VERSION = 1
_LIGHTWEIGHT_STATUS_SOURCE_BASIS_SCHEMA_VERSION = 1
_LIGHTWEIGHT_STATUS_SOURCE_CONTRACT_SCHEMA_VERSION = 1
_FAVORITE_AUTOSTOCK_AUXILIARY_RESERVE_CALLS = 20
# Cover the favorite query limit plus the auto-appended recommendation pool.
_FAVORITE_AUTOSTOCK_AUXILIARY_MAX_REQUEST_CODES = 300
_payload_cache_lock = threading.Lock()
_payload_cache: dict[str, dict[str, Any]] = {}
_payload_cache_inflight: dict[str, threading.Event] = {}
_api_prewarm_lock = threading.Lock()
_api_prewarm_started = False
_autostock_client_lock = threading.Lock()
_autostock_client: Any | None = None
_core_runtime_config_lock = threading.Lock()
_core_runtime_config: dict[str, Any] | None = None
_API_RUNTIME_STARTED_AT = datetime.now().isoformat(timespec="seconds")


def _is_frontend_no_store_path(path: str | None) -> bool:
    normalized = str(path or "").strip() or "/"
    return normalized in _FRONTEND_PAGE_PATHS or normalized.endswith(_FRONTEND_ASSET_SUFFIXES)


@app.after_request
def _apply_frontend_no_store_headers(response: Response) -> Response:
    if request.method in {"GET", "HEAD"} and _is_frontend_no_store_path(request.path):
        versioned_asset = (
            request.path.endswith(_FRONTEND_ASSET_SUFFIXES)
            and "/" not in request.path.lstrip("/")
            and request.args.get("v") == __version__
            and response.status_code in {200, 304}
        )
        if versioned_asset:
            response.headers["Cache-Control"] = "public, max-age=31536000, immutable"
            response.headers.pop("Pragma", None)
            response.headers.pop("Expires", None)
            return response
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
        response.headers.pop("ETag", None)
    return response


def _extract_audit_runtime_scope_summary(payload: dict[str, Any] | None) -> dict[str, Any]:
    current = dict(payload or {})
    replay_precision = dict(current.get("replay_precision") or {})
    return {
        "run_id": current.get("run_id"),
        "latest_canonical_run_id": current.get("latest_canonical_run_id"),
        "replay_precision_status": current.get("replay_precision_status") or replay_precision.get("status"),
        "replay_precision_status_label": current.get("replay_precision_status_label") or replay_precision.get("status_label"),
        "auxiliary_scope": current.get("auxiliary_scope"),
        "auxiliary_scope_label": current.get("auxiliary_scope_label"),
        "scope_detail": current.get("scope_detail"),
    }

RECOMMENDATION_EXPORT_COLUMNS = [
    "序号",
    "基金代码",
    "基金名称",
    "建议权重",
    "正式投资评分",
    "投资优先池得分",
    "实时估值",
    "正式榜分组",
    "债基风格标签",
    "长期稳定性标签",
    "基金公司",
    "出现次数&出现频率(%)",
    "最新净值日期",
    "基金规模(亿)",
    "1年-年化收益(%)",
    "平台成立以来收益年化(%)",
    "累计净值比年化(%)",
    "近1周(%)",
    "近1月(%)",
    "近3月(%)",
    "近6月(%)",
    "1年-夏普比率",
    "1年-最大回撤(%)",
    "1年-波动率",
    "项目质量信心",
    "AutoStock原始校验状态",
    *DATA_VALIDATION_COLUMNS,
    "费率合计(%)",
    "入选原因",
    "风险提示",
]

FAVORITE_FUND_HISTORY_LIMIT = 20
FAVORITE_FUND_QUERY_MAX_CODES = 300
FAVORITE_HISTORY_ENTRY_TYPE_QUERY = "query"
FAVORITE_HISTORY_ENTRY_TYPE_FAVORITE = "favorite"
FAVORITE_FUND_SCORE_PAYLOAD_MODE_COMPACT = "compact"
FAVORITE_FUND_SCORE_PAYLOAD_MODE_FULL = "full"
FAVORITE_SCORE_SOURCE_BASIS_SCHEMA_VERSION = 2
FAVORITE_SCORE_SOURCE_CONTRACT_SCHEMA_VERSION = 2
FAVORITE_FUND_EXPORT_COLUMNS = [
    "序号",
    "基金代码",
    "基金名称",
    "当前层级",
    "建议动作",
    "实时估值",
    "实时估值时间",
    "实时估算净值",
    "最新单位净值",
    "基金经理",
    "基金类型",
    "出现次数&出现频率(%)",
    "1年-年化收益(%)",
    "平台成立以来收益年化(%)",
    "累计净值比年化(%)",
    "近1周(%)",
    "近1月(%)",
    "近3月(%)",
    "近6月(%)",
    "1年-夏普比率",
    "1年-最大回撤(%)",
    "1年-波动率",
    "投资优先池得分",
    "正式投资评分",
    "最新净值日期",
    "基金规模(亿)",
    "正式榜分组",
    "债基风格标签",
    "推荐解读",
    "基金公司",
    "费率合计(%)",
    "长期稳定性标签",
]

_DASHBOARD_LATEST_REPORT_CACHE_TTL_SECONDS = 300.0
_DASHBOARD_OVERVIEW_CACHE_TTL_SECONDS = 300.0
_DASHBOARD_OVERVIEW_SIDECAR_SCHEMA_VERSION = 2
_DASHBOARD_OVERVIEW_SOURCE_BASIS_SCHEMA_VERSION = 1
_DASHBOARD_OVERVIEW_SOURCE_CONTRACT_SCHEMA_VERSION = 1
_RECOMMENDATIONS_DETAIL_SIDECAR_SCHEMA_VERSION = 3
_RECOMMENDATIONS_DETAIL_SIDECAR_META_LABEL = "__recommendations_detail_sidecar"
_RECOMMENDATIONS_DETAIL_DIAGNOSTICS_LABEL = "recommendations_detail_diagnostics"
_SCHEDULER_STATUS_CACHE_TTL_SECONDS = 15.0
_SCHEDULER_STATUS_DETAIL_CACHE_TTL_SECONDS = 60.0
_SCHEDULER_SUMMARY_FRAGMENT_CACHE_TTL_SECONDS = 60.0
_CURRENT_FINAL_SELECTION_GOVERNANCE_QUEUE_CACHE_TTL_SECONDS = 15.0
_SCHEDULER_DETAIL_INDEX_SCHEMA_VERSION = 1
SCHEDULER_SUMMARY_BUILD_PROFILE_SCHEMA_VERSION = 2
SCHEDULER_DETAIL_BUILD_PROFILE_SCHEMA_VERSION = 1
_DATABASE_INTEGRITY_CACHE_TTL_SECONDS = 300.0
_DATABASE_OVERVIEW_CACHE_TTL_SECONDS = 300.0
_RISK_LAG_SUMMARY_CACHE_TTL_SECONDS = 300.0
_RISK_SYNC_SUMMARY_CACHE_TTL_SECONDS = 300.0
_DISK_USAGE_CACHE_TTL_SECONDS = 30.0
_PUBLICATION_REGISTRY_CACHE_TTL_SECONDS = 300.0
_DASHBOARD_COMPACT_CACHE_TTL_SECONDS = 60.0
_DASHBOARD_SCOPED_DETAIL_CACHE_TTL_SECONDS = 60.0
_LONG_TERM_NO_STALE_AUDIT_CACHE_TTL_SECONDS = 300.0
_TRACKER_CURRENT_DEFAULT_PAGE_SIZE = 50
_TRACKER_CURRENT_MAX_PAGE_SIZE = 200
_COMPACT_REPORT_COUNT_KEYS = (
    "final_count",
    "dynamic_final_count",
    "stable_bond_final_count",
    "enhanced_bond_final_count",
    "project_tracker_current_active_count",
    "project_tracker_total_periods",
    "project_tracker_fund_count",
    "project_tracker_repeated_fund_count",
    "screenable_universe_count",
    "publishable_pool_count",
    "has_latest_nav_count",
    "has_1y_risk_metrics_count",
)
_COMPACT_REPORT_ARTIFACT_KEYS = (
    "filtered_excel",
    "filtered_csv",
    "project_tracker_excel",
    "report_markdown",
    "report_json",
    "publish_chain_snapshot_json",
    "forward_validation_excel",
    "forward_validation_json",
)
_COMPACT_REPORT_SNAPSHOT_KEYS = (
    "candidate_date",
    "candidate_fund_count",
    "coverage_ratio_vs_total",
    "coverage_ratio_vs_candidate",
    "coverage_threshold_ratio",
    "risk_1y_effective_end_date",
    "risk_1y_effective_fund_count",
    "risk_1y_coverage_ratio_vs_candidate",
    "risk_1y_coverage_ratio_vs_total",
    "risk_publish_ready",
)
_SINCE_INCEPTION_ANNUALIZED_RETURN_LABEL = "平台成立以来收益年化(%)"
_SINCE_INCEPTION_ANNUALIZED_MIN_HISTORY_DAYS = 365
_SINCE_INCEPTION_ANNUALIZED_MAX_ESTAB_GAP_DAYS = 30
_SINCE_INCEPTION_ANNUALIZED_NEGATIVE_ESTAB_TOLERANCE_DAYS = 7
_SINCE_INCEPTION_ANNUALIZED_YEAR_DAYS = 365.2425
_SINCE_INCEPTION_ANNUAL_RETURN_BREAKDOWN_LABEL = "成立以来年度收益明细"
_SINCE_INCEPTION_ANNUAL_RETURN_VOLATILITY_LABEL = "__favorite_since_inception_annual_return_volatility_pct"
_FAVORITE_PERIODIC_RETURN_DETAILS_LABEL = "__favorite_periodic_return_details"
_FAVORITE_PERIODIC_WEEKLY_DETAIL_KEY = "weekly_recent"
_FAVORITE_PERIODIC_MONTHLY_DETAIL_KEY = "monthly_ytd"
_FAVORITE_LIVE_RETURN_1W_LABEL = "__favorite_live_return_1w_pct"
_FAVORITE_LIVE_RETURN_1M_LABEL = "__favorite_live_return_1m_pct"
_FAVORITE_LIVE_RETURN_3M_LABEL = "__favorite_live_return_3m_pct"
_FAVORITE_LIVE_RETURN_6M_LABEL = "__favorite_live_return_6m_pct"
_FAVORITE_RETURN_1W_SOURCE_LABEL = "__favorite_return_1w_source"
_FAVORITE_RETURN_1M_SOURCE_LABEL = "__favorite_return_1m_source"
_FAVORITE_RETURN_3M_SOURCE_LABEL = "__favorite_return_3m_source"
_FAVORITE_RETURN_6M_SOURCE_LABEL = "__favorite_return_6m_source"
_FAVORITE_METRIC_DETAIL_CACHE_TTL_SECONDS = 300.0
_FAVORITE_METRIC_DETAIL_MAX_CODES = 20
FAVORITE_METRIC_DETAIL_SOURCE_BASIS_SCHEMA_VERSION = 2
FAVORITE_METRIC_DETAIL_SOURCE_CONTRACT_SCHEMA_VERSION = 1
_FAVORITE_METRIC_DETAIL_ANNUAL_BREAKDOWN_KEY = "since_inception_annual_return_breakdown"
_FAVORITE_METRIC_DETAIL_ANNUAL_VOLATILITY_KEY = "since_inception_annual_return_volatility_pct"
_STYLE_METRIC_REFERENCES_LABEL = "__style_metric_references"
_STYLE_METRIC_BENCHMARKS_CACHE_TTL_SECONDS = 300.0
_STYLE_METRIC_BENCHMARK_MIN_DISTRIBUTION_SIZE = 3
_STYLE_METRIC_BENCHMARK_PREFERRED_SAMPLE_SIZE = 10
_STYLE_METRIC_BENCHMARK_GLOBAL_LABEL = "全项目债基"
_TRACKER_CURRENT_DERIVED_FIELDS_SCHEMA_VERSION = 2
_TRACKER_CURRENT_DETAIL_DIAGNOSTICS_LABEL = "tracker_current_detail_diagnostics"
_FAVORITE_STYLE_THRESHOLD_FIELDS = (
    "债基风格标签",
    "样本数",
    "阈值来源",
    "综合加权得分下限",
    "近期得分下限",
    "长期得分下限",
    "1年年化收益下限",
    "1年夏普下限",
    "1年索提诺下限",
    "1年卡玛下限",
    "1年正收益率下限",
    "费率上限(%)",
    "1年最大回撤上限",
    "1年波动率上限",
    "规模下限(亿)",
)
_STYLE_METRIC_REFERENCE_SPECS = (
    {
        "field": "正式投资评分",
        "metric_label": "正式投资评分",
        "digits": 2,
        "suffix": "",
        "lower_is_better": False,
        "threshold_field": "综合加权得分下限",
    },
    {
        "field": "1年-年化收益(%)",
        "metric_label": "1年年化收益",
        "digits": 2,
        "suffix": "%",
        "lower_is_better": False,
        "threshold_field": "1年年化收益下限",
    },
    {
        "field": "平台成立以来收益年化(%)",
        "metric_label": "平台成立以来收益年化",
        "digits": 2,
        "suffix": "%",
        "lower_is_better": False,
        "threshold_field": None,
    },
    {
        "field": "近1周(%)",
        "metric_label": "近1周收益",
        "digits": 2,
        "suffix": "%",
        "lower_is_better": False,
        "threshold_field": None,
    },
    {
        "field": "近1月(%)",
        "metric_label": "近1月收益",
        "digits": 2,
        "suffix": "%",
        "lower_is_better": False,
        "threshold_field": None,
    },
    {
        "field": "近3月(%)",
        "metric_label": "近3月收益",
        "digits": 2,
        "suffix": "%",
        "lower_is_better": False,
        "threshold_field": None,
    },
    {
        "field": "1年-夏普比率",
        "metric_label": "1年策略夏普",
        "digits": 4,
        "suffix": "",
        "lower_is_better": False,
        "threshold_field": "1年夏普下限",
    },
    {
        "field": "1年-最大回撤(%)",
        "metric_label": "1年最大回撤",
        "digits": 4,
        "suffix": "%",
        "lower_is_better": True,
        "threshold_field": "1年最大回撤上限",
    },
    {
        "field": "1年-波动率",
        "metric_label": "1年波动率",
        "digits": 4,
        "suffix": "",
        "lower_is_better": True,
        "threshold_field": "1年波动率上限",
    },
)

RECOMMENDATION_EXPORT_COLUMN_WIDTHS = {
    "推荐分组": 16,
    "序号": 8,
    "基金代码": 12,
    "基金名称": 24,
    "建议权重": 12,
    "实时估值": 12,
    "出现次数&出现频率(%)": 16,
    "1年-年化收益(%)": 14,
    "平台成立以来收益年化(%)": 18,
    "近1月(%)": 12,
    "近3月(%)": 12,
    "1年-夏普比率": 12,
    "1年-最大回撤(%)": 14,
    "1年-波动率": 12,
    "投资优先池得分": 14,
    "正式投资评分": 14,
    "最新净值日期": 12,
    "基金规模(亿)": 12,
    "正式榜分组": 14,
    "债基风格标签": 14,
    "项目质量信心": 12,
    "AutoStock原始校验状态": 18,
    "基金公司": 18,
    "费率合计(%)": 12,
    "长期稳定性标签": 16,
    "入选原因": 42,
    "风险提示": 36,
}


class TrackerValuationFetchError(RuntimeError):
    """实时估值批量补取失败。"""


class FavoriteAutostockSnapshot(dict):
    """收藏评分 AutoStock 辅助快照，附带预算元数据。"""

    def __init__(self, rows: dict[str, dict[str, Any]] | None = None, *, meta: dict[str, Any] | None = None):
        super().__init__(rows or {})
        self.autostock_budget_meta = dict(meta or {})


def _runtime_scheduler_settings() -> Any:
    current_settings = settings
    config_path = getattr(current_settings, "config_path", None)
    if not config_path:
        return current_settings
    try:
        return load_settings(config_path)
    except Exception:
        return current_settings


def _scheduler_state_with_runtime_evidence(
    runtime_settings: Any,
    state_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    state = dict(
        state_payload
        if isinstance(state_payload, dict)
        else SchedulerStateStore(runtime_settings.scheduler_state_file).snapshot()
    )
    evidence = ProjectScheduler._scheduler_runtime_evidence(
        runtime_settings,
        state_payload=state,
    )
    state["scheduler_state_claimed_running"] = bool(state.get("scheduler_running"))
    state["scheduler_running"] = bool(evidence.get("alive"))
    state["scheduler_runtime_evidence"] = evidence
    state["scheduler_recovery_deferred"] = bool(
        state["scheduler_state_claimed_running"] and not evidence.get("alive")
    )
    state["scheduler_recovery_boundary"] = "exclusive_scheduler_lease_owner_only"
    try:
        effective_snapshot = load_effective_nav_snapshot(
            runtime_settings.database_path,
            coverage_ratio=runtime_settings.effective_nav_coverage_ratio,
            weekday_only=runtime_settings.effective_nav_weekday_only,
        )
        state["failed_screening_retry_cooldown"] = ProjectScheduler(
            settings=runtime_settings
        )._failed_screening_retry_cooldown(
            effective_snapshot,
            state_payload=state,
        )
    except Exception:
        state.setdefault("failed_screening_retry_cooldown", {})
    return state


def _should_bootstrap_embedded_scheduler() -> bool:
    runtime_settings = _runtime_scheduler_settings()
    if not runtime_settings.scheduler_enabled:
        return False
    if (
        str(getattr(runtime_settings, "scheduler_host_mode", "embedded_web") or "embedded_web").strip().lower()
        != "embedded_web"
    ):
        return False
    if os.environ.get("FUND_ANA_DISABLE_EMBEDDED_SCHEDULER") == "1":
        return False
    if "pytest" in sys.modules or os.environ.get("PYTEST_CURRENT_TEST"):
        return False
    if os.environ.get("WERKZEUG_RUN_MAIN") == "false":
        return False
    state_payload = SchedulerStateStore(runtime_settings.scheduler_state_file).snapshot()
    runtime_evidence = ProjectScheduler._scheduler_runtime_evidence(
        runtime_settings,
        state_payload=state_payload,
    )
    if runtime_evidence.get("alive"):
        return False
    return True


def _embedded_scheduler_should_remain_running() -> bool:
    runtime_settings = _runtime_scheduler_settings()
    if not runtime_settings.scheduler_enabled:
        return False
    if (
        str(getattr(runtime_settings, "scheduler_host_mode", "embedded_web") or "embedded_web").strip().lower()
        != "embedded_web"
    ):
        return False
    if os.environ.get("FUND_ANA_DISABLE_EMBEDDED_SCHEDULER") == "1":
        return False
    state_payload = SchedulerStateStore(runtime_settings.scheduler_state_file).snapshot()
    host_context = str(state_payload.get("host_context") or "").strip()
    runtime_evidence = ProjectScheduler._scheduler_runtime_evidence(
        runtime_settings,
        state_payload=state_payload,
    )
    if (
        runtime_evidence.get("alive")
        and host_context == "external_scheduler_process"
    ):
        return False
    return True


def _retire_embedded_scheduler_if_disallowed() -> None:
    global _embedded_scheduler
    scheduler = _embedded_scheduler
    if scheduler is None:
        return
    if not getattr(scheduler, "_started", False):
        _embedded_scheduler = None
        return
    if _embedded_scheduler_should_remain_running():
        return
    try:
        scheduler.shutdown()
    except Exception:
        pass
    finally:
        _embedded_scheduler = None


def ensure_embedded_scheduler() -> None:
    global _scheduler_bootstrap_inflight, _scheduler_bootstrap_retry_after, _embedded_scheduler
    _retire_embedded_scheduler_if_disallowed()
    if not _should_bootstrap_embedded_scheduler():
        return

    with _scheduler_bootstrap_lock:
        if _embedded_scheduler is not None and getattr(_embedded_scheduler, "_started", False):
            return
        if _scheduler_bootstrap_inflight:
            return
        if time.time() < _scheduler_bootstrap_retry_after:
            return
        _scheduler_bootstrap_inflight = True

    def runner() -> None:
        global _scheduler_bootstrap_inflight, _scheduler_bootstrap_retry_after, _embedded_scheduler
        try:
            runtime_settings = _runtime_scheduler_settings()
            scheduler = ProjectScheduler(settings=runtime_settings, workflow_manager=manager)
            scheduler.start()
            _embedded_scheduler = scheduler
        except RuntimeError:
            _embedded_scheduler = None
            _scheduler_bootstrap_retry_after = time.time() + 300
        except Exception:
            _embedded_scheduler = None
            _scheduler_bootstrap_retry_after = time.time() + 60
        finally:
            _scheduler_bootstrap_inflight = False

    threading.Thread(target=runner, daemon=True).start()


ensure_embedded_scheduler()


def _get_live_scheduler() -> ProjectScheduler | None:
    _retire_embedded_scheduler_if_disallowed()
    ensure_embedded_scheduler()
    scheduler = _embedded_scheduler
    if scheduler is None:
        return None
    if not getattr(scheduler, "_started", False):
        return None
    return scheduler


def _is_loopback_request() -> bool:
    remote_addr = str(request.remote_addr or "").strip()
    return remote_addr in {"127.0.0.1", "::1", "::ffff:127.0.0.1"}


def _dispatch_scheduler_run_request(job_id: str, *, requested_by: str) -> tuple[dict[str, Any], int]:
    runtime_settings = _runtime_scheduler_settings()
    scheduler = _get_live_scheduler()
    if scheduler is None:
        if requested_by != "scheduler_cli":
            return {"error": "当前在线调度器不可用", "status": "scheduler_unavailable"}, 503
        state_store = SchedulerStateStore(runtime_settings.scheduler_state_file)
        snapshot = state_store.snapshot()
        host_context = str(snapshot.get("host_context") or "").strip()
        runtime_evidence = ProjectScheduler._scheduler_runtime_evidence(
            runtime_settings,
            state_payload=snapshot,
        )
        external_scheduler_alive = bool(
            runtime_evidence.get("alive")
            and host_context == "external_scheduler_process"
        )
        if not external_scheduler_alive:
            return {"error": "当前在线调度器不可用", "status": "scheduler_unavailable"}, 503
        external_scheduler = ProjectScheduler(settings=runtime_settings)
        try:
            result = external_scheduler.enqueue_manual_run_request(job_id, requested_by=requested_by)
        except ValueError as exc:
            return {"error": str(exc), "status": "invalid_request"}, 400
        status = str(result.get("status") or "").strip()
        if status == "rejected":
            payload = dict(result)
            payload["error"] = "调度器当前正忙，暂不接受补跑请求"
            return payload, 409
        return dict(result), (202 if status == "accepted" else 200)

    try:
        result = scheduler.request_manual_run(job_id, requested_by=requested_by)
    except ValueError as exc:
        return {"error": str(exc), "status": "invalid_request"}, 400

    status = str(result.get("status") or "").strip()
    if status == "rejected":
        payload = dict(result)
        payload["error"] = "调度器当前正忙，暂不接受补跑请求"
        return payload, 409
    return dict(result), (202 if status == "accepted" else 200)

def _resolve_project_file(relative_path: str) -> Path:
    file_path = (settings.project_root / relative_path).resolve()
    if not file_path.is_relative_to(settings.project_root.resolve()):
        raise ValueError("非法路径")
    return file_path


def _deduplicate_headers(headers: list[str]) -> list[str]:
    counts: dict[str, int] = {}
    normalized: list[str] = []
    for raw in headers:
        text = str(raw or "").strip() or "未命名列"
        counts[text] = counts.get(text, 0) + 1
        if counts[text] == 1:
            normalized.append(text)
        else:
            normalized.append(f"{text} ({counts[text]})")
    return normalized


def _query_scalar(conn: sqlite3.Connection, sql: str) -> int | str | None:
    row = conn.execute(sql).fetchone()
    if not row:
        return None
    return row[0]


def _format_size(size_bytes: int) -> str:
    value = float(size_bytes)
    units = ["B", "KB", "MB", "GB", "TB"]
    index = 0
    while value >= 1024 and index < len(units) - 1:
        value /= 1024
        index += 1
    return f"{value:.2f} {units[index]}"


def _is_truthy_text(value: object) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _request_arg_truthy(*names: str) -> bool:
    return any(_is_truthy_text(request.args.get(name)) for name in names)


def _normalize_positive_int(value: object, *, default: int, minimum: int = 1, maximum: int | None = None) -> int:
    try:
        normalized = int(float(str(value).strip()))
    except (TypeError, ValueError):
        normalized = default
    normalized = max(int(minimum), normalized)
    if maximum is not None:
        normalized = min(int(maximum), normalized)
    return normalized


def _compact_dict(source: dict[str, Any] | None, keys: tuple[str, ...]) -> dict[str, Any]:
    current = source if isinstance(source, dict) else {}
    return {key: current.get(key) for key in keys if key in current}


def _normalize_number_text(value: object, digits: int = 4) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    try:
        number = float(text)
    except (TypeError, ValueError):
        return text
    normalized = f"{number:.{digits}f}"
    if digits > 0:
        normalized = normalized.rstrip("0").rstrip(".")
    return normalized if normalized != "-0" else "0"


def _to_float(value: object) -> float | None:
    try:
        number = float(str(value).strip())
    except (TypeError, ValueError):
        return None
    return number if pd.notna(number) else None


def _to_int_or_none(value: object) -> int | None:
    try:
        number = int(float(str(value).strip()))
    except (TypeError, ValueError):
        return None
    return number


def _ensure_core_root_on_path() -> None:
    core_root = str(settings.core_root.resolve())
    if core_root not in sys.path:
        sys.path.insert(0, core_root)


def _load_core_runtime_config() -> dict[str, Any]:
    global _core_runtime_config
    with _core_runtime_config_lock:
        if _core_runtime_config is not None:
            return dict(_core_runtime_config)

    _ensure_core_root_on_path()
    from utils.project_runtime_config import load_runtime_config  # type: ignore

    loaded = load_runtime_config(settings.project_root / "config" / "project.yaml")
    with _core_runtime_config_lock:
        _core_runtime_config = dict(loaded or {})
        return dict(_core_runtime_config)


def _normalize_api_base_url(value: Any, *, field_name: str) -> tuple[str | None, str | None]:
    text = str(value or "").strip().rstrip("/")
    if not text:
        return None, f"{field_name}_empty"
    parsed = urlparse(text)
    if parsed.scheme not in {"http", "https"}:
        return None, f"{field_name}_scheme_invalid"
    if not parsed.netloc:
        return None, f"{field_name}_host_missing"
    if parsed.params or parsed.query or parsed.fragment:
        return None, f"{field_name}_must_be_base_url"
    return text, None


def _read_yaml_mapping(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    return payload if isinstance(payload, dict) else {}


def _write_yaml_mapping_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_file = path.parent / f"{path.name}.{uuid4().hex}.tmp"
    temp_file.write_text(
        yaml.safe_dump(payload, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )
    temp_file.replace(path)


def _sync_yaml_api_mirror(
    path: Path,
    *,
    tiantian_base_url: str | None,
    autostock_base_url: str | None,
    project_yaml_shape: bool = False,
) -> bool:
    if not path.exists():
        return False
    payload = _read_yaml_mapping(path)
    if not payload:
        return False
    changed = False
    if project_yaml_shape:
        apis = payload.setdefault("apis", {})
        if isinstance(apis, dict):
            if tiantian_base_url and apis.get("tiantian_base_url") != tiantian_base_url:
                apis["tiantian_base_url"] = tiantian_base_url
                changed = True
            if autostock_base_url and apis.get("autostock_base_url") != autostock_base_url:
                apis["autostock_base_url"] = autostock_base_url
                changed = True
    else:
        api = payload.setdefault("api", {})
        services = payload.setdefault("services", {})
        if isinstance(api, dict):
            if tiantian_base_url and api.get("base_url") != tiantian_base_url:
                api["base_url"] = tiantian_base_url
                changed = True
            tiantian = api.setdefault("tiantian", {})
            if isinstance(tiantian, dict) and tiantian_base_url and tiantian.get("base_url") != tiantian_base_url:
                tiantian["base_url"] = tiantian_base_url
                changed = True
            autostock = api.setdefault("autostock", {})
            if isinstance(autostock, dict) and autostock_base_url and autostock.get("base_url") != autostock_base_url:
                autostock["base_url"] = autostock_base_url
                changed = True
        if isinstance(services, dict) and tiantian_base_url and services.get("tiantian_api_url") != tiantian_base_url:
            services["tiantian_api_url"] = tiantian_base_url
            changed = True
    if changed:
        _write_yaml_mapping_atomic(path, payload)
    return changed


def _sync_api_settings_compatibility_files(
    *,
    tiantian_base_url: str | None,
    autostock_base_url: str | None,
) -> list[dict[str, Any]]:
    targets = [
        (settings.project_root / "config" / "project.yaml.bak", True),
        (settings.project_root / "core" / "config" / "config.yaml", False),
        (settings.project_root / "core" / "config" / "config.yaml.template", False),
    ]
    results: list[dict[str, Any]] = []
    for path, project_yaml_shape in targets:
        try:
            changed = _sync_yaml_api_mirror(
                path,
                tiantian_base_url=tiantian_base_url,
                autostock_base_url=autostock_base_url,
                project_yaml_shape=project_yaml_shape,
            )
            results.append(
                {
                    "path": str(path),
                    "exists": path.exists(),
                    "changed": bool(changed),
                    "status": "updated" if changed else "unchanged",
                }
            )
        except Exception as exc:
            results.append(
                {
                    "path": str(path),
                    "exists": path.exists(),
                    "changed": False,
                    "status": "error",
                    "error": str(exc),
                }
            )
    return results


def _refresh_runtime_settings_after_api_update() -> None:
    global settings, _core_runtime_config, _autostock_client
    new_settings = load_settings(settings.config_path)
    settings = new_settings
    manager.settings = new_settings
    with _core_runtime_config_lock:
        _core_runtime_config = None
    with _autostock_client_lock:
        _autostock_client = None
    with _payload_cache_lock:
        _payload_cache.clear()
        for inflight in _payload_cache_inflight.values():
            inflight.set()
        _payload_cache_inflight.clear()
    if _embedded_scheduler is not None:
        try:
            _embedded_scheduler.settings = new_settings
            _embedded_scheduler.workflow_manager.settings = new_settings
        except Exception:
            pass


def _build_api_settings_payload() -> dict[str, Any]:
    default_config_path = Path(__file__).resolve().parent.parent / "config" / "project.yaml"
    config_path = getattr(settings, "config_path", None) or default_config_path
    try:
        configured_settings = load_settings(config_path)
    except Exception:
        configured_settings = settings
    tiantian_env = os.environ.get("TIANTIAN_API_URL", "").strip()
    scheduler_host_mode = str(getattr(configured_settings, "scheduler_host_mode", "") or "").strip()
    external_scheduler = scheduler_host_mode == "external_process"
    configured_tiantian = str(getattr(configured_settings, "tiantian_base_url", "") or "").strip()
    configured_autostock = str(getattr(configured_settings, "autostock_base_url", "") or "").strip()
    runtime_tiantian = str(getattr(settings, "tiantian_base_url", configured_tiantian) or "").strip()
    runtime_autostock = str(getattr(settings, "autostock_base_url", configured_autostock) or "").strip()
    validation_fields = {
        "autostock_enabled": True,
        "publication_validation_provider": "autostock",
        "tiantian_valuation_primary_url": "",
        "tiantian_valuation_fallback_url": "",
        "eastmoney_fund_info_url": "",
    }
    configured_validation = {
        key: getattr(configured_settings, key, default)
        for key, default in validation_fields.items()
    }
    runtime_validation = {
        key: getattr(settings, key, default)
        for key, default in validation_fields.items()
    }
    scheduler_state: dict[str, Any] = {}
    try:
        scheduler_state = _scheduler_state_with_runtime_evidence(configured_settings)
    except Exception:
        scheduler_state = {}
    scheduler_runtime = (
        scheduler_state.get("runtime_api_config")
        if isinstance(scheduler_state.get("runtime_api_config"), dict)
        else {}
    )
    scheduler_runtime_tiantian = str(scheduler_runtime.get("tiantian_base_url") or "").strip()
    scheduler_runtime_autostock = str(scheduler_runtime.get("autostock_base_url") or "").strip()
    scheduler_runtime_available = bool(scheduler_runtime_tiantian or scheduler_runtime_autostock)
    scheduler_validation = {key: scheduler_runtime.get(key) for key in validation_fields}
    scheduler_validation_matches = {}
    for key, configured_value in configured_validation.items():
        observed = scheduler_validation[key]
        if key == "autostock_enabled":
            scheduler_validation_matches[key] = (
                observed == configured_value if isinstance(observed, bool) else None
            )
        else:
            scheduler_validation_matches[key] = (
                observed == configured_value if isinstance(observed, str) and observed.strip() else None
            )
    return {
        "status": "ok",
        "fetched_at": datetime.now().isoformat(timespec="seconds"),
        "config_file": str(getattr(configured_settings, "config_path", config_path) or config_path),
        "manual_operations_enabled": MANUAL_OPERATION_MODE,
        "frontend": {
            "settings_page": "/api-settings",
            "settings_aliases": ["/settings"],
            "asset_version": __version__,
            "cache_policy": "no-store",
            "homepage_api_source": "/api/settings/apis",
            "homepage_health_fallback_allowed": False,
            "homepage_historical_data_sources_allowed": False,
            "visual_api_settings_enabled": True,
            "editable_from_frontend": True,
            "save_endpoint": "/api/settings/apis",
            "probe_endpoint": "/api/settings/apis/probe",
        },
        "configured": {
            "tiantian_base_url": configured_tiantian,
            "autostock_base_url": configured_autostock,
            **configured_validation,
        },
        "runtime": {
            "tiantian_base_url": runtime_tiantian,
            "autostock_base_url": runtime_autostock,
            **runtime_validation,
            "validation_supplier_independent": False if getattr(settings, "publication_validation_provider", "autostock") == "eastmoney_cross_endpoint" else None,
            "runtime_started_at": _API_RUNTIME_STARTED_AT,
        },
        "environment_overrides": {
            "TIANTIAN_API_URL": tiantian_env,
            "tiantian_override_active": bool(
                tiantian_env and tiantian_env.rstrip("/") != configured_tiantian
            ),
        },
        "runtime_matches_config": {
            "tiantian_base_url": runtime_tiantian == configured_tiantian,
            "autostock_base_url": runtime_autostock == configured_autostock,
            **{key: runtime_validation[key] == value for key, value in configured_validation.items()},
        },
        "scheduler": {
            "host_mode": scheduler_host_mode,
            "external_process": external_scheduler,
            "restart_required_after_change": external_scheduler,
            "runtime_evidence_available": scheduler_runtime_available,
            "runtime": {
                "tiantian_base_url": scheduler_runtime_tiantian,
                "autostock_base_url": scheduler_runtime_autostock,
                **scheduler_validation,
                "captured_at": scheduler_runtime.get("captured_at"),
                "config_file": scheduler_runtime.get("config_file"),
                "host_context": scheduler_runtime.get("host_context") or scheduler_state.get("host_context"),
                "scheduler_started_at": scheduler_state.get("started_at"),
                "last_heartbeat_at": scheduler_state.get("last_heartbeat_at"),
            },
            "runtime_matches_config": {
                "tiantian_base_url": (
                    scheduler_runtime_tiantian == configured_tiantian
                    if scheduler_runtime_tiantian
                    else None
                ),
                "autostock_base_url": (
                    scheduler_runtime_autostock == configured_autostock
                    if scheduler_runtime_autostock
                    else None
                ),
                **scheduler_validation_matches,
            },
            "restart_note": (
                "当前调度器为独立进程/容器，保存后需要重启调度容器或重新部署才会读取新接口地址。"
                if external_scheduler
                else "当前 Web 进程内调度器会在保存后同步刷新运行配置。"
            ),
        },
        "editable_fields": ["tiantian_base_url", "autostock_base_url"],
        "truth_note": (
            "此页面只配置接口地址；基金业务数据仍必须来自真实 API、数据库或真实运行产物，"
            "缺失或失败不会被前端显示为成功数据。"
        ),
    }


def _update_api_settings(payload: dict[str, Any]) -> tuple[dict[str, Any], int]:
    raw_tiantian = payload.get("tiantian_base_url")
    if raw_tiantian in (None, "") and isinstance(payload.get("tiantian"), dict):
        raw_tiantian = payload["tiantian"].get("base_url")
    raw_autostock = payload.get("autostock_base_url")
    if raw_autostock in (None, "") and isinstance(payload.get("autostock"), dict):
        raw_autostock = payload["autostock"].get("base_url")

    updates: dict[str, str] = {}
    errors: dict[str, str] = {}
    if raw_tiantian not in (None, ""):
        normalized, error = _normalize_api_base_url(raw_tiantian, field_name="tiantian_base_url")
        if error:
            errors["tiantian_base_url"] = error
        elif normalized:
            updates["tiantian_base_url"] = normalized
    if raw_autostock not in (None, ""):
        normalized, error = _normalize_api_base_url(raw_autostock, field_name="autostock_base_url")
        if error:
            errors["autostock_base_url"] = error
        elif normalized:
            updates["autostock_base_url"] = normalized

    if errors:
        return {"status": "invalid_request", "errors": errors}, 400
    if not updates:
        return {"status": "invalid_request", "error": "没有可保存的 API 地址字段"}, 400

    config_path = settings.config_path
    config_payload = _read_yaml_mapping(config_path)
    apis = config_payload.setdefault("apis", {})
    if not isinstance(apis, dict):
        return {"status": "invalid_config", "error": "config/project.yaml apis 节点不是对象"}, 500
    before = {
        "tiantian_base_url": str(apis.get("tiantian_base_url") or "").strip(),
        "autostock_base_url": str(apis.get("autostock_base_url") or "").strip(),
    }
    for key, value in updates.items():
        apis[key] = value
    try:
        _write_yaml_mapping_atomic(config_path, config_payload)
    except OSError as exc:
        return {
            "status": "write_failed",
            "error": str(exc),
            "config_file": str(config_path),
            "updated_fields": sorted(updates),
        }, 500

    compatibility_sync = _sync_api_settings_compatibility_files(
        tiantian_base_url=updates.get("tiantian_base_url"),
        autostock_base_url=updates.get("autostock_base_url"),
    )
    _refresh_runtime_settings_after_api_update()
    response = _build_api_settings_payload()
    response.update(
        {
            "status": "saved",
            "saved_at": datetime.now().isoformat(timespec="seconds"),
            "updated_fields": sorted(updates),
            "before": before,
            "after": {
                "tiantian_base_url": settings.tiantian_base_url,
                "autostock_base_url": settings.autostock_base_url,
            },
            "compatibility_sync": compatibility_sync,
        }
    )
    return response, 200


def _probe_tiantian_base_url(base_url: str, *, timeout_seconds: float = 10.0) -> dict[str, Any]:
    _ensure_core_root_on_path()
    from utils.service_checks import probe_tiantian_api_business_status  # type: ignore

    return dict(
        probe_tiantian_api_business_status(
            base_url=base_url,
            timeout_seconds=timeout_seconds,
        )
        or {}
    )


def _probe_api_settings(payload: dict[str, Any]) -> tuple[dict[str, Any], int]:
    raw_tiantian = payload.get("tiantian_base_url")
    if raw_tiantian in (None, "") and isinstance(payload.get("tiantian"), dict):
        raw_tiantian = payload["tiantian"].get("base_url")
    if raw_tiantian in (None, ""):
        raw_tiantian = settings.tiantian_base_url
    normalized, error = _normalize_api_base_url(raw_tiantian, field_name="tiantian_base_url")
    if error or not normalized:
        return {"status": "invalid_request", "errors": {"tiantian_base_url": error or "invalid"}}, 400
    timeout_seconds = max(1.0, min(float(payload.get("timeout_seconds") or 10.0), 10.0))
    try:
        probe = _probe_tiantian_base_url(normalized, timeout_seconds=timeout_seconds)
    except Exception as exc:
        probe = {
            "ok": False,
            "status": "probe_error",
            "message": str(exc),
            "base_url": normalized,
            "errors": [str(exc)],
        }
    probe_status = str(probe.get("status") or "").strip()
    response_semantics = {
        "schema_version": 1,
        "target": "tiantian",
        "probe_endpoint": "/fundMNRank",
        "probe_params": {
            "FundType": "31",
            "pageIndex": 1,
            "pageSize": 1,
        },
        "writes_database": False,
        "writes_business_artifacts": False,
        "business_payload_required": True,
        "success_criteria": {
            "http_status_2xx": bool(
                isinstance(probe.get("http_status"), int)
                and 200 <= int(probe.get("http_status") or 0) < 300
            ),
            "payload_success_true": probe.get("payload_success") is True,
            "datas_non_empty": (
                isinstance(probe.get("datas_count"), int)
                and int(probe.get("datas_count") or 0) > 0
            ),
        },
        "failure_status_meanings": {
            "invalid_json": "HTTP 响应不是可解析 JSON，常见于 HTML/风控页/代理错误页。",
            "invalid_payload": "JSON 结构缺少 Tiantian 业务字段或 Datas 不是列表。",
            "upstream_degraded": "接口可达但业务 payload Success 非 true。",
            "empty_datas": "接口可达且 Success 为 true，但 Datas 为空，不能视为可采集业务数据。",
            "request_error": "网络连接、超时或 DNS 等请求层失败。",
        },
        "current_business_status": probe_status or "unknown",
        "truth_policy": "read_only_business_probe_no_database_or_artifact_writes",
    }
    return {
        "status": "ok" if bool(probe.get("ok")) else "probe_failed",
        "target": "tiantian",
        "base_url": normalized,
        "probe": probe,
        "response_semantics": response_semantics,
        "truth_note": "连接测试只读取 Tiantian 业务探针响应，不写入数据库，也不生成基金业务数据。",
    }, 200


def _get_autostock_client():
    if not getattr(settings, "autostock_enabled", True):
        raise RuntimeError("autostock_provider_disabled")
    global _autostock_client
    with _autostock_client_lock:
        if _autostock_client is not None:
            return _autostock_client

        _ensure_core_root_on_path()
        from api.autostock_client import AutoStockClient  # type: ignore

        # P0-04.2 / B15: 统一从 settings (apis.autostock.rate_limit.*) 读取，
        # 不再混用 core 的 runtime config / 硬编码 fallback。retry.timeout 依旧从
        # core runtime config 取，保持与 tiantian client 的 timeout 语义一致。
        core_config = _load_core_runtime_config()
        retry_cfg = ((core_config.get("api") or {}).get("autostock") or {}).get("retry") or {}
        _autostock_client = AutoStockClient(
            base_url=settings.autostock_base_url,
            min_interval=float(settings.autostock_rate_limit_min_interval_seconds),
            timeout=int(retry_cfg.get("timeout", 30)),
            max_calls_per_hour=int(settings.autostock_rate_limit_max_calls_per_hour),
            shared_state_path=str(settings.autostock_rate_limit_shared_state_path),
            directory_cache_path=str(settings.autostock_directory_cache_path),
            directory_cache_ttl_seconds=settings.autostock_directory_cache_ttl_seconds,
        )
        return _autostock_client


def _get_autostock_batch_size() -> int:
    # P0-04.2 / B15: 统一读 settings.autostock_batch_fund_default，
    # 上限 50 与 AutoStockClient.SAFE_FUND_BATCH_SIZE 对齐。
    return max(1, min(int(settings.autostock_batch_fund_default), 50))


def _normalize_code_text(value: object) -> str:
    text = str(value or "").strip()
    return text.zfill(6) if text.isdigit() else text


def _normalize_favorite_history_entry_type(value: object, *, label: str | None = None) -> str:
    text = str(value or "").strip().lower()
    if text in {FAVORITE_HISTORY_ENTRY_TYPE_QUERY, FAVORITE_HISTORY_ENTRY_TYPE_FAVORITE}:
        return text
    label_text = str(label or "").strip()
    if label_text.startswith("推荐页收藏"):
        return FAVORITE_HISTORY_ENTRY_TYPE_FAVORITE
    return FAVORITE_HISTORY_ENTRY_TYPE_QUERY


def _compute_since_inception_annualized_return_map(codes: list[str]) -> dict[str, str]:
    from backend.lifetime_returns import platform_annualized_map
    return platform_annualized_map(settings.database_path, [_normalize_code_text(code) for code in codes])


def _compute_accum_nav_ratio_annualized_return_map(codes: list[str]) -> dict[str, str]:
    from backend.report_builder import _compute_report_accum_nav_ratio_annualized_return_map
    return _compute_report_accum_nav_ratio_annualized_return_map(settings, codes)


def _load_since_inception_annual_return_breakdown_map(codes: list[str]) -> dict[str, list[dict[str, Any]]]:
    normalized_codes: list[str] = []
    seen: set[str] = set()
    for code in codes:
        normalized = _normalize_code_text(code)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        normalized_codes.append(normalized)
    if not normalized_codes or not settings.database_path.exists():
        return {}

    placeholders = ",".join("?" for _ in normalized_codes)
    query = f"""
        SELECT
            f.fcode,
            f.estab_date,
            ar.year,
            ar.annual_return,
            ar.rank_in_category,
            ar.category_count,
            ar.percentile
        FROM funds AS f
        LEFT JOIN fund_annual_returns AS ar
            ON ar.fcode = f.fcode
        WHERE f.fcode IN ({placeholders})
        ORDER BY f.fcode, ar.year
    """
    conn = sqlite3.connect(str(settings.database_path))
    try:
        rows = conn.execute(query, normalized_codes).fetchall()
    except sqlite3.OperationalError as exc:
        if "no such table" in str(exc).lower() and "fund_annual_returns" in str(exc):
            return {}
        raise
    finally:
        conn.close()

    estab_year_by_code: dict[str, int] = {}
    entries_by_code: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        code = _normalize_code_text(row[0])
        if not code:
            continue
        estab_text = str(row[1] or "").strip()
        if code not in estab_year_by_code:
            try:
                estab_year_by_code[code] = datetime.fromisoformat(estab_text).year if estab_text else 0
            except ValueError:
                estab_year_by_code[code] = 0
        year_value = row[2]
        if year_value is None:
            continue
        year = int(year_value)
        annual_return = _to_float(row[3])
        rank_in_category = int(_to_float(row[4]) or 0) if row[4] is not None else None
        category_count = int(_to_float(row[5]) or 0) if row[5] is not None else None
        percentile = _to_float(row[6])
        is_partial = bool(estab_year_by_code.get(code) and year <= estab_year_by_code.get(code))
        entries_by_code.setdefault(code, []).append(
            {
                "year": year,
                "annual_return": _normalize_number_text(annual_return, digits=2) if annual_return is not None else "",
                "rank_in_category": rank_in_category,
                "category_count": category_count,
                "percentile": _normalize_number_text(percentile * 100.0, digits=2) if percentile is not None else "",
                "is_partial_year": is_partial,
            }
        )
    return entries_by_code


def _compute_since_inception_annual_return_volatility_map(
    breakdown_by_code: dict[str, list[dict[str, Any]]] | None,
) -> dict[str, str]:
    result: dict[str, str] = {}
    for code, items in dict(breakdown_by_code or {}).items():
        annual_returns: list[float] = []
        for item in list(items or []):
            if bool((item or {}).get("is_partial_year")):
                continue
            annual_return = _to_float((item or {}).get("annual_return"))
            if annual_return is None:
                continue
            annual_returns.append(float(annual_return))
        if len(annual_returns) < 2:
            continue
        volatility = pd.Series(annual_returns, dtype=float).std(ddof=0)
        if pd.isna(volatility):
            continue
        result[_normalize_code_text(code)] = _normalize_number_text(volatility, digits=2)
    return result


def _load_favorite_periodic_return_details_map(codes: list[str]) -> dict[str, dict[str, Any]]:
    normalized_codes: list[str] = []
    seen: set[str] = set()
    for code in codes:
        normalized = _normalize_code_text(code)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        normalized_codes.append(normalized)
    if not normalized_codes or not settings.database_path.exists():
        return {}

    today = datetime.now().date()
    weekly_cutoff = pd.Timestamp(today - timedelta(days=62))
    current_year_start = pd.Timestamp(date(today.year, 1, 1))
    data_start_date = min(weekly_cutoff.date(), current_year_start.date()).isoformat()
    value_rows = ",".join("(?)" for _ in normalized_codes)
    query = f"""
        WITH requested_codes(fcode) AS (
            VALUES {value_rows}
        ),
        windowed_nav AS (
            SELECT
                h.fcode,
                h.nav_date,
                h.unit_nav,
                h.accum_nav
            FROM fund_nav_history h
            JOIN requested_codes c ON c.fcode = h.fcode
            WHERE h.nav_date >= ?
        ),
        baseline_nav AS (
            SELECT
                h.fcode,
                h.nav_date,
                h.unit_nav,
                h.accum_nav
            FROM requested_codes c
            JOIN fund_nav_history h
                ON h.rowid = (
                    SELECT h2.rowid
                    FROM fund_nav_history h2
                    WHERE h2.fcode = c.fcode
                      AND h2.nav_date < ?
                    ORDER BY h2.nav_date DESC, h2.rowid DESC
                    LIMIT 1
                )
        )
        SELECT
            fcode,
            nav_date,
            unit_nav,
            accum_nav
        FROM windowed_nav
        UNION ALL
        SELECT
            fcode,
            nav_date,
            unit_nav,
            accum_nav
        FROM baseline_nav
    """
    conn = sqlite3.connect(str(settings.database_path))
    try:
        frame = pd.read_sql_query(query, conn, params=[*normalized_codes, data_start_date, data_start_date])
    finally:
        conn.close()

    if frame.empty:
        return {}

    frame["fcode"] = frame["fcode"].map(_normalize_code_text)
    frame["nav_date"] = pd.to_datetime(frame["nav_date"], errors="coerce")
    frame["unit_nav"] = pd.to_numeric(frame["unit_nav"], errors="coerce")
    frame["accum_nav"] = pd.to_numeric(frame["accum_nav"], errors="coerce")
    frame["effective_nav"] = frame["accum_nav"].where(frame["accum_nav"].gt(0), frame["unit_nav"])
    frame = frame.dropna(subset=["fcode", "nav_date", "effective_nav"]).copy()
    if frame.empty:
        return {}

    result: dict[str, dict[str, Any]] = {}

    for code, group in frame.groupby("fcode", sort=False):
        series = group[["nav_date", "effective_nav"]].drop_duplicates(subset=["nav_date"], keep="last").sort_values(
            "nav_date",
            kind="stable",
        )
        if series.empty:
            continue
        ts = pd.Series(series["effective_nav"].to_numpy(dtype=float), index=pd.DatetimeIndex(series["nav_date"]))
        ts = ts[~ts.index.duplicated(keep="last")].sort_index()
        if ts.empty:
            continue

        weekly_baseline = ts[ts.index < weekly_cutoff].tail(1)
        weekly_source = pd.concat([weekly_baseline, ts[ts.index >= weekly_cutoff]])
        weekly_source = weekly_source[~weekly_source.index.duplicated(keep="last")].sort_index()
        weekly_records: list[dict[str, Any]] = []
        weekly_volatility = None
        if not weekly_source.empty:
            weekly_agg = weekly_source.groupby(weekly_source.index.to_period("W-FRI")).last()
            weekly_returns = weekly_agg.pct_change().dropna()
            if not weekly_returns.empty:
                weekly_records = [
                    {
                        "label": str(period.end_time.date()),
                        "period_start": str(period.start_time.date()),
                        "period_end": str(period.end_time.date()),
                        "return_pct": _normalize_number_text(value * 100.0, digits=2),
                    }
                    for period, value in weekly_returns.tail(8).items()
                ]
                if len(weekly_returns.index) >= 2:
                    weekly_volatility = _normalize_number_text(float(weekly_returns.std(ddof=0)) * 100.0, digits=2)

        monthly_baseline = ts[ts.index < current_year_start].tail(1)
        monthly_source = pd.concat([monthly_baseline, ts[ts.index >= current_year_start]])
        monthly_source = monthly_source[~monthly_source.index.duplicated(keep="last")].sort_index()
        monthly_records: list[dict[str, Any]] = []
        monthly_volatility = None
        if not monthly_source.empty:
            monthly_agg = monthly_source.groupby(monthly_source.index.to_period("M")).last()
            monthly_returns = monthly_agg.pct_change().dropna()
            if not monthly_returns.empty:
                monthly_records = [
                    {
                        "label": str(period),
                        "period_start": str(period.start_time.date()),
                        "period_end": str(period.end_time.date()),
                        "return_pct": _normalize_number_text(value * 100.0, digits=2),
                    }
                    for period, value in monthly_returns.items()
                ]
                if len(monthly_returns.index) >= 2:
                    monthly_volatility = _normalize_number_text(float(monthly_returns.std(ddof=0)) * 100.0, digits=2)

        result[code] = {
            _FAVORITE_PERIODIC_WEEKLY_DETAIL_KEY: {
                "records": weekly_records,
                "total_volatility_pct": weekly_volatility or "",
            },
            _FAVORITE_PERIODIC_MONTHLY_DETAIL_KEY: {
                "records": monthly_records,
                "total_volatility_pct": monthly_volatility or "",
            },
        }
    return result


def _load_cached_favorite_metric_detail_payload(codes: list[str]) -> dict[str, dict[str, Any]]:
    normalized_codes = sorted({
        _normalize_code_text(code)
        for code in list(codes or [])
        if _normalize_code_text(code)
    })
    if not normalized_codes:
        return {}
    limited_codes = normalized_codes[:_FAVORITE_METRIC_DETAIL_MAX_CODES]
    cache_key = (
        "favorite_metric_detail::"
        f"{_sqlite_file_cache_token(settings.database_path)}::"
        f"{','.join(limited_codes)}"
    )
    return _cached_payload(
        cache_key,
        _FAVORITE_METRIC_DETAIL_CACHE_TTL_SECONDS,
        lambda: _build_favorite_metric_detail_payload(limited_codes),
    )


def _build_favorite_metric_detail_payload(codes: list[str]) -> dict[str, dict[str, Any]]:
    normalized_codes: list[str] = []
    seen: set[str] = set()
    for code in codes:
        normalized = _normalize_code_text(code)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        normalized_codes.append(normalized)
    if not normalized_codes:
        return {}

    periodic_details = _load_favorite_periodic_return_details_map(normalized_codes)
    annual_breakdown_by_code = _load_since_inception_annual_return_breakdown_map(normalized_codes)
    annual_volatility_by_code = _compute_since_inception_annual_return_volatility_map(annual_breakdown_by_code)

    result: dict[str, dict[str, Any]] = {}
    for code in normalized_codes:
        detail = dict(periodic_details.get(code) or {})
        annual_breakdown = annual_breakdown_by_code.get(code) or []
        annual_volatility = str(annual_volatility_by_code.get(code) or "").strip()
        if annual_breakdown or annual_volatility:
            detail[_FAVORITE_METRIC_DETAIL_ANNUAL_BREAKDOWN_KEY] = annual_breakdown
            detail[_FAVORITE_METRIC_DETAIL_ANNUAL_VOLATILITY_KEY] = annual_volatility
        if detail:
            result[code] = detail
    return result


def _build_favorite_metric_detail_source_basis(
    codes: list[str],
    *,
    details: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    normalized_codes: list[str] = []
    seen: set[str] = set()
    for code in list(codes or []):
        normalized = _normalize_code_text(code)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        normalized_codes.append(normalized)
    limited_codes = normalized_codes[:_FAVORITE_METRIC_DETAIL_MAX_CODES]
    detail_payload = dict(details or {})
    returned_codes = sorted(
        code
        for code in (_normalize_code_text(item) for item in detail_payload.keys())
        if code
    )
    basis: dict[str, Any] = {
        "schema_version": FAVORITE_METRIC_DETAIL_SOURCE_BASIS_SCHEMA_VERSION,
        "source": "favorite_metric_details_runtime_query",
        "requested_codes": normalized_codes,
        "effective_codes": limited_codes,
        "returned_codes": returned_codes,
        "database_file": _sqlite_file_cache_token(settings.database_path),
        "database_file_path": _project_relative_path_text(settings.database_path),
        "cache_ttl_seconds": _FAVORITE_METRIC_DETAIL_CACHE_TTL_SECONDS,
        "max_request_codes": _FAVORITE_METRIC_DETAIL_MAX_CODES,
        "detail_fields": [
            _FAVORITE_PERIODIC_WEEKLY_DETAIL_KEY,
            _FAVORITE_PERIODIC_MONTHLY_DETAIL_KEY,
            _FAVORITE_METRIC_DETAIL_ANNUAL_BREAKDOWN_KEY,
            _FAVORITE_METRIC_DETAIL_ANNUAL_VOLATILITY_KEY,
        ],
        "source_tables": [
            "fund_nav_history",
            "fund_annual_returns",
            "funds",
        ],
        "calculation_basis": {
            "weekly_recent": "SQLite fund_nav_history effective NAV grouped by W-FRI over the recent window plus one pre-window baseline.",
            "monthly_ytd": "SQLite fund_nav_history effective NAV grouped by calendar month for current year plus one pre-year baseline.",
            "since_inception_annual_return_breakdown": "SQLite fund_annual_returns joined with funds.estab_date; partial establishment year is marked and excluded from volatility.",
            "since_inception_annual_return_volatility_pct": "Population standard deviation of non-partial yearly annual_return values computed locally.",
        },
        "missing_value_policy": "preserve_missing_semantics",
        "source_note": (
            "收藏评分重型指标明细只读取项目 SQLite 与本地计算结果；"
            "缺少净值或年度收益时不补造明细、不返回伪 0。"
        ),
    }
    basis["digest"] = _stable_json_digest(basis)
    return basis


def _build_favorite_metric_detail_source_contract(source_basis: dict[str, Any]) -> dict[str, Any]:
    basis = dict(source_basis or {})
    return {
        "schema_version": FAVORITE_METRIC_DETAIL_SOURCE_CONTRACT_SCHEMA_VERSION,
        "source": "favorite_metric_details_runtime_query",
        "truth_policy": "real_sqlite_nav_annual_return_and_local_calculation_only",
        "missing_value_policy": "preserve_missing_semantics",
        "business_value_synthesis": "forbidden",
        "official_publication_truth": "false_favorites_detail_diagnostics_only",
        "frontend_rendered_truth_allowed": False,
        "cache_basis_schema_version": basis.get("schema_version"),
        "cache_basis_digest": basis.get("digest"),
    }


def _style_metric_benchmark_scope_title(scope: str) -> str:
    normalized = str(scope or "").strip()
    if normalized == "group":
        return "同分组参考"
    if normalized == "global":
        return "全项目参考"
    return "同风格参考"


def _style_metric_reference_status_label(
    *,
    current_value: float,
    baseline_value: float,
    lower_is_better: bool,
) -> str:
    delta = current_value - baseline_value
    tolerance = max(abs(baseline_value) * 0.03, 0.0001)
    if lower_is_better:
        if current_value <= baseline_value - tolerance:
            return "优于基准"
        if current_value >= baseline_value + tolerance:
            return "弱于基准"
        return "接近基准"
    if current_value >= baseline_value + tolerance:
        return "优于基准"
    if current_value <= baseline_value - tolerance:
        return "弱于基准"
    return "接近基准"


def _style_metric_reference_distribution_label(scope: str, scope_label: str) -> str:
    normalized_scope = str(scope or "").strip()
    normalized_label = str(scope_label or "").strip()
    if normalized_scope == "group":
        return f"同分组样本中位值：{normalized_label}" if normalized_label else "同分组样本中位值"
    if normalized_scope == "global":
        return f"全项目债基样本中位值：{normalized_label}" if normalized_label else "全项目债基样本中位值"
    return f"同风格样本中位值：{normalized_label}" if normalized_label else "同风格样本中位值"


def _build_style_metric_reference_map(
    rows: list[dict[str, Any]] | None,
    *,
    style_threshold_entries: list[dict[str, Any]] | None,
    global_style_threshold_entry: dict[str, Any] | None,
    benchmark_map: dict[str, dict[str, Any]] | None = None,
) -> dict[str, dict[str, Any]]:
    items = [dict(item or {}) for item in list(rows or [])]
    if not items:
        return {}

    style_threshold_map: dict[str, dict[str, Any]] = {}
    for entry in list(style_threshold_entries or []):
        if not isinstance(entry, dict):
            continue
        style = str(entry.get("债基风格标签") or "").strip()
        if style:
            style_threshold_map[style] = dict(entry)
    global_threshold = dict(global_style_threshold_entry or {})

    tracker_benchmarks = dict(benchmark_map or _build_style_metric_benchmark_map(items))
    result: dict[str, dict[str, Any]] = {}
    for item in items:
        code = _normalize_code_text(item.get("基金代码"))
        if not code:
            continue
        style = str(item.get("债基风格标签") or "").strip()
        references: dict[str, Any] = {}
        style_threshold_entry = dict(style_threshold_map.get(style) or {})
        tracker_reference = dict(tracker_benchmarks.get(code) or {})
        tracker_metrics = dict(tracker_reference.get("metrics") or {})
        for spec in _STYLE_METRIC_REFERENCE_SPECS:
            field = str(spec["field"])
            current_value = _to_float(item.get(field))
            if current_value is None:
                continue
            threshold_field = spec.get("threshold_field")
            baseline_value = None
            reference_source = ""
            reference_label = ""
            if threshold_field:
                baseline_value = _to_float(style_threshold_entry.get(str(threshold_field)))
                if baseline_value is not None:
                    reference_source = "style_threshold"
                    reference_label = f"风格阈值：{style or '未标注风格'}"
                else:
                    baseline_value = _to_float(global_threshold.get(str(threshold_field)))
                    if baseline_value is not None:
                        reference_source = "global_threshold"
                        reference_label = "全局阈值"
            if baseline_value is None:
                tracker_metric = dict(tracker_metrics.get(field) or {})
                baseline_value = _to_float(tracker_metric.get("median"))
                if baseline_value is not None:
                    tracker_scope = str(tracker_metric.get("benchmark_scope") or "").strip()
                    tracker_scope_label = str(tracker_metric.get("benchmark_scope_label") or "").strip()
                    reference_source = "tracker_median"
                    reference_label = _style_metric_reference_distribution_label(
                        tracker_scope,
                        tracker_scope_label,
                    )
            if baseline_value is None:
                continue
            references[field] = {
                "metric_label": str(spec["metric_label"]),
                "current": _normalize_number_text(current_value, digits=int(spec["digits"])),
                "baseline": _normalize_number_text(baseline_value, digits=int(spec["digits"])),
                "baseline_delta": _normalize_number_text(
                    current_value - baseline_value,
                    digits=int(spec["digits"]),
                ),
                "suffix": str(spec["suffix"]),
                "lower_is_better": bool(spec["lower_is_better"]),
                "reference_source": reference_source,
                "reference_label": reference_label,
                "status_label": _style_metric_reference_status_label(
                    current_value=float(current_value),
                    baseline_value=float(baseline_value),
                    lower_is_better=bool(spec["lower_is_better"]),
                ),
            }
        if references:
            result[code] = {
                "style_label": style,
                "metrics": references,
            }
    return result


def _build_style_metric_distribution_payload(
    values: list[float],
    *,
    metric_label: str,
    digits: int,
    suffix: str,
    lower_is_better: bool,
) -> dict[str, Any]:
    if len(values) < _STYLE_METRIC_BENCHMARK_MIN_DISTRIBUTION_SIZE:
        return {}
    series = pd.Series(values, dtype=float)
    p25 = float(series.quantile(0.25))
    median = float(series.quantile(0.5))
    p75 = float(series.quantile(0.75))
    return {
        "metric_label": metric_label,
        "sample_count": len(values),
        "lower_is_better": bool(lower_is_better),
        "p25": _normalize_number_text(p25, digits=digits),
        "median": _normalize_number_text(median, digits=digits),
        "p75": _normalize_number_text(p75, digits=digits),
        "suffix": suffix,
    }


def _select_style_metric_benchmark_distribution(
    *,
    style_label: str,
    group_label: str,
    style_distribution: dict[str, Any] | None,
    group_distribution: dict[str, Any] | None,
    global_distribution: dict[str, Any] | None,
) -> tuple[str, str, dict[str, Any]] | None:
    style_payload = dict(style_distribution or {})
    group_payload = dict(group_distribution or {})
    global_payload = dict(global_distribution or {})
    style_count = int(style_payload.get("sample_count") or 0)
    group_count = int(group_payload.get("sample_count") or 0)
    global_count = int(global_payload.get("sample_count") or 0)

    if style_label and style_payload and style_count >= _STYLE_METRIC_BENCHMARK_PREFERRED_SAMPLE_SIZE:
        return ("style", style_label, style_payload)
    if group_label and group_payload and group_count >= _STYLE_METRIC_BENCHMARK_PREFERRED_SAMPLE_SIZE:
        return ("group", group_label, group_payload)
    if global_payload and global_count >= _STYLE_METRIC_BENCHMARK_PREFERRED_SAMPLE_SIZE:
        return ("global", _STYLE_METRIC_BENCHMARK_GLOBAL_LABEL, global_payload)

    if (
        style_label
        and style_payload
        and style_count >= _STYLE_METRIC_BENCHMARK_MIN_DISTRIBUTION_SIZE
        and style_count >= group_count
        and style_count >= global_count
    ):
        return ("style", style_label, style_payload)
    if group_label and group_payload:
        return ("group", group_label, group_payload)
    if global_payload:
        return ("global", _STYLE_METRIC_BENCHMARK_GLOBAL_LABEL, global_payload)
    if style_label and style_payload:
        return ("style", style_label, style_payload)
    return None


def _style_metric_benchmark_signature_parts(rows: list[dict[str, Any]] | None) -> list[str]:
    items = [dict(item or {}) for item in list(rows or [])]
    signature_parts: list[str] = []
    for item in items:
        code = _normalize_code_text(item.get("基金代码"))
        style = str(item.get("债基风格标签") or "").strip()
        group = str(item.get("正式榜分组") or "").strip()
        metric_values = [
            str(item.get(str(spec["field"])) or "").strip()
            for spec in _STYLE_METRIC_REFERENCE_SPECS
        ]
        signature_parts.append("|".join([code, style, group, *metric_values]))
    return signature_parts


def _build_style_metric_benchmark_map(
    rows: list[dict[str, Any]] | None,
    *,
    benchmark_rows: list[dict[str, Any]] | None = None,
) -> dict[str, dict[str, Any]]:
    items = [dict(item or {}) for item in list(rows or [])]
    source_items = [dict(item or {}) for item in list(benchmark_rows if benchmark_rows is not None else rows or [])]
    if not items or not source_items:
        return {}

    rows_by_code: dict[str, dict[str, Any]] = {}
    for item in items:
        code = _normalize_code_text(item.get("基金代码"))
        if code:
            rows_by_code[code] = item

    metric_distribution_by_style: dict[str, dict[str, dict[str, Any]]] = {}
    metric_distribution_by_group: dict[str, dict[str, dict[str, Any]]] = {}
    metric_distribution_global: dict[str, dict[str, Any]] = {}
    for metric in _STYLE_METRIC_REFERENCE_SPECS:
        field = str(metric["field"])
        style_grouped: dict[str, list[float]] = {}
        group_grouped: dict[str, list[float]] = {}
        global_values: list[float] = []
        for item in source_items:
            style = str(item.get("债基风格标签") or "").strip()
            group = str(item.get("正式榜分组") or "").strip()
            value = _to_float(item.get(field))
            if value is None:
                continue
            numeric_value = float(value)
            if style:
                style_grouped.setdefault(style, []).append(numeric_value)
            if group:
                group_grouped.setdefault(group, []).append(numeric_value)
            global_values.append(numeric_value)
        for style, values in style_grouped.items():
            distribution = _build_style_metric_distribution_payload(
                values,
                metric_label=str(metric["metric_label"]),
                digits=int(metric["digits"]),
                suffix=str(metric["suffix"]),
                lower_is_better=bool(metric["lower_is_better"]),
            )
            if distribution:
                metric_distribution_by_style.setdefault(style, {})[field] = distribution
        for group, values in group_grouped.items():
            distribution = _build_style_metric_distribution_payload(
                values,
                metric_label=str(metric["metric_label"]),
                digits=int(metric["digits"]),
                suffix=str(metric["suffix"]),
                lower_is_better=bool(metric["lower_is_better"]),
            )
            if distribution:
                metric_distribution_by_group.setdefault(group, {})[field] = distribution
        global_distribution = _build_style_metric_distribution_payload(
            global_values,
            metric_label=str(metric["metric_label"]),
            digits=int(metric["digits"]),
            suffix=str(metric["suffix"]),
            lower_is_better=bool(metric["lower_is_better"]),
        )
        if global_distribution:
            metric_distribution_global[field] = global_distribution

    result: dict[str, dict[str, Any]] = {}
    for code, row in rows_by_code.items():
        style = str(row.get("债基风格标签") or "").strip()
        group = str(row.get("正式榜分组") or "").strip()
        metric_payload: dict[str, Any] = {}
        for metric in _STYLE_METRIC_REFERENCE_SPECS:
            field = str(metric["field"])
            selected_distribution = _select_style_metric_benchmark_distribution(
                style_label=style,
                group_label=group,
                style_distribution=(metric_distribution_by_style.get(style) or {}).get(field),
                group_distribution=(metric_distribution_by_group.get(group) or {}).get(field),
                global_distribution=metric_distribution_global.get(field),
            )
            if not selected_distribution:
                continue
            scope, scope_label, distribution = selected_distribution
            current_value = _to_float(row.get(field))
            if current_value is None:
                continue
            metric_payload[field] = {
                **distribution,
                "current": _normalize_number_text(current_value, digits=int(metric["digits"])),
                "benchmark_scope": scope,
                "benchmark_scope_label": scope_label,
                "benchmark_scope_title": _style_metric_benchmark_scope_title(scope),
                "fallback_applied": scope != "style",
            }
        if metric_payload:
            result[code] = {
                "style_label": style,
                "group_label": group,
                "has_fallback_metrics": any(bool(item.get("fallback_applied")) for item in metric_payload.values()),
                "metrics": metric_payload,
            }
    return result


def _load_cached_style_metric_benchmark_map(
    rows: list[dict[str, Any]] | None,
    *,
    benchmark_rows: list[dict[str, Any]] | None = None,
    cache_namespace: str = "style_metric_benchmarks",
) -> dict[str, dict[str, Any]]:
    target_items = [dict(item or {}) for item in list(rows or [])]
    source_items = [dict(item or {}) for item in list(benchmark_rows if benchmark_rows is not None else rows or [])]
    if not target_items or not source_items:
        return {}
    source_signature = hash(tuple(_style_metric_benchmark_signature_parts(source_items)))
    target_signature = hash(tuple(_style_metric_benchmark_signature_parts(target_items)))
    cache_key = (
        f"{cache_namespace}::"
        f"{_path_cache_token(settings.database_path)}::"
        f"{source_signature}::{target_signature}"
    )
    return _cached_payload(
        cache_key,
        _STYLE_METRIC_BENCHMARKS_CACHE_TTL_SECONDS,
        lambda: _build_style_metric_benchmark_map(target_items, benchmark_rows=source_items),
    )


def _load_tracker_style_metric_benchmark_map(
    payload: dict[str, Any],
    rows: list[dict[str, Any]] | None = None,
) -> dict[str, dict[str, Any]]:
    tracker_rows = (
        recommendation_payloads.build_tracker_current_response(
            payload,
            load_tracker_dataset_fn=_load_tracker_dataset,
            normalize_portfolio_groups=_normalize_portfolio_groups,
            normalize_number_text=_normalize_number_text,
            format_tracker_occurrence_text=_format_tracker_occurrence_text,
            apply_since_inception_annualized_return_to_funds_fn=_apply_since_inception_annualized_return_to_funds,
        ).get("rows")
        or []
    )
    target_rows = rows if rows is not None else tracker_rows
    run_token = str(payload.get("run_id") or payload.get("generated_at") or "unknown").strip() or "unknown"
    return _load_cached_style_metric_benchmark_map(
        target_rows,
        benchmark_rows=tracker_rows,
        cache_namespace=f"tracker_style_metric_benchmarks::{run_token}",
    )


def _apply_since_inception_annualized_return_to_funds(funds: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    current_funds = funds or []
    annualized_return_by_code = _compute_since_inception_annualized_return_map(
        [str(fund.get("基金代码") or "") for fund in current_funds]
    )
    codes = [str(fund.get("基金代码") or "") for fund in current_funds]
    accum_map = _compute_accum_nav_ratio_annualized_return_map(codes)
    observations = load_platform_lifetime_returns(settings.database_path, codes)
    normalized_funds: list[dict[str, Any]] = []
    for fund in current_funds:
        current = dict(fund or {})
        code = _normalize_code_text(current.get("基金代码"))
        current[_SINCE_INCEPTION_ANNUALIZED_RETURN_LABEL] = annualized_return_by_code.get(code, "")
        current["累计净值比年化(%)"] = accum_map.get(code, "")
        current["成立以来收益口径"] = observations.get(code) or {"status": "missing_input"}
        normalized_funds.append(current)
    return normalized_funds


def _apply_since_inception_annualized_return_to_portfolio_summary(
    portfolio_summary: dict[str, Any] | None,
) -> dict[str, Any]:
    summary = dict(portfolio_summary or {})
    groups = summary.get("groups") or []
    all_funds = summary.get("funds") or []
    funds_with_metric = _apply_since_inception_annualized_return_to_funds(all_funds)
    funds_by_code = {
        _normalize_code_text(item.get("基金代码")): item
        for item in funds_with_metric
        if _normalize_code_text(item.get("基金代码"))
    }
    normalized_groups: list[dict[str, Any]] = []
    for group in groups:
        current_group = dict(group or {})
        group_funds = []
        for fund in (group.get("funds") or []):
            code = _normalize_code_text((fund or {}).get("基金代码"))
            group_funds.append(dict(funds_by_code.get(code) or fund or {}))
        current_group["funds"] = group_funds
        normalized_groups.append(current_group)
    summary["funds"] = funds_with_metric
    summary["groups"] = normalized_groups
    return summary


def _format_tracker_occurrence_text(count_value: object, frequency_value: object) -> str:
    count_text = _normalize_number_text(count_value, digits=0) or "-"
    frequency_text = _normalize_number_text(frequency_value, digits=2)
    frequency_display = f"{frequency_text}%" if frequency_text else "-"
    return f"{count_text} 次 / {frequency_display}"


def _safe_json_load(path: Path) -> dict[str, Any]:
    try:
        from backend.task_audit_storage import is_maintenance_audit_path, read_task_audit

        if path.name.endswith((".json.gz", ".json.legacy.gz")) or is_maintenance_audit_path(path):
            return read_task_audit(path)
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _path_cache_token(path: Path | str | None) -> str:
    if not path:
        return "missing"
    try:
        stat = Path(path).stat()
    except OSError:
        if has_stored_csv(path):
            return stored_csv_cache_token(path)
        from backend.task_audit_storage import (
            is_task_log_path, task_log_metadata, is_maintenance_audit_path, maintenance_audit_cache_token,
            is_legacy_audit_csv_path, migrated_audit_cache_token,
        )

        if is_task_log_path(path):
            try:
                metadata = task_log_metadata(path)
                return f"{metadata['mtime_ns']}:{metadata['size']}"
            except (OSError, ValueError):
                pass
        if is_maintenance_audit_path(path):
            try:
                return maintenance_audit_cache_token(Path(path))
            except (OSError, ValueError):
                pass
        if is_legacy_audit_csv_path(path):
            try:
                return migrated_audit_cache_token(Path(path))
            except (OSError, ValueError):
                pass
        return "missing"
    return f"{int(stat.st_mtime_ns)}:{int(stat.st_size)}"


def _file_content_cache_token(path: Path | str | None) -> str:
    if not path:
        return "missing"
    current = Path(path)
    try:
        data = read_core_artifact_bytes(current)
    except OSError:
        from backend.task_audit_storage import (
            is_task_log_path, is_maintenance_audit_path, read_original_artifact_bytes,
            is_legacy_audit_csv_path,
        )

        if not (is_task_log_path(current) or is_maintenance_audit_path(current) or is_legacy_audit_csv_path(current)):
            return "missing"
        try:
            data = read_original_artifact_bytes(current)
        except (OSError, ValueError):
            return "missing"
    return f"{len(data)}:{hashlib.sha1(data).hexdigest()}"


def _sqlite_file_cache_token(path: Path | str | None) -> str:
    if not path:
        return "missing"
    base = Path(path)
    wal = Path(f"{base}-wal")
    wal_token = "empty_or_missing_wal"
    try:
        wal_stat = wal.stat()
        if int(wal_stat.st_size) > 0:
            wal_token = f"{int(wal_stat.st_mtime_ns)}:{int(wal_stat.st_size)}"
    except OSError:
        pass
    return "::".join([_path_cache_token(base), wal_token])


def _report_artifact_token(raw_path: object) -> dict[str, Any]:
    resolved_path = _resolve_existing_report_artifact_path(raw_path)
    return {
        "path": str(raw_path or "").strip() or None,
        "resolved_path": _project_relative_path_text(resolved_path),
        "token": _path_cache_token(resolved_path),
        "content_token": _file_content_cache_token(resolved_path),
    }


def _stable_json_digest(value: Any) -> str:
    normalized = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha1(normalized.encode("utf-8")).hexdigest()


_SCHEDULER_STATE_CACHE_VOLATILE_KEYS = frozenset(
    {
        "last_heartbeat_at",
        "heartbeat_at",
        "heartbeat",
        "pid",
        "captured_at",
    }
)


def _strip_scheduler_state_cache_volatile_fields(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            str(key): _strip_scheduler_state_cache_volatile_fields(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
            if str(key) not in _SCHEDULER_STATE_CACHE_VOLATILE_KEYS
        }
    if isinstance(value, list):
        return [_strip_scheduler_state_cache_volatile_fields(item) for item in value]
    return value


def _scheduler_state_semantic_cache_token(path: Path | str | None) -> str:
    if not path:
        return "missing"
    payload = _safe_json_load(Path(path))
    if not payload:
        return f"unreadable::{_path_cache_token(path)}"
    normalized = _strip_scheduler_state_cache_volatile_fields(payload)
    encoded = json.dumps(normalized, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha1(encoded.encode("utf-8")).hexdigest()


def _glob_cache_token(directory: Path | str | None, filename_glob: str) -> str:
    from backend.task_audit_storage import glob_json_artifacts

    if not directory:
        return "missing"
    base = Path(directory)
    if not base.exists():
        return f"{base}:missing"
    entries: list[str] = []
    try:
        paths = sorted(glob_json_artifacts(base, filename_glob), key=lambda path: path.name)
    except OSError:
        return f"{base}:error"
    for path in paths:
        entries.append(f"{path.name}:{_path_cache_token(path)}")
    digest = hashlib.sha1("\n".join(entries).encode("utf-8")).hexdigest()
    return f"{base}:{len(entries)}:{digest}"


def _glob_content_cache_token(directory: Path | str | None, filename_glob: str) -> str:
    token = _glob_cache_token(directory, filename_glob)
    parts = token.rsplit(":", 2)
    if len(parts) == 3 and parts[1].isdigit():
        return f"{parts[1]}:{parts[2]}"
    return token


def _final_selection_contraction_shadow_cache_token() -> str:
    reports_dir = getattr(settings, "reports_dir", None)
    if not reports_dir:
        return "missing"
    return _glob_content_cache_token(
        Path(reports_dir) / "audit" / "final_selection_contraction_shadow",
        "final_selection_contraction_shadow_*.json",
    )


def _cached_payload(cache_key: str, ttl_seconds: float, builder: Any) -> Any:
    test_scope = os.environ.get("PYTEST_CURRENT_TEST", "").strip()
    scoped_cache_key = f"{cache_key}::{test_scope}" if test_scope else cache_key
    now = time.time()
    should_build = False
    wait_event: threading.Event | None = None
    with _payload_cache_lock:
        cached = _payload_cache.get(scoped_cache_key)
        if cached and (now - float(cached.get("stored_at") or 0.0) <= ttl_seconds):
            return deepcopy(cached.get("value"))
        wait_event = _payload_cache_inflight.get(scoped_cache_key)
        if wait_event is None:
            wait_event = threading.Event()
            _payload_cache_inflight[scoped_cache_key] = wait_event
            should_build = True
    if not should_build and wait_event is not None:
        wait_event.wait(timeout=max(float(ttl_seconds), 1.0))
        with _payload_cache_lock:
            cached = _payload_cache.get(scoped_cache_key)
            if cached and (time.time() - float(cached.get("stored_at") or 0.0) <= ttl_seconds):
                return deepcopy(cached.get("value"))
        should_build = True
    try:
        value = builder()
    except Exception:
        with _payload_cache_lock:
            inflight = _payload_cache_inflight.pop(scoped_cache_key, None)
            if inflight is not None:
                inflight.set()
        raise
    with _payload_cache_lock:
        _payload_cache[scoped_cache_key] = {
            "stored_at": time.time(),
            "value": deepcopy(value),
        }
        inflight = _payload_cache_inflight.pop(scoped_cache_key, None)
        if inflight is not None:
            inflight.set()
    return deepcopy(value)


def _cached_payload_with_status(
    cache_key: str,
    ttl_seconds: float,
    builder: Any,
) -> tuple[Any, dict[str, Any]]:
    test_scope = os.environ.get("PYTEST_CURRENT_TEST", "").strip()
    scoped_cache_key = f"{cache_key}::{test_scope}" if test_scope else cache_key
    started_at = time.perf_counter()
    now = time.time()
    with _payload_cache_lock:
        cached = _payload_cache.get(scoped_cache_key)
        if cached and (now - float(cached.get("stored_at") or 0.0) <= ttl_seconds):
            return deepcopy(cached.get("value")), {
                "status": "hit",
                "hit": True,
                "seconds": round(max(0.0, time.perf_counter() - started_at), 6),
                "ttl_seconds": ttl_seconds,
            }

    value = builder()
    with _payload_cache_lock:
        _payload_cache[scoped_cache_key] = {
            "stored_at": time.time(),
            "value": deepcopy(value),
        }
    return deepcopy(value), {
        "status": "miss",
        "hit": False,
        "seconds": round(max(0.0, time.perf_counter() - started_at), 6),
        "ttl_seconds": ttl_seconds,
    }


def _scheduler_summary_fragment_cache_key(
    fragment_name: str,
    cache_basis: dict[str, Any],
) -> str:
    digest = _stable_json_digest(cache_basis)
    return f"scheduler_summary_fragment::{fragment_name}::{digest}"


def _load_scheduler_summary_fragment_cached(
    fragment_name: str,
    cache_basis: dict[str, Any],
    builder: Any,
) -> tuple[Any, dict[str, Any]]:
    normalized_name = str(fragment_name or "").strip() or "unknown"
    basis = {
        "schema_version": SCHEDULER_SUMMARY_BUILD_PROFILE_SCHEMA_VERSION,
        "fragment": normalized_name,
        **dict(cache_basis or {}),
    }
    value, cache_status = _cached_payload_with_status(
        _scheduler_summary_fragment_cache_key(normalized_name, basis),
        _SCHEDULER_SUMMARY_FRAGMENT_CACHE_TTL_SECONDS,
        builder,
    )
    status = {
        **dict(cache_status or {}),
        "fragment": normalized_name,
        "basis_digest": _stable_json_digest(basis),
    }
    return value, status


def _scheduler_summary_common_file_cache_basis() -> dict[str, Any]:
    reports_dir = Path(settings.reports_dir)
    audit_dir = reports_dir / "audit"
    return {
        "latest_report_file": _file_content_cache_token(settings.latest_report_file),
        "publication_registry_file": _file_content_cache_token(settings.publication_registry_file),
        "scheduler_state_semantic": _scheduler_state_semantic_cache_token(
            _runtime_scheduler_settings().scheduler_state_file
        ),
        "database_file": _sqlite_file_cache_token(settings.database_path),
        "risk_coverage_gap_audit": _glob_content_cache_token(
            audit_dir / "risk_coverage_gap_audit",
            "risk_coverage_gap_audit_*.json",
        ),
        "final_selection_contraction_shadow": _final_selection_contraction_shadow_cache_token(),
    }


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_file = path.parent / f"{path.name}.{uuid4().hex}.tmp"
    temp_file.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temp_file.replace(path)


def _dashboard_overview_sidecar_path() -> Path:
    return Path(settings.reports_dir) / ".cache" / "dashboard_overview_response.json"


def _low_vol_overlap_audit_cache_token() -> str:
    report = _load_current_published_report_cached() or _load_latest_report_snapshot_cached() or _load_latest_report_cached() or {}
    return low_vol_overlap_cache_token(settings, str(report.get("run_id") or ""))


def _build_dashboard_overview_cache_basis() -> dict[str, Any]:
    runtime_settings = _runtime_scheduler_settings()
    reports_dir = Path(settings.reports_dir)
    audit_dir = reports_dir / "audit"
    return {
        "schema_version": _DASHBOARD_OVERVIEW_SIDECAR_SCHEMA_VERSION,
        "low_vol_overlap_audit": _low_vol_overlap_audit_cache_token(),
        "latest_report_file": _file_content_cache_token(settings.latest_report_file),
        "publication_registry_file": _file_content_cache_token(settings.publication_registry_file),
        "scheduler_state_semantic": _scheduler_state_semantic_cache_token(
            runtime_settings.scheduler_state_file
        ),
        "database_file": _sqlite_file_cache_token(settings.database_path),
        "nav_history_maintenance": _glob_content_cache_token(
            audit_dir / "nav_history_maintenance",
            "nav_history_maintenance_*.json",
        ),
        "nav_lag_audit": _glob_content_cache_token(audit_dir / "nav_lag_audit", "nav_lag_audit_*.json"),
        "risk_sync_audit": _glob_content_cache_token(audit_dir / "risk_sync_audit", "risk_sync_audit_*.json"),
        "risk_coverage_gap_audit": _glob_content_cache_token(
            audit_dir / "risk_coverage_gap_audit",
            "risk_coverage_gap_audit_*.json",
        ),
    }


def _dashboard_overview_cache_key(cache_basis: dict[str, Any] | None = None) -> str:
    basis = cache_basis or _build_dashboard_overview_cache_basis()
    normalized = json.dumps(basis, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha1(normalized.encode("utf-8")).hexdigest()
    return f"dashboard_overview_response::{digest}"


def _load_dashboard_overview_sidecar(cache_basis: dict[str, Any]) -> dict[str, Any] | None:
    payload = _safe_json_load(_dashboard_overview_sidecar_path())
    if not payload:
        return None
    if int(payload.get("schema_version") or 0) != _DASHBOARD_OVERVIEW_SIDECAR_SCHEMA_VERSION:
        return None
    if payload.get("cache_basis") != cache_basis:
        return None
    response = payload.get("response")
    return deepcopy(response) if isinstance(response, dict) else None


def _refresh_dashboard_overview_runtime_fields(response: dict[str, Any]) -> dict[str, Any]:
    current = deepcopy(response)
    database_overview = current.get("database_overview")
    if isinstance(database_overview, dict):
        try:
            freshness_payload = load_valid_database_freshness_summary(settings)
        except Exception:
            freshness_payload = None
        if freshness_payload:
            database_overview["database_freshness_summary"] = summarize_database_freshness_payload(
                freshness_payload,
                source="sidecar",
                artifact_path=database_freshness_summary_path(settings),
            )
            current["database_overview"] = database_overview
    scheduler_overview = current.get("scheduler_overview")
    if not isinstance(scheduler_overview, dict):
        return current
    scheduler_overview["publication_dependency"] = configured_publication_blocker(_runtime_scheduler_settings())
    try:
        scheduler_state = _load_scheduler_state_snapshot_cached()
    except Exception:
        return current
    for key in (
        "scheduler_running",
        "last_heartbeat_at",
        "last_error",
        "current_task",
        "screening_backlog_due",
        "screening_backlog_reason",
        "startup_catch_up_pending",
        "startup_catch_up_due_jobs",
    ):
        if key in scheduler_state:
            scheduler_overview[key] = deepcopy(scheduler_state.get(key))
    current["scheduler_overview"] = scheduler_overview
    return current


def _write_dashboard_overview_sidecar(
    cache_basis: dict[str, Any],
    response: dict[str, Any],
) -> None:
    if not isinstance(response, dict) or response.get("error"):
        return
    try:
        sidecar_path = _dashboard_overview_sidecar_path()
        _write_json_atomic(
            sidecar_path,
            {
                "schema_version": _DASHBOARD_OVERVIEW_SIDECAR_SCHEMA_VERSION,
                "artifact_type": "dashboard_overview_response_cache",
                "generated_at": datetime.now().isoformat(timespec="seconds"),
                "cache_key": _dashboard_overview_cache_key(cache_basis),
                "cache_basis": cache_basis,
                "response": response,
            },
        )
    except Exception:
        return


def _build_dashboard_overview_source_basis(
    response_payload: dict[str, Any],
    cache_basis: dict[str, Any],
) -> dict[str, Any]:
    response = dict(response_payload or {})
    basis_source = dict(cache_basis or {})
    counts = response.get("counts") if isinstance(response.get("counts"), dict) else {}
    publication_summary = (
        response.get("publication_summary") if isinstance(response.get("publication_summary"), dict) else {}
    )
    publication_status_summary = (
        response.get("publication_status_summary")
        if isinstance(response.get("publication_status_summary"), dict)
        else {}
    )
    scope_contract = response.get("scope_contract") if isinstance(response.get("scope_contract"), dict) else {}
    automation = response.get("automation_autonomy") if isinstance(response.get("automation_autonomy"), dict) else {}
    automation_lineage = (
        automation.get("automation_lineage") if isinstance(automation.get("automation_lineage"), dict) else {}
    )
    build_profile = (
        response.get("dashboard_build_profile")
        if isinstance(response.get("dashboard_build_profile"), dict)
        else {}
    )
    reports_dir = Path(settings.reports_dir)
    audit_dir = reports_dir / "audit"

    basis: dict[str, Any] = {
        "schema_version": _DASHBOARD_OVERVIEW_SOURCE_BASIS_SCHEMA_VERSION,
        "source": "dashboard_overview_current_published_report_scheduler_database_artifacts",
        "payload_mode": "overview",
        "detail_scope": "overview",
        "run_id": str(response.get("run_id") or "").strip(),
        "generated_at": str(response.get("generated_at") or "").strip(),
        "latest_report_file": basis_source.get("latest_report_file")
        or _file_content_cache_token(settings.latest_report_file),
        "latest_report_file_path": _project_relative_path_text(settings.latest_report_file),
        "publication_registry_file": basis_source.get("publication_registry_file")
        or _file_content_cache_token(settings.publication_registry_file),
        "publication_registry_file_path": _project_relative_path_text(settings.publication_registry_file),
        "scheduler_state_file": basis_source.get("scheduler_state_semantic")
        or _scheduler_state_semantic_cache_token(_runtime_scheduler_settings().scheduler_state_file),
        "scheduler_state_file_path": _project_relative_path_text(
            _runtime_scheduler_settings().scheduler_state_file
        ),
        "database_file": basis_source.get("database_file") or _sqlite_file_cache_token(settings.database_path),
        "database_file_path": _project_relative_path_text(settings.database_path),
        "nav_history_maintenance": basis_source.get("nav_history_maintenance")
        or _glob_content_cache_token(
            audit_dir / "nav_history_maintenance",
            "nav_history_maintenance_*.json",
        ),
        "nav_lag_audit": basis_source.get("nav_lag_audit")
        or _glob_content_cache_token(audit_dir / "nav_lag_audit", "nav_lag_audit_*.json"),
        "risk_sync_audit": basis_source.get("risk_sync_audit")
        or _glob_content_cache_token(audit_dir / "risk_sync_audit", "risk_sync_audit_*.json"),
        "risk_coverage_gap_audit": basis_source.get("risk_coverage_gap_audit")
        or _glob_content_cache_token(
            audit_dir / "risk_coverage_gap_audit",
            "risk_coverage_gap_audit_*.json",
        ),
        "latest_canonical_run_id": str(
            publication_summary.get("latest_canonical_run_id")
            or publication_status_summary.get("latest_canonical_run_id")
            or automation_lineage.get("latest_canonical_run_id")
            or ""
        ).strip(),
        "current_published_run_id": str(
            publication_summary.get("current_published_run_id")
            or publication_status_summary.get("current_published_run_id")
            or automation_lineage.get("current_published_run_id")
            or ""
        ).strip(),
        "latest_successful_run_id": str(
            publication_summary.get("latest_successful_run_id")
            or publication_status_summary.get("latest_successful_run_id")
            or automation_lineage.get("latest_successful_run_id")
            or ""
        ).strip(),
        "daily_screening_run_id": str(automation_lineage.get("daily_screening_run_id") or "").strip() or None,
        "daily_screening_evidence": dict(automation_lineage.get("daily_screening_evidence") or {}),
        "final_count": int(counts.get("final_count") or 0),
        "stable_bond_final_count": int(counts.get("stable_bond_final_count") or 0),
        "enhanced_bond_final_count": int(counts.get("enhanced_bond_final_count") or 0),
        "dynamic_final_count": int(counts.get("dynamic_final_count") or 0),
        "scope_contract_digest": _stable_json_digest(scope_contract) if scope_contract else "",
        "dashboard_build_profile_schema_version": build_profile.get("schema_version"),
        "dashboard_build_step_count": build_profile.get("step_count"),
        "cache_basis_schema_version": basis_source.get("schema_version"),
        "cache_basis_digest": _stable_json_digest(basis_source),
        "missing_value_policy": "preserve_missing_semantics",
        "business_value_synthesis": "forbidden",
        "frontend_rendered_truth_allowed": False,
        "truth_note": (
            "/api/dashboard/overview is assembled from the current published report, "
            "publication registry, scheduler state, SQLite database, and runtime audit artifacts. "
            "Frontend-rendered values are never publication truth."
        ),
    }
    basis["digest"] = _stable_json_digest(basis)
    return basis


def _build_dashboard_overview_source_contract(source_basis: dict[str, Any]) -> dict[str, Any]:
    basis = dict(source_basis or {})
    return {
        "schema_version": _DASHBOARD_OVERVIEW_SOURCE_CONTRACT_SCHEMA_VERSION,
        "source": "dashboard_overview_current_published_report_scheduler_database_artifacts",
        "truth_policy": "real_publication_registry_report_artifact_scheduler_state_database_and_runtime_artifacts_only",
        "missing_value_policy": "preserve_missing_semantics",
        "business_value_synthesis": "forbidden",
        "official_publication_truth": "publication_registry_and_current_published_report",
        "frontend_rendered_truth_allowed": False,
        "cache_basis_schema_version": basis.get("schema_version"),
        "cache_basis_digest": basis.get("digest"),
    }


def _attach_dashboard_overview_source_contract(
    response_payload: dict[str, Any],
    cache_basis: dict[str, Any] | None = None,
) -> dict[str, Any]:
    response = deepcopy(response_payload or {})
    if response.get("error"):
        return response
    basis = _build_dashboard_overview_source_basis(
        response,
        cache_basis or _build_dashboard_overview_cache_basis(),
    )
    response["source_basis"] = basis
    response["cache_basis"] = dict(basis)
    response["source_contract"] = _build_dashboard_overview_source_contract(basis)
    return response


def _build_dashboard_latest_publication_status_source(
    publication_status: dict[str, Any],
) -> dict[str, Any]:
    status = publication_status if isinstance(publication_status, dict) else {}
    current_record = (
        status.get("current_published")
        if isinstance(status.get("current_published"), dict)
        else {}
    )
    latest_record = (
        status.get("latest_successful")
        if isinstance(status.get("latest_successful"), dict)
        else {}
    )
    basis: dict[str, Any] = {
        "source": "publication_registry_runtime_overlay",
        "truth_policy": "real_publication_registry_artifact_only",
        "publication_registry_file": _file_content_cache_token(settings.publication_registry_file),
        "publication_registry_file_path": _project_relative_path_text(settings.publication_registry_file),
        "official_publication_truth": "publication_registry_and_current_published_report",
        "publication_status": status.get("status"),
        "publication_status_label": status.get("status_label"),
        "current_published_run_id": status.get("current_published_run_id"),
        "latest_successful_run_id": status.get("latest_successful_run_id"),
        "latest_publishable_run_id": status.get("latest_publishable_run_id"),
        "latest_canonical_run_id": status.get("latest_canonical_run_id"),
        "current_effective_nav_date": status.get("current_effective_nav_date"),
        "latest_successful_effective_nav_date": status.get("latest_successful_effective_nav_date"),
        "publish_hold": status.get("publish_hold"),
        "publish_blocked": status.get("publish_blocked"),
        "latest_successful_publish_hold": status.get("latest_successful_publish_hold"),
        "latest_successful_publish_blocked": status.get("latest_successful_publish_blocked"),
        "latest_successful_is_currently_published": status.get(
            "latest_successful_is_currently_published"
        ),
        "latest_successful_is_publishable": status.get("latest_successful_is_publishable"),
        "current_published_final_count": current_record.get("final_count"),
        "current_published_dynamic_final_count": current_record.get("dynamic_final_count"),
        "latest_successful_final_count": latest_record.get("final_count"),
        "latest_successful_dynamic_final_count": latest_record.get("dynamic_final_count"),
        "frontend_rendered_truth_allowed": False,
        "missing_value_policy": "preserve_missing_semantics",
        "business_value_synthesis": "forbidden",
    }
    basis["digest"] = _stable_json_digest(basis)
    return basis


def _build_dashboard_latest_source_basis(
    response_payload: dict[str, Any],
    *,
    detail_scope: str,
    compact: bool,
    sample_limit: Any,
    include_full_samples: bool,
) -> dict[str, Any]:
    response = dict(response_payload or {})
    counts = response.get("counts") if isinstance(response.get("counts"), dict) else {}
    publication_summary = (
        response.get("publication_summary") if isinstance(response.get("publication_summary"), dict) else {}
    )
    publication_status_summary = (
        response.get("publication_status_summary")
        if isinstance(response.get("publication_status_summary"), dict)
        else {}
    )
    publication_status_source = _build_dashboard_latest_publication_status_source(
        publication_status_summary
    )
    automation = response.get("automation_autonomy") if isinstance(response.get("automation_autonomy"), dict) else {}
    automation_lineage = (
        automation.get("automation_lineage") if isinstance(automation.get("automation_lineage"), dict) else {}
    )
    daily_run = response.get("daily_run") if isinstance(response.get("daily_run"), dict) else {}

    def _resolve_daily_screening_source() -> tuple[str | None, dict[str, Any]]:
        direct = str(
            automation_lineage.get("daily_screening_run_id")
            or automation.get("daily_screening_run_id")
            or daily_run.get("screening_run_id")
            or daily_run.get("report_refresh_run_id")
            or ""
        ).strip()
        evidence = automation_lineage.get("daily_screening_evidence")
        evidence = dict(evidence) if isinstance(evidence, dict) else daily_screening_evidence(daily_run)
        if direct or daily_run or automation_lineage.get("daily_screening_evidence"):
            return direct or None, evidence
        try:
            scheduler_state = _load_scheduler_state_snapshot_cached()
        except Exception:
            scheduler_state = {}
        if not isinstance(scheduler_state, dict):
            return None, {}
        last_runs = scheduler_state.get("last_runs") if isinstance(scheduler_state.get("last_runs"), dict) else {}
        scheduled_daily = (
            last_runs.get("scheduled_daily_bond_refresh")
            if isinstance(last_runs.get("scheduled_daily_bond_refresh"), dict)
            else {}
        )
        run_id = str(
            scheduled_daily.get("screening_run_id")
            or scheduled_daily.get("report_refresh_run_id")
            or ""
        ).strip()
        return run_id or None, daily_screening_evidence(scheduled_daily)

    daily_screening_run_id, daily_evidence = _resolve_daily_screening_source()
    scope_contract = response.get("scope_contract") if isinstance(response.get("scope_contract"), dict) else {}
    build_profile = (
        response.get("dashboard_build_profile")
        if isinstance(response.get("dashboard_build_profile"), dict)
        else {}
    )
    sample_policy = (
        response.get("dashboard_sample_policy")
        if isinstance(response.get("dashboard_sample_policy"), dict)
        else {}
    )
    normalized_scope = str(response.get("detail_scope") or detail_scope or "").strip() or (
        "compact" if compact else "diagnostic_summary"
    )
    basis: dict[str, Any] = {
        "schema_version": _DASHBOARD_LATEST_SOURCE_BASIS_SCHEMA_VERSION,
        "source": "dashboard_latest_current_published_report_scheduler_database_artifacts",
        "payload_mode": "compact" if bool(response.get("compact") or compact) else "detail",
        "detail_scope": normalized_scope,
        "compact": bool(response.get("compact") or compact),
        "sample_limit": sample_policy.get("sample_limit")
        if "sample_limit" in sample_policy
        else dashboard_payloads.normalize_dashboard_sample_limit(sample_limit),
        "include_full_samples": bool(
            sample_policy.get("include_full_samples")
            if "include_full_samples" in sample_policy
            else include_full_samples
        ),
        "sample_policy_mode": str(sample_policy.get("mode") or "").strip(),
        "run_id": str(response.get("run_id") or "").strip(),
        "latest_report_run_id": str(
            response.get("run_id")
            or publication_summary.get("latest_canonical_run_id")
            or publication_status_summary.get("latest_canonical_run_id")
            or automation_lineage.get("latest_canonical_run_id")
            or ""
        ).strip(),
        "generated_at": str(response.get("generated_at") or "").strip(),
        "latest_report_file": _file_content_cache_token(settings.latest_report_file),
        "latest_report_file_path": _project_relative_path_text(settings.latest_report_file),
        "publication_registry_file": _file_content_cache_token(settings.publication_registry_file),
        "publication_registry_file_path": _project_relative_path_text(settings.publication_registry_file),
        "scheduler_state_file": _scheduler_state_semantic_cache_token(
            _runtime_scheduler_settings().scheduler_state_file
        ),
        "scheduler_state_file_path": _project_relative_path_text(
            _runtime_scheduler_settings().scheduler_state_file
        ),
        "database_file": _sqlite_file_cache_token(settings.database_path),
        "database_file_path": _project_relative_path_text(settings.database_path),
        "latest_canonical_run_id": str(
            publication_summary.get("latest_canonical_run_id")
            or publication_status_summary.get("latest_canonical_run_id")
            or automation_lineage.get("latest_canonical_run_id")
            or ""
        ).strip(),
        "current_published_run_id": str(
            publication_summary.get("current_published_run_id")
            or publication_status_summary.get("current_published_run_id")
            or automation_lineage.get("current_published_run_id")
            or ""
        ).strip(),
        "latest_successful_run_id": str(
            publication_summary.get("latest_successful_run_id")
            or publication_status_summary.get("latest_successful_run_id")
            or automation_lineage.get("latest_successful_run_id")
            or ""
        ).strip(),
        "publication_status": str(publication_status_summary.get("status") or "").strip(),
        "publication_status_label": str(publication_status_summary.get("status_label") or "").strip(),
        "publication_status_source": publication_status_source,
        "current_effective_nav_date": str(
            publication_status_summary.get("current_effective_nav_date") or ""
        ).strip(),
        "latest_successful_effective_nav_date": str(
            publication_status_summary.get("latest_successful_effective_nav_date") or ""
        ).strip(),
        "publish_hold": publication_status_summary.get("publish_hold"),
        "publish_blocked": publication_status_summary.get("publish_blocked"),
        "latest_successful_publish_hold": publication_status_summary.get(
            "latest_successful_publish_hold"
        ),
        "latest_successful_publish_blocked": publication_status_summary.get(
            "latest_successful_publish_blocked"
        ),
        "latest_successful_is_currently_published": publication_status_summary.get(
            "latest_successful_is_currently_published"
        ),
        "latest_successful_is_publishable": publication_status_summary.get(
            "latest_successful_is_publishable"
        ),
        "daily_screening_run_id": daily_screening_run_id,
        "daily_screening_evidence": daily_evidence,
        "final_count": counts.get("final_count")
        if counts.get("final_count") is not None
        else publication_summary.get("final_count"),
        "stable_bond_final_count": counts.get("stable_bond_final_count"),
        "enhanced_bond_final_count": counts.get("enhanced_bond_final_count"),
        "dynamic_final_count": counts.get("dynamic_final_count"),
        "publication_integrity_aligned": (
            response.get("publication_integrity", {}).get("aligned")
            if isinstance(response.get("publication_integrity"), dict)
            else None
        ),
        "artifact_consistency_aligned": (
            response.get("artifact_consistency", {}).get("aligned")
            if isinstance(response.get("artifact_consistency"), dict)
            else None
        ),
        "scope_contract_digest": _stable_json_digest(scope_contract) if scope_contract else "",
        "dashboard_build_profile_schema_version": build_profile.get("schema_version"),
        "dashboard_build_detail_scope": build_profile.get("detail_scope"),
        "dashboard_build_compact": build_profile.get("compact"),
        "dashboard_build_step_count": build_profile.get("step_count"),
        "missing_value_policy": "preserve_missing_semantics",
        "business_value_synthesis": "forbidden",
        "frontend_rendered_truth_allowed": False,
        "truth_note": (
            "/api/dashboard/latest is derived from the current published/latest report, "
            "publication registry, scheduler state, SQLite database, and runtime artifacts. "
            "Frontend-rendered values are never publication truth."
        ),
    }
    basis["digest"] = _stable_json_digest(basis)
    return basis


def _build_dashboard_latest_source_contract(source_basis: dict[str, Any]) -> dict[str, Any]:
    basis = dict(source_basis or {})
    return {
        "schema_version": _DASHBOARD_LATEST_SOURCE_CONTRACT_SCHEMA_VERSION,
        "source": "dashboard_latest_current_published_report_scheduler_database_artifacts",
        "truth_policy": "real_publication_registry_report_artifact_scheduler_state_database_and_runtime_artifacts_only",
        "missing_value_policy": "preserve_missing_semantics",
        "business_value_synthesis": "forbidden",
        "official_publication_truth": "publication_registry_and_current_published_report",
        "publication_status_source": "publication_registry_runtime_overlay",
        "frontend_rendered_truth_allowed": False,
        "cache_basis_schema_version": basis.get("schema_version"),
        "cache_basis_digest": basis.get("digest"),
    }


def _attach_dashboard_latest_source_contract(
    response_payload: dict[str, Any],
    *,
    detail_scope: str,
    compact: bool,
    sample_limit: Any,
    include_full_samples: bool,
) -> dict[str, Any]:
    response = deepcopy(response_payload or {})
    if response.get("error"):
        return response
    response = _sync_publication_compat_fields(response)
    basis = _build_dashboard_latest_source_basis(
        response,
        detail_scope=detail_scope,
        compact=compact,
        sample_limit=sample_limit,
        include_full_samples=include_full_samples,
    )
    response["source_basis"] = basis
    response["cache_basis"] = dict(basis)
    response["source_contract"] = _build_dashboard_latest_source_contract(basis)
    return response


def _has_business_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    return True


def _first_business_value(*values: Any) -> Any:
    for value in values:
        if _has_business_value(value):
            return value
    return None


def _sync_publication_compat_fields(response_payload: dict[str, Any]) -> dict[str, Any]:
    """Expose current published truth through legacy top-level API fields."""
    response = dict(response_payload or {})
    publication_status = (
        response.get("publication_status_summary")
        if isinstance(response.get("publication_status_summary"), dict)
        else {}
    )
    current_record = (
        publication_status.get("current_published")
        if isinstance(publication_status.get("current_published"), dict)
        else {}
    )
    latest_record = (
        publication_status.get("latest_successful")
        if isinstance(publication_status.get("latest_successful"), dict)
        else {}
    )
    current_snapshot = (
        current_record.get("snapshot")
        if isinstance(current_record.get("snapshot"), dict)
        else {}
    )
    latest_snapshot = (
        latest_record.get("snapshot")
        if isinstance(latest_record.get("snapshot"), dict)
        else {}
    )
    counts = response.get("counts") if isinstance(response.get("counts"), dict) else {}
    tracker_summary = (
        response.get("tracker_summary") if isinstance(response.get("tracker_summary"), dict) else {}
    )

    current_nav_date = _first_business_value(
        publication_status.get("current_effective_nav_date"),
        current_record.get("effective_nav_date"),
        current_snapshot.get("candidate_date"),
        tracker_summary.get("latest_nav_snapshot_date"),
        response.get("current_effective_nav_date"),
        response.get("effective_nav_date"),
        response.get("candidate_date"),
    )
    latest_nav_date = _first_business_value(
        publication_status.get("latest_successful_effective_nav_date"),
        latest_record.get("effective_nav_date"),
        latest_snapshot.get("candidate_date"),
    )

    field_values = {
        "current_published_run_id": publication_status.get("current_published_run_id"),
        "latest_successful_run_id": publication_status.get("latest_successful_run_id"),
        "latest_publishable_run_id": publication_status.get("latest_publishable_run_id"),
        "latest_canonical_run_id": publication_status.get("latest_canonical_run_id"),
        "current_effective_nav_date": current_nav_date,
        "effective_nav_date": current_nav_date,
        "candidate_date": current_nav_date,
        "latest_successful_effective_nav_date": latest_nav_date,
        "final_count": _first_business_value(current_record.get("final_count"), counts.get("final_count")),
        "dynamic_final_count": _first_business_value(
            current_record.get("dynamic_final_count"),
            counts.get("dynamic_final_count"),
        ),
        "stable_bond_final_count": _first_business_value(
            current_record.get("stable_bond_final_count"),
            counts.get("stable_bond_final_count"),
        ),
        "enhanced_bond_final_count": _first_business_value(
            current_record.get("enhanced_bond_final_count"),
            counts.get("enhanced_bond_final_count"),
        ),
        "project_tracker_current_active_count": _first_business_value(
            current_record.get("project_tracker_current_active_count"),
            counts.get("project_tracker_current_active_count"),
        ),
        "publication_status": publication_status.get("status"),
        "publication_status_label": publication_status.get("status_label"),
        "publish_hold": publication_status.get("publish_hold"),
        "publish_blocked": publication_status.get("publish_blocked"),
        "latest_successful_publish_hold": publication_status.get("latest_successful_publish_hold"),
        "latest_successful_publish_blocked": publication_status.get("latest_successful_publish_blocked"),
        "latest_successful_is_currently_published": publication_status.get(
            "latest_successful_is_currently_published"
        ),
        "latest_successful_is_publishable": publication_status.get("latest_successful_is_publishable"),
    }
    for key, value in field_values.items():
        if _has_business_value(value):
            response[key] = value
    return response


def _safe_run_id_path_component(value: object) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-")
    if any(ch not in allowed for ch in text):
        return ""
    return text


def _recommendations_detail_sidecar_path(payload: dict[str, Any]) -> Path | None:
    from backend.recommendations_sidecar import recommendations_detail_sidecar_path

    return recommendations_detail_sidecar_path(settings, payload)


def _project_relative_path_text(path: Path | str | None) -> str | None:
    if path is None:
        return None
    current = Path(path)
    try:
        return str(current.relative_to(settings.project_root))
    except ValueError:
        return str(current)


def _recommendations_detail_artifact_token(raw_path: object, *, content: bool = False) -> str:
    path = _resolve_existing_report_artifact_path(raw_path)
    if path is None:
        return "missing"
    return _file_content_cache_token(path) if content else _path_cache_token(path)


def _build_recommendations_detail_cache_basis(payload: dict[str, Any]) -> dict[str, Any]:
    return build_run_local_recommendations_detail_cache_basis(settings, payload)


def _probe_recommendations_detail_sidecar_status(
    payload: dict[str, Any],
    cache_basis: dict[str, Any],
) -> dict[str, Any]:
    sidecar_path = _recommendations_detail_sidecar_path(payload)
    status = {
        "sidecar_path": _project_relative_path_text(sidecar_path),
        "schema_version": _RECOMMENDATIONS_DETAIL_SIDECAR_SCHEMA_VERSION,
        "status": "missing",
        "hit": False,
    }
    if sidecar_path is None:
        return {**status, "status": "unavailable_run_id"}
    if not sidecar_path.exists():
        return status
    try:
        sidecar_payload = json.loads(sidecar_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, TypeError, ValueError) as exc:
        return {
            **status,
            "status": "read_error",
            "error_type": type(exc).__name__,
        }
    if not isinstance(sidecar_payload, dict):
        return {**status, "status": "invalid_payload_type"}
    if str(sidecar_payload.get("artifact_type") or "").strip() != "recommendations_detail_sidecar":
        return {**status, "status": "artifact_type_mismatch"}
    if int(sidecar_payload.get("schema_version") or 0) != _RECOMMENDATIONS_DETAIL_SIDECAR_SCHEMA_VERSION:
        return {
            **status,
            "status": "schema_mismatch",
            "actual_schema_version": sidecar_payload.get("schema_version"),
        }
    if sidecar_payload.get("cache_basis") != cache_basis:
        return {**status, "status": "fingerprint_mismatch"}
    if not isinstance(sidecar_payload.get("response"), dict):
        return {**status, "status": "missing_response"}
    return {
        **status,
        "status": "hit",
        "hit": True,
        "generated_at": sidecar_payload.get("generated_at"),
    }


def _load_recommendations_detail_sidecar(
    payload: dict[str, Any],
    cache_basis: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    sidecar_path = _recommendations_detail_sidecar_path(payload)
    if sidecar_path is None or not sidecar_path.exists():
        return None
    try:
        sidecar_payload = json.loads(sidecar_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        return None
    if not isinstance(sidecar_payload, dict):
        return None
    if str(sidecar_payload.get("artifact_type") or "").strip() != "recommendations_detail_sidecar":
        return None
    if int(sidecar_payload.get("schema_version") or 0) != _RECOMMENDATIONS_DETAIL_SIDECAR_SCHEMA_VERSION:
        return None
    expected_basis = cache_basis or _build_recommendations_detail_cache_basis(payload)
    if sidecar_payload.get("cache_basis") != expected_basis:
        return None
    response = sidecar_payload.get("response")
    if not isinstance(response, dict):
        return None
    result = attach_recommendations_detail_source_contract(deepcopy(response), expected_basis)
    payload_scope = result.get("payload_scope") if isinstance(result.get("payload_scope"), dict) else {}
    result["payload_scope"] = {
        **payload_scope,
        _RECOMMENDATIONS_DETAIL_SIDECAR_META_LABEL: {
            "source": "run_local_recommendations_detail_sidecar",
            "sidecar_path": _project_relative_path_text(sidecar_path),
            "schema_version": _RECOMMENDATIONS_DETAIL_SIDECAR_SCHEMA_VERSION,
        },
    }
    return result


def _write_recommendations_detail_sidecar(
    payload: dict[str, Any],
    cache_basis: dict[str, Any],
    response: dict[str, Any],
) -> Path | None:
    if (payload.get("publication_control") or {}).get("commit_protocol"):
        return None  # Committed artifacts are immutable; API caches stay in memory.
    sidecar_path = _recommendations_detail_sidecar_path(payload)
    if sidecar_path is None:
        return None
    if not isinstance(response, dict) or response.get("error"):
        return None
    response_with_contract = attach_recommendations_detail_source_contract(
        deepcopy(response),
        cache_basis,
    )
    try:
        sidecar_path.parent.mkdir(parents=True, exist_ok=True)
        _write_json_atomic(
            sidecar_path,
            {
                "artifact_type": "recommendations_detail_sidecar",
                "schema_version": _RECOMMENDATIONS_DETAIL_SIDECAR_SCHEMA_VERSION,
                "generated_at": datetime.now().isoformat(timespec="seconds"),
                "run_id": payload.get("run_id"),
                "cache_basis": cache_basis,
                "response": response_with_contract,
            },
        )
        return sidecar_path
    except Exception:
        return None


def _recommendations_detail_sidecar_enabled() -> bool:
    return not ("pytest" in sys.modules or os.environ.get("PYTEST_CURRENT_TEST"))


def _favorite_query_history_snapshot() -> dict[str, Any]:
    payload = _safe_json_load(settings.favorites_history_file)
    records = payload.get("records")
    if not isinstance(records, list):
        return {"records": []}
    normalized: list[dict[str, Any]] = []
    for item in records[:FAVORITE_FUND_HISTORY_LIMIT]:
        if not isinstance(item, dict):
            continue
        codes = item.get("codes")
        if not isinstance(codes, list):
            continue
        normalized_codes = [
            code
            for code in (_normalize_code_text(value) for value in codes)
            if code
        ]
        if not normalized_codes:
            continue
        label_text = str(item.get("label") or "").strip() or f"查询 {len(normalized_codes)} 只基金"
        normalized.append(
            {
                "id": str(item.get("id") or "").strip() or str(int(time.time() * 1000)),
                "label": label_text,
                "codes": normalized_codes,
                "entry_type": _normalize_favorite_history_entry_type(item.get("entry_type"), label=label_text),
                "created_at": str(item.get("created_at") or "").strip(),
                "updated_at": str(item.get("updated_at") or "").strip(),
            }
        )
    return {"records": normalized}


def _save_favorite_query_history(
    codes: list[str],
    *,
    label: str | None = None,
    entry_type: str = FAVORITE_HISTORY_ENTRY_TYPE_QUERY,
) -> dict[str, Any]:
    snapshot = _favorite_query_history_snapshot()
    normalized_codes = []
    seen: set[str] = set()
    for code in codes:
        normalized = _normalize_code_text(code)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        normalized_codes.append(normalized)
    if not normalized_codes:
        return snapshot

    now = datetime.now().isoformat(timespec="seconds")
    records = list(snapshot.get("records") or [])
    label_text = str(label or "").strip() or f"查询 {len(normalized_codes)} 只基金"
    entry_type_text = _normalize_favorite_history_entry_type(entry_type, label=label_text)
    signature = f"{entry_type_text}:{','.join(normalized_codes)}"
    updated = False
    for item in records:
        existing_entry_type = _normalize_favorite_history_entry_type(
            item.get("entry_type"),
            label=str(item.get("label") or "").strip(),
        )
        existing_signature = ",".join(
            code
            for code in (_normalize_code_text(value) for value in (item.get("codes") or []))
            if code
        )
        if f"{existing_entry_type}:{existing_signature}" != signature:
            continue
        item["label"] = label_text
        item["codes"] = normalized_codes
        item["entry_type"] = entry_type_text
        item["updated_at"] = now
        updated = True
        break
    if not updated:
        records.insert(
            0,
            {
                "id": str(int(time.time() * 1000)),
                "label": label_text,
                "codes": normalized_codes,
                "entry_type": entry_type_text,
                "created_at": now,
                "updated_at": now,
            },
        )
    payload = {"records": records[:FAVORITE_FUND_HISTORY_LIMIT]}
    _write_json_atomic(settings.favorites_history_file, payload)
    return payload


def _normalize_portfolio_groups(portfolio_summary: dict[str, Any] | None) -> dict[str, Any]:
    summary = portfolio_summary or {}
    groups = summary.get("groups") or []
    normalized_groups: list[dict[str, Any]] = []
    normalized_all_funds: list[dict[str, Any]] = []
    total_fund_count = 0

    for group in groups:
        funds = group.get("funds") or []
        normalized_funds: list[dict[str, Any]] = []
        for fund in funds:
            normalized_funds.append(
                {
                    "基金代码": _normalize_code_text(fund.get("基金代码")),
                    "基金名称": str(fund.get("基金名称") or "").strip(),
                    "基金公司": str(fund.get("基金公司") or "").strip(),
                    "数据来源": str(fund.get("数据来源") or "").strip(),
                    "正式榜分组": str(fund.get("正式榜分组") or "").strip(),
                    "持有期状态": str(fund.get("持有期状态") or "").strip(),
                    "债基风格标签": str(fund.get("债基风格标签") or "").strip(),
                    "基金规模(亿)": _normalize_number_text(fund.get("基金规模(亿)"), digits=2),
                    "最新净值日期": str(fund.get("最新净值日期") or "").strip(),
                    "费率合计(%)": _normalize_number_text(fund.get("费率合计(%)"), digits=2),
                    "正式投资评分": _normalize_number_text(fund.get("正式投资评分"), digits=4),
                    "投资优先池得分": _normalize_number_text(fund.get("投资优先池得分"), digits=4),
                    "项目质量信心": str(fund.get("项目质量信心") or "").strip(),
                    "出现次数": int(_to_float(fund.get("出现次数")) or 0),
                    "出现频率(%)": _normalize_number_text(fund.get("出现频率(%)"), digits=2),
                    "出现次数&出现频率(%)": _format_tracker_occurrence_text(
                        fund.get("出现次数"),
                        fund.get("出现频率(%)"),
                    ),
                    "1年-年化收益(%)": _normalize_number_text(fund.get("1年-年化收益(%)"), digits=2),
                    "平台成立以来收益年化(%)": _normalize_number_text(fund.get("平台成立以来收益年化(%)"), digits=2),
                    "累计净值比年化(%)": _normalize_number_text(fund.get("累计净值比年化(%)"), digits=2),
                    "近1月(%)": _normalize_number_text(fund.get("近1月(%)"), digits=2),
                    "近3月(%)": _normalize_number_text(fund.get("近3月(%)"), digits=2),
                    "1年-夏普比率": _normalize_number_text(fund.get("1年-夏普比率"), digits=4),
                    "1年-最大回撤(%)": _normalize_number_text(fund.get("1年-最大回撤(%)"), digits=4),
                    "1年-波动率": _normalize_number_text(fund.get("1年-波动率"), digits=4),
                    **{key: fund.get(key) for key in DATA_VALIDATION_COLUMNS if key in fund},
                    "AutoStock原始校验状态": str(fund.get("AutoStock原始校验状态") or "").strip(),
                    "长期稳定性标签": str(fund.get("长期稳定性标签") or "").strip(),
                    "建议权重": str(fund.get("建议权重") or "").strip(),
                    "入选原因": str(fund.get("入选原因") or "").strip(),
                    "风险提示": str(fund.get("风险提示") or "").strip(),
                }
            )
        normalized_all_funds.extend(normalized_funds)
        total_fund_count += len(normalized_funds)
        normalized_groups.append(
            {
                "name": str(group.get("name") or "").strip(),
                "description": str(group.get("description") or "").strip(),
                "count": int(group.get("count") or len(normalized_funds)),
                "funds": normalized_funds,
                "downgrade_info": group.get("downgrade_info") if isinstance(group.get("downgrade_info"), dict) else None,
            }
        )

    quality_gate_summary = summary.get("quality_gate")
    if not isinstance(quality_gate_summary, dict):
        quality_gate_summary = summary.get("core_quality_gate")
    if not isinstance(quality_gate_summary, dict):
        quality_gate_summary = {}

    return {
        "available": bool(summary.get("available")) and bool(normalized_groups),
        "group_count": len(normalized_groups),
        "fund_count": total_fund_count,
        "target_count": int(summary.get("target_count") or total_fund_count),
        "source_candidate_count": int(summary.get("source_candidate_count") or summary.get("candidate_count") or total_fund_count),
        "candidate_count": int(summary.get("candidate_count") or total_fund_count),
        "company_cap": int(summary.get("company_cap") or 0),
        "share_class_unique_count": int(summary.get("share_class_unique_count") or 0),
        "target_policy": summary.get("target_policy") if isinstance(summary.get("target_policy"), dict) else {},
        "quality_gate": quality_gate_summary,
        "core_quality_gate": quality_gate_summary,
        "compression_audit": summary.get("compression_audit") if isinstance(summary.get("compression_audit"), dict) else {},
        "selection_policy": str(summary.get("selection_policy") or "").strip(),
        "funds": normalized_all_funds,
        "groups": normalized_groups,
    }


def _split_semicolon_text(value: Any, *, limit: int | None = None) -> list[str]:
    text = str(value or "").strip()
    if not text:
        return []
    parts = [item.strip() for item in text.replace(";", "；").split("；") if item.strip()]
    if limit is not None:
        return parts[:limit]
    return parts


def _build_portfolio_group_brief(portfolio_summary: dict[str, Any] | None) -> dict[str, Any]:
    summary = portfolio_summary or {}
    groups = summary.get("groups") or []
    result: dict[str, Any] = {}
    for group in groups:
        group_name = str(group.get("name") or "").strip()
        funds = group.get("funds") or []
        if not group_name:
            continue
        style_labels = sorted({str(item.get("债基风格标签") or "").strip() for item in funds if str(item.get("债基风格标签") or "").strip()})
        confidence_counts = Counter(str(item.get("项目质量信心") or "").strip() for item in funds if str(item.get("项目质量信心") or "").strip())
        autostock_counts = Counter(str(validation_status_value(item) or "").strip() for item in funds if str(validation_status_value(item) or "").strip())
        stability_counts = Counter(str(item.get("长期稳定性标签") or "").strip() for item in funds if str(item.get("长期稳定性标签") or "").strip())
        holding_status_counts = Counter(str(item.get("持有期状态") or "").strip() for item in funds if str(item.get("持有期状态") or "").strip())
        risk_counter: Counter[str] = Counter()
        reason_counter: Counter[str] = Counter()
        for fund in funds:
            for item in _split_semicolon_text(fund.get("风险提示"), limit=3):
                if item and item != "当前未见额外显式风险提示":
                    risk_counter[item] += 1
            for item in _split_semicolon_text(fund.get("入选原因"), limit=4):
                if item:
                    reason_counter[item] += 1
        result[group_name] = {
            "style_count": len(style_labels),
            "style_preview": style_labels[:4],
            "high_confidence_count": int(confidence_counts.get("高信心", 0)),
            "strict_match_count": int(autostock_counts.get("strict_match", 0)),
            "long_term_strong_count": int(stability_counts.get("长期稳定性强", 0)),
            "confirmed_holding_count": int(holding_status_counts.get("已确认<=90天", 0)),
            "no_signal_holding_count": int(holding_status_counts.get(HOLDING_PERIOD_NO_SIGNAL_STATUS, 0)),
            "name_unparsed_holding_count": int(holding_status_counts.get(HOLDING_PERIOD_NAME_UNPARSED_STATUS, 0)),
            "unknown_holding_count": int(holding_status_counts.get("结构化持有期未知", 0)),
            "generic_placeholder_holding_count": int(holding_status_counts.get(HOLDING_PERIOD_GENERIC_PLACEHOLDER_STATUS, 0)),
            "top_reason_tags": [
                {"label": label, "count": int(count)}
                for label, count in reason_counter.most_common(4)
            ],
            "top_risk_tags": [
                {"label": label, "count": int(count)}
                for label, count in risk_counter.most_common(4)
            ],
        }
    return result


def _build_autostock_valuation_payload(row: dict[str, Any]) -> dict[str, Any]:
    latest_nav = _normalize_number_text(row.get("netWorth"), digits=4)
    latest_nav_date = str(row.get("netWorthDate") or row.get("day") or "").strip()
    if row.get("_source") == "tiantian_fund_valuation_last":
        payload = _build_tiantian_valuation_payload(row.get("_raw_fields") or {})
        return {**payload, "latest_nav": latest_nav, "latest_nav_date": latest_nav_date}
    return {
        "available": False,
        "source": "autostock_batch",
        "reason": "valuation_lookup_pending",
        "message": "AutoStock 仅提供实时基础资料；盘中估值统一待天天基金实时估值接口补查",
        "latest_nav": latest_nav,
        "latest_nav_date": latest_nav_date,
    }


def _normalize_autostock_scale_text(value: object) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    normalized = text.replace("亿元", "").replace("亿", "").strip()
    return _normalize_number_text(normalized, digits=2)


def _fetch_favorite_autostock_snapshot(codes: list[str]) -> FavoriteAutostockSnapshot:
    # Keep the legacy helper name for integrations; source metadata identifies
    # the actual provider. A same-supplier snapshot is never an AutoStock pass.
    if not getattr(settings, "autostock_enabled", True):
        from backend.fund_snapshot import fetch_fund_snapshots
        rows, meta = fetch_fund_snapshots(
            settings, _normalize_tracker_valuation_codes(codes),
            max_codes=_FAVORITE_AUTOSTOCK_AUXILIARY_MAX_REQUEST_CODES,
        )
        return FavoriteAutostockSnapshot(rows, meta=meta)
    normalized_codes: list[str] = []
    seen: set[str] = set()
    for code in codes:
        text = _normalize_code_text(code)
        if not text or text in seen:
            continue
        seen.add(text)
        normalized_codes.append(text)
    empty_meta = {
        "status": "empty",
        "source": "autostock_batch",
        "request_source": "favorites_score",
        "requested_count": len(normalized_codes),
        "returned_count": 0,
        "upstream_requested_count": 0,
        "upstream_returned_count": 0,
        "skipped_count": 0,
        "skipped_over_limit_count": 0,
        "skipped_quota_count": 0,
        "batch_count": 0,
        "quota_exhausted": False,
        "quota_unavailable": False,
        "quota_reserved_for_automation": 0,
        "cache_ttl_seconds": 0,
        "max_request_codes": _FAVORITE_AUTOSTOCK_AUXILIARY_MAX_REQUEST_CODES,
        "effective_batch_size": _get_autostock_batch_size(),
    }
    if not normalized_codes:
        return FavoriteAutostockSnapshot({}, meta=empty_meta)

    max_request_codes = max(1, int(_FAVORITE_AUTOSTOCK_AUXILIARY_MAX_REQUEST_CODES))
    requestable_codes = normalized_codes[:max_request_codes]
    skipped_over_limit_codes = normalized_codes[max_request_codes:]
    effective_batch_size = _get_autostock_batch_size()
    quota_snapshot = _read_autostock_quota_snapshot()
    needed_call_count = (len(requestable_codes) + effective_batch_size - 1) // effective_batch_size
    budget_plan = plan_autostock_call_budget(
        quota_snapshot=quota_snapshot,
        needed_call_count=needed_call_count,
        reserve_calls=_FAVORITE_AUTOSTOCK_AUXILIARY_RESERVE_CALLS,
    )
    allowed_call_count = int(budget_plan.get("allowed_call_count") or 0)
    fetchable_count = min(len(requestable_codes), allowed_call_count * effective_batch_size)
    fetch_codes = requestable_codes[:fetchable_count]
    skipped_budget_codes = requestable_codes[fetchable_count:]
    skipped_upstream_codes: list[str] = []
    results: dict[str, dict[str, Any]] = {}
    upstream_requested_count = 0
    upstream_returned_count = 0
    batch_count = 0
    quota_exhausted = bool(quota_snapshot.get("quota_exhausted"))
    upstream_error: str | None = None

    if fetch_codes:
        try:
            autostock_client = _get_autostock_client()
        except Exception as exc:
            upstream_error = str(exc)
            skipped_upstream_codes = list(fetch_codes)
            autostock_client = None
    else:
        autostock_client = None

    if autostock_client is not None:
        for offset in range(0, len(fetch_codes), effective_batch_size):
            batch_codes = fetch_codes[offset:offset + effective_batch_size]
            batch_count += 1
            upstream_requested_count += len(batch_codes)
            try:
                if hasattr(autostock_client, "get_fund_batch_with_meta"):
                    autostock_rows, batch_meta = autostock_client.get_fund_batch_with_meta(
                        batch_codes,
                        batch_size=effective_batch_size,
                    )
                    batch_quota_exhausted = bool((batch_meta or {}).get("quota_exhausted"))
                    quota_exhausted = quota_exhausted or batch_quota_exhausted
                    upstream_error = str((batch_meta or {}).get("last_error") or upstream_error or "").strip() or None
                else:
                    autostock_rows = autostock_client.get_fund_batch(
                        batch_codes,
                        batch_size=effective_batch_size,
                    )
                    batch_quota_exhausted = False
            except Exception as exc:
                upstream_error = str(exc)
                skipped_upstream_codes = batch_codes + fetch_codes[offset + effective_batch_size:]
                break

            by_code = {
                _normalize_code_text(item.get("code")): item
                for item in (autostock_rows or [])
                if isinstance(item, dict) and _normalize_code_text(item.get("code"))
            }
            upstream_returned_count += len(by_code)
            for code, row in by_code.items():
                results[code] = row
            if batch_quota_exhausted:
                skipped_budget_codes = fetch_codes[offset + effective_batch_size:] + skipped_budget_codes
                break

    skipped_count = len(skipped_over_limit_codes) + len(skipped_budget_codes) + len(skipped_upstream_codes)
    status = "ok"
    if quota_exhausted or bool(budget_plan.get("quota_unavailable")):
        status = "quota_degraded"
    elif upstream_error or skipped_count:
        status = "partial"
    meta = {
        **empty_meta,
        "status": status,
        "requested_count": len(normalized_codes),
        "returned_count": len(results),
        "upstream_requested_count": upstream_requested_count,
        "upstream_returned_count": upstream_returned_count,
        "skipped_count": skipped_count,
        "skipped_over_limit_count": len(skipped_over_limit_codes),
        "skipped_quota_count": len(skipped_budget_codes),
        "skipped_upstream_failed_count": len(skipped_upstream_codes),
        "skipped_codes_sample": (skipped_budget_codes + skipped_upstream_codes + skipped_over_limit_codes)[:50],
        "batch_count": batch_count,
        "quota_exhausted": quota_exhausted,
        "quota_unavailable": bool(budget_plan.get("quota_unavailable")) or bool(skipped_budget_codes),
        "quota_recent_call_count": quota_snapshot.get("recent_call_count"),
        "quota_remaining": quota_snapshot.get("quota_remaining"),
        "quota_reserved_for_automation": int(budget_plan.get("quota_reserved_for_automation") or 0),
        "max_calls_per_hour": quota_snapshot.get("max_calls_per_hour"),
        "rate_limit_wait_seconds": float(quota_snapshot.get("rate_limit_wait_seconds") or 0.0),
        "max_request_codes": max_request_codes,
        "effective_batch_size": effective_batch_size,
        "upstream_error": upstream_error,
        "state_read_error": quota_snapshot.get("state_read_error"),
    }
    return FavoriteAutostockSnapshot(results, meta=meta)


def _overlay_favorite_row_with_autostock(
    row: dict[str, Any],
    autostock_row: dict[str, Any] | None,
    *,
    prefer_live_periodic_fields: bool = False,
) -> dict[str, Any]:
    normalized = dict(row)
    normalized["__favorite_snapshot_return_date"] = row.get("区间收益基准日") or None
    if not autostock_row:
        if not str(normalized.get("实时估值") or "").strip():
            normalized["实时估值"] = "暂无估值"
        normalized.setdefault("实时估值时间", "")
        normalized.setdefault("实时估算净值", "")
        normalized.setdefault("最新单位净值", normalized.get("最新单位净值", ""))
        normalized.setdefault("基金经理", "")
        normalized.setdefault("基金类型", "")
        normalized.setdefault(_FAVORITE_LIVE_RETURN_1W_LABEL, "")
        normalized.setdefault(_FAVORITE_LIVE_RETURN_1M_LABEL, "")
        normalized.setdefault(_FAVORITE_LIVE_RETURN_3M_LABEL, "")
        normalized.setdefault(_FAVORITE_RETURN_1W_SOURCE_LABEL, "")
        normalized.setdefault(_FAVORITE_RETURN_1M_SOURCE_LABEL, "")
        normalized.setdefault(_FAVORITE_RETURN_3M_SOURCE_LABEL, "")
        return normalized

    valuation = _build_autostock_valuation_payload(autostock_row)
    latest_nav = str(valuation.get("latest_nav") or "").strip()
    latest_nav_date = str(valuation.get("latest_nav_date") or "").strip()
    normalized["__favorite_live_return_date"] = latest_nav_date or None
    normalized["__favorite_live_source"] = autostock_row.get("_source") or "autostock_batch"
    normalized["__favorite_live_source_evidence"] = autostock_row.get("_source_evidence")
    normalized["__favorite_live_field_states"] = autostock_row.get("_field_states")
    level = str(normalized.get("当前层级") or "").strip()
    allow_live_fallback_for_periodic_fields = level in {"项目外样本", "未收录"}

    if str(autostock_row.get("name") or "").strip():
        normalized["基金名称"] = str(autostock_row.get("name") or "").strip()
    if str(autostock_row.get("type") or "").strip():
        normalized["基金类型"] = str(autostock_row.get("type") or "").strip()
    if str(autostock_row.get("manager") or "").strip():
        normalized["基金经理"] = str(autostock_row.get("manager") or "").strip()

    scale_text = _normalize_autostock_scale_text(autostock_row.get("fundScale"))
    if scale_text:
        normalized["基金规模(亿)"] = scale_text

    normalized["最新单位净值"] = latest_nav or str(normalized.get("最新单位净值") or "").strip()
    normalized["最新净值日期"] = latest_nav_date or str(normalized.get("最新净值日期") or "").strip()

    if valuation.get("available"):
        normalized["实时估值"] = str(valuation.get("estimated_change_pct") or "").strip()
        normalized["实时估值时间"] = str(valuation.get("valuation_date") or "").strip()
        normalized["实时估算净值"] = str(valuation.get("estimated_nav") or "").strip()
        normalized["__valuation_state"] = "ready"
        normalized["__valuation_status_label"] = "已返回"
        normalized["__valuation_reason"] = ""
    else:
        normalized["实时估值"] = ""
        normalized["实时估值时间"] = ""
        normalized["实时估算净值"] = ""
        normalized["__valuation_state"] = "unresolved"
        normalized["__valuation_status_label"] = "待实时估值接口补查"
        normalized["__valuation_reason"] = str(valuation.get("reason") or "").strip()

    for source_key, target_key, digits, live_value_label, source_label in [
        ("dayGrowth", "最新日增长率(%)", 2, "", ""),
        ("lastWeekGrowth", "近1周(%)", 2, _FAVORITE_LIVE_RETURN_1W_LABEL, _FAVORITE_RETURN_1W_SOURCE_LABEL),
        ("lastMonthGrowth", "近1月(%)", 2, _FAVORITE_LIVE_RETURN_1M_LABEL, _FAVORITE_RETURN_1M_SOURCE_LABEL),
        ("lastThreeMonthsGrowth", "近3月(%)", 2, _FAVORITE_LIVE_RETURN_3M_LABEL, _FAVORITE_RETURN_3M_SOURCE_LABEL),
        ("lastSixMonthsGrowth", "近6月(%)", 2, _FAVORITE_LIVE_RETURN_6M_LABEL, _FAVORITE_RETURN_6M_SOURCE_LABEL),
        ("lastYearGrowth", "近1年(%)", 2, "", ""),
    ]:
        value = _normalize_number_text(autostock_row.get(source_key), digits=digits)
        if not value:
            if live_value_label:
                normalized.setdefault(live_value_label, "")
            if source_label:
                normalized.setdefault(source_label, "")
            continue
        if live_value_label:
            normalized[live_value_label] = value
        if prefer_live_periodic_fields and target_key:
            normalized[target_key] = value
            if source_label:
                normalized[source_label] = "live_api"
            continue
        current_text = str(normalized.get(target_key) or "").strip()
        if current_text:
            if source_label:
                normalized[source_label] = "snapshot"
            continue
        if live_value_label and not allow_live_fallback_for_periodic_fields:
            if source_label:
                normalized[source_label] = ""
            continue
        if source_label:
            normalized[source_label] = "live_fallback"
        normalized[target_key] = value

    normalized["推荐解读"] = _favorite_digest_from_row(normalized)
    return normalized


def _overlay_favorite_row_with_live_autostock(
    row: dict[str, Any],
    autostock_row: dict[str, Any] | None,
) -> dict[str, Any]:
    """Apply the live-API overlay while retaining test/plugin call compatibility.

    Older integrations monkeypatch the two-argument overlay helper.  The
    production helper supports the explicit live-periodic-field keyword; if a
    compatible replacement does not, retry with its legacy two-argument
    contract without changing the real-data path.
    """
    try:
        return _overlay_favorite_row_with_autostock(
            row,
            autostock_row,
            prefer_live_periodic_fields=True,
        )
    except TypeError as exc:
        if "prefer_live_periodic_fields" not in str(exc):
            raise
        return _overlay_favorite_row_with_autostock(row, autostock_row)


def _read_excel_cached(
    file_path: Path,
    *,
    sheet_name: str | None = None,
    usecols: list[str] | None = None,
    header: int | None = 0,
) -> pd.DataFrame:
    stat = file_path.stat()
    cache_key = json.dumps(
        {
            "path": str(file_path),
            "sheet_name": sheet_name or "",
            "usecols": usecols or [],
            "header": header,
            "mtime_ns": stat.st_mtime_ns,
            "size": stat.st_size,
        },
        ensure_ascii=False,
        sort_keys=True,
    )
    with _excel_cache_lock:
        cached = _excel_cache.get(cache_key)
        if cached is not None:
            return cached["df"].copy()

    df = pd.read_excel(file_path, sheet_name=sheet_name, dtype=str, usecols=usecols, header=header).fillna("")

    with _excel_cache_lock:
        _excel_cache[cache_key] = {"df": df.copy()}
        while len(_excel_cache) > _EXCEL_CACHE_MAX_ENTRIES:
            oldest_key = next(iter(_excel_cache))
            _excel_cache.pop(oldest_key, None)
    return df


def _resolve_existing_report_artifact_path(
    raw_path: object,
    *,
    fallback_glob_patterns: list[str] | None = None,
) -> Path | None:
    text = str(raw_path or "").strip()
    if not text:
        return None

    from backend.task_audit_storage import artifact_exists

    candidate = settings.project_root / text
    if artifact_exists(candidate):
        return candidate

    basename = Path(text).name
    if basename:
        exact_matches = sorted(
            glob_artifacts(settings.runs_dir, f"*/{basename}"),
            key=lambda path: str(path),
            reverse=True,
        )
        if exact_matches:
            return exact_matches[0]

    for pattern in (fallback_glob_patterns or []):
        matches = [
            path
            for path in sorted(glob_artifacts(settings.runs_dir, f"*/{pattern}"), key=lambda path: str(path), reverse=True)
            if artifact_is_file(path)
        ]
        if matches:
            return matches[0]
    return None


def _read_tracker_workbook_sheet(
    file_path: Path,
    *,
    sheet_name: str,
    usecols: list[str] | None = None,
) -> pd.DataFrame:
    last_error: Exception | None = None
    for header in (2, 0):
        try:
            return _read_excel_cached(
                file_path,
                sheet_name=sheet_name,
                usecols=usecols,
                header=header,
            )
        except ValueError as exc:
            last_error = exc
            continue
    if last_error is not None:
        raise last_error
    raise ValueError(f"{sheet_name} sheet 读取失败")


def _workflow_status_summary() -> dict:
    state = manager.status_summary()
    payload = {
        "status": state.get("status"),
        "phase": state.get("phase"),
        "run_id": state.get("run_id"),
        "started_at": state.get("started_at"),
        "finished_at": state.get("finished_at"),
        "error": state.get("error"),
        "logs_tail": state.get("logs_tail") or [],
    }
    return _attach_lightweight_status_source_contract(
        payload,
        source="workflow_status_summary_runtime_manager",
        payload_mode="workflow_summary",
        official_publication_truth="false_workflow_runtime_diagnostics_only",
        basis_extra={
            "workflow_status": payload.get("status"),
            "workflow_phase": payload.get("phase"),
            "logs_tail_count": len(payload.get("logs_tail") or []),
        },
        truth_note=(
            "/api/status/summary reports the workflow manager runtime summary only. It must not "
            "synthesize fund business values or treat workflow runtime state as publication truth."
        ),
    )


def _workflow_header_summary() -> dict:
    state = manager.status_summary()
    return {
        "status": state.get("status"),
        "phase": state.get("phase"),
        "run_id": state.get("run_id"),
        "started_at": state.get("started_at"),
        "finished_at": state.get("finished_at"),
        "error": state.get("error"),
    }


def _format_quality_tier(raw_value: object) -> str:
    text = str(raw_value or "").strip()
    mapping = {
        "高信心": "核心通过",
        "中信心": "审慎通过",
        "边界观察": "边界观察",
    }
    return mapping.get(text, text or "未标注")


def _build_quality_tier_note(raw_value: object, holding_status: object = None) -> str:
    text = str(raw_value or "").strip()
    holding_text = str(holding_status or "").strip()
    if text == "高信心":
        return "多项关键指标完整，当前可按正式主榜口径直接跟踪。"
    if text == "中信心":
        if holding_text == "名称未识别持有期限制":
            return "已进入正式主榜，但仍需继续复核持有期限制等尾部信息。"
        return "正式主榜通过，但部分辅助项或边际指标仍建议持续复核。"
    if text == "边界观察":
        return "质量边际接近观察线，建议仅作边界跟踪。"
    return ""


def _build_coverage_layers_from_snapshots(
    bond_universe_snapshot: dict | None,
    long_term_snapshot: dict | None,
    counts: dict | None,
) -> dict[str, Any]:
    universe = dict(bond_universe_snapshot or {})
    long_term = dict(long_term_snapshot or {})
    count_payload = dict(counts or {})

    stored_universe_count = int(universe.get("stored_universe_count") or count_payload.get("stored_universe_count") or 0)
    active_universe_count = int(universe.get("active_universe_count") or count_payload.get("active_universe_count") or 0)
    screenable_universe_count = int(universe.get("screenable_universe_count") or count_payload.get("screenable_universe_count") or 0)
    stage_b_candidate_pool_count = int(
        count_payload.get("stage_b_candidate_pool_count")
        or count_payload.get("publishable_pool_count")
        or 0
    )

    stored_has_latest_nav_count = int(universe.get("stored_has_latest_nav_count") or universe.get("has_latest_nav_count") or 0)
    stored_has_1y_risk_metrics_count = int(
        universe.get("stored_has_1y_risk_metrics_count") or universe.get("has_1y_risk_metrics_count") or 0
    )
    stored_holding_period_blocked_count = int(
        universe.get("stored_holding_period_blocked_count") or universe.get("holding_period_blocked_count") or 0
    )
    active_has_latest_nav_count = int(universe.get("has_latest_nav_count") or 0)
    active_has_1y_risk_metrics_count = int(universe.get("has_1y_risk_metrics_count") or 0)
    active_holding_period_blocked_count = int(universe.get("holding_period_blocked_count") or 0)
    annual_return_history_fund_count = int(long_term.get("annual_return_history_fund_count") or count_payload.get("annual_return_history_fund_count") or 0)
    rank_history_fund_count = int(long_term.get("rank_history_fund_count") or count_payload.get("rank_history_fund_count") or 0)
    full_feature_ready_fund_count = int(long_term.get("full_feature_ready_fund_count") or count_payload.get("full_feature_ready_fund_count") or 0)

    def _ratio(numerator: int, denominator: int) -> float:
        if denominator <= 0:
            return 0.0
        return round(numerator / denominator, 4)

    stage_b_scope_note = (
        "项目治理候选池沿项目内部治理/对账口径构建，不等同于正式可筛选宇宙；"
        "该口径允许保留部分用于后续治理、观察和差异审计的候选，因此数量可能高于正式可筛选宇宙。"
    )

    return {
        "definitions": {
            "table_coverage": "全库存储债基口径，用于判断基金全集和主表字段是否收全。",
            "bond_universe_coverage": "活跃债基正式宇宙口径，用于判断净值、风险和持有期拦截的主链就绪度。",
            "screenable_universe_coverage": "已通过最新净值、1年风险与持有期硬门槛的正式可筛选宇宙口径。",
            "stage_b_candidate_coverage": stage_b_scope_note,
            "publishable_universe_coverage": "兼容旧字段：等同于 stage_b_candidate_coverage，表示项目治理候选池，不代表已通过最终正式发布。",
        },
        "table_coverage": {
            "base_fund_count": stored_universe_count,
            "has_latest_nav_count": stored_has_latest_nav_count,
            "has_1y_risk_metrics_count": stored_has_1y_risk_metrics_count,
            "holding_period_blocked_count": stored_holding_period_blocked_count,
            "has_latest_nav_ratio": _ratio(stored_has_latest_nav_count, stored_universe_count),
            "has_1y_risk_metrics_ratio": _ratio(stored_has_1y_risk_metrics_count, stored_universe_count),
        },
        "bond_universe_coverage": {
            "base_fund_count": active_universe_count,
            "has_latest_nav_count": active_has_latest_nav_count,
            "has_1y_risk_metrics_count": active_has_1y_risk_metrics_count,
            "holding_period_blocked_count": active_holding_period_blocked_count,
            "screenable_universe_count": screenable_universe_count,
            "has_latest_nav_ratio": _ratio(active_has_latest_nav_count, active_universe_count),
            "has_1y_risk_metrics_ratio": _ratio(active_has_1y_risk_metrics_count, active_universe_count),
            "screenable_universe_ratio": _ratio(screenable_universe_count, active_universe_count),
        },
        "screenable_universe_coverage": {
            "base_fund_count": screenable_universe_count,
            "annual_return_history_fund_count": annual_return_history_fund_count,
            "rank_history_fund_count": rank_history_fund_count,
            "full_feature_ready_fund_count": full_feature_ready_fund_count,
            "annual_return_history_ratio": _ratio(annual_return_history_fund_count, screenable_universe_count),
            "rank_history_ratio": _ratio(rank_history_fund_count, screenable_universe_count),
            "full_feature_ready_ratio": _ratio(full_feature_ready_fund_count, screenable_universe_count),
        },
        "stage_b_candidate_coverage": {
            "base_fund_count": stage_b_candidate_pool_count,
            "stage_b_candidate_pool_count": stage_b_candidate_pool_count,
            "governance_candidate_pool_count": stage_b_candidate_pool_count,
            "publishable_pool_count": stage_b_candidate_pool_count,
            "ratio_vs_screenable_universe": _ratio(stage_b_candidate_pool_count, screenable_universe_count),
            "ratio_vs_active_bond_universe": _ratio(stage_b_candidate_pool_count, active_universe_count),
            "is_broader_than_screenable_universe": stage_b_candidate_pool_count > screenable_universe_count,
            "scope_note": stage_b_scope_note,
        },
        "publishable_universe_coverage": {
            "base_fund_count": stage_b_candidate_pool_count,
            "stage_b_candidate_pool_count": stage_b_candidate_pool_count,
            "governance_candidate_pool_count": stage_b_candidate_pool_count,
            "publishable_pool_count": stage_b_candidate_pool_count,
            "ratio_vs_screenable_universe": _ratio(stage_b_candidate_pool_count, screenable_universe_count),
            "ratio_vs_active_bond_universe": _ratio(stage_b_candidate_pool_count, active_universe_count),
            "is_broader_than_screenable_universe": stage_b_candidate_pool_count > screenable_universe_count,
            "scope_note": stage_b_scope_note,
        },
    }


def _normalize_coverage_layers(
    coverage_layers: dict | None,
    *,
    counts: dict | None = None,
) -> dict[str, Any]:
    normalized = dict(coverage_layers or {})
    count_payload = dict(counts or {})
    definitions = dict(normalized.get("definitions") or {})
    stage_b = dict(normalized.get("stage_b_candidate_coverage") or {})
    publishable = dict(normalized.get("publishable_universe_coverage") or {})

    stage_b_candidate_pool_count = int(
        stage_b.get("governance_candidate_pool_count")
        or stage_b.get("stage_b_candidate_pool_count")
        or stage_b.get("publishable_pool_count")
        or publishable.get("governance_candidate_pool_count")
        or publishable.get("stage_b_candidate_pool_count")
        or publishable.get("publishable_pool_count")
        or count_payload.get("stage_b_candidate_pool_count")
        or count_payload.get("publishable_pool_count")
        or 0
    )
    screenable_universe_count = int(
        (normalized.get("bond_universe_coverage") or {}).get("screenable_universe_count")
        or count_payload.get("screenable_universe_count")
        or 0
    )
    stage_b_scope_note = (
        "项目治理候选池沿项目内部治理/对账口径构建，不等同于正式可筛选宇宙；"
        "该口径允许保留部分用于后续治理、观察和差异审计的候选，因此数量可能高于正式可筛选宇宙。"
    )

    definitions.setdefault("stage_b_candidate_coverage", stage_b_scope_note)
    definitions.setdefault(
        "publishable_universe_coverage",
        "兼容旧字段：等同于 stage_b_candidate_coverage，表示项目治理候选池，不代表已通过最终正式发布。",
    )
    normalized["definitions"] = definitions

    if stage_b_candidate_pool_count > 0:
        stage_b.setdefault("base_fund_count", stage_b_candidate_pool_count)
        stage_b.setdefault("stage_b_candidate_pool_count", stage_b_candidate_pool_count)
        stage_b.setdefault("governance_candidate_pool_count", stage_b_candidate_pool_count)
        stage_b.setdefault("publishable_pool_count", stage_b_candidate_pool_count)
        stage_b.setdefault(
            "is_broader_than_screenable_universe",
            stage_b_candidate_pool_count > screenable_universe_count if screenable_universe_count > 0 else False,
        )
        stage_b.setdefault("scope_note", stage_b_scope_note)

        publishable.setdefault("base_fund_count", stage_b_candidate_pool_count)
        publishable.setdefault("stage_b_candidate_pool_count", stage_b_candidate_pool_count)
        publishable.setdefault("governance_candidate_pool_count", stage_b_candidate_pool_count)
        publishable.setdefault("publishable_pool_count", stage_b_candidate_pool_count)
        publishable.setdefault(
            "is_broader_than_screenable_universe",
            stage_b_candidate_pool_count > screenable_universe_count if screenable_universe_count > 0 else False,
        )
        publishable.setdefault("scope_note", stage_b_scope_note)

    if stage_b:
        normalized["stage_b_candidate_coverage"] = stage_b
    if publishable:
        normalized["publishable_universe_coverage"] = publishable
    return normalized


def _build_publish_funnel_summary(payload: dict, coverage_layers: dict[str, Any] | None = None) -> dict[str, Any]:
    counts = payload.get("counts") or {}
    publish_summary = payload.get("project_publish_summary") or {}
    threshold_comparison = publish_summary.get("threshold_comparison") or {}
    layers = coverage_layers or {}
    bond_universe_coverage = layers.get("bond_universe_coverage") or {}
    stage_b_coverage = layers.get("stage_b_candidate_coverage") or layers.get("publishable_universe_coverage") or {}

    screenable_universe_count = int(
        bond_universe_coverage.get("screenable_universe_count")
        or counts.get("screenable_universe_count")
        or 0
    )
    governance_candidate_pool_count = int(
        stage_b_coverage.get("governance_candidate_pool_count")
        or stage_b_coverage.get("stage_b_candidate_pool_count")
        or stage_b_coverage.get("publishable_pool_count")
        or counts.get("stage_b_candidate_pool_count")
        or counts.get("publishable_pool_count")
        or 0
    )
    cross_validated_candidate_count = int(
        counts.get("autostock_passed_count")
        or counts.get("autostock_validated_count")
        or 0
    )
    autostock_filtered_count = int(counts.get("autostock_filtered_count") or 0)
    official_publish_count = int(counts.get("final_count") or 0)
    fixed_threshold_baseline_count = int(
        threshold_comparison.get("fixed_baseline_count")
        or threshold_comparison.get("fixed_official_count")
        or 0
    )
    fixed_threshold_post_review_count = int(threshold_comparison.get("fixed_post_review_count") or 0)
    dynamic_threshold_baseline_count = int(
        threshold_comparison.get("dynamic_baseline_count")
        or counts.get("dynamic_final_count")
        or threshold_comparison.get("dynamic_official_count")
        or 0
    )
    dynamic_reference_export_count = int(
        threshold_comparison.get("dynamic_reference_export_count")
        or counts.get("dynamic_final_count")
        or 0
    )
    dynamic_threshold_post_review_count = int(
        threshold_comparison.get("dynamic_post_review_count")
        or dynamic_threshold_baseline_count
        or 0
    )
    dynamic_baseline_hard_block_removed_count = max(
        dynamic_threshold_baseline_count - dynamic_reference_export_count,
        0,
    )
    dynamic_reference_count = dynamic_reference_export_count

    governance_scope_note = str(
        stage_b_coverage.get("scope_note")
        or "项目治理候选池用于治理、观察和差异审计，不等同于正式可筛选宇宙。"
    )

    return {
        "screenable_universe_count": screenable_universe_count,
        "governance_candidate_pool_count": governance_candidate_pool_count,
        "cross_validated_candidate_count": cross_validated_candidate_count,
        "autostock_filtered_count": autostock_filtered_count,
        "official_publish_count": official_publish_count,
        "dynamic_reference_count": dynamic_reference_count,
        "dynamic_reference_export_count": dynamic_reference_export_count,
        "raw_dynamic_baseline_pass_count": dynamic_threshold_baseline_count,
        "dynamic_baseline_after_hard_block_count": dynamic_reference_export_count,
        "dynamic_baseline_hard_block_removed_count": dynamic_baseline_hard_block_removed_count,
        "fixed_threshold_baseline_count": fixed_threshold_baseline_count,
        "fixed_threshold_post_review_count": fixed_threshold_post_review_count,
        "dynamic_threshold_baseline_count": dynamic_threshold_baseline_count,
        "dynamic_threshold_post_review_count": dynamic_threshold_post_review_count,
        "fixed_threshold_official_count": fixed_threshold_baseline_count,
        "dynamic_threshold_official_count": dynamic_threshold_baseline_count,
        "threshold_overlap_count": int(threshold_comparison.get("overlap_count") or 0),
        "threshold_fixed_only_count": int(threshold_comparison.get("fixed_only_count") or 0),
        "threshold_dynamic_only_count": int(threshold_comparison.get("dynamic_only_count") or 0),
        "final_selection_removed_count": int(threshold_comparison.get("final_selection_removed_count") or 0),
        "risk_metric_cap_audit": threshold_comparison.get("risk_metric_cap_audit") or {},
        "governance_pool_is_broader_than_screenable": governance_candidate_pool_count > screenable_universe_count,
        "governance_pool_scope_note": governance_scope_note,
    }


def _scheduler_snapshot_signature(snapshot: dict | None) -> tuple[str, str, int, str]:
    return scheduler_diagnostics.scheduler_snapshot_signature(snapshot)


def _scheduler_snapshot_has_payload(snapshot: dict | None) -> bool:
    return scheduler_diagnostics.scheduler_snapshot_has_payload(snapshot)


def _scheduler_snapshot_changed(before_snapshot: dict | None, after_snapshot: dict | None) -> bool:
    return scheduler_diagnostics.scheduler_snapshot_changed(before_snapshot, after_snapshot)


def _build_scheduler_maintenance_change_items(change_summary: dict | None) -> list[dict[str, Any]]:
    return scheduler_diagnostics.build_scheduler_maintenance_change_items(change_summary)


def _build_scheduler_child_task_digest(child_tasks: dict | None) -> dict[str, Any]:
    return scheduler_diagnostics.build_scheduler_child_task_digest(child_tasks)


def _build_scheduler_task_closure_digest(scheduler_overview: dict | None) -> dict[str, Any]:
    return scheduler_diagnostics.build_scheduler_task_closure_digest(
        scheduler_overview,
        resolve_run_latest_screened_snapshot_fn=_resolve_run_latest_screened_snapshot,
        scheduler_snapshot_has_payload_fn=_scheduler_snapshot_has_payload,
        scheduler_snapshot_changed_fn=_scheduler_snapshot_changed,
    )


def _parse_local_datetime(value: Any) -> datetime | None:
    return scheduler_diagnostics.parse_local_datetime(value)


def _resolve_current_canonical_run_context() -> dict[str, Any]:
    latest_payload = _load_latest_report_cached() or _load_latest_report_snapshot_cached() or {}
    publication = _load_publication_registry_cached() or _load_publication_registry_snapshot_cached() or {}
    latest_successful_run = ((publication.get("automation_layers") or {}).get("latest_successful_run") or {})
    effective_nav_snapshot = (latest_payload.get("database_freshness") or {}).get("effective_nav_snapshot") or {}
    latest_successful_snapshot = latest_successful_run.get("snapshot") or {}

    run_id = (
        str(latest_payload.get("run_id") or "").strip()
        or str(publication.get("latest_canonical_run_id") or "").strip()
        or str(latest_successful_run.get("run_id") or "").strip()
        or None
    )
    generated_at = (
        str(latest_payload.get("generated_at") or "").strip()
        or str(latest_successful_run.get("generated_at") or "").strip()
        or None
    )
    candidate_date = (
        str(effective_nav_snapshot.get("candidate_date") or "").strip()
        or str(latest_successful_snapshot.get("candidate_date") or "").strip()
        or None
    )
    effective_risk_end_date = (
        str(effective_nav_snapshot.get("risk_1y_effective_end_date") or "").strip()
        or str(latest_successful_snapshot.get("risk_1y_effective_end_date") or "").strip()
        or None
    )
    return {
        "run_id": run_id,
        "latest_canonical_run_id": (
            str(publication.get("latest_canonical_run_id") or "").strip()
            or str(latest_successful_run.get("run_id") or "").strip()
            or run_id
            or None
        ),
        "generated_at": generated_at,
        "candidate_date": candidate_date,
        "effective_risk_end_date": effective_risk_end_date,
    }


def _read_machine_boot_time() -> datetime | None:
    return scheduler_diagnostics.read_machine_boot_time()


def _resolve_task_expected_run_time(task_key: str, started_at: Any) -> datetime | None:
    return scheduler_diagnostics.resolve_task_expected_run_time(
        task_key,
        started_at,
        settings=settings,
    )


def _build_task_schedule_health(task_key: str, run: dict[str, Any], scheduler_started_at: Any) -> dict[str, Any]:
    return scheduler_diagnostics.build_task_schedule_health(
        task_key,
        run,
        scheduler_started_at,
        settings=settings,
        parse_local_datetime_fn=_parse_local_datetime,
        read_machine_boot_time_fn=_read_machine_boot_time,
        resolve_task_expected_run_time_fn=_resolve_task_expected_run_time,
    )


def _resolve_run_latest_screened_snapshot(
    run: dict[str, Any] | None,
    closure: dict[str, Any] | None,
    overview_latest_screened_snapshot: dict[str, Any] | None,
) -> dict[str, Any]:
    return scheduler_diagnostics.resolve_run_latest_screened_snapshot(
        run,
        closure,
        overview_latest_screened_snapshot,
        scheduler_snapshot_has_payload_fn=_scheduler_snapshot_has_payload,
    )


def _build_schedule_stability_summary(scheduler_overview: dict | None) -> dict[str, Any]:
    return scheduler_diagnostics.build_schedule_stability_summary(
        scheduler_overview,
        build_task_schedule_health_fn=_build_task_schedule_health,
    )


def _build_scheduler_overview_summary(scheduler_state: dict | None) -> dict:
    overview = scheduler_diagnostics.build_scheduler_overview_summary(
        scheduler_state,
        settings=settings,
        project_scheduler_cls=ProjectScheduler,
        select_task_closure_source_fn=_select_task_closure_source,
        build_task_schedule_health_fn=_build_task_schedule_health,
        resolve_run_latest_screened_snapshot_fn=_resolve_run_latest_screened_snapshot,
        build_schedule_stability_summary_fn=_build_schedule_stability_summary,
        build_scheduler_task_closure_digest_fn=_build_scheduler_task_closure_digest,
    )
    overview["recent_failure_recovery_summary"] = scheduler_diagnostics.build_recent_scheduler_failure_recovery_summary(
        settings,
    )
    overview["enabled"] = _resolve_scheduler_enabled_state(scheduler_state or overview)
    return overview


def _scheduler_schedule_stability_digest_incomplete(
    schedule_stability_summary: dict[str, Any] | None,
) -> bool:
    payload = dict(schedule_stability_summary or {})
    summary = payload.get("summary") or {}
    return bool(
        not summary
        or not (payload.get("tasks") or [])
        or "late_slot_total" not in summary
    )


def _scheduler_automation_closure_digest_incomplete(
    automation_closure_digest: dict[str, Any] | None,
) -> bool:
    payload = dict(automation_closure_digest or {})
    summary = payload.get("summary") or {}
    return bool(
        not summary
        or (
            not (payload.get("alerts") or [])
            and not (payload.get("ordered_tasks") or [])
        )
    )


def _resolve_lightweight_scheduler_runtime_digest_sources(
    scheduler_state: dict | None,
    *,
    schedule_stability_summary: dict[str, Any] | None,
    automation_closure_digest: dict[str, Any] | None,
    allow_runtime_overview_fallback: bool = True,
) -> tuple[dict[str, Any], dict[str, Any]]:
    schedule_stability_summary_source = dict(schedule_stability_summary or {})
    automation_closure_digest_source = dict(automation_closure_digest or {})
    schedule_digest_incomplete = _scheduler_schedule_stability_digest_incomplete(
        schedule_stability_summary_source
    )
    automation_digest_incomplete = _scheduler_automation_closure_digest_incomplete(
        automation_closure_digest_source
    )
    need_runtime_overview_fallback = bool(
        schedule_digest_incomplete
        or automation_digest_incomplete
    )
    if not need_runtime_overview_fallback:
        return schedule_stability_summary_source, automation_closure_digest_source
    if not allow_runtime_overview_fallback:
        return schedule_stability_summary_source, automation_closure_digest_source

    scheduler_overview = _build_scheduler_overview_summary(scheduler_state)
    if schedule_digest_incomplete:
        schedule_stability_summary_source = dict(
            scheduler_overview.get("schedule_stability_summary") or schedule_stability_summary_source
        )
    if automation_digest_incomplete:
        automation_closure_digest_source = dict(
            scheduler_overview.get("automation_closure_digest") or automation_closure_digest_source
        )
    return schedule_stability_summary_source, automation_closure_digest_source


def _build_dashboard_scheduler_lightweight_overview(
    scheduler_state: dict | None,
    *,
    publication: dict[str, Any] | None = None,
    include_recent_failure_recovery: bool = True,
    allow_runtime_digest_fallback: bool = True,
) -> dict[str, Any]:
    state = dict(scheduler_state or {})
    last_runs = dict(state.get("last_runs") or {})
    scheduler_enabled = _resolve_scheduler_enabled_state(state)
    overview = {
        "enabled": scheduler_enabled,
        "scheduler_running": bool(state.get("scheduler_running")),
        "automation_host_mode": state.get("automation_host_mode"),
        "host_context": state.get("host_context"),
        "configured_host_context": state.get("configured_host_context"),
        "host_context_mismatch": bool(state.get("host_context_mismatch")),
        "scheduler_service_runtime": state.get("scheduler_service_runtime") or {},
        "web_service_runtime": state.get("web_service_runtime") or {},
        "started_at": state.get("started_at"),
        "stopped_at": state.get("stopped_at"),
        "last_heartbeat_at": state.get("last_heartbeat_at"),
        "last_error": state.get("last_error"),
        "screening_backlog_due": bool(state.get("screening_backlog_due")),
        "screening_backlog_reason": state.get("screening_backlog_reason"),
        "startup_catch_up_pending": bool(state.get("startup_catch_up_pending")),
        "startup_catch_up_due_jobs": state.get("startup_catch_up_due_jobs") or [],
        "failed_screening_retry_cooldown": state.get(
            "failed_screening_retry_cooldown"
        )
        or {},
        "configured_job_ids": state.get("configured_job_ids") or [],
        "missing_job_ids": state.get("missing_job_ids") or [],
        "unexpected_job_ids": state.get("unexpected_job_ids") or [],
        "job_drift_detected": bool(state.get("job_drift_detected")),
        "effective_nav_snapshot": state.get("effective_nav_snapshot") or {},
        "latest_screened_snapshot": state.get("latest_screened_snapshot") or {},
        "nav_freshness": state.get("nav_freshness") or {},
        "current_task": state.get("current_task") or {},
        "manual_run_history": state.get("manual_run_history") or [],
        "pending_manual_run_requests": state.get("pending_manual_run_requests") or [],
        "task_closures": state.get("task_closures") or {},
        "child_task_idle_streaks": state.get("child_task_idle_streaks") or {},
        "schedule_stability_summary": state.get("schedule_stability_summary") or {"summary": {}, "tasks": []},
        "automation_closure_digest": state.get("automation_closure_digest")
        or {"summary": {}, "alerts": [], "ordered_tasks": []},
        "recent_failure_recovery_summary": (
            scheduler_diagnostics.build_recent_scheduler_failure_recovery_summary(settings)
            if include_recent_failure_recovery
            else state.get("recent_failure_recovery_summary") or {}
        ),
        "maintenance_summary": state.get("maintenance_summary") or {},
        "daily_run": _summarize_scheduler_run_for_api(last_runs.get("scheduled_daily_bond_refresh") or {}),
        "intraday_run": _summarize_scheduler_run_for_api(last_runs.get("scheduled_intraday_bond_refresh") or {}),
        "weekly_run": _summarize_scheduler_run_for_api(last_runs.get("scheduled_weekly_bond_maintenance") or {}),
        "snapshot_watchdog_run": _summarize_scheduler_run_for_api(
            last_runs.get("scheduled_snapshot_watchdog_refresh") or {}
        ),
    }
    (
        overview["schedule_stability_summary"],
        overview["automation_closure_digest"],
    ) = _resolve_lightweight_scheduler_runtime_digest_sources(
        scheduler_state,
        schedule_stability_summary=overview.get("schedule_stability_summary") or {},
        automation_closure_digest=overview.get("automation_closure_digest") or {},
        allow_runtime_overview_fallback=allow_runtime_digest_fallback,
    )
    if publication is not None:
        overview["publication_layers"] = dict(publication.get("automation_layers") or {})
    return overview


def _build_scheduler_chain_audit(scheduler_overview: dict | None) -> dict:
    return scheduler_diagnostics.build_scheduler_chain_audit(scheduler_overview)


def _build_automation_autonomy_summary(
    scheduler_overview: dict | None,
    scheduler_chain_audit: dict | None,
    *,
    manual_operations_enabled: bool,
    load_database_overview_fn=None,
) -> dict[str, Any]:
    if load_database_overview_fn is None:
        load_database_overview_fn = _load_database_overview
    from backend.weekly_warning_correction import apply_historical_warning_correction

    overview = dict(scheduler_overview or {})
    if overview.get("weekly_run"):
        overview["weekly_run"] = apply_historical_warning_correction(settings, overview["weekly_run"])
    return scheduler_diagnostics.build_automation_autonomy_summary(
        overview,
        scheduler_chain_audit,
        manual_operations_enabled=manual_operations_enabled,
        load_database_overview_fn=load_database_overview_fn,
        autostock_observation_enabled=(
            settings.autostock_enabled and settings.publication_validation_provider == "autostock"
        ),
    )


def _resolve_scheduler_enabled_state(state: dict[str, Any] | None) -> bool:
    payload = dict(state or {})
    if payload.get("enabled") is not None:
        return bool(payload.get("enabled"))
    return bool(getattr(_runtime_scheduler_settings(), "scheduler_enabled", False))


def _build_dashboard_overview_response(payload: dict, publication: dict, scheduler_state: dict) -> dict:
    return dashboard_payloads.build_dashboard_overview_response(
        payload,
        publication,
        scheduler_state,
        settings=settings,
        manual_operation_mode=MANUAL_OPERATION_MODE,
        normalize_portfolio_groups=_normalize_portfolio_groups,
        build_portfolio_group_brief=_build_portfolio_group_brief,
        build_database_source_audit=build_database_source_audit,
        load_database_overview=_load_database_overview,
        load_database_integrity_summary=_load_database_integrity_summary,
        load_nav_lag_summary=_load_nav_lag_summary,
        load_risk_lag_summary=lambda: _load_default_risk_lag_summary(payload),
        load_risk_sync_summary=_load_risk_sync_summary,
        load_risk_coverage_gap_summary=_load_risk_coverage_gap_summary,
        load_bond_universe_snapshot=_load_bond_universe_snapshot,
        load_exclusion_reason_summary=_load_exclusion_reason_summary,
        normalize_coverage_layers=_normalize_coverage_layers,
        build_coverage_layers_from_snapshots=_build_coverage_layers_from_snapshots,
        build_publish_funnel_summary=_build_publish_funnel_summary,
        build_scheduler_overview_summary=_build_scheduler_overview_summary,
        build_scheduler_overview_api_summary=_build_scheduler_overview_api_summary,
        build_scheduler_chain_audit=_build_scheduler_chain_audit,
        build_automation_autonomy_summary=_build_automation_autonomy_summary,
    )


def _build_dashboard_overview_cached() -> dict[str, Any]:
    cache_basis = _build_dashboard_overview_cache_basis()

    def _build() -> dict[str, Any]:
        sidecar_payload = _load_dashboard_overview_sidecar(cache_basis)
        if sidecar_payload is not None:
            return sidecar_payload
        response = (
            lambda payload, publication, scheduler_state: _build_dashboard_overview_response(
                payload,
                publication,
                scheduler_state,
            )
        )(
            _load_current_published_report_cached() or _load_latest_report_cached() or _load_latest_report_snapshot_cached() or {},
            _load_publication_registry_cached() or _load_publication_registry_snapshot_cached(),
            _load_scheduler_state_snapshot_cached(),
        )
        _write_dashboard_overview_sidecar(cache_basis, response)
        return response

    response = _cached_payload(
        _dashboard_overview_cache_key(cache_basis),
        _DASHBOARD_OVERVIEW_CACHE_TTL_SECONDS,
        _build,
    )
    refreshed = _refresh_dashboard_overview_runtime_fields(response)
    return _attach_dashboard_overview_source_contract(refreshed, cache_basis)


def _test_overridden_callable(name: str, original: Callable[..., Any] | None) -> Callable[..., Any] | None:
    if "pytest" not in sys.modules and not os.environ.get("PYTEST_CURRENT_TEST"):
        return None
    current = getattr(sys.modules[__name__], name, None)
    if callable(current) and current is not original:
        return current
    return None


def _load_dashboard_payload_for_pytest() -> dict[str, Any]:
    for loader_name, original in (
        ("_load_current_published_report_cached", _ORIGINAL_LOAD_CURRENT_PUBLISHED_REPORT_CACHED),
        ("_load_latest_report_snapshot_cached", _ORIGINAL_LOAD_LATEST_REPORT_SNAPSHOT_CACHED),
        ("_load_latest_report_cached", _ORIGINAL_LOAD_LATEST_REPORT_CACHED),
    ):
        loader = _test_overridden_callable(loader_name, original)
        if loader is not None:
            return loader() or {}
    return (
        manager.latest_report()
        or _load_latest_report_snapshot_cached()
        or _load_current_published_report_cached()
        or _load_latest_report_cached()
        or {}
    )


def _load_dashboard_detail_payload_for_pytest() -> dict[str, Any]:
    for loader_name, original in (
        ("_load_current_published_report_cached", _ORIGINAL_LOAD_CURRENT_PUBLISHED_REPORT_CACHED),
        ("_load_latest_report_snapshot_cached", _ORIGINAL_LOAD_LATEST_REPORT_SNAPSHOT_CACHED),
        ("_load_latest_report_cached", _ORIGINAL_LOAD_LATEST_REPORT_CACHED),
    ):
        loader = _test_overridden_callable(loader_name, original)
        if loader is not None:
            return loader() or {}
    return (
        manager.latest_report()
        or _load_current_published_report_cached()
        or _load_latest_report_cached()
        or _load_latest_report_snapshot_cached()
        or {}
    )


def _load_dashboard_publication_for_pytest() -> dict[str, Any]:
    for loader_name, original in (
        ("_load_publication_registry_cached", _ORIGINAL_LOAD_PUBLICATION_REGISTRY_CACHED),
        ("_load_publication_registry_snapshot_cached", _ORIGINAL_LOAD_PUBLICATION_REGISTRY_SNAPSHOT_CACHED),
    ):
        loader = _test_overridden_callable(loader_name, original)
        if loader is not None:
            return loader() or {}
    return (
        load_publication_registry(settings)
        or _load_publication_registry_cached()
        or _load_publication_registry_snapshot_cached()
        or {}
    )


def _load_dashboard_detail_publication_for_pytest() -> dict[str, Any]:
    for loader_name, original in (
        ("_load_publication_registry_cached", _ORIGINAL_LOAD_PUBLICATION_REGISTRY_CACHED),
        ("_load_publication_registry_snapshot_cached", _ORIGINAL_LOAD_PUBLICATION_REGISTRY_SNAPSHOT_CACHED),
    ):
        loader = _test_overridden_callable(loader_name, original)
        if loader is not None:
            return loader() or {}
    return (
        load_publication_registry(settings)
        or _load_publication_registry_cached()
        or _load_publication_registry_snapshot_cached()
        or {}
    )


def _pytest_prefers_scoped_detail_cache() -> bool:
    return _test_overridden_callable(
        "_build_latest_dashboard_scoped_detail_cached",
        _ORIGINAL_BUILD_LATEST_DASHBOARD_SCOPED_DETAIL_CACHED,
    ) is not None


def _load_dashboard_overview_payload_for_pytest() -> dict[str, Any]:
    return manager.latest_report() or {}


def _load_dashboard_overview_publication_for_pytest() -> dict[str, Any]:
    return load_publication_registry(settings) or {}


def _load_recommendations_payload_for_pytest() -> dict[str, Any]:
    loader = _test_overridden_callable(
        "_load_current_published_report_cached",
        _ORIGINAL_LOAD_CURRENT_PUBLISHED_REPORT_CACHED,
    )
    if loader is not None:
        return loader() or {}
    return (
        _load_latest_report_snapshot_cached()
        or _load_latest_report_cached()
        or manager.latest_report()
        or {}
    )


def _load_tracker_dataset(payload: dict) -> dict:
    return recommendation_payloads.load_tracker_dataset(
        payload,
        settings=settings,
        read_excel_cached=_read_excel_cached,
        read_tracker_workbook_sheet=_read_tracker_workbook_sheet,
    )


def _load_tracker_occurrence_map(payload: dict) -> dict[str, dict[str, str]]:
    return recommendation_payloads.load_tracker_occurrence_map(
        payload,
        settings=settings,
        normalize_code_text=_normalize_code_text,
        read_tracker_workbook_sheet=_read_tracker_workbook_sheet,
    )


def _load_fund_data_source_map(codes: list[str]) -> dict[str, str]:
    normalized_codes: list[str] = []
    seen: set[str] = set()
    for code in codes:
        normalized = _normalize_code_text(code)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        normalized_codes.append(normalized)
    if not normalized_codes or not settings.database_path.exists():
        return {}

    conn = sqlite3.connect(str(settings.database_path))
    try:
        fund_columns = {
            str(row[1]).strip()
            for row in conn.execute("PRAGMA table_info(funds)").fetchall()
            if len(row) > 1 and str(row[1]).strip()
        }
        data_source_select = (
            "COALESCE(data_source, '')"
            if "data_source" in fund_columns
            else "''"
        )
        placeholders = ",".join("?" for _ in normalized_codes)
        query = f"""
            SELECT fcode, {data_source_select}
            FROM funds
            WHERE fcode IN ({placeholders})
        """
        rows = conn.execute(query, normalized_codes).fetchall()
    finally:
        conn.close()

    result: dict[str, str] = {}
    for row in rows:
        code = _normalize_code_text(row[0])
        if not code:
            continue
        result[code] = str(row[1] or "").strip()
    return result


def _tracker_current_detail_sidecar_path_from_scope(payload_scope: dict[str, Any] | None) -> Path | None:
    if not isinstance(payload_scope, dict):
        return None
    sidecar_file = str(payload_scope.get("sidecar_file") or "").strip()
    if not sidecar_file:
        return None
    return _resolve_existing_report_artifact_path(sidecar_file)


def _tracker_current_derived_rows_signature(rows: list[dict[str, Any]]) -> str:
    row_basis: list[dict[str, Any]] = []
    for item in list(rows or []):
        row = dict(item or {})
        row_basis.append(
            {
                "code": _normalize_code_text(row.get("基金代码")),
                "group": str(row.get("正式榜分组") or "").strip(),
                "style": str(row.get("债基风格标签") or "").strip(),
                "data_source": str(row.get("数据来源") or "").strip(),
                "metrics": {
                    str(spec["field"]): str(row.get(str(spec["field"])) or "").strip()
                    for spec in _STYLE_METRIC_REFERENCE_SPECS
                },
            }
        )
    return _stable_json_digest(row_basis)


def _build_tracker_current_derived_fields_cache_basis(
    payload: dict[str, Any],
    all_rows: list[dict[str, Any]],
    style_threshold_payload: dict[str, Any],
) -> dict[str, Any]:
    control = payload.get("publication_control") or {}
    if control.get("commit_protocol"):
        return {
            "schema_version": _TRACKER_CURRENT_DERIVED_FIELDS_SCHEMA_VERSION,
            "basis_kind": "immutable_publication_revision",
            "run_id": str(payload.get("run_id") or ""),
            "generated_at": str(payload.get("generated_at") or ""),
            "revision": control.get("revision"),
            "tracker_rows_signature": _tracker_current_derived_rows_signature(all_rows),
            "style_threshold_signature": _stable_json_digest(style_threshold_payload),
        }
    return {
        "schema_version": _TRACKER_CURRENT_DERIVED_FIELDS_SCHEMA_VERSION,
        "run_id": str(payload.get("run_id") or "").strip(),
        "generated_at": str(payload.get("generated_at") or "").strip(),
        "database_file": _sqlite_file_cache_token(settings.database_path),
        "tracker_rows_signature": _tracker_current_derived_rows_signature(all_rows),
        "style_threshold_signature": _stable_json_digest(style_threshold_payload),
    }


def _probe_tracker_current_detail_derived_fields_status(
    payload_scope: dict[str, Any] | None,
    cache_basis: dict[str, Any],
) -> dict[str, Any]:
    sidecar_path = _tracker_current_detail_sidecar_path_from_scope(payload_scope)
    status = {
        "sidecar_path": _project_relative_path_text(sidecar_path),
        "schema_version": _TRACKER_CURRENT_DERIVED_FIELDS_SCHEMA_VERSION,
        "status": "missing",
        "hit": False,
    }
    if sidecar_path is None:
        return {**status, "status": "unavailable_sidecar_path"}
    if not sidecar_path.exists():
        return status
    try:
        sidecar_payload = json.loads(sidecar_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, TypeError, ValueError) as exc:
        return {
            **status,
            "status": "read_error",
            "error_type": type(exc).__name__,
        }
    if not isinstance(sidecar_payload, dict):
        return {**status, "status": "invalid_payload_type"}
    if str(sidecar_payload.get("artifact_type") or "").strip() != "project_tracker_current_detail_sidecar":
        return {**status, "status": "artifact_type_mismatch"}
    derived_fields = sidecar_payload.get("derived_fields")
    if not isinstance(derived_fields, dict):
        return {**status, "status": "missing_derived_fields"}
    if int(derived_fields.get("schema_version") or 0) != _TRACKER_CURRENT_DERIVED_FIELDS_SCHEMA_VERSION:
        return {
            **status,
            "status": "schema_mismatch",
            "actual_schema_version": derived_fields.get("schema_version"),
        }
    if derived_fields.get("cache_basis") != cache_basis:
        return {**status, "status": "fingerprint_mismatch"}
    return {
        **status,
        "status": "hit",
        "hit": True,
        "generated_at": derived_fields.get("generated_at"),
    }


def _load_tracker_current_detail_derived_fields(
    payload_scope: dict[str, Any] | None,
    cache_basis: dict[str, Any],
) -> dict[str, Any] | None:
    sidecar_path = _tracker_current_detail_sidecar_path_from_scope(payload_scope)
    if sidecar_path is None or not sidecar_path.exists():
        return None
    sidecar_payload = _safe_json_load(sidecar_path)
    if str(sidecar_payload.get("artifact_type") or "").strip() != "project_tracker_current_detail_sidecar":
        return None
    derived_fields = sidecar_payload.get("derived_fields")
    if not isinstance(derived_fields, dict):
        return None
    if int(derived_fields.get("schema_version") or 0) != _TRACKER_CURRENT_DERIVED_FIELDS_SCHEMA_VERSION:
        return None
    if derived_fields.get("cache_basis") != cache_basis:
        return None
    return {
        "source": "run_local_tracker_current_derived_fields",
        "sidecar_path": _project_relative_path_text(sidecar_path),
        "schema_version": _TRACKER_CURRENT_DERIVED_FIELDS_SCHEMA_VERSION,
        "data_source_by_code": dict(derived_fields.get("data_source_by_code") or {}),
        "annualized_return_by_code": dict(derived_fields.get("annualized_return_by_code") or {}),
        "accum_nav_ratio_annualized_by_code": dict(derived_fields.get("accum_nav_ratio_annualized_by_code") or {}),
        "lifetime_return_observations": dict(derived_fields.get("lifetime_return_observations") or {}),
        "annual_return_breakdown_by_code": dict(derived_fields.get("annual_return_breakdown_by_code") or {}),
        "style_metric_references": dict(derived_fields.get("style_metric_references") or {}),
    }


def _write_tracker_current_detail_derived_fields(
    payload_scope: dict[str, Any] | None,
    cache_basis: dict[str, Any],
    derived_fields: dict[str, Any],
) -> Path | None:
    sidecar_path = _tracker_current_detail_sidecar_path_from_scope(payload_scope)
    if sidecar_path is None or not sidecar_path.exists():
        return None
    if (cache_basis.get("basis_kind") == "immutable_publication_revision"
            or (sidecar_path.parent / "validated.json").exists()
            or sidecar_path.parent.parent.name == "revisions"):
        return None
    sidecar_payload = _safe_json_load(sidecar_path)
    if str(sidecar_payload.get("artifact_type") or "").strip() != "project_tracker_current_detail_sidecar":
        return None
    if not isinstance(sidecar_payload.get("rows"), list):
        return None
    sidecar_payload["derived_fields"] = {
        "schema_version": _TRACKER_CURRENT_DERIVED_FIELDS_SCHEMA_VERSION,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "cache_basis": cache_basis,
        "data_source_by_code": dict(derived_fields.get("data_source_by_code") or {}),
        "annualized_return_by_code": dict(derived_fields.get("annualized_return_by_code") or {}),
        "accum_nav_ratio_annualized_by_code": dict(derived_fields.get("accum_nav_ratio_annualized_by_code") or {}),
        "lifetime_return_observations": dict(derived_fields.get("lifetime_return_observations") or {}),
        "annual_return_breakdown_by_code": dict(derived_fields.get("annual_return_breakdown_by_code") or {}),
        "style_metric_references": dict(derived_fields.get("style_metric_references") or {}),
    }
    try:
        _write_json_atomic(sidecar_path, sidecar_payload)
        return sidecar_path
    except Exception:
        return None


def _build_tracker_current_detail_derived_fields(
    payload: dict[str, Any],
    all_rows: list[dict[str, Any]],
    *,
    style_threshold_payload: dict[str, Any],
) -> dict[str, Any]:
    all_rows_by_code: dict[str, dict[str, Any]] = {}
    all_codes: list[str] = []
    for row in list(all_rows or []):
        code = _normalize_code_text((row or {}).get("基金代码"))
        if not code or code in all_rows_by_code:
            continue
        all_rows_by_code[code] = dict(row or {})
        all_codes.append(code)
    if not all_codes:
        return {
            "data_source_by_code": {},
            "annualized_return_by_code": {},
            "annual_return_breakdown_by_code": {},
            "style_metric_references": {},
        }

    run_token = str(payload.get("run_id") or payload.get("generated_at") or "unknown").strip() or "unknown"
    style_metric_benchmark_map = _load_cached_style_metric_benchmark_map(
        all_rows,
        benchmark_rows=all_rows,
        cache_namespace=f"tracker_current_derived_fields::{run_token}",
    )
    if (payload.get("publication_control") or {}).get("commit_protocol"):
        artifacts = payload.get("artifacts") or {}
        path = _resolve_existing_report_artifact_path(
            artifacts.get("project_tracker_current_detail_json")
            or artifacts.get("run_local_project_tracker_current_detail_json")
        )
        frozen = _safe_json_load(path) if path is not None else {}
        try:
            source_time_valid = (
                datetime.fromisoformat(str(frozen.get("generated_at") or ""))
                <= datetime.fromisoformat(str(payload.get("generated_at") or ""))
            )
        except (TypeError, ValueError):
            source_time_valid = False
        if (frozen.get("run_id") != payload.get("run_id")
                or not source_time_valid
                or not isinstance(frozen.get("derived_fields"), dict)):
            raise RuntimeError("committed_tracker_derived_inputs_missing_or_mismatched")
        fields = frozen["derived_fields"]
        return {
            "data_source_by_code": dict(fields.get("data_source_by_code") or {}),
            "annualized_return_by_code": (
                dict(fields.get("annualized_return_by_code") or {})
                if fields.get("schema_version") == 2
                and isinstance(fields.get("annualized_return_by_code"), dict)
                else {}
            ),
            "accum_nav_ratio_annualized_by_code": dict(fields.get("accum_nav_ratio_annualized_by_code") or {}),
            "lifetime_return_observations": dict(fields.get("lifetime_return_observations") or {}),
            "annual_return_breakdown_by_code": dict(fields.get("annual_return_breakdown_by_code") or {}),
            "style_metric_references": _build_style_metric_reference_map(
                all_rows,
                style_threshold_entries=style_threshold_payload.get("styles") or [],
                global_style_threshold_entry=style_threshold_payload.get("global") or {},
                benchmark_map=style_metric_benchmark_map,
            ),
        }
    return {
        "data_source_by_code": _load_fund_data_source_map(all_codes),
        "annualized_return_by_code": _compute_since_inception_annualized_return_map(all_codes),
        "accum_nav_ratio_annualized_by_code": _compute_accum_nav_ratio_annualized_return_map(all_codes),
        "lifetime_return_observations": load_platform_lifetime_returns(settings.database_path, all_codes),
        "annual_return_breakdown_by_code": _load_since_inception_annual_return_breakdown_map(all_codes),
        "style_metric_references": _build_style_metric_reference_map(
            all_rows,
            style_threshold_entries=style_threshold_payload.get("styles") or [],
            global_style_threshold_entry=style_threshold_payload.get("global") or {},
            benchmark_map=style_metric_benchmark_map,
        ),
    }


def _apply_tracker_current_derived_fields_to_rows(
    rows_by_code: dict[str, dict[str, Any]],
    codes: list[str],
    derived_fields: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    data_source_by_code = dict(derived_fields.get("data_source_by_code") or {})
    annualized_return_by_code = dict(derived_fields.get("annualized_return_by_code") or {})
    annual_breakdown_by_code = dict(derived_fields.get("annual_return_breakdown_by_code") or {})
    style_metric_references = dict(derived_fields.get("style_metric_references") or {})
    for code, row in rows_by_code.items():
        row.pop("成立以来年化收益率(%)", None)
        current_source = str(row.get("数据来源") or "").strip()
        if not current_source:
            row["数据来源"] = str(data_source_by_code.get(code) or "").strip()
        value = annualized_return_by_code.get(code)
        row[_SINCE_INCEPTION_ANNUALIZED_RETURN_LABEL] = "" if value is None else str(value).strip()
        row["累计净值比年化(%)"] = (derived_fields.get("accum_nav_ratio_annualized_by_code") or {}).get(code, "")
        row["成立以来收益口径"] = (derived_fields.get("lifetime_return_observations") or {}).get(code) or {"status": "legacy_definition_unknown"}
        row[_SINCE_INCEPTION_ANNUAL_RETURN_BREAKDOWN_LABEL] = annual_breakdown_by_code.get(code) or []
    return {
        code: style_metric_references.get(code) or {}
        for code in codes
        if style_metric_references.get(code)
    }


def _build_tracker_current_source_basis(
    payload: dict[str, Any],
    response_payload: dict[str, Any],
    *,
    payload_mode: str,
    audit_read_only: bool,
) -> dict[str, Any]:
    artifacts = payload.get("artifacts") if isinstance(payload.get("artifacts"), dict) else {}
    payload_scope = (
        response_payload.get("payload_scope")
        if isinstance(response_payload.get("payload_scope"), dict)
        else {}
    )
    pagination = (
        response_payload.get("pagination")
        if isinstance(response_payload.get("pagination"), dict)
        else {}
    )
    diagnostics = (
        payload_scope.get(_TRACKER_CURRENT_DETAIL_DIAGNOSTICS_LABEL)
        if isinstance(payload_scope.get(_TRACKER_CURRENT_DETAIL_DIAGNOSTICS_LABEL), dict)
        else {}
    )
    rows = response_payload.get("rows") if isinstance(response_payload.get("rows"), list) else []
    row_fields = sorted(
        {
            str(key)
            for row in rows[:5]
            if isinstance(row, dict)
            for key in row.keys()
            if str(key).strip()
        }
    )
    tracker_detail_artifact = (
        artifacts.get("project_tracker_current_detail_json")
        or artifacts.get("run_local_project_tracker_current_detail_json")
    )
    basis: dict[str, Any] = {
        "schema_version": _TRACKER_CURRENT_SOURCE_BASIS_SCHEMA_VERSION,
        "source": "tracker_current_published_report_tracker_artifact",
        "payload_mode": str(payload_mode or payload_scope.get("mode") or "").strip() or "unknown",
        "dataset_source": str(payload_scope.get("source") or "").strip(),
        "run_id": str(payload.get("run_id") or response_payload.get("run_id") or "").strip(),
        "generated_at": str(payload.get("generated_at") or response_payload.get("generated_at") or "").strip(),
        "latest_report_file": _file_content_cache_token(settings.latest_report_file),
        "latest_report_file_path": _project_relative_path_text(settings.latest_report_file),
        "publication_registry_file": _file_content_cache_token(settings.publication_registry_file),
        "publication_registry_file_path": _project_relative_path_text(settings.publication_registry_file),
        "database_file": _sqlite_file_cache_token(settings.database_path),
        "database_file_path": _project_relative_path_text(settings.database_path),
        "report_artifacts": {
            "report_json": _report_artifact_token(artifacts.get("report_json")),
            "filtered_excel": _report_artifact_token(artifacts.get("filtered_excel")),
            "project_tracker_excel": _report_artifact_token(artifacts.get("project_tracker_excel")),
            "project_tracker_current_detail_json": _report_artifact_token(tracker_detail_artifact),
        },
        "audit_read_only": bool(audit_read_only),
        "row_count": int(response_payload.get("row_count") or len(rows) or 0),
        "returned_count": int(pagination.get("returned_count") or len(rows) or 0),
        "funds_count": (
            len(response_payload.get("funds"))
            if isinstance(response_payload.get("funds"), list)
            else None
        ),
        "pagination": {
            "page": pagination.get("page"),
            "page_size": pagination.get("page_size"),
            "filtered_total_count": pagination.get("filtered_total_count"),
            "global_total_count": pagination.get("global_total_count"),
            "materialized_count": pagination.get("materialized_count"),
            "global_materialized_count": pagination.get("global_materialized_count"),
            "group": pagination.get("group"),
        },
        "detail_diagnostics": {
            "present": bool(diagnostics),
            "audit_read_only": diagnostics.get("audit_read_only"),
            "derived_source": diagnostics.get("source"),
            "derived_sidecar_write_status": diagnostics.get("derived_sidecar_write_status"),
            "derived_sidecar_status": diagnostics.get("derived_sidecar_status"),
            "derived_sidecar_path": diagnostics.get("derived_sidecar_path"),
            "processed_code_count": diagnostics.get("processed_code_count"),
        },
        "payload_scope_keys": sorted(str(key) for key in payload_scope.keys()),
        "row_fields_sample": row_fields,
        "missing_value_policy": "preserve_missing_semantics",
        "business_value_synthesis": "forbidden",
        "truth_note": (
            "/api/tracker/current exposes current tracker rows from the current published/latest "
            "report payload, project tracker artifact, optional run-local sidecar, SQLite-backed "
            "local calculations, and style benchmark calculations. Missing business values must "
            "remain missing; frontend state must not become publication truth."
        ),
    }
    basis["digest"] = _stable_json_digest(basis)
    return basis


def _build_tracker_current_source_contract(source_basis: dict[str, Any]) -> dict[str, Any]:
    basis = dict(source_basis or {})
    return {
        "schema_version": _TRACKER_CURRENT_SOURCE_CONTRACT_SCHEMA_VERSION,
        "source": "tracker_current_published_report_tracker_artifact",
        "truth_policy": "real_publication_registry_report_artifact_tracker_artifact_sqlite_and_local_calculation_only",
        "missing_value_policy": "preserve_missing_semantics",
        "business_value_synthesis": "forbidden",
        "official_publication_truth": "current_published_report_tracker_artifact",
        "frontend_rendered_truth_allowed": False,
        "cache_basis_schema_version": basis.get("schema_version"),
        "cache_basis_digest": basis.get("digest"),
    }


def _attach_tracker_current_source_contract(
    payload: dict[str, Any],
    response_payload: dict[str, Any],
    *,
    payload_mode: str,
    audit_read_only: bool = False,
) -> dict[str, Any]:
    source_basis = _build_tracker_current_source_basis(
        payload,
        response_payload,
        payload_mode=payload_mode,
        audit_read_only=audit_read_only,
    )
    response_payload["source_basis"] = source_basis
    response_payload["cache_basis"] = dict(source_basis)
    response_payload["source_contract"] = _build_tracker_current_source_contract(source_basis)
    return response_payload


def _build_tracker_current_response(payload: dict, *, audit_read_only: bool = False) -> dict:
    total_started_at = time.perf_counter()
    base_started_at = time.perf_counter()
    response_payload = recommendation_payloads.build_tracker_current_response(
        payload,
        load_tracker_dataset_fn=_load_tracker_dataset,
        normalize_portfolio_groups=_normalize_portfolio_groups,
        normalize_number_text=_normalize_number_text,
        format_tracker_occurrence_text=_format_tracker_occurrence_text,
        apply_since_inception_annualized_return_to_funds_fn=lambda rows: rows,
    )
    response_payload["formal_score_contract"] = build_formal_score_contract()
    base_build_seconds = round(max(0.0, time.perf_counter() - base_started_at), 6)
    all_rows = [dict(item or {}) for item in list(response_payload.get("rows") or [])]
    rows = all_rows
    pagination_filter_seconds = 0.0
    detail_pagination_requested = (
        has_request_context()
        and any(key in request.args for key in ("page", "page_size", "group"))
    )
    group_filter = ""
    if detail_pagination_requested:
        pagination_started_at = time.perf_counter()
        group_filter = str(request.args.get("group") or "").strip()
        filtered_rows = [
            row
            for row in all_rows
            if not group_filter or str(row.get("正式榜分组") or "").strip() == group_filter
        ]
        page_size = _normalize_positive_int(
            request.args.get("page_size"),
            default=_TRACKER_CURRENT_DEFAULT_PAGE_SIZE,
            minimum=1,
            maximum=_TRACKER_CURRENT_MAX_PAGE_SIZE,
        )
        page = _normalize_positive_int(request.args.get("page"), default=1, minimum=1)
        start = (page - 1) * page_size
        rows = [dict(item or {}) for item in filtered_rows[start : start + page_size]]
        filtered_total_count = len(filtered_rows)
        global_total_count = len(all_rows)
        response_payload["row_count"] = filtered_total_count
        response_payload["rows"] = rows
        response_payload["funds"] = rows
        response_payload["pagination"] = {
            "page": page,
            "page_size": page_size,
            "returned_count": len(rows),
            "total_count": filtered_total_count,
            "filtered_total_count": filtered_total_count,
            "global_total_count": global_total_count,
            "has_more": start + len(rows) < filtered_total_count,
            "group": group_filter,
            "pagination_scope": "detail",
        }
        payload_scope = response_payload.get("payload_scope")
        if isinstance(payload_scope, dict):
            payload_scope["pagination"] = "paged"
            payload_scope["group"] = group_filter
            payload_scope["page_size"] = page_size
            payload_scope["page"] = page
            payload_scope["filtered_total_count"] = filtered_total_count
            payload_scope["global_total_count"] = global_total_count
            payload_scope["compat_full_endpoint"] = "/api/tracker/current?full=1"
        pagination_filter_seconds = round(max(0.0, time.perf_counter() - pagination_started_at), 6)
    response_payload["rows"] = rows
    response_payload["funds"] = rows
    rows_by_code: dict[str, dict[str, Any]] = {}
    codes: list[str] = []
    for row in rows:
        code = _normalize_code_text(row.get("基金代码"))
        if not code:
            continue
        rows_by_code[code] = row
        codes.append(code)
    payload_scope = response_payload.get("payload_scope") if isinstance(response_payload.get("payload_scope"), dict) else {}
    sidecar_path = _tracker_current_detail_sidecar_path_from_scope(payload_scope)
    style_threshold_payload = _extract_style_threshold_payload(payload, rows)
    style_metric_references: dict[str, dict[str, Any]] = {}
    derived_source = "not_applicable_no_rows"
    derived_load_seconds = 0.0
    derived_build_seconds = 0.0
    derived_write_seconds = 0.0
    derived_apply_seconds = 0.0
    fallback_seconds = 0.0
    style_reference_seconds = 0.0
    derived_sidecar_write_status = "not_attempted"
    derived_sidecar_probe_seconds = 0.0
    derived_sidecar_probe: dict[str, Any] = {
        "sidecar_path": _project_relative_path_text(sidecar_path),
        "schema_version": _TRACKER_CURRENT_DERIVED_FIELDS_SCHEMA_VERSION,
        "status": "unavailable_sidecar_path" if sidecar_path is None else "not_checked",
        "hit": False,
    }
    if rows_by_code:
        derived_source = "runtime_fallback_no_sidecar"
        derived_fields: dict[str, Any] | None = None
        derived_fields_meta: dict[str, Any] | None = None
        if sidecar_path is not None:
            all_style_threshold_payload = _extract_style_threshold_payload(payload, all_rows)
            cache_basis = _build_tracker_current_derived_fields_cache_basis(
                payload,
                all_rows,
                all_style_threshold_payload,
            )
            probe_started_at = time.perf_counter()
            derived_sidecar_probe = _probe_tracker_current_detail_derived_fields_status(
                payload_scope,
                cache_basis,
            )
            derived_sidecar_probe_seconds = round(max(0.0, time.perf_counter() - probe_started_at), 6)
            load_started_at = time.perf_counter()
            loaded_derived_fields = _load_tracker_current_detail_derived_fields(payload_scope, cache_basis)
            derived_load_seconds = round(max(0.0, time.perf_counter() - load_started_at), 6)
            if loaded_derived_fields is not None:
                derived_fields = loaded_derived_fields
                derived_source = str(loaded_derived_fields.get("source") or "run_local_tracker_current_derived_fields")
                derived_sidecar_write_status = "hit"
                derived_fields_meta = {
                    "source": loaded_derived_fields.get("source"),
                    "sidecar_path": loaded_derived_fields.get("sidecar_path"),
                    "schema_version": _TRACKER_CURRENT_DERIVED_FIELDS_SCHEMA_VERSION,
                }
            else:
                build_started_at = time.perf_counter()
                built_derived_fields = _build_tracker_current_detail_derived_fields(
                    payload,
                    all_rows,
                    style_threshold_payload=all_style_threshold_payload,
                )
                derived_build_seconds = round(max(0.0, time.perf_counter() - build_started_at), 6)
                write_started_at = time.perf_counter()
                written_path = None
                if not audit_read_only:
                    written_path = _write_tracker_current_detail_derived_fields(
                        payload_scope,
                        cache_basis,
                        built_derived_fields,
                    )
                derived_write_seconds = round(max(0.0, time.perf_counter() - write_started_at), 6)
                derived_fields = built_derived_fields
                if audit_read_only:
                    derived_source = "runtime_build_read_only"
                    derived_sidecar_write_status = "audit_read_only"
                else:
                    derived_source = "runtime_build_written" if written_path is not None else "runtime_build_unwritten"
                    derived_sidecar_write_status = "written" if written_path is not None else "unwritten"
                derived_fields_meta = {
                    "source": derived_source,
                    "sidecar_path": _project_relative_path_text(written_path) if written_path is not None else None,
                    "schema_version": _TRACKER_CURRENT_DERIVED_FIELDS_SCHEMA_VERSION,
                }

        if derived_fields is not None:
            apply_started_at = time.perf_counter()
            style_metric_references = _apply_tracker_current_derived_fields_to_rows(
                rows_by_code,
                codes,
                derived_fields,
            )
            derived_apply_seconds = round(max(0.0, time.perf_counter() - apply_started_at), 6)
            if derived_fields_meta is not None:
                response_payload["payload_scope"] = {
                    **payload_scope,
                    "tracker_current_derived_fields": derived_fields_meta,
                }
        else:
            fallback_started_at = time.perf_counter()
            data_source_by_code = _load_fund_data_source_map(codes)
            annualized_return_by_code = _compute_since_inception_annualized_return_map(codes)
            accum_map = _compute_accum_nav_ratio_annualized_return_map(codes)
            observations = load_platform_lifetime_returns(settings.database_path, codes)
            for code, row in rows_by_code.items():
                current_source = str(row.get("数据来源") or "").strip()
                if not current_source:
                    row["数据来源"] = str(data_source_by_code.get(code) or "").strip()
                value = annualized_return_by_code.get(code)
                row[_SINCE_INCEPTION_ANNUALIZED_RETURN_LABEL] = "" if value is None else str(value).strip()
                row["累计净值比年化(%)"] = accum_map.get(code, "")
                row["成立以来收益口径"] = observations.get(code) or {"status": "missing_input"}
            _attach_since_inception_annual_return_breakdown_rows(rows_by_code, codes)
            style_reference_started_at = time.perf_counter()
            style_metric_benchmark_map = (
                _load_cached_style_metric_benchmark_map(
                    rows,
                    benchmark_rows=all_rows,
                    cache_namespace=f"tracker_current_detail::{str(payload.get('run_id') or payload.get('generated_at') or 'unknown').strip() or 'unknown'}",
                )
                if detail_pagination_requested
                else _load_cached_style_metric_benchmark_map(rows)
            )
            style_metric_references = _build_style_metric_reference_map(
                rows,
                style_threshold_entries=style_threshold_payload.get("styles") or [],
                global_style_threshold_entry=style_threshold_payload.get("global") or {},
                benchmark_map=style_metric_benchmark_map,
            )
            style_reference_seconds = round(max(0.0, time.perf_counter() - style_reference_started_at), 6)
            fallback_seconds = round(max(0.0, time.perf_counter() - fallback_started_at), 6)
    response_payload["style_thresholds"] = style_threshold_payload.get("styles") or []
    response_payload["global_style_threshold"] = style_threshold_payload.get("global") or {}
    response_payload["missing_style_thresholds"] = style_threshold_payload.get("missing_styles") or []
    for item in rows:
        code = _normalize_code_text(item.get("基金代码"))
        item[_STYLE_METRIC_REFERENCES_LABEL] = style_metric_references.get(code) or {}
    response_payload["style_metric_references"] = style_metric_references
    final_payload_scope = (
        response_payload.get("payload_scope")
        if isinstance(response_payload.get("payload_scope"), dict)
        else payload_scope
    )
    pagination_payload = response_payload.get("pagination") if isinstance(response_payload.get("pagination"), dict) else {}
    derived_fields_scope_meta = final_payload_scope.get("tracker_current_derived_fields")
    derived_sidecar_path_text = derived_sidecar_probe.get("sidecar_path")
    if isinstance(derived_fields_scope_meta, dict):
        derived_sidecar_path_text = derived_fields_scope_meta.get("sidecar_path") or derived_sidecar_path_text
    response_payload["payload_scope"] = {
        **final_payload_scope,
        _TRACKER_CURRENT_DETAIL_DIAGNOSTICS_LABEL: {
            "source": derived_source,
            "derived_sidecar_enabled": sidecar_path is not None,
            "audit_read_only": bool(audit_read_only),
            "derived_sidecar_status": derived_sidecar_probe.get("status"),
            "derived_sidecar_hit": bool(derived_sidecar_probe.get("hit"))
            and derived_build_seconds == 0.0
            and derived_source == "run_local_tracker_current_derived_fields",
            "derived_sidecar_write_status": derived_sidecar_write_status,
            "derived_sidecar_path": derived_sidecar_path_text,
            "schema_version": _TRACKER_CURRENT_DERIVED_FIELDS_SCHEMA_VERSION,
            "base_build_seconds": base_build_seconds,
            "pagination_filter_seconds": pagination_filter_seconds,
            "derived_probe_seconds": derived_sidecar_probe_seconds,
            "derived_load_seconds": derived_load_seconds,
            "derived_build_seconds": derived_build_seconds,
            "derived_write_seconds": derived_write_seconds,
            "derived_apply_seconds": derived_apply_seconds,
            "fallback_seconds": fallback_seconds,
            "style_reference_seconds": style_reference_seconds,
            "total_seconds": round(max(0.0, time.perf_counter() - total_started_at), 6),
            "detail_pagination_requested": bool(detail_pagination_requested),
            "group": group_filter,
            "page": pagination_payload.get("page"),
            "page_size": pagination_payload.get("page_size"),
            "returned_count": pagination_payload.get("returned_count", len(rows)),
            "filtered_total_count": pagination_payload.get("filtered_total_count", len(rows)),
            "global_total_count": pagination_payload.get("global_total_count", len(all_rows)),
            "row_count": response_payload.get("row_count", len(rows)),
            "all_row_count": len(all_rows),
            "processed_code_count": len(codes),
        },
    }
    return _attach_tracker_current_source_contract(
        payload,
        response_payload,
        payload_mode="detail",
        audit_read_only=audit_read_only,
    )


def _resolve_tracker_current_total_count(
    *,
    counts: dict[str, Any],
    tracker_summary: dict[str, Any],
    group_filter: str,
    filtered_materialized_count: int,
    global_materialized_count: int,
) -> tuple[int, int]:
    global_total = int(
        counts.get("project_tracker_current_active_count")
        or tracker_summary.get("current_active_count")
        or global_materialized_count
        or 0
    )
    if not group_filter:
        return global_total, global_total
    if group_filter == "纯债稳健榜":
        return global_total, int(counts.get("stable_bond_final_count") or filtered_materialized_count or 0)
    if group_filter == "增强债弹性榜":
        return global_total, int(counts.get("enhanced_bond_final_count") or filtered_materialized_count or 0)
    return global_total, filtered_materialized_count


def _build_tracker_current_compact_response(payload: dict[str, Any]) -> dict[str, Any]:
    counts = payload.get("counts") if isinstance(payload.get("counts"), dict) else {}
    publish_summary = (
        payload.get("project_publish_summary")
        if isinstance(payload.get("project_publish_summary"), dict)
        else {}
    )
    tracker_summary = (
        publish_summary.get("project_tracker_summary")
        if isinstance(publish_summary.get("project_tracker_summary"), dict)
        else {}
    )
    portfolio_summary = _normalize_portfolio_groups(
        publish_summary.get("portfolio_suggestions")
        if isinstance(publish_summary.get("portfolio_suggestions"), dict)
        else {}
    )
    all_preview_rows = [
        dict(item)
        for item in (
            payload.get("project_tracker_current_preview")
            if isinstance(payload.get("project_tracker_current_preview"), list)
            else []
        )
        if isinstance(item, dict)
    ]
    group_filter = str(request.args.get("group") or "").strip()
    preview_rows = list(all_preview_rows)
    if group_filter:
        preview_rows = [
            row
            for row in preview_rows
            if str(row.get("正式榜分组") or "").strip() == group_filter
        ]
    page_size = _normalize_positive_int(
        request.args.get("page_size"),
        default=_TRACKER_CURRENT_DEFAULT_PAGE_SIZE,
        minimum=1,
        maximum=_TRACKER_CURRENT_MAX_PAGE_SIZE,
    )
    page = _normalize_positive_int(request.args.get("page"), default=1, minimum=1)
    start = (page - 1) * page_size
    selected_rows = [normalize_drawdown_duration_record(row) for row in preview_rows[start : start + page_size]]
    missing_duration_codes = {
        str(row.get("基金代码") or "").strip()
        for row in selected_rows if ANNUAL_DRAWDOWN_OBSERVATIONS not in row
    } - {""}
    duration_values = recommendation_payloads.load_tracker_drawdown_duration_values(
        payload, settings=settings, codes=missing_duration_codes,
    )
    page_rows: list[dict[str, Any]] = []
    for row in selected_rows:
        for internal_name, canonical_name in PROJECT_TRACKER_INTERNAL_TO_EXPORT_RENAME.items():
            if canonical_name == ANNUAL_DRAWDOWN_OBSERVATIONS and canonical_name in row:
                continue
            canonical_value = row.get(canonical_name)
            internal_value = row.get(internal_name)
            if canonical_value not in (None, "") or internal_value in (None, ""):
                continue
            row[canonical_name] = internal_value
        if ANNUAL_DRAWDOWN_OBSERVATIONS not in row:
            row[ANNUAL_DRAWDOWN_OBSERVATIONS] = duration_values.get(str(row.get("基金代码") or "").strip())
        page_rows.append(row)
    global_total_count, filtered_total_count = _resolve_tracker_current_total_count(
        counts=counts,
        tracker_summary=tracker_summary,
        group_filter=group_filter,
        filtered_materialized_count=len(preview_rows),
        global_materialized_count=len(all_preview_rows),
    )
    response_payload = {
        "run_id": payload.get("run_id"),
        "generated_at": payload.get("generated_at"),
        "tracker_file": (payload.get("artifacts") or {}).get("project_tracker_excel")
        if isinstance(payload.get("artifacts"), dict)
        else None,
        "tracker_summary": tracker_summary,
        "portfolio_suggestions": portfolio_summary,
        "formal_score_contract": build_formal_score_contract(),
        "metric_units": metric_unit_metadata(),
        "row_count": filtered_total_count,
        "counts": {
            "project_tracker_current_active_count": global_total_count,
            "project_tracker_current_filtered_count": filtered_total_count,
            "stable_bond_final_count": int(counts.get("stable_bond_final_count") or 0),
            "enhanced_bond_final_count": int(counts.get("enhanced_bond_final_count") or 0),
            "project_tracker_total_periods": int(counts.get("project_tracker_total_periods") or 0),
            "project_tracker_fund_count": int(counts.get("project_tracker_fund_count") or 0),
            "project_tracker_repeated_fund_count": int(counts.get("project_tracker_repeated_fund_count") or 0),
            "portfolio_group_count": int(portfolio_summary.get("group_count") or 0),
            "portfolio_fund_count": int(portfolio_summary.get("fund_count") or 0),
        },
        "rows": page_rows,
        "pagination": {
            "page": page,
            "page_size": page_size,
            "returned_count": len(page_rows),
            "materialized_count": len(preview_rows),
            "materialized_preview_count": len(preview_rows),
            "global_materialized_count": len(all_preview_rows),
            "total_count": filtered_total_count,
            "filtered_total_count": filtered_total_count,
            "global_total_count": global_total_count,
            "has_more_materialized_rows": start + len(page_rows) < len(preview_rows),
            "group": group_filter,
        },
        "payload_scope": {
            "mode": "compact",
            "source": "latest_report_project_tracker_current_preview",
            "field_alias_contract": "project_tracker_internal_to_export_rename",
            "drawdown_duration_source": "preview_or_same_publication_tracker_detail",
            "full_endpoint": "/api/tracker/current?full=1",
            "omitted_fields": [
                "funds",
                "style_metric_references",
                "style_thresholds",
                "global_style_threshold",
                "missing_style_thresholds",
                "since_inception_annual_return_breakdown",
            ],
        },
    }
    return _attach_tracker_current_source_contract(
        payload,
        response_payload,
        payload_mode="compact",
        audit_read_only=False,
    )


def _compact_publish_blockers(blockers: Any) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for item in blockers or []:
        if not isinstance(item, dict):
            continue
        result.append(
            {
                key: item.get(key)
                for key in ("code", "severity", "blocking", "message", "detail")
                if key in item
            }
        )
    return result


def _build_reports_compact_response(publication: dict[str, Any] | None) -> dict[str, Any]:
    current = dict(publication or {})
    published_run_id = str(
        current.get("current_published_run_id")
        or current.get("latest_canonical_run_id")
        or ""
    ).strip()
    records = [
        dict(item)
        for item in (current.get("canonical_runs") or [])
        if isinstance(item, dict)
    ]
    if published_run_id:
        records.sort(
            key=lambda item: (
                1 if str(item.get("run_id") or "").strip() == published_run_id else 0,
                str(item.get("generated_at") or ""),
            ),
            reverse=True,
        )
    else:
        records.sort(key=lambda item: str(item.get("generated_at") or ""), reverse=True)

    reports: list[dict[str, Any]] = []
    for item in records:
        snapshot = item.get("snapshot") if isinstance(item.get("snapshot"), dict) else {}
        replay_precision = (
            item.get("replay_precision")
            if isinstance(item.get("replay_precision"), dict)
            else {}
        )
        final_selection_contraction = (
            item.get("final_selection_contraction")
            if isinstance(item.get("final_selection_contraction"), dict)
            else {}
        )
        run_id = str(item.get("run_id") or "").strip()
        reports.append(
            {
                "run_id": run_id,
                "generated_at": item.get("generated_at"),
                "business_date": item.get("business_date"),
                "group_key": item.get("group_key"),
                "published": bool(item.get("published")),
                "publish_state": item.get("publish_state"),
                "publishable": bool(item.get("publishable")),
                "risk_publish_ready": bool(item.get("risk_publish_ready")),
                "final_count": item.get("final_count"),
                "dynamic_final_count": item.get("dynamic_final_count"),
                "counts": _compact_dict(item.get("counts"), _COMPACT_REPORT_COUNT_KEYS),
                "snapshot": _compact_dict(snapshot, _COMPACT_REPORT_SNAPSHOT_KEYS),
                "artifacts": _compact_dict(item.get("artifacts"), _COMPACT_REPORT_ARTIFACT_KEYS),
                "report_path": item.get("report_path"),
                "run_dir": item.get("run_dir"),
                "publish_blockers": _compact_publish_blockers(item.get("publish_blockers")),
                "publish_blocker_count": len(item.get("publish_blockers") or []),
                "run_semantics": item.get("run_semantics"),
                "publication_status": item.get("publication_status"),
                "superseded_run_count": len(item.get("superseded_run_ids") or []),
                "replay_precision": {
                    "status": replay_precision.get("status"),
                    "status_label": replay_precision.get("status_label"),
                    "exact_replay_inputs_available": replay_precision.get("exact_replay_inputs_available"),
                    "publish_gate_exact_replay_available": replay_precision.get("publish_gate_exact_replay_available"),
                    "final_layer_exact_replay_available": replay_precision.get("final_layer_exact_replay_available"),
                },
                "final_selection_contraction": {
                    key: final_selection_contraction.get(key)
                    for key in (
                        "removed_count",
                        "input_count",
                        "passed_count",
                        "stability_cap_removed_count",
                        "stability_cap_present",
                    )
                    if key in final_selection_contraction
                },
            }
        )

    from backend.report_history import mode as history_mode, storage_metadata as history_storage_metadata
    if history_mode(settings) != 'legacy':
        history_storage_metadata(settings, reports)
    return {
        "payload_scope": {
            "mode": "compact",
            "source": "publication_registry.canonical_runs",
            "full_endpoint": "/api/reports?full=1",
            "omitted_fields": [
                "report_payloads",
                "preview_rows",
                "audit_samples",
                "project_publish_summary",
                "database_freshness_detail",
            ],
        },
        "latest_canonical_run_id": current.get("latest_canonical_run_id"),
        "current_published_run_id": current.get("current_published_run_id"),
        "latest_canonical_report_path": current.get("latest_canonical_report_path"),
        "current_published_report_path": current.get("current_published_report_path"),
        "report_count": len(reports),
        "reports": reports,
    }


def _overlay_portfolio_occurrence_metrics(
    portfolio_summary: dict[str, Any] | None,
    occurrence_by_code: dict[str, dict[str, str]] | None,
) -> dict[str, Any]:
    return recommendation_payloads.overlay_portfolio_occurrence_metrics(
        portfolio_summary,
        occurrence_by_code,
        normalize_code_text=_normalize_code_text,
        normalize_number_text=_normalize_number_text,
        to_float=_to_float,
        format_tracker_occurrence_text=_format_tracker_occurrence_text,
    )


def _build_recommendations_response(payload: dict) -> dict:
    payload = recommendations_display_report(settings, payload)
    response_payload = recommendation_payloads.build_recommendations_response(
        payload,
        settings=settings,
        normalize_portfolio_groups=_normalize_portfolio_groups,
        load_tracker_occurrence_map_fn=_load_tracker_occurrence_map,
        overlay_portfolio_occurrence_metrics_fn=_overlay_portfolio_occurrence_metrics,
        apply_since_inception_annualized_return_fn=_apply_since_inception_annualized_return_to_portfolio_summary,
        build_portfolio_group_brief=_build_portfolio_group_brief,
        build_publish_funnel_summary=_build_publish_funnel_summary,
    )
    response_payload["formal_score_contract"] = build_formal_score_contract()
    portfolio_summary = response_payload.get("portfolio_suggestions") or {}
    funds = portfolio_summary.get("funds") or []
    funds_by_code: dict[str, dict[str, Any]] = {}
    codes: list[str] = []
    for row in funds:
        code = _normalize_code_text(row.get("基金代码"))
        if not code:
            continue
        funds_by_code[code] = row
        codes.append(code)
    if funds_by_code:
        _attach_since_inception_annual_return_breakdown_rows(funds_by_code, codes)
        group_rows = portfolio_summary.get("groups") or []
        for group in group_rows:
            normalized_funds: list[dict[str, Any]] = []
            for fund in (group.get("funds") or []):
                code = _normalize_code_text((fund or {}).get("基金代码"))
                normalized_funds.append(dict(funds_by_code.get(code) or fund or {}))
            group["funds"] = normalized_funds
    style_threshold_payload = _extract_style_threshold_payload(payload, funds)
    response_payload["style_thresholds"] = style_threshold_payload.get("styles") or []
    response_payload["global_style_threshold"] = style_threshold_payload.get("global") or {}
    response_payload["missing_style_thresholds"] = style_threshold_payload.get("missing_styles") or []
    style_metric_benchmark_map = _load_cached_style_metric_benchmark_map(funds)
    style_metric_references = _build_style_metric_reference_map(
        funds,
        style_threshold_entries=style_threshold_payload.get("styles") or [],
        global_style_threshold_entry=style_threshold_payload.get("global") or {},
        benchmark_map=style_metric_benchmark_map,
    )
    for item in funds:
        code = _normalize_code_text(item.get("基金代码"))
        item[_STYLE_METRIC_REFERENCES_LABEL] = style_metric_references.get(code) or {}
    response_payload["style_metric_references"] = style_metric_references
    return response_payload


def _publication_record_count(record: dict[str, Any], key: str) -> int | None:
    counts = record.get("counts") if isinstance(record.get("counts"), dict) else {}
    value = record.get(key)
    if value is None:
        value = counts.get(key)
    return _to_int_or_none(value)


def _publication_record_snapshot(record: dict[str, Any]) -> dict[str, Any]:
    snapshot = record.get("snapshot") if isinstance(record.get("snapshot"), dict) else {}
    return {
        "candidate_date": str(snapshot.get("candidate_date") or "").strip() or None,
        "candidate_fund_count": _to_int_or_none(snapshot.get("candidate_fund_count")),
        "coverage_ratio_vs_total": _to_float(snapshot.get("coverage_ratio_vs_total")),
        "risk_1y_effective_end_date": str(snapshot.get("risk_1y_effective_end_date") or "").strip() or None,
        "risk_1y_effective_fund_count": _to_int_or_none(snapshot.get("risk_1y_effective_fund_count")),
        "risk_1y_effective_coverage_ratio_vs_candidate": _to_float(
            snapshot.get("risk_1y_effective_coverage_ratio_vs_candidate")
        ),
    }


def _short_publication_blocker(blocker: dict[str, Any] | None) -> dict[str, Any]:
    payload = blocker if isinstance(blocker, dict) else {}
    return {
        "code": str(payload.get("code") or "").strip(),
        "label": str(payload.get("label") or "").strip(),
        "detail": str(payload.get("detail") or "").strip(),
        "severity": str(payload.get("severity") or "").strip(),
        "action_path": str(payload.get("action_path") or "").strip(),
        "governance_stage": str(payload.get("governance_stage") or "").strip(),
    }


def _compact_publication_record(record: dict[str, Any] | None) -> dict[str, Any]:
    payload = record if isinstance(record, dict) else {}
    if not payload:
        return {}
    blockers = [
        _short_publication_blocker(item)
        for item in list(payload.get("publish_blockers") or [])
        if isinstance(item, dict)
    ]
    primary_blocker = blockers[0] if blockers else {}
    snapshot = _publication_record_snapshot(payload)
    report_path = str(payload.get("report_path") or "").strip()
    report_path_obj = Path(report_path) if report_path else None
    return {
        "run_id": str(payload.get("run_id") or "").strip() or None,
        "generated_at": str(payload.get("generated_at") or "").strip() or None,
        "group_key": str(payload.get("group_key") or "").strip() or None,
        "report_path": _project_relative_path_text(report_path_obj) if report_path_obj else None,
        "final_count": _publication_record_count(payload, "final_count"),
        "dynamic_final_count": _publication_record_count(payload, "dynamic_final_count"),
        "project_tracker_current_active_count": _publication_record_count(
            payload,
            "project_tracker_current_active_count",
        ),
        "stable_bond_final_count": _publication_record_count(payload, "stable_bond_final_count"),
        "enhanced_bond_final_count": _publication_record_count(payload, "enhanced_bond_final_count"),
        "screenable_universe_count": _publication_record_count(payload, "screenable_universe_count"),
        "publishable_pool_count": _publication_record_count(payload, "publishable_pool_count"),
        "snapshot": snapshot,
        "effective_nav_date": snapshot.get("candidate_date"),
        "risk_effective_end_date": snapshot.get("risk_1y_effective_end_date"),
        "publishable": bool(payload.get("publishable")),
        "published": bool(payload.get("published")),
        "publish_hold": bool(payload.get("publish_hold")),
        "publish_blocked": bool(payload.get("publish_blocked")),
        "publish_hold_reason_codes": [
            str(item or "").strip()
            for item in list(payload.get("publish_hold_reason_codes") or [])
            if str(item or "").strip()
        ],
        "primary_blocker": primary_blocker,
        "blockers_preview": blockers[:3],
    }


def _compact_contraction_guard(blockers: list[dict[str, Any]]) -> dict[str, Any]:
    blocker = next(
        (
            item
            for item in blockers
            if str(item.get("code") or "").strip() in {
                "publication_contraction_watch",
                "publish_hold_by_contraction_guard",
            }
        ),
        None,
    )
    if not blocker:
        return {"available": False}

    signals: list[dict[str, Any]] = []
    for item in list(blocker.get("signals") or []):
        if not isinstance(item, dict):
            continue
        drop_ratio = _to_float(item.get("drop_ratio"))
        signals.append(
            {
                "field": str(item.get("field") or "").strip(),
                "label": str(item.get("label") or "").strip(),
                "baseline_value": _to_int_or_none(item.get("baseline_value")),
                "candidate_value": _to_int_or_none(item.get("candidate_value")),
                "drop_count": _to_int_or_none(item.get("drop_count")),
                "drop_ratio": drop_ratio,
                "drop_ratio_pct": round(drop_ratio * 100, 4) if drop_ratio is not None else None,
            }
        )

    return {
        "available": True,
        "code": str(blocker.get("code") or "").strip(),
        "label": str(blocker.get("label") or "").strip(),
        "detail": str(blocker.get("detail") or "").strip(),
        "baseline_run_id": str(blocker.get("baseline_run_id") or "").strip() or None,
        "candidate_run_id": str(blocker.get("candidate_run_id") or "").strip() or None,
        "signal_count": _to_int_or_none(blocker.get("signal_count")),
        "signals": signals,
    }


def _build_payload_publication_fallback_record(
    payload: dict[str, Any],
    response_payload: dict[str, Any],
) -> dict[str, Any]:
    counts = response_payload.get("counts") if isinstance(response_payload.get("counts"), dict) else {}
    tracker_summary = (
        response_payload.get("tracker_summary")
        if isinstance(response_payload.get("tracker_summary"), dict)
        else {}
    )
    effective_nav_date = str(tracker_summary.get("latest_nav_snapshot_date") or "").strip()
    return {
        "run_id": str(payload.get("run_id") or response_payload.get("run_id") or "").strip() or None,
        "generated_at": str(payload.get("generated_at") or response_payload.get("generated_at") or "").strip() or None,
        "final_count": _to_int_or_none(counts.get("final_count")),
        "dynamic_final_count": _to_int_or_none(counts.get("dynamic_final_count")),
        "project_tracker_current_active_count": _to_int_or_none(
            counts.get("project_tracker_current_active_count")
        ),
        "snapshot": {
            "candidate_date": effective_nav_date or None,
        },
        "effective_nav_date": effective_nav_date or None,
        "published": False,
        "publishable": False,
        "publish_hold": False,
        "publish_blocked": False,
    }


def _build_recommendations_publication_status_summary(
    payload: dict[str, Any],
    response_payload: dict[str, Any],
) -> dict[str, Any]:
    publication = _load_publication_registry_cached() or _load_publication_registry_snapshot_cached() or {}
    layers = publication.get("automation_layers") if isinstance(publication.get("automation_layers"), dict) else {}
    current_record = _compact_publication_record(layers.get("current_published_run"))
    latest_successful_record = _compact_publication_record(layers.get("latest_successful_run"))
    latest_publishable_record = _compact_publication_record(layers.get("latest_publishable_run"))
    latest_database_snapshot = (
        layers.get("latest_database_snapshot")
        if isinstance(layers.get("latest_database_snapshot"), dict)
        else {}
    )
    if not current_record:
        current_record = _build_payload_publication_fallback_record(payload, response_payload)

    tracker_summary = (
        response_payload.get("tracker_summary")
        if isinstance(response_payload.get("tracker_summary"), dict)
        else {}
    )
    status_summary = build_publication_status_summary(publication, tracker_summary=tracker_summary)
    current_run_id = str(
        current_record.get("run_id")
        or publication.get("current_published_run_id")
        or publication.get("latest_canonical_run_id")
        or ""
    ).strip()
    latest_successful_run_id = str((latest_successful_record or {}).get("run_id") or "").strip()
    latest_publishable_run_id = str((latest_publishable_record or {}).get("run_id") or "").strip()
    latest_successful_blockers = list(
        (layers.get("latest_successful_blockers") if isinstance(layers, dict) else [])
        or (layers.get("latest_successful_run") or {}).get("publish_blockers")
        or []
    )
    latest_successful_primary_blocker = (
        latest_successful_record.get("primary_blocker")
        if isinstance(latest_successful_record.get("primary_blocker"), dict)
        else {}
    )
    current_final_count = _to_int_or_none(current_record.get("final_count"))
    latest_final_count = _to_int_or_none((latest_successful_record or {}).get("final_count"))
    current_dynamic_count = _to_int_or_none(current_record.get("dynamic_final_count"))
    latest_dynamic_count = _to_int_or_none((latest_successful_record or {}).get("dynamic_final_count"))
    formal_stale = bool(
        current_run_id
        and latest_successful_run_id
        and current_run_id != latest_successful_run_id
    )
    latest_database_candidate_date = str(latest_database_snapshot.get("candidate_date") or "").strip() or None
    latest_successful_effective_nav_date = (
        latest_successful_record.get("effective_nav_date")
        or latest_database_candidate_date
    )
    current_effective_nav_date = (
        current_record.get("effective_nav_date")
        or str(tracker_summary.get("latest_nav_snapshot_date") or "").strip()
        or None
    )
    status_detail = str(status_summary.get("detail") or "").strip()
    primary_detail = str(latest_successful_primary_blocker.get("detail") or "").strip()
    if primary_detail and primary_detail not in status_detail:
        status_detail = f"{status_detail} {primary_detail}".strip()

    return {
        "available": bool(publication),
        "official_publication_truth": "current_published_report_artifact",
        "current_published_run_id": current_run_id or None,
        "latest_successful_run_id": latest_successful_run_id or None,
        "latest_publishable_run_id": latest_publishable_run_id or None,
        "latest_canonical_run_id": str(publication.get("latest_canonical_run_id") or "").strip() or None,
        "current_published": current_record,
        "latest_successful": latest_successful_record,
        "latest_publishable": latest_publishable_record,
        "latest_database_snapshot": {
            "candidate_date": latest_database_candidate_date,
            "candidate_fund_count": _to_int_or_none(latest_database_snapshot.get("candidate_fund_count")),
            "coverage_ratio_vs_total": _to_float(latest_database_snapshot.get("coverage_ratio_vs_total")),
            "risk_1y_effective_end_date": str(
                latest_database_snapshot.get("risk_1y_effective_end_date") or ""
            ).strip() or None,
            "risk_1y_effective_fund_count": _to_int_or_none(
                latest_database_snapshot.get("risk_1y_effective_fund_count")
            ),
            "risk_1y_effective_coverage_ratio_vs_candidate": _to_float(
                latest_database_snapshot.get("risk_1y_effective_coverage_ratio_vs_candidate")
            ),
        },
        "current_effective_nav_date": current_effective_nav_date,
        "latest_successful_effective_nav_date": latest_successful_effective_nav_date,
        "formal_publication_stale_against_latest_successful": formal_stale,
        "latest_successful_is_currently_published": bool(
            status_summary.get("latest_successful_is_currently_published")
        ),
        "latest_successful_is_publishable": bool(status_summary.get("latest_successful_is_publishable")),
        "publish_hold": bool(status_summary.get("publish_hold")),
        "publish_blocked": bool(status_summary.get("publish_blocked")),
        "latest_successful_publish_hold": bool((latest_successful_record or {}).get("publish_hold")),
        "latest_successful_publish_blocked": bool((latest_successful_record or {}).get("publish_blocked")),
        "status": status_summary.get("status"),
        "status_label": status_summary.get("status_label"),
        "headline": status_summary.get("headline"),
        "detail": status_detail,
        "recommended_action": status_summary.get("recommended_action"),
        "primary_blocker": latest_successful_primary_blocker,
        "contraction_guard": _compact_contraction_guard(latest_successful_blockers),
        "count_comparison": {
            "final_count": {
                "current_published": current_final_count,
                "latest_successful": latest_final_count,
                "delta": (
                    latest_final_count - current_final_count
                    if latest_final_count is not None and current_final_count is not None
                    else None
                ),
            },
            "dynamic_final_count": {
                "current_published": current_dynamic_count,
                "latest_successful": latest_dynamic_count,
                "delta": (
                    latest_dynamic_count - current_dynamic_count
                    if latest_dynamic_count is not None and current_dynamic_count is not None
                    else None
                ),
            },
        },
        "truth_note": (
            "推荐页正式名单仍以 current_published_report_artifact 为准；latest_successful 只表示"
            "最近一次真实成功运行及其发布门禁状态，不会在发布保护未解除前冒充正式发布结果。"
        ),
    }


def _attach_recommendations_publication_status(
    payload: dict[str, Any],
    response_payload: dict[str, Any],
) -> dict[str, Any]:
    response_payload["publication_status_summary"] = _build_recommendations_publication_status_summary(
        payload,
        response_payload,
    )
    return response_payload


def _attach_recommendations_detail_publication_source_contract(
    response_payload: dict[str, Any],
) -> dict[str, Any]:
    response = dict(response_payload or {})
    source_basis = dict(response.get("source_basis") or {})
    cache_basis = dict(response.get("cache_basis") or source_basis)
    source_contract = dict(response.get("source_contract") or {})
    publication_status = (
        response.get("publication_status_summary")
        if isinstance(response.get("publication_status_summary"), dict)
        else {}
    )
    current_record = (
        publication_status.get("current_published")
        if isinstance(publication_status.get("current_published"), dict)
        else {}
    )
    latest_record = (
        publication_status.get("latest_successful")
        if isinstance(publication_status.get("latest_successful"), dict)
        else {}
    )
    publication_status_basis = {
        "source": "publication_registry_runtime_overlay",
        "truth_policy": "real_publication_registry_artifact_only",
        "publication_registry_file": _file_content_cache_token(settings.publication_registry_file),
        "publication_registry_file_path": _project_relative_path_text(settings.publication_registry_file),
        "official_publication_truth": publication_status.get("official_publication_truth")
        or "current_published_report_artifact",
        "publication_status": publication_status.get("status"),
        "current_published_run_id": publication_status.get("current_published_run_id"),
        "latest_successful_run_id": publication_status.get("latest_successful_run_id"),
        "latest_publishable_run_id": publication_status.get("latest_publishable_run_id"),
        "latest_canonical_run_id": publication_status.get("latest_canonical_run_id"),
        "current_effective_nav_date": publication_status.get("current_effective_nav_date"),
        "latest_successful_effective_nav_date": publication_status.get(
            "latest_successful_effective_nav_date"
        ),
        "publish_hold": publication_status.get("publish_hold"),
        "publish_blocked": publication_status.get("publish_blocked"),
        "latest_successful_publish_hold": publication_status.get("latest_successful_publish_hold"),
        "latest_successful_publish_blocked": publication_status.get(
            "latest_successful_publish_blocked"
        ),
        "latest_successful_is_currently_published": publication_status.get(
            "latest_successful_is_currently_published"
        ),
        "latest_successful_is_publishable": publication_status.get("latest_successful_is_publishable"),
        "current_published_final_count": current_record.get("final_count"),
        "latest_successful_final_count": latest_record.get("final_count"),
        "frontend_rendered_truth_allowed": False,
        "missing_value_policy": "preserve_missing_semantics",
        "business_value_synthesis": "forbidden",
    }
    publication_status_basis["digest"] = _stable_json_digest(publication_status_basis)
    response_basis = {
        **source_basis,
        "publication_status_source": publication_status_basis,
        "publication_registry_file": publication_status_basis["publication_registry_file"],
        "publication_registry_file_path": publication_status_basis["publication_registry_file_path"],
        "current_published_run_id": publication_status_basis["current_published_run_id"],
        "publication_status": publication_status_basis["publication_status"],
    }
    response_basis["digest"] = _stable_json_digest(response_basis)
    response["source_basis"] = response_basis
    response["cache_basis"] = dict(response_basis)
    response["source_contract"] = {
        **source_contract,
        "schema_version": _RECOMMENDATIONS_DETAIL_SOURCE_CONTRACT_SCHEMA_VERSION,
        "source": "run_local_recommendations_detail",
        "truth_policy": "real_api_database_runtime_artifact_and_publication_registry_only",
        "missing_value_policy": "preserve_missing_semantics",
        "business_value_synthesis": "forbidden",
        "official_publication_truth": "publication_registry",
        "publication_status_source": "publication_registry_runtime_overlay",
        "frontend_rendered_truth_allowed": False,
        "cache_basis_schema_version": response_basis.get("schema_version"),
        "cache_basis_digest": response_basis.get("digest"),
    }
    payload_scope = response.get("payload_scope") if isinstance(response.get("payload_scope"), dict) else {}
    source_scope = (
        payload_scope.get("source_contract")
        if isinstance(payload_scope.get("source_contract"), dict)
        else {}
    )
    response["payload_scope"] = {
        **payload_scope,
        "source_contract": {
            **source_scope,
            "source": "run_local_recommendations_detail",
            "cache_basis_present": bool(response_basis),
            "source_basis_matches_cache_basis": True,
            "truth_policy": "real_api_database_runtime_artifact_and_publication_registry_only",
            "missing_value_policy": "preserve_missing_semantics",
            "publication_status_source": "publication_registry_runtime_overlay",
            "official_publication_truth": "publication_registry",
        },
    }
    return response


def _build_recommendations_summary_source_basis(
    payload: dict[str, Any],
    response_payload: dict[str, Any],
) -> dict[str, Any]:
    artifacts = payload.get("artifacts") if isinstance(payload.get("artifacts"), dict) else {}
    publication = _load_publication_registry_cached() or _load_publication_registry_snapshot_cached()
    automation_layers = (
        publication.get("automation_layers")
        if isinstance((publication or {}).get("automation_layers"), dict)
        else {}
    )
    latest_successful_run = (
        automation_layers.get("latest_successful_run")
        if isinstance(automation_layers.get("latest_successful_run"), dict)
        else {}
    )
    current_published_run = (
        automation_layers.get("current_published_run")
        if isinstance(automation_layers.get("current_published_run"), dict)
        else {}
    )
    latest_successful_snapshot = (
        latest_successful_run.get("snapshot")
        if isinstance(latest_successful_run.get("snapshot"), dict)
        else {}
    )
    current_published_snapshot = (
        current_published_run.get("snapshot")
        if isinstance(current_published_run.get("snapshot"), dict)
        else {}
    )
    publication_status = (
        response_payload.get("publication_status_summary")
        if isinstance(response_payload.get("publication_status_summary"), dict)
        else {}
    )
    portfolio_summary = (
        response_payload.get("portfolio_suggestions")
        if isinstance(response_payload.get("portfolio_suggestions"), dict)
        else {}
    )
    counts = response_payload.get("counts") if isinstance(response_payload.get("counts"), dict) else {}
    payload_scope = (
        response_payload.get("payload_scope")
        if isinstance(response_payload.get("payload_scope"), dict)
        else {}
    )
    report_json_artifact = artifacts.get("report_json")
    tracker_detail_artifact = (
        artifacts.get("project_tracker_current_detail_json")
        or artifacts.get("run_local_project_tracker_current_detail_json")
    )
    basis: dict[str, Any] = {
        "schema_version": _RECOMMENDATIONS_SUMMARY_SOURCE_BASIS_SCHEMA_VERSION,
        "source": "recommendations_summary_current_published_report_artifact",
        "payload_mode": str(payload_scope.get("mode") or "").strip() or "summary",
        "run_id": str(payload.get("run_id") or response_payload.get("run_id") or "").strip(),
        "generated_at": str(payload.get("generated_at") or response_payload.get("generated_at") or "").strip(),
        "latest_report_file": _file_content_cache_token(settings.latest_report_file),
        "latest_report_file_path": _project_relative_path_text(settings.latest_report_file),
        "publication_registry_file": _file_content_cache_token(settings.publication_registry_file),
        "publication_registry_file_path": _project_relative_path_text(settings.publication_registry_file),
        "latest_canonical_run_id": str((publication or {}).get("latest_canonical_run_id") or "").strip(),
        "current_published_run_id": str((publication or {}).get("current_published_run_id") or "").strip(),
        "latest_successful_run_id": str(latest_successful_run.get("run_id") or "").strip(),
        "latest_successful_is_currently_published": bool(
            publication_status.get("latest_successful_is_currently_published")
        ),
        "latest_successful_publish_hold": bool(publication_status.get("latest_successful_publish_hold")),
        "latest_successful_publish_blocked": bool(
            publication_status.get("latest_successful_publish_blocked")
        ),
        "current_published_effective_nav_date": str(
            current_published_snapshot.get("candidate_date")
            or publication_status.get("current_effective_nav_date")
            or ""
        ).strip(),
        "latest_successful_effective_nav_date": str(
            latest_successful_snapshot.get("candidate_date")
            or publication_status.get("latest_successful_effective_nav_date")
            or ""
        ).strip(),
        "publication_status": str(publication_status.get("status") or "").strip(),
        "latest_canonical_report_path": str((publication or {}).get("latest_canonical_report_path") or "").strip(),
        "current_published_report_path": str((publication or {}).get("current_published_report_path") or "").strip(),
        "final_count": counts.get("final_count"),
        "dynamic_final_count": counts.get("dynamic_final_count"),
        "portfolio_group_count": counts.get("portfolio_group_count"),
        "portfolio_fund_count": counts.get("portfolio_fund_count"),
        "summary_group_count": portfolio_summary.get("group_count"),
        "summary_fund_count": portfolio_summary.get("fund_count"),
        "summary_rows_omitted": "funds" not in response_payload and "rows" not in response_payload,
        "portfolio_group_funds_omitted": all(
            "funds" not in group
            for group in (
                portfolio_summary.get("groups")
                if isinstance(portfolio_summary.get("groups"), list)
                else []
            )
            if isinstance(group, dict)
        ),
        "omitted_fields": list(payload_scope.get("omitted_fields") or []),
        "report_artifacts": {
            "report_json": _report_artifact_token(report_json_artifact),
            "filtered_excel": _report_artifact_token(artifacts.get("filtered_excel")),
            "report_markdown": _report_artifact_token(artifacts.get("report_markdown")),
            "project_tracker_excel": _report_artifact_token(artifacts.get("project_tracker_excel")),
            "recommendations_detail_json": _report_artifact_token(
                artifacts.get("recommendations_detail_json") or artifacts.get("recommendations_detail")
            ),
            "project_tracker_current_detail_json": _report_artifact_token(tracker_detail_artifact),
        },
        "artifact_keys": sorted(str(key) for key in artifacts.keys()),
        "missing_value_policy": "preserve_missing_semantics",
        "business_value_synthesis": "forbidden",
        "truth_note": (
            "/api/recommendations/latest summary exposes a compact view derived from the current "
            "published/latest report payload and declared runtime artifacts. It intentionally "
            "omits fund rows and must not synthesize missing fund business values or treat "
            "frontend-rendered state as publication truth."
        ),
    }
    basis["digest"] = _stable_json_digest(basis)
    return basis


def _build_recommendations_summary_source_contract(source_basis: dict[str, Any]) -> dict[str, Any]:
    basis = dict(source_basis or {})
    return {
        "schema_version": _RECOMMENDATIONS_SUMMARY_SOURCE_CONTRACT_SCHEMA_VERSION,
        "source": "recommendations_summary_current_published_report_artifact",
        "truth_policy": "real_publication_registry_and_report_artifact_summary_only",
        "missing_value_policy": "preserve_missing_semantics",
        "business_value_synthesis": "forbidden",
        "official_publication_truth": "current_published_report_artifact",
        "frontend_rendered_truth_allowed": False,
        "cache_basis_schema_version": basis.get("schema_version"),
        "cache_basis_digest": basis.get("digest"),
    }


def _attach_recommendations_summary_source_contract(
    payload: dict[str, Any],
    response_payload: dict[str, Any],
) -> dict[str, Any]:
    response_payload = _attach_recommendations_publication_status(payload, response_payload)
    response_payload = _sync_publication_compat_fields(response_payload)
    source_basis = _build_recommendations_summary_source_basis(payload, response_payload)
    response_payload["source_basis"] = source_basis
    response_payload["cache_basis"] = dict(source_basis)
    response_payload["source_contract"] = _build_recommendations_summary_source_contract(source_basis)
    payload_scope = response_payload.get("payload_scope") if isinstance(response_payload.get("payload_scope"), dict) else {}
    response_payload["payload_scope"] = {
        **payload_scope,
        "source_contract": {
            "source": "recommendations_summary_current_published_report_artifact",
            "cache_basis_present": True,
            "source_basis_matches_cache_basis": True,
            "truth_policy": "real_publication_registry_and_report_artifact_summary_only",
            "missing_value_policy": "preserve_missing_semantics",
        },
    }
    return response_payload


def _build_recommendations_summary_response(payload: dict) -> dict:
    payload = recommendations_display_report(settings, payload)
    response_payload = recommendation_payloads.build_recommendations_summary_response(
        payload,
        settings=settings,
        normalize_portfolio_groups=_normalize_portfolio_groups,
        build_portfolio_group_brief=_build_portfolio_group_brief,
        build_publish_funnel_summary=_build_publish_funnel_summary,
    )
    response_payload["tracker_occurrence_basis"] = payload["tracker_occurrence_basis"]
    response_payload["formal_score_contract"] = build_formal_score_contract()
    return _attach_recommendations_summary_source_contract(payload, response_payload)


def _scope_recommendations_detail_response_by_group(
    response_payload: dict[str, Any],
    detail_group: str,
) -> dict[str, Any]:
    selected_group_name = str(detail_group or "").strip()
    if not selected_group_name:
        return response_payload

    portfolio_summary = (
        response_payload.get("portfolio_suggestions")
        if isinstance(response_payload.get("portfolio_suggestions"), dict)
        else {}
    )
    groups = [
        dict(item)
        for item in (portfolio_summary.get("groups") or [])
        if isinstance(item, dict)
    ]
    available_groups: list[dict[str, Any]] = []
    scoped_groups: list[dict[str, Any]] = []
    selected_funds: list[dict[str, Any]] = []
    selected_found = False
    for group in groups:
        group_name = str(group.get("name") or "").strip()
        group_funds = [
            dict(item)
            for item in (group.get("funds") or [])
            if isinstance(item, dict)
        ]
        group_count = int(group.get("count") or len(group_funds) or 0)
        group_summary = {
            "name": group_name,
            "description": group.get("description"),
            "count": group_count,
            "downgrade_info": group.get("downgrade_info") or {},
        }
        available_groups.append(group_summary)
        scoped_group = dict(group_summary)
        if group_name == selected_group_name:
            selected_found = True
            selected_funds = group_funds
            scoped_group["funds"] = group_funds
        scoped_groups.append(scoped_group)

    scoped_portfolio = dict(portfolio_summary)
    scoped_portfolio["groups"] = scoped_groups
    scoped_portfolio["funds"] = selected_funds
    scoped_portfolio["detail_group_name"] = selected_group_name
    scoped_portfolio["detail_group_found"] = selected_found
    scoped_portfolio["detail_group_fund_count"] = len(selected_funds)
    response_payload["portfolio_suggestions"] = scoped_portfolio
    response_payload["rows"] = selected_funds
    response_payload["funds"] = selected_funds
    counts = response_payload.get("counts")
    if isinstance(counts, dict):
        counts["detail_group_fund_count"] = len(selected_funds)
    selected_codes = {
        _normalize_code_text(item.get("基金代码"))
        for item in selected_funds
        if _normalize_code_text(item.get("基金代码"))
    }
    style_metric_references = response_payload.get("style_metric_references")
    if isinstance(style_metric_references, dict):
        response_payload["style_metric_references"] = {
            code: value
            for code, value in style_metric_references.items()
            if _normalize_code_text(code) in selected_codes
        }
    payload_scope = (
        response_payload.get("payload_scope")
        if isinstance(response_payload.get("payload_scope"), dict)
        else {}
    )
    response_payload["payload_scope"] = {
        **payload_scope,
        "detail_group_name": selected_group_name,
        "detail_group_found": selected_found,
        "detail_group_fund_count": len(selected_funds),
        "available_groups": available_groups,
    }
    return response_payload


def _normalize_recommendations_detail_runtime_scopes(
    response_payload: dict[str, Any],
    *,
    run_id: str | None,
) -> dict[str, Any]:
    normalized_run_id = str(run_id or "").strip() or None
    tracker_summary = (
        response_payload.get("tracker_summary")
        if isinstance(response_payload.get("tracker_summary"), dict)
        else {}
    )
    response_payload["tracker_summary"] = dashboard_payloads._build_tracker_summary_runtime_scope(
        tracker_summary,
        run_id=normalized_run_id,
    )
    aggregate_overrides = {
        "auxiliary_scope": "same_run_report_aggregate",
        "auxiliary_scope_label": "同 run 报告聚合",
    }
    portfolio_display_scope = (
        response_payload.get("portfolio_display_scope")
        if isinstance(response_payload.get("portfolio_display_scope"), dict)
        else {}
    )
    response_payload["portfolio_display_scope"] = dashboard_payloads._build_portfolio_display_scope_runtime_scope(
        {**portfolio_display_scope, **aggregate_overrides},
        run_id=normalized_run_id,
    )
    publish_layer_semantics = (
        response_payload.get("publish_layer_semantics_summary")
        if isinstance(response_payload.get("publish_layer_semantics_summary"), dict)
        else {}
    )
    response_payload["publish_layer_semantics_summary"] = (
        dashboard_payloads._build_publish_layer_semantics_runtime_scope(
            {**publish_layer_semantics, **aggregate_overrides},
            run_id=normalized_run_id,
        )
    )
    portfolio_summary = (
        response_payload.get("portfolio_suggestions")
        if isinstance(response_payload.get("portfolio_suggestions"), dict)
        else {}
    )
    portfolio_group_brief = (
        response_payload.get("portfolio_group_brief")
        if isinstance(response_payload.get("portfolio_group_brief"), dict)
        else {}
    )
    if portfolio_summary:
        portfolio_group_brief = _build_portfolio_group_brief(portfolio_summary)
    response_payload["portfolio_group_brief"] = dashboard_payloads._build_portfolio_group_brief_runtime_scope(
        {**portfolio_group_brief, **aggregate_overrides},
        run_id=normalized_run_id,
    )
    report_timing_summary = (
        response_payload.get("report_timing_summary")
        if isinstance(response_payload.get("report_timing_summary"), dict)
        else {}
    )
    response_payload["report_timing_summary"] = dashboard_payloads._build_report_timing_runtime_scope(
        {
            **report_timing_summary,
            "auxiliary_scope": "same_run_report_snapshot",
            "auxiliary_scope_label": "同 run 报告快照",
        },
        run_id=normalized_run_id,
    )
    low_vol_watch = (
        response_payload.get("low_vol_positive_ratio_shadow_watch")
        if isinstance(response_payload.get("low_vol_positive_ratio_shadow_watch"), dict)
        else {}
    )
    response_payload["low_vol_positive_ratio_shadow_watch"] = (
        dashboard_payloads._build_low_vol_positive_ratio_shadow_watch_runtime_scope(
            {**low_vol_watch, **aggregate_overrides},
            run_id=normalized_run_id,
        )
    )
    positive_ratio_semantics = (
        response_payload.get("positive_ratio_semantics")
        if isinstance(response_payload.get("positive_ratio_semantics"), dict)
        else {}
    )
    response_payload["positive_ratio_semantics"] = (
        dashboard_payloads._build_positive_ratio_semantics_runtime_scope(
            {**positive_ratio_semantics, **aggregate_overrides},
            run_id=normalized_run_id,
        )
    )
    counts = (
        response_payload.get("counts")
        if isinstance(response_payload.get("counts"), dict)
        else {}
    )
    generic_watch = (
        response_payload.get("generic_holding_placeholder_watch")
        if isinstance(response_payload.get("generic_holding_placeholder_watch"), dict)
        else {}
    )
    response_payload["counts"] = {
        **counts,
        "generic_holding_placeholder_excluded_count": int(
            counts.get("generic_holding_placeholder_excluded_count")
            or generic_watch.get("count")
            or 0
        ),
        "generic_holding_placeholder_buyable_count": int(
            counts.get("generic_holding_placeholder_buyable_count")
            or generic_watch.get("buyable_count")
            or 0
        ),
        "generic_holding_placeholder_high_score_count": int(
            counts.get("generic_holding_placeholder_high_score_count")
            or generic_watch.get("high_score_count")
            or 0
        ),
        "low_vol_positive_ratio_focus_count": int(
            counts.get("low_vol_positive_ratio_focus_count")
            or low_vol_watch.get("focus_sample_count")
            or 0
        ),
        "low_vol_positive_ratio_gate_hit_count": int(
            counts.get("low_vol_positive_ratio_gate_hit_count")
            or low_vol_watch.get("positive_ratio_gate_hit_count")
            or 0
        ),
        "low_vol_positive_ratio_gap_alert_count": int(
            counts.get("low_vol_positive_ratio_gap_alert_count")
            or low_vol_watch.get("positive_ratio_gap_alert_count")
            or 0
        ),
    }
    return response_payload


def _build_recommendations_detail_response(
    payload: dict,
    detail_group: str = "",
    *,
    audit_read_only: bool = False,
) -> dict:
    total_started_at = time.perf_counter()
    use_sidecar = _recommendations_detail_sidecar_enabled()
    cache_basis = _build_recommendations_detail_cache_basis(payload)
    sidecar_probe = (
        _probe_recommendations_detail_sidecar_status(payload, cache_basis)
        if use_sidecar
        else {
            "status": "disabled",
            "hit": False,
            "schema_version": _RECOMMENDATIONS_DETAIL_SIDECAR_SCHEMA_VERSION,
        }
    )
    load_started_at = time.perf_counter()
    response_payload = (
        _load_recommendations_detail_sidecar(payload, cache_basis)
        if use_sidecar
        else None
    )
    load_seconds = round(max(0.0, time.perf_counter() - load_started_at), 6)
    build_seconds = 0.0
    write_seconds = 0.0
    sidecar_write_status = "not_attempted"
    sidecar_path_text = sidecar_probe.get("sidecar_path")
    if response_payload is None:
        build_started_at = time.perf_counter()
        response_payload = attach_recommendations_detail_source_contract(
            build_run_local_recommendations_detail_response(settings, payload),
            cache_basis,
        )
        build_seconds = round(max(0.0, time.perf_counter() - build_started_at), 6)
        response_payload["payload_scope"] = {
            **(
                response_payload.get("payload_scope")
                if isinstance(response_payload.get("payload_scope"), dict)
                else {}
            ),
            "mode": "detail",
            "summary_endpoint": "/api/recommendations/latest",
            "detail_endpoint": "/api/recommendations/latest/detail",
            "group_parameter": "group",
        }
        if use_sidecar and not audit_read_only:
            write_started_at = time.perf_counter()
            sidecar_path = _write_recommendations_detail_sidecar(
                payload,
                cache_basis,
                response_payload,
            )
            write_seconds = round(max(0.0, time.perf_counter() - write_started_at), 6)
            if sidecar_path is not None:
                sidecar_write_status = "written"
                sidecar_path_text = _project_relative_path_text(sidecar_path)
                response_payload["payload_scope"] = {
                    **response_payload.get("payload_scope", {}),
                    _RECOMMENDATIONS_DETAIL_SIDECAR_META_LABEL: {
                        "source": "runtime_build_written",
                        "sidecar_path": sidecar_path_text,
                        "schema_version": _RECOMMENDATIONS_DETAIL_SIDECAR_SCHEMA_VERSION,
                    },
                }
            else:
                sidecar_write_status = "unwritten"
        elif use_sidecar and audit_read_only:
            sidecar_write_status = "audit_read_only"
            response_payload["payload_scope"] = {
                **response_payload.get("payload_scope", {}),
                _RECOMMENDATIONS_DETAIL_SIDECAR_META_LABEL: {
                    "source": "runtime_build_read_only",
                    "sidecar_path": sidecar_path_text,
                    "schema_version": _RECOMMENDATIONS_DETAIL_SIDECAR_SCHEMA_VERSION,
                },
            }
        else:
            sidecar_write_status = "disabled"
    else:
        sidecar_write_status = "hit"
    response_payload = overlay_recommendations_tracker_occurrences(settings, payload, response_payload)
    # Persisted detail files may predate the audit evidence contract. Revalidate
    # their explanation from the bound report without rewriting that revision.
    publish_summary = payload.get("project_publish_summary") or {}
    low_vol_watch = normalize_low_vol_positive_ratio_shadow_watch(publish_summary, report_identity=payload, settings=settings)
    response_payload["low_vol_positive_ratio_shadow_watch"] = low_vol_watch
    response_payload["positive_ratio_semantics"] = build_positive_ratio_semantics_summary(
        publish_summary, low_vol_watch,
    )
    response_payload["formal_score_contract"] = build_formal_score_contract()
    response_payload = attach_recommendations_detail_source_contract(response_payload, cache_basis)
    response_payload = _normalize_recommendations_detail_runtime_scopes(
        response_payload,
        run_id=str(response_payload.get("run_id") or payload.get("run_id") or "").strip() or None,
    )
    scope_started_at = time.perf_counter()
    response_payload = _scope_recommendations_detail_response_by_group(
        response_payload,
        detail_group,
    )
    scope_seconds = round(max(0.0, time.perf_counter() - scope_started_at), 6)
    payload_scope = (
        response_payload.get("payload_scope")
        if isinstance(response_payload.get("payload_scope"), dict)
        else {}
    )
    sidecar_meta = (
        payload_scope.get(_RECOMMENDATIONS_DETAIL_SIDECAR_META_LABEL)
        if isinstance(payload_scope.get(_RECOMMENDATIONS_DETAIL_SIDECAR_META_LABEL), dict)
        else {}
    )
    source = str(sidecar_meta.get("source") or "").strip()
    if not source:
        if response_payload is not None and bool(sidecar_probe.get("hit")) and build_seconds == 0.0:
            source = "run_local_recommendations_detail_sidecar"
        elif use_sidecar:
            source = "runtime_build_unwritten"
        else:
            source = "runtime_build_sidecar_disabled"
    response_payload["payload_scope"] = {
        **payload_scope,
        _RECOMMENDATIONS_DETAIL_DIAGNOSTICS_LABEL: {
            "source": source,
            "sidecar_enabled": bool(use_sidecar),
            "audit_read_only": bool(audit_read_only),
            "sidecar_status": sidecar_probe.get("status"),
            "sidecar_hit": bool(sidecar_probe.get("hit")) and build_seconds == 0.0,
            "sidecar_write_status": sidecar_write_status,
            "sidecar_path": sidecar_path_text,
            "schema_version": _RECOMMENDATIONS_DETAIL_SIDECAR_SCHEMA_VERSION,
            "load_seconds": load_seconds,
            "build_seconds": build_seconds,
            "write_seconds": write_seconds,
            "group_filter_seconds": scope_seconds,
            "total_seconds": round(max(0.0, time.perf_counter() - total_started_at), 6),
            "detail_group_requested": bool(str(detail_group or "").strip()),
            "detail_group_name": payload_scope.get("detail_group_name"),
            "detail_group_found": payload_scope.get("detail_group_found"),
            "detail_group_fund_count": payload_scope.get("detail_group_fund_count"),
            "available_group_count": len(payload_scope.get("available_groups") or [])
            if isinstance(payload_scope.get("available_groups"), list)
            else 0,
        },
    }
    response_payload = _attach_recommendations_publication_status(payload, response_payload)
    response_payload = _sync_publication_compat_fields(response_payload)
    response_payload = _attach_recommendations_detail_publication_source_contract(response_payload)
    return response_payload


def _favorite_action_from_level(level: str, risk_text: str) -> str:
    normalized_level = str(level or "").strip()
    risk = str(risk_text or "").strip()
    if normalized_level == "优质推荐池":
        if "持有期限制未确认" in risk:
            return "优先持有并观察"
        return "优先持有"
    if normalized_level == "正式主榜":
        if "持有期限制未确认" in risk:
            return "继续持有观察"
        return "可继续持有"
    if normalized_level == "自动诊断池":
        return "建议复核是否调仓"
    if normalized_level == "持有期排除":
        return "建议评估卖出"
    if normalized_level == "项目外样本":
        return "仅供信息参考"
    return "待判断"


def _favorite_digest_from_row(row: dict[str, Any]) -> str:
    parts: list[str] = []
    level = str(row.get("当前层级") or "").strip()
    conclusion = str(row.get("项目推荐结论") or "").strip()
    confidence = str(row.get("项目质量信心") or "").strip()
    stability = str(row.get("长期稳定性标签") or "").strip()
    risk = str(row.get("风险提示") or "").strip()
    reason = str(row.get("推荐理由") or row.get("入选原因") or "").strip()
    autostock = str(validation_status_value(row) or "").strip()

    if level:
        parts.append(f"层级：{level}")
    if conclusion:
        parts.append(f"结论：{conclusion}")
    if confidence:
        parts.append(f"信心：{confidence}")
    if stability:
        parts.append(f"长期：{stability}")
    if autostock:
        parts.append(f"校验：{autostock}")
    if reason:
        compact_reason = "；".join(
            part.strip()
            for part in str(reason).replace(";", "；").split("；")
            if part.strip()
        )
        if compact_reason:
            parts.append(f"理由：{'；'.join(compact_reason.split('；')[:3])}")
    if risk:
        parts.append(f"风险：{risk}")
    return " | ".join(parts)


def _normalize_favorite_row(row: dict[str, Any], *, level: str) -> dict[str, Any]:
    normalized = dict(row)
    normalized["基金代码"] = _normalize_code_text(row.get("基金代码"))
    normalized["基金名称"] = str(row.get("基金名称") or "").strip()
    normalized["基金经理"] = str(row.get("基金经理") or "").strip()
    normalized["基金类型"] = str(row.get("基金类型") or row.get("基金类别") or "").strip()
    normalized["当前层级"] = level
    normalized["正式投资评分"] = _normalize_number_text(row.get("正式投资评分"), digits=4)
    normalized["投资优先池得分"] = _normalize_number_text(row.get("投资优先池得分"), digits=4)
    normalized["基金规模(亿)"] = _normalize_number_text(row.get("基金规模(亿)"), digits=2)
    normalized["费率合计(%)"] = _normalize_number_text(row.get("费率合计(%)"), digits=2)
    normalized["费率筛选值(%)"] = _normalize_number_text(row.get("费率筛选值(%)"), digits=2)
    normalized["费率下界(%)"] = _normalize_number_text(row.get("费率下界(%)"), digits=2)
    normalized["费率口径"] = str(row.get("费率口径") or "").strip()
    normalized["费率筛选就绪"] = fee_semantics_ready(row)
    normalized["出现次数"] = int(_to_float(row.get("出现次数")) or 0)
    normalized["出现频率(%)"] = _normalize_number_text(row.get("出现频率(%)"), digits=2)
    normalized["出现次数&出现频率(%)"] = _format_tracker_occurrence_text(
        normalized.get("出现次数"),
        normalized.get("出现频率(%)"),
    )
    for key in [
        "1年-年化收益(%)",
        "平台成立以来收益年化(%)",
    "累计净值比年化(%)",
        "近1周(%)",
        "近1月(%)",
        "近3月(%)",
        "1年-夏普比率",
        "1年-最大回撤(%)",
        "1年-波动率",
        "实时估值",
        "实时估值时间",
        "实时估算净值",
        "最新净值日期",
        "最新单位净值",
        "正式榜分组",
        "债基风格标签",
        "基金公司",
        "项目质量信心",
        "长期稳定性标签",
        "AutoStock原始校验状态",
        "持有期状态",
        "项目推荐结论",
        "推荐理由",
        "风险提示",
    ]:
        normalized[key] = str(row.get(key) or "").strip()

    if not normalized["风险提示"] and level == "自动诊断池":
        normalized["风险提示"] = str(row.get("原因分组汇总") or row.get("主原因分组") or "").strip()
    if not normalized["风险提示"] and level == "持有期排除":
        normalized["风险提示"] = str(row.get("排除原因") or "持有期超过项目门槛").strip()
    normalized["建议动作"] = _favorite_action_from_level(level, normalized["风险提示"])
    normalized["推荐解读"] = _favorite_digest_from_row(normalized)
    return normalized


def _fetch_favorite_fund_database_snapshot(codes: list[str]) -> dict[str, dict[str, Any]]:
    from backend.risk_metric_status import prepare_risk_display_row

    normalized_codes = []
    seen: set[str] = set()
    for code in codes:
        normalized = _normalize_code_text(code)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        normalized_codes.append(normalized)
    if not normalized_codes:
        return {}

    conn = sqlite3.connect(str(settings.database_path))
    try:
        placeholders = ",".join("?" for _ in normalized_codes)
        risk_ready_predicate = _risk_metric_ready_predicate(conn, "rm")
        latest_risk_rowid_predicate = _latest_risk_rowid_predicate("rm", "rm_latest")
        risk_columns = {record[1] for record in conn.execute("PRAGMA table_info(fund_risk_metrics)")}
        fund_columns = {record[1] for record in conn.execute("PRAGMA table_info(funds)")}
        holding_flag_select = (
            'f.has_holding_period AS "是否有持有期限制"'
            if "has_holding_period" in fund_columns
            else 'NULL AS "是否有持有期限制"'
        )
        holding_source_select = (
            'f.holding_period_source AS "持有期来源"'
            if "holding_period_source" in fund_columns
            else 'NULL AS "持有期来源"'
        )
        fullname_select = 'f.fullname AS "基金全称"' if "fullname" in fund_columns else 'NULL AS "基金全称"'
        risk_reason_select = (
            'rm.undefined_metric_reasons_json AS "__risk_undefined_reasons_json"'
            if "undefined_metric_reasons_json" in risk_columns else 'NULL AS "__risk_undefined_reasons_json"'
        )
        risk_points_select = 'rm.data_points AS "__risk_data_points"' if "data_points" in risk_columns else 'NULL AS "__risk_data_points"'
        return_columns = {record[1] for record in conn.execute("PRAGMA table_info(fund_returns)")}
        return_provenance = {
            "period_return_date": "区间收益基准日", "period_return_observed_at": "区间收益获取时间",
            "period_return_source": "区间收益来源", "period_return_source_sha256": "区间收益来源SHA256",
        }
        return_provenance_select = ", ".join(
            f'fr.{field} AS "{label}"' if field in return_columns else f'NULL AS "{label}"'
            for field, label in return_provenance.items()
        )
        query = f"""
        SELECT
            f.fcode AS "基金代码",
            f.shortname AS "基金名称",
            {fullname_select},
            f.company_name AS "基金公司",
            f.ftype AS "基金类型",
            f.risk_level AS "风险等级",
            f.estab_date AS "成立日期",
            f.scale_billion AS "基金规模(亿)",
            {holding_flag_select},
            f.holding_days AS "持有期天数",
            {holding_source_select},
            f.holding_keywords AS "持有期关键词",
            f.purchase_status AS "申购状态",
            f.is_buyable AS "是否可申购",
            f.mgr_fee_rate,
            f.trust_fee_rate,
            f.sales_fee_rate,
            f.benchmark,
            fr.daily_growth AS "最新日增长率(%)",
            fr.latest_nav_date AS "最新净值日期",
            fr.latest_nav AS "最新单位净值",
            fr.return_1w AS "近1周(%)",
            fr.return_1m AS "近1月(%)",
            fr.return_3m AS "近3月(%)",
            fr.return_6m AS "近6月(%)",
            fr.return_1y AS "近1年(%)",
            fr.return_2y AS "近2年(%)",
            fr.return_3y AS "近3年(%)",
            fr.return_ytd AS "今年来(%)",
            fr.return_since AS "成立来(%)",
            {return_provenance_select},
            rm.volatility AS "1年-波动率",
            rm.max_drawdown AS "1年-最大回撤(%)",
            rm.annual_return AS "1年-年化收益(%)",
            COALESCE(rm.raw_sharpe_ratio, rm.sharpe_ratio) AS "1年-夏普比率",
            COALESCE(rm.raw_sortino_ratio, rm.sortino_ratio) AS "1年-索提诺比率",
            rm.calmar_ratio AS "1年-卡玛比率",
            rm.var_95 AS "1年-VaR95(%)",
            rm.var_99 AS "1年-VaR99(%)",
            rm.positive_ratio AS "1年-正收益比率",
            rm.end_date AS "__risk_end_date",
            CASE WHEN {risk_ready_predicate} THEN 1 ELSE 0 END AS "__risk_snapshot_ready",
            {risk_reason_select},
            {risk_points_select}
        FROM funds f
        LEFT JOIN fund_returns fr ON fr.fcode = f.fcode
        LEFT JOIN fund_risk_metrics rm
          ON rm.fcode = f.fcode
         AND rm.period = '1y'
         AND {latest_risk_rowid_predicate}
        WHERE f.fcode IN ({placeholders})
        """
        frame = pd.read_sql_query(query, conn, params=normalized_codes)
        from utils.period_return_snapshot import has_dated_period_snapshot
        dated = frame.apply(lambda row: has_dated_period_snapshot({
            field: row.get(label) for field, label in return_provenance.items()
        }), axis=1) if not frame.empty else None
        if dated is not None:
            for column in ("近1周(%)", "近1月(%)", "近3月(%)", "近6月(%)", "近1年(%)",
                           "近2年(%)", "近3年(%)", "今年来(%)", "成立来(%)"):
                frame.loc[~dated, column] = None
    finally:
        conn.close()

    sales_fee_validation_map = load_sales_fee_validation_classification_map()
    result: dict[str, dict[str, Any]] = {}
    for source_row in frame.fillna("").to_dict(orient="records"):
        row = prepare_risk_display_row(source_row) if "__risk_snapshot_ready" in source_row else source_row
        code = _normalize_code_text(row.get("基金代码"))
        if not code:
            continue
        fee_semantics = evaluate_fee_semantics(
            fcode=code,
            mgr_fee_rate=row.get("mgr_fee_rate"),
            trust_fee_rate=row.get("trust_fee_rate"),
            sales_fee_rate=row.get("sales_fee_rate"),
            validation_map=sales_fee_validation_map,
        )
        holding_days = _to_float(row.get("持有期天数"))
        holding_status = resolve_holding_period_status(
            has_holding_period=int(_to_float(row.get("是否有持有期限制")) or 0),
            holding_days=(int(holding_days) if holding_days is not None and holding_days.is_integer() else None),
            threshold_days=int(settings.exclude_holding_days_gt),
            holding_period_source=row.get("持有期来源"),
            fund_name=combine_holding_period_names(row.get("基金名称"), row.get("基金全称")),
            holding_keywords=row.get("持有期关键词"),
        )
        result[code] = {
            "基金代码": code,
            "基金名称": str(row.get("基金名称") or "").strip(),
            "基金公司": str(row.get("基金公司") or "").strip(),
            "基金类型": str(row.get("基金类型") or "").strip(),
            "基金规模(亿)": _normalize_number_text(row.get("基金规模(亿)"), digits=2),
            "费率合计(%)": _normalize_number_text(fee_semantics["exact_total"], digits=2),
            "费率筛选值(%)": _normalize_number_text(fee_semantics["screening_value"], digits=2),
            "费率下界(%)": _normalize_number_text(fee_semantics["lower_bound"], digits=2),
            "费率口径": str(fee_semantics["status"]),
            "费率筛选就绪": bool(fee_semantics["screening_ready"]),
            "最新单位净值": _normalize_number_text(row.get("最新单位净值"), digits=4),
            "最新净值日期": str(row.get("最新净值日期") or "").strip(),
            "最新日增长率(%)": _normalize_number_text(row.get("最新日增长率(%)"), digits=2),
            "近1周(%)": _normalize_number_text(row.get("近1周(%)"), digits=2),
            "近1月(%)": _normalize_number_text(row.get("近1月(%)"), digits=2),
            "近3月(%)": _normalize_number_text(row.get("近3月(%)"), digits=2),
            "近6月(%)": _normalize_number_text(row.get("近6月(%)"), digits=2),
            "近1年(%)": _normalize_number_text(row.get("近1年(%)"), digits=2),
            "1年-波动率": _normalize_number_text(row.get("1年-波动率"), digits=4),
            "1年-最大回撤(%)": _normalize_number_text(row.get("1年-最大回撤(%)"), digits=4),
            "1年-年化收益(%)": _normalize_number_text(row.get("1年-年化收益(%)"), digits=2),
            "1年-夏普比率": _normalize_number_text(row.get("1年-夏普比率"), digits=4),
            "持有期状态": holding_status,
            "持有期来源": str(row.get("持有期来源") or "").strip(),
        }
        if row.get("__risk_metric_unavailability"):
            result[code]["__risk_metric_unavailability"] = row["__risk_metric_unavailability"]
    return result


def _overlay_favorite_row_with_database_snapshot(
    row: dict[str, Any],
    db_snapshot: dict[str, Any] | None,
) -> dict[str, Any]:
    if not db_snapshot:
        return dict(row)

    from backend.risk_metric_status import overlay_risk_unavailability

    # Match against the row's original date. Filling a missing date first would
    # manufacture evidence that a historical value belongs to this window.
    normalized = overlay_risk_unavailability(row, db_snapshot)
    for key in [
        "基金名称",
        "基金公司",
        "基金类型",
        "基金规模(亿)",
        "费率合计(%)",
        "费率筛选值(%)",
        "费率下界(%)",
        "费率口径",
        "最新单位净值",
        "最新净值日期",
        "近1周(%)",
        "近1月(%)",
        "近3月(%)",
        "1年-年化收益(%)",
        "1年-夏普比率",
        "1年-最大回撤(%)",
        "1年-波动率",
        "持有期状态",
        "持有期来源",
    ]:
        current_value = normalized.get(key)
        next_value = db_snapshot.get(key)
        current_text = "" if current_value is None else str(current_value).strip()
        next_text = "" if next_value is None else str(next_value).strip()
        if not current_text and next_text:
            normalized[key] = next_text
    if "费率筛选就绪" not in normalized and "费率筛选就绪" in db_snapshot:
        normalized["费率筛选就绪"] = bool(db_snapshot.get("费率筛选就绪"))
    return normalized


def _attach_since_inception_annual_return_breakdown_rows(
    rows_by_code: dict[str, dict[str, Any]],
    codes: list[str],
) -> None:
    breakdown_by_code = _load_since_inception_annual_return_breakdown_map(codes)
    for code, row in rows_by_code.items():
        row[_SINCE_INCEPTION_ANNUAL_RETURN_BREAKDOWN_LABEL] = breakdown_by_code.get(code) or []


def _load_favorite_fund_score_dataset(payload: dict) -> dict[str, dict[str, Any]]:
    payload = recommendations_display_report(settings, payload)
    artifacts = payload.get("artifacts") or {}
    result: dict[str, dict[str, Any]] = {}
    occurrence_by_code = _load_tracker_occurrence_map(payload)
    portfolio_summary = _normalize_portfolio_groups(((payload.get("project_publish_summary") or {}).get("portfolio_suggestions") or {}))
    portfolio_by_code: dict[str, dict[str, Any]] = {}
    for group in (portfolio_summary.get("groups") or []):
        for fund in (group.get("funds") or []):
            code = _normalize_code_text(fund.get("基金代码"))
            if code:
                portfolio_by_code[code] = dict(fund)

    filtered_file = artifacts.get("filtered_excel")
    if filtered_file:
        filtered_path = _resolve_existing_report_artifact_path(
            filtered_file,
            fallback_glob_patterns=["正式推荐债券基金_[0-9]*.xlsx"],
        )
        if filtered_path and filtered_path.exists():
            main_df = _read_excel_cached(filtered_path, sheet_name="Sheet1")
            reason_df = _read_excel_cached(filtered_path, sheet_name="推荐理由与风格")
            merged_df = main_df.merge(
                reason_df,
                on=["基金代码", "基金名称", "正式投资评分", "持有期状态", "正式榜分组"],
                how="left",
                suffixes=("", "_detail"),
            )
            tracker_dataset = _load_tracker_dataset(payload)
            tracker_by_code = {
                _normalize_code_text(item.get("基金代码")): item
                for item in (tracker_dataset.get("tracker_records") or [])
                if _normalize_code_text(item.get("基金代码"))
            }
            for row in merged_df.fillna("").astype(str).to_dict(orient="records"):
                code = _normalize_code_text(row.get("基金代码"))
                if not code:
                    continue
                tracker_row = tracker_by_code.get(code, {})
                portfolio_row = portfolio_by_code.get(code, {})
                normalized = _normalize_favorite_row(
                    {
                        **row,
                        "投资优先池得分": portfolio_row.get("投资优先池得分") or row.get("投资优先池得分") or "",
                        "风险提示": portfolio_row.get("风险提示") or row.get("质量边际说明") or "",
                        "推荐理由": portfolio_row.get("入选原因") or row.get("推荐理由") or row.get("推荐备注") or "",
                        "项目质量信心": portfolio_row.get("项目质量信心") or row.get("项目质量信心") or "",
                        "AutoStock原始校验状态": portfolio_row.get("AutoStock原始校验状态") or row.get("AutoStock原始校验状态") or "",
                        "长期稳定性标签": portfolio_row.get("长期稳定性标签") or row.get("长期稳定性标签") or "",
                        "出现次数": tracker_row.get("出现次数") or (occurrence_by_code.get(code) or {}).get("出现次数") or "",
                        "出现频率(%)": tracker_row.get("出现频率(%)") or (occurrence_by_code.get(code) or {}).get("出现频率(%)") or "",
                    },
                    level="优质推荐池" if code in portfolio_by_code else "正式主榜",
                )
                result[code] = normalized

    watch_file = artifacts.get("watch_excel")
    if watch_file:
        watch_path = _resolve_existing_report_artifact_path(
            watch_file,
            fallback_glob_patterns=["自动诊断池_债券基金_[0-9]*.xlsx"],
        )
        if watch_path and watch_path.exists():
            watch_df = _read_excel_cached(watch_path, sheet_name="Sheet1")
            for row in watch_df.fillna("").astype(str).to_dict(orient="records"):
                code = _normalize_code_text(row.get("基金代码"))
                if not code or code in result:
                    continue
                normalized = _normalize_favorite_row(
                    {
                        **row,
                        "出现次数": (occurrence_by_code.get(code) or {}).get("出现次数") or "",
                        "出现频率(%)": (occurrence_by_code.get(code) or {}).get("出现频率(%)") or "",
                    },
                    level="自动诊断池",
                )
                result[code] = normalized

    excluded_file = artifacts.get("excluded_excel")
    if excluded_file:
        excluded_path = _resolve_existing_report_artifact_path(
            excluded_file,
            fallback_glob_patterns=["当日最终优质基金_持有期排除清单_[0-9]*.xlsx"],
        )
        if excluded_path and excluded_path.exists():
            excluded_df = _read_excel_cached(excluded_path, sheet_name="Sheet1")
            for row in excluded_df.fillna("").astype(str).to_dict(orient="records"):
                code = _normalize_code_text(row.get("基金代码"))
                if not code or code in result:
                    continue
                normalized = _normalize_favorite_row(
                    {
                        **row,
                        "出现次数": (occurrence_by_code.get(code) or {}).get("出现次数") or "",
                        "出现频率(%)": (occurrence_by_code.get(code) or {}).get("出现频率(%)") or "",
                    },
                    level="持有期排除",
                )
                result[code] = normalized

    return result


def _load_favorite_recommendation_pool_codes(payload: dict) -> list[str]:
    publish_summary = payload.get("project_publish_summary") or {}
    portfolio_summary = _normalize_portfolio_groups(publish_summary.get("portfolio_suggestions") or {})
    result: list[str] = []
    seen: set[str] = set()
    for group in (portfolio_summary.get("groups") or []):
        for fund in (group.get("funds") or []):
            code = _normalize_code_text(fund.get("基金代码"))
            if not code or code in seen:
                continue
            seen.add(code)
            result.append(code)
    return result


def _report_artifact_cache_basis(raw_path: object, *, fallback_glob_patterns: list[str] | None = None) -> dict[str, Any]:
    resolved_path = _resolve_existing_report_artifact_path(
        raw_path,
        fallback_glob_patterns=fallback_glob_patterns,
    )
    return {
        "path": _project_relative_path_text(resolved_path),
        "token": _path_cache_token(resolved_path),
    }


def _build_favorite_score_source_basis(
    payload: dict[str, Any],
    *,
    user_codes: list[str],
    normalized_codes: list[str],
    recommendation_pool_codes: list[str],
    include_details: bool,
    save_history: bool,
    payload_mode: str,
    autostock_auxiliary_meta: dict[str, Any],
    include_recommendation_pool: bool = True,
    include_autostock: bool = True,
) -> dict[str, Any]:
    artifacts = payload.get("artifacts") if isinstance(payload.get("artifacts"), dict) else {}
    tracker_detail_path = _resolve_existing_report_artifact_path(
        artifacts.get("project_tracker_current_detail_json")
        or artifacts.get("run_local_project_tracker_current_detail_json")
    )
    basis: dict[str, Any] = {
        "schema_version": FAVORITE_SCORE_SOURCE_BASIS_SCHEMA_VERSION,
        "source": "favorite_fund_score_runtime_query",
        "run_id": str(payload.get("run_id") or "").strip(),
        "generated_at": str(payload.get("generated_at") or "").strip(),
        "latest_report_file": _file_content_cache_token(settings.latest_report_file),
        "latest_report_file_path": _project_relative_path_text(settings.latest_report_file),
        "database_file": _sqlite_file_cache_token(settings.database_path),
        "database_file_path": _project_relative_path_text(settings.database_path),
        "requested_codes": list(user_codes),
        "response_codes": list(normalized_codes),
        "recommendation_pool_codes": list(recommendation_pool_codes),
        "recommendation_pool_code_count": len(recommendation_pool_codes),
        "auto_appended_recommendation_count": len(
            [code for code in recommendation_pool_codes if code not in set(user_codes)]
            if include_recommendation_pool
            else []
        ),
        "payload_mode": payload_mode,
        "detail_included": bool(include_details),
        "recommendation_pool_included": bool(include_recommendation_pool),
        "autostock_included": bool(include_autostock),
        "save_history": bool(save_history),
        "report_artifacts": {
            "filtered_excel": _report_artifact_cache_basis(
                artifacts.get("filtered_excel"),
                fallback_glob_patterns=["正式推荐债券基金_[0-9]*.xlsx"],
            ),
            "watch_excel": _report_artifact_cache_basis(
                artifacts.get("watch_excel"),
                fallback_glob_patterns=["自动诊断池_债券基金_[0-9]*.xlsx"],
            ),
            "excluded_excel": _report_artifact_cache_basis(
                artifacts.get("excluded_excel"),
                fallback_glob_patterns=["当日最终优质基金_持有期排除清单_[0-9]*.xlsx"],
            ),
            "project_tracker_excel": _report_artifact_cache_basis(artifacts.get("project_tracker_excel")),
            "project_tracker_current_detail_json": {
                "path": _project_relative_path_text(tracker_detail_path),
                "token": _path_cache_token(tracker_detail_path),
            },
        },
        "autostock_auxiliary": {
            "status": str(autostock_auxiliary_meta.get("status") or "").strip(),
            "source": str(autostock_auxiliary_meta.get("source") or "").strip(),
            "request_source": str(autostock_auxiliary_meta.get("request_source") or "").strip(),
            "requested_count": int(autostock_auxiliary_meta.get("requested_count") or 0),
            "returned_count": int(autostock_auxiliary_meta.get("returned_count") or 0),
            "upstream_requested_count": int(autostock_auxiliary_meta.get("upstream_requested_count") or 0),
            "upstream_returned_count": int(autostock_auxiliary_meta.get("upstream_returned_count") or 0),
            "quota_unavailable": bool(autostock_auxiliary_meta.get("quota_unavailable")),
            "quota_exhausted": bool(autostock_auxiliary_meta.get("quota_exhausted")),
            "quota_reserved_for_automation": int(
                autostock_auxiliary_meta.get("quota_reserved_for_automation") or 0
            ),
            "max_request_codes": int(autostock_auxiliary_meta.get("max_request_codes") or 0),
        },
        "source_note": (
            "收藏评分响应混合使用最新主报告产物、项目 SQLite 数据库和来源元数据标识的 API 最新快照；"
            "平台成立以来收益年化仅使用 SYL_LN、成立日和区间收益来源日期；"
            "累计净值比年化另列且不替代平台业绩；两者缺失均不回退到另一口径；"
            "缺失业务值不由前端或接口合成 0、通过状态或虚假基金数据。"
        ),
    }
    basis["digest"] = _stable_json_digest(basis)
    return basis


def _build_favorite_score_source_contract(source_basis: dict[str, Any]) -> dict[str, Any]:
    basis = dict(source_basis or {})
    return {
        "schema_version": FAVORITE_SCORE_SOURCE_CONTRACT_SCHEMA_VERSION,
        "source": "favorite_fund_score_runtime_query",
        "truth_policy": "real_report_artifact_sqlite_database_and_optional_provider_snapshot_only",
        "snapshot_provider": (basis.get("autostock_auxiliary") or {}).get("source"),
        "snapshot_is_publication_validation": False,
        "missing_value_policy": "preserve_missing_semantics",
        "business_value_synthesis": "forbidden",
        "official_publication_truth": "false_favorites_diagnostics_only",
        "frontend_rendered_truth_allowed": False,
        "cache_basis_schema_version": basis.get("schema_version"),
        "cache_basis_digest": basis.get("digest"),
    }


def _normalize_favorite_style_threshold_entry(entry: dict[str, Any]) -> dict[str, Any]:
    normalized = {
        field: entry.get(field, "")
        for field in _FAVORITE_STYLE_THRESHOLD_FIELDS
    }
    normalized["债基风格标签"] = str(normalized.get("债基风格标签") or "").strip()
    normalized["阈值来源"] = str(normalized.get("阈值来源") or "").strip()
    return normalized


def _extract_style_threshold_payload(
    payload: dict[str, Any],
    rows: list[dict[str, Any]],
) -> dict[str, Any]:
    threshold_rows = (
        (
            (payload.get("project_publish_summary") or {}).get("threshold_comparison")
            or {}
        ).get("style_thresholds")
        or []
    )
    if not isinstance(threshold_rows, list) or not threshold_rows:
        return {
            "styles": [],
            "global": {},
            "missing_styles": [],
        }

    style_order: list[str] = []
    style_entries: dict[str, dict[str, Any]] = {}
    global_entry: dict[str, Any] = {}
    for item in threshold_rows:
        if not isinstance(item, dict):
            continue
        normalized = _normalize_favorite_style_threshold_entry(item)
        style = str(normalized.get("债基风格标签") or "").strip()
        if not style:
            continue
        if style == "全局基准":
            global_entry = normalized
            continue
        style_entries[style] = normalized
        style_order.append(style)

    relevant_style_set = {
        str(row.get("债基风格标签") or "").strip()
        for row in rows
        if str(row.get("债基风格标签") or "").strip()
    }
    ordered_styles = [style for style in style_order if style in relevant_style_set]
    missing_styles = sorted(
        style for style in relevant_style_set
        if style not in style_entries
    )
    return {
        "styles": [style_entries[style] for style in ordered_styles],
        "global": global_entry,
        "missing_styles": missing_styles,
    }


def _extract_favorite_style_threshold_payload(
    payload: dict[str, Any],
    rows: list[dict[str, Any]],
) -> dict[str, Any]:
    return _extract_style_threshold_payload(payload, rows)


def _fetch_project_universe_fund_snapshot(codes: list[str]) -> dict[str, dict[str, Any]]:
    snapshot_rows = _fetch_favorite_fund_database_snapshot(codes)
    result: dict[str, dict[str, Any]] = {}
    for code, snapshot_row in snapshot_rows.items():
        normalized = _normalize_favorite_row(
            {
                **snapshot_row,
                "项目推荐结论": "项目外样本",
                "风险提示": "当前最新主版本未收录该基金的项目评分结果",
            },
            level="项目外样本",
        )
        normalized["建议动作"] = "仅供信息参考"
        result[code] = normalized
    return result


def _build_favorite_fund_scores_response(
    payload: dict,
    codes: list[str],
    *,
    label: str | None = None,
    save_history: bool = True,
    include_details: bool = False,
    include_recommendation_pool: bool = True,
    include_autostock: bool = True,
) -> dict[str, Any]:
    user_codes = []
    seen: set[str] = set()
    for code in codes:
        normalized = _normalize_code_text(code)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        user_codes.append(normalized)
    if not user_codes:
        return {"error": "缺少基金代码"}

    # 正式推荐池始终用于判断用户输入代码是否“共同命中”。
    # include_recommendation_pool 只控制是否把非用户输入的推荐基金追加到响应表格，
    # 不得同时关闭推荐池成员识别，否则 local-first 查询会把全部输入误标为“用户输入”。
    recommendation_pool_codes = _load_favorite_recommendation_pool_codes(payload)
    appended_recommendation_pool_codes = (
        recommendation_pool_codes
        if include_recommendation_pool
        else []
    )
    normalized_codes: list[str] = []
    merged_seen: set[str] = set()
    for code in [*user_codes, *appended_recommendation_pool_codes]:
        if not code or code in merged_seen:
            continue
        merged_seen.add(code)
        normalized_codes.append(code)

    user_code_set = set(user_codes)
    recommendation_code_set = set(recommendation_pool_codes)

    payload_rows = _load_favorite_fund_score_dataset(payload)
    database_rows = _fetch_favorite_fund_database_snapshot(normalized_codes)
    since_inception_annualized_return_by_code = _compute_since_inception_annualized_return_map(normalized_codes)
    accum_nav_ratio_by_code = _compute_accum_nav_ratio_annualized_return_map(normalized_codes)
    lifetime_observations = load_platform_lifetime_returns(settings.database_path, normalized_codes)
    annual_breakdown_by_code = _load_since_inception_annual_return_breakdown_map(normalized_codes)
    annual_volatility_by_code = _compute_since_inception_annual_return_volatility_map(annual_breakdown_by_code)
    for code, row in list(payload_rows.items()):
        normalized_row = _overlay_favorite_row_with_database_snapshot(row, database_rows.get(code))
        normalized_row["累计净值比年化(%)"] = accum_nav_ratio_by_code.get(code, "")
        normalized_row["成立以来收益口径"] = lifetime_observations.get(code) or {"status": "missing_input"}
        if not str(normalized_row.get(_SINCE_INCEPTION_ANNUALIZED_RETURN_LABEL) or "").strip():
            normalized_row[_SINCE_INCEPTION_ANNUALIZED_RETURN_LABEL] = (
                since_inception_annualized_return_by_code.get(code) or ""
            )
        normalized_row[_SINCE_INCEPTION_ANNUAL_RETURN_VOLATILITY_LABEL] = annual_volatility_by_code.get(code) or ""
        payload_rows[code] = normalized_row
    missing_codes = [code for code in normalized_codes if code not in payload_rows]
    payload_rows.update(_fetch_project_universe_fund_snapshot(missing_codes))
    for code, row in payload_rows.items():
        row[_SINCE_INCEPTION_ANNUAL_RETURN_BREAKDOWN_LABEL] = annual_breakdown_by_code.get(code) or []
        row[_SINCE_INCEPTION_ANNUAL_RETURN_VOLATILITY_LABEL] = (
            str(row.get(_SINCE_INCEPTION_ANNUAL_RETURN_VOLATILITY_LABEL) or "").strip()
            or annual_volatility_by_code.get(code)
            or ""
        )
    # 收藏页每次查询都对最终表格全集执行实时 API 覆盖，包括用户输入基金与
    # 自动追加的推荐池基金；配额不足或上游未返回时保留明确缺失语义。
    autostock_target_codes = normalized_codes
    autostock_rows = (
        _fetch_favorite_autostock_snapshot(autostock_target_codes)
        if include_autostock
        else FavoriteAutostockSnapshot(
            {},
            meta={
                "status": "deferred",
                "source": "autostock_batch",
                "request_source": "favorites_score",
                "requested_count": len(autostock_target_codes),
                "returned_count": 0,
                "upstream_requested_count": 0,
                "upstream_returned_count": 0,
                "quota_unavailable": False,
                "quota_exhausted": False,
                "quota_reserved_for_automation": 0,
                "max_request_codes": _FAVORITE_AUTOSTOCK_AUXILIARY_MAX_REQUEST_CODES,
                "deferred": True,
            },
        )
    )
    autostock_auxiliary_meta = dict(getattr(autostock_rows, "autostock_budget_meta", {}) or {})

    rows: list[dict[str, Any]] = []
    unresolved_user_codes: list[str] = []
    for index, code in enumerate(normalized_codes, start=1):
        in_user_input = code in user_code_set
        in_recommendation_pool = code in recommendation_code_set
        if in_user_input and in_recommendation_pool:
            source_label = "共同命中"
        elif in_user_input:
            source_label = "用户输入"
        else:
            source_label = "优质推荐池"

        row = payload_rows.get(code)
        if row is None:
            if in_user_input:
                unresolved_user_codes.append(code)
            autostock_row = autostock_rows.get(code)
            autostock_type = str((autostock_row or {}).get("type") or "").strip()
            if autostock_type and "债" not in autostock_type:
                unresolved_risk = f"基金类型为{autostock_type}，不属于当前债基评分项目"
                unresolved_action = "仅供信息参考"
            elif autostock_type:
                unresolved_risk = f"基金类型为{autostock_type}，但当前未纳入本地债基评分数据库"
                unresolved_action = "待判断"
            else:
                unresolved_risk = "基金代码未命中当前项目数据"
                unresolved_action = "无法评估"
            unresolved_row = _overlay_favorite_row_with_live_autostock(
                {
                    "基金代码": code,
                    "基金名称": "",
                    "当前层级": "未收录",
                    "建议动作": unresolved_action,
                    "出现次数&出现频率(%)": "- 次 / -",
                    "1年-年化收益(%)": "",
                    "平台成立以来收益年化(%)": since_inception_annualized_return_by_code.get(code) or "",
                    "累计净值比年化(%)": accum_nav_ratio_by_code.get(code, ""),
                    "成立以来收益口径": lifetime_observations.get(code) or {"status": "missing_input"},
                    _SINCE_INCEPTION_ANNUAL_RETURN_VOLATILITY_LABEL: annual_volatility_by_code.get(code) or "",
                    _SINCE_INCEPTION_ANNUAL_RETURN_BREAKDOWN_LABEL: [],
                    "近1月(%)": "",
                    "近1周(%)": "",
                    "近3月(%)": "",
                    "近6月(%)": "",
                    "1年-夏普比率": "",
                    "1年-最大回撤(%)": "",
                    "1年-波动率": "",
                    "投资优先池得分": "",
                    "正式投资评分": "",
                    "最新净值日期": "",
                    "最新单位净值": "",
                    "实时估值": "",
                    "实时估值时间": "",
                    "实时估算净值": "",
                    "基金经理": "",
                    "基金类型": "",
                    "基金规模(亿)": "",
                    "正式榜分组": "",
                    "债基风格标签": "",
                    "推荐解读": "当前数据库与最新主版本均未找到该基金代码",
                    "基金公司": "",
                    "费率合计(%)": "",
                    "费率筛选值(%)": "",
                    "费率下界(%)": "",
                    "费率口径": "",
                    "费率筛选就绪": False,
                    "长期稳定性标签": "",
                    "项目质量信心": "",
                    "AutoStock原始校验状态": "",
                    "风险提示": unresolved_risk,
                    "持有期状态": "",
                    "对比来源": source_label,
                    "是否用户输入": "是" if in_user_input else "否",
                    "是否优质推荐池": "是" if in_recommendation_pool else "否",
                },
                autostock_row,
            )
            unresolved_row["序号"] = index
            rows.append(unresolved_row)
            continue
        row = _overlay_favorite_row_with_live_autostock(row, autostock_rows.get(code))
        rows.append(
            {
                "序号": index,
                "基金代码": row.get("基金代码", ""),
                "基金名称": row.get("基金名称", ""),
                "当前层级": row.get("当前层级", ""),
                "建议动作": row.get("建议动作", ""),
                "实时估值": row.get("实时估值", ""),
                "实时估值时间": row.get("实时估值时间", ""),
                "实时估算净值": row.get("实时估算净值", ""),
                "__valuation_state": row.get("__valuation_state", ""),
                "__valuation_status_label": row.get("__valuation_status_label", ""),
                "__valuation_reason": row.get("__valuation_reason", ""),
                "最新单位净值": row.get("最新单位净值", ""),
                "基金经理": row.get("基金经理", ""),
                "基金类型": row.get("基金类型", ""),
                "出现次数&出现频率(%)": row.get("出现次数&出现频率(%)", ""),
                "1年-年化收益(%)": row.get("1年-年化收益(%)", ""),
                "平台成立以来收益年化(%)": row.get("平台成立以来收益年化(%)", ""),
                "累计净值比年化(%)": row.get("累计净值比年化(%)", ""),
                "成立以来收益口径": row.get("成立以来收益口径") or {},
                _SINCE_INCEPTION_ANNUAL_RETURN_VOLATILITY_LABEL: row.get(_SINCE_INCEPTION_ANNUAL_RETURN_VOLATILITY_LABEL, ""),
                _SINCE_INCEPTION_ANNUAL_RETURN_BREAKDOWN_LABEL: row.get(_SINCE_INCEPTION_ANNUAL_RETURN_BREAKDOWN_LABEL) or [],
                _FAVORITE_LIVE_RETURN_1W_LABEL: row.get(_FAVORITE_LIVE_RETURN_1W_LABEL, ""),
                _FAVORITE_LIVE_RETURN_1M_LABEL: row.get(_FAVORITE_LIVE_RETURN_1M_LABEL, ""),
                _FAVORITE_LIVE_RETURN_3M_LABEL: row.get(_FAVORITE_LIVE_RETURN_3M_LABEL, ""),
                _FAVORITE_LIVE_RETURN_6M_LABEL: row.get(_FAVORITE_LIVE_RETURN_6M_LABEL, ""),
                _FAVORITE_RETURN_1W_SOURCE_LABEL: row.get(_FAVORITE_RETURN_1W_SOURCE_LABEL, ""),
                _FAVORITE_RETURN_1M_SOURCE_LABEL: row.get(_FAVORITE_RETURN_1M_SOURCE_LABEL, ""),
                _FAVORITE_RETURN_3M_SOURCE_LABEL: row.get(_FAVORITE_RETURN_3M_SOURCE_LABEL, ""),
                _FAVORITE_RETURN_6M_SOURCE_LABEL: row.get(_FAVORITE_RETURN_6M_SOURCE_LABEL, ""),
                "__favorite_snapshot_return_date": row.get("__favorite_snapshot_return_date"),
                "__favorite_live_return_date": row.get("__favorite_live_return_date"),
                "__favorite_live_source": row.get("__favorite_live_source"),
                "__favorite_live_source_evidence": row.get("__favorite_live_source_evidence"),
                "__favorite_live_field_states": row.get("__favorite_live_field_states"),
                "__risk_metric_unavailability": row.get("__risk_metric_unavailability"),
                "近1周(%)": row.get("近1周(%)", ""),
                "近1月(%)": row.get("近1月(%)", ""),
                "近3月(%)": row.get("近3月(%)", ""),
                "近6月(%)": row.get("近6月(%)", ""),
                "1年-夏普比率": row.get("1年-夏普比率", ""),
                "1年-最大回撤(%)": row.get("1年-最大回撤(%)", ""),
                "1年-波动率": row.get("1年-波动率", ""),
                "投资优先池得分": row.get("投资优先池得分", ""),
                "正式投资评分": row.get("正式投资评分", ""),
                "最新净值日期": row.get("最新净值日期", ""),
                "基金规模(亿)": row.get("基金规模(亿)", ""),
                "正式榜分组": row.get("正式榜分组", ""),
                "债基风格标签": row.get("债基风格标签", ""),
                "推荐解读": row.get("推荐解读", ""),
                "基金公司": row.get("基金公司", ""),
                "费率合计(%)": row.get("费率合计(%)", ""),
                "费率筛选值(%)": row.get("费率筛选值(%)", ""),
                "费率下界(%)": row.get("费率下界(%)", ""),
                "费率口径": row.get("费率口径", ""),
                "费率筛选就绪": bool(row.get("费率筛选就绪")),
                "长期稳定性标签": row.get("长期稳定性标签", ""),
                "项目质量信心": row.get("项目质量信心", ""),
                "AutoStock原始校验状态": row.get("AutoStock原始校验状态", ""),
                "风险提示": row.get("风险提示", ""),
                "持有期状态": row.get("持有期状态", ""),
                "对比来源": source_label,
                "是否用户输入": "是" if in_user_input else "否",
                "是否优质推荐池": "是" if in_recommendation_pool else "否",
            }
        )

    history_payload = (
        _save_favorite_query_history(
            user_codes,
            label=label,
            entry_type=FAVORITE_HISTORY_ENTRY_TYPE_QUERY,
        )
        if save_history
        else _favorite_query_history_snapshot()
    )
    level_counter = Counter(str(item.get("当前层级") or "").strip() for item in rows if str(item.get("当前层级") or "").strip())
    source_counter = Counter(str(item.get("对比来源") or "").strip() for item in rows if str(item.get("对比来源") or "").strip())
    style_threshold_payload = _extract_favorite_style_threshold_payload(payload, rows)
    style_metric_benchmark_map = _load_tracker_style_metric_benchmark_map(payload, rows)
    style_metric_references = _build_style_metric_reference_map(
        rows,
        style_threshold_entries=style_threshold_payload.get("styles") or [],
        global_style_threshold_entry=style_threshold_payload.get("global") or {},
        benchmark_map=style_metric_benchmark_map,
    )
    for item in rows:
        code = _normalize_code_text(item.get("基金代码"))
        item[_STYLE_METRIC_REFERENCES_LABEL] = style_metric_references.get(code) or {}
    payload_mode = (
        FAVORITE_FUND_SCORE_PAYLOAD_MODE_FULL
        if include_details
        else FAVORITE_FUND_SCORE_PAYLOAD_MODE_COMPACT
    )
    omitted_detail_fields: list[str] = []
    if not include_details:
        omitted_detail_fields = [
            _SINCE_INCEPTION_ANNUAL_RETURN_BREAKDOWN_LABEL,
            _STYLE_METRIC_REFERENCES_LABEL,
        ]
        for item in rows:
            for field in omitted_detail_fields:
                item.pop(field, None)

    source_basis = _build_favorite_score_source_basis(
        payload,
        user_codes=user_codes,
        normalized_codes=normalized_codes,
        recommendation_pool_codes=recommendation_pool_codes,
        include_details=include_details,
        save_history=save_history,
        payload_mode=payload_mode,
        autostock_auxiliary_meta=autostock_auxiliary_meta,
        include_recommendation_pool=include_recommendation_pool,
        include_autostock=include_autostock,
    )

    return {
        "run_id": payload.get("run_id"),
        "generated_at": payload.get("generated_at"),
        "payload_mode": payload_mode,
        "detail_included": bool(include_details),
        "recommendation_pool_included": bool(include_recommendation_pool),
        "autostock_included": bool(include_autostock),
        "omitted_detail_fields": omitted_detail_fields,
        "payload_slimming": {
            "mode": payload_mode,
            "detail_endpoint": "/api/favorites/metric-details",
            "full_compatibility": {
                "json": {"include_details": True},
                "query": "?detail=1",
            },
        },
        "source_basis": source_basis,
        "cache_basis": source_basis,
        "source_contract": _build_favorite_score_source_contract(source_basis),
        "requested_count": len(user_codes),
        "matched_count": len(user_codes) - len(unresolved_user_codes),
        "unresolved_count": len(unresolved_user_codes),
        "unresolved_codes": unresolved_user_codes,
        "table_count": len(normalized_codes),
        "recommendation_pool_count": len(recommendation_pool_codes),
        "auto_appended_recommendation_count": len(
            [code for code in appended_recommendation_pool_codes if code not in user_code_set]
        ),
        "counts": {
            "优质推荐池": int(level_counter.get("优质推荐池") or 0),
            "正式主榜": int(level_counter.get("正式主榜") or 0),
            "自动诊断池": int(level_counter.get("自动诊断池") or 0),
            "持有期排除": int(level_counter.get("持有期排除") or 0),
            "项目外样本": int(level_counter.get("项目外样本") or 0),
            "未收录": int(level_counter.get("未收录") or 0),
        },
        "source_counts": {
            "用户输入": int(source_counter.get("用户输入") or 0),
            "共同命中": int(source_counter.get("共同命中") or 0),
            "优质推荐池": int(source_counter.get("优质推荐池") or 0),
        },
        "style_thresholds": style_threshold_payload.get("styles") or [],
        "global_style_threshold": style_threshold_payload.get("global") or {},
        "missing_style_thresholds": style_threshold_payload.get("missing_styles") or [],
        "style_metric_references": style_metric_references,
        "metric_units": metric_unit_metadata(),
        "autostock_auxiliary": autostock_auxiliary_meta,
        "snapshot_auxiliary": autostock_auxiliary_meta,
        "autostock_provider_enabled": getattr(settings, "autostock_enabled", True),
        "autostock_validation_unavailable": (
            not getattr(settings, "autostock_enabled", True)
            or bool(autostock_auxiliary_meta.get("quota_unavailable"))
        ),
        "quota_exceeded": bool(autostock_auxiliary_meta.get("quota_exhausted")),
        "rows": rows,
        "funds": rows,
        "history": history_payload.get("records") or [],
        "display_columns": FAVORITE_FUND_EXPORT_COLUMNS,
    }


def _recommendation_export_rows(funds: list[dict[str, Any]], *, include_group_name: str | None = None) -> pd.DataFrame:
    return recommendation_payloads.recommendation_export_rows(
        funds,
        include_group_name=include_group_name,
        normalize_code_text=_normalize_code_text,
        normalize_number_text=_normalize_number_text,
        to_float=_to_float,
        format_tracker_occurrence_text=_format_tracker_occurrence_text,
        export_columns=RECOMMENDATION_EXPORT_COLUMNS,
    )


def _build_recommendation_audit_export_rows(payload: dict, portfolio_summary: dict[str, Any]) -> pd.DataFrame:
    return recommendation_payloads.build_recommendation_audit_export_rows(
        payload,
        portfolio_summary,
        normalize_number_text=_normalize_number_text,
        to_float=_to_float,
    )


def _build_recommendations_export_workbook(payload: dict) -> tuple[BytesIO, str]:
    payload = recommendations_display_report(settings, payload)
    annualized_return_fn = _apply_since_inception_annualized_return_to_portfolio_summary
    if (payload.get("publication_control") or {}).get("commit_protocol"):
        detail = _build_recommendations_detail_response(payload, audit_read_only=True)
        payload = {
            **payload,
            "project_publish_summary": {
                **(payload.get("project_publish_summary") or {}),
                "portfolio_suggestions": detail["portfolio_suggestions"],
            },
        }
        # Export the same frozen lifetime observations as the detail API.
        annualized_return_fn = lambda summary: summary
    return recommendation_payloads.build_recommendations_export_workbook(
        payload,
        normalize_portfolio_groups=_normalize_portfolio_groups,
        load_tracker_occurrence_map_fn=_load_tracker_occurrence_map,
        overlay_portfolio_occurrence_metrics_fn=_overlay_portfolio_occurrence_metrics,
        apply_since_inception_annualized_return_fn=annualized_return_fn,
        build_recommendation_audit_export_rows_fn=_build_recommendation_audit_export_rows,
        recommendation_export_rows_fn=_recommendation_export_rows,
        export_columns=RECOMMENDATION_EXPORT_COLUMNS,
        export_column_widths=RECOMMENDATION_EXPORT_COLUMN_WIDTHS,
    )


def _normalize_tracker_valuation_codes(codes: list[str]) -> list[str]:
    normalized_codes: list[str] = []
    seen: set[str] = set()
    for code in codes:
        text = _normalize_code_text(code)
        if not text or text in seen:
            continue
        seen.add(text)
        normalized_codes.append(text)
    return normalized_codes


def _get_tracker_valuation_max_request_codes() -> int:
    return max(1, min(_TRACKER_VALUATION_SAFE_MAX_REQUEST_CODES, 50))


def _read_autostock_quota_snapshot(now: float | None = None) -> dict[str, Any]:
    return read_autostock_quota_snapshot(
        shared_state_path=settings.autostock_rate_limit_shared_state_path,
        max_calls_per_hour=int(settings.autostock_rate_limit_max_calls_per_hour or 0),
        min_interval_seconds=float(settings.autostock_rate_limit_min_interval_seconds),
        now=now,
        fail_closed=True,
    )


def _build_tracker_valuation_unavailable_payload(reason: str, message: str) -> dict[str, Any]:
    return {
        "available": False,
        "reason": reason,
        "message": message,
    }


def _tracker_valuation_cache_ttl(payload: dict[str, Any] | None) -> int:
    return (
        _VALUATION_CACHE_TTL_SECONDS
        if isinstance(payload, dict) and payload.get("available")
        else _VALUATION_MISSING_CACHE_TTL_SECONDS
    )


def _is_tracker_valuation_cache_entry_fresh(cached: dict[str, Any] | None, now: float) -> bool:
    if not isinstance(cached, dict):
        return False
    payload = cached.get("payload") if isinstance(cached.get("payload"), dict) else {}
    age_seconds = now - float(cached.get("fetched_at") or 0.0)
    return age_seconds <= _tracker_valuation_cache_ttl(payload)


def _build_tiantian_valuation_payload(row: dict[str, Any], *, now: datetime | None = None) -> dict[str, Any]:
    from backend.valuation_contract import validate_valuation
    validation = validate_valuation(
        row, now=now, max_age_seconds=getattr(settings, "valuation_max_age_seconds", 1800),
        max_base_age_days=getattr(settings, "valuation_max_base_age_days", 10),
    )
    payload = {**validation, "source": "tiantian_fund_valuation_last", "raw_fields": dict(row),
               "valuation_date": str(row.get("GZTIME") or ""),
               "fund_name": str(row.get("SHORTNAME") or ""),
               "latest_nav": _normalize_number_text(row.get("NAV"), digits=4),
               "latest_nav_date": str(row.get("PDATE") or "")}
    if validation["available"]:
        payload.update(estimated_nav=_normalize_number_text(row["GSZ"], digits=4),
                       estimated_change_pct=_normalize_number_text(row["GSZZL"], digits=2),
                       estimated_change_value=_normalize_number_text(float(row["GSZ"]) - float(row["NAV"]), digits=4))
    return payload


def _post_tiantian_tracker_valuation_request(
    endpoint: str,
    *,
    data: dict[str, str],
    headers: dict[str, str],
    timeout: float,
) -> requests.Response:
    return requests.post(endpoint, data=data, headers=headers, timeout=timeout)


def _fetch_tiantian_tracker_valuation_rows(codes: list[str]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    normalized_codes = _normalize_tracker_valuation_codes(codes)
    if not normalized_codes:
        return [], {
            "endpoint": "",
            "attempted_endpoints": [],
            "returned_count": 0,
        }

    request_data = {
        "FCODES": ",".join(normalized_codes),
        "FIELDS": _TRACKER_VALUATION_FIELDS,
    }
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Origin": "https://h5.1234567.com.cn",
        "Referer": "https://h5.1234567.com.cn/",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    }
    attempted_endpoints: list[str] = []
    errors: list[str] = []
    for endpoint in _TRACKER_VALUATION_ENDPOINTS:
        attempted_endpoints.append(endpoint)
        try:
            response = _post_tiantian_tracker_valuation_request(
                endpoint,
                data=request_data,
                headers=headers,
                timeout=_TRACKER_VALUATION_HTTP_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict):
                raise ValueError("响应不是 JSON 对象")
            if payload.get("success") is not True or str(payload.get("errorCode")) != "0":
                raise ValueError(str(payload.get("firstError") or payload.get("errorCode") or "业务响应失败"))
            rows = payload.get("data")
            if not isinstance(rows, list):
                raise ValueError("响应缺少 data 列表")
            if any(not isinstance(item, dict) for item in rows):
                raise ValueError("响应包含非对象记录")
            row_codes = [_normalize_code_text(item.get("FCODE")) for item in rows]
            if len(set(row_codes)) != len(row_codes) or any(code not in normalized_codes for code in row_codes):
                raise ValueError("响应基金代码缺失、重复或与请求不符")
            normalized_rows = rows
            # Keep the exact HTTP body and request metadata for later source audits.
            raw_body = getattr(response, "content", None)
            hash_basis = "http_response_body"
            if not isinstance(raw_body, bytes):
                raw_body = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
                hash_basis = "parsed_json_test_response"
            from backend.valuation_contract import SHANGHAI
            fetched_at = datetime.now(SHANGHAI)
            response_sha256 = hashlib.sha256(raw_body).hexdigest()
            audit_dir = settings.reports_dir / "audit" / "valuation_observations" / fetched_at.strftime("%Y%m%d")
            audit_dir.mkdir(parents=True, exist_ok=True)
            response_path = audit_dir / f"{response_sha256}.response.json"
            if not response_path.exists():
                response_path.write_bytes(raw_body)
            metadata_path = audit_dir / f"{fetched_at.strftime('%H%M%S_%f')}_{response_sha256[:12]}.json"
            metadata_path.write_text(json.dumps({"endpoint": endpoint, "fetched_at": fetched_at.isoformat(),
                "requested_codes": normalized_codes, "fields": _TRACKER_VALUATION_FIELDS,
                "response_sha256": response_sha256, "hash_basis": hash_basis,
                "response_path": _project_relative_path_text(response_path)}, ensure_ascii=False, indent=2), encoding="utf-8")
            return normalized_rows, {
                "endpoint": endpoint,
                "attempted_endpoints": attempted_endpoints,
                "returned_count": len(normalized_rows),
                "fetched_at": fetched_at.isoformat(), "response_sha256": response_sha256,
                "response_artifact": _project_relative_path_text(response_path), "response_state": "rows" if rows else "empty",
                "error_code": payload.get("errorCode"),
                "success": payload.get("success"),
            }
        except (requests.RequestException, ValueError, TypeError) as exc:
            errors.append(f"{endpoint}: {exc}")
    raise TrackerValuationFetchError("；".join(errors) or "天天基金实时估值接口请求失败")


def _summarize_tracker_valuation_counts(valuations: dict[str, dict[str, Any]]) -> dict[str, int]:
    available_count = 0
    missing_count = 0
    for item in valuations.values():
        if isinstance(item, dict) and item.get("available"):
            available_count += 1
        else:
            missing_count += 1
    return {
        "available_count": available_count,
        "missing_count": missing_count,
    }


def _build_tracker_valuation_rows(valuations: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for code, payload in valuations.items():
        item = dict(payload or {})
        rows.append(
            {
                "fcode": _normalize_code_text(code),
                "available": bool(item.get("available")),
                "source": str(item.get("source") or "tiantian_fund_valuation_last").strip(),
                "reason": str(item.get("reason") or "").strip(),
                "valuation_date": str(item.get("valuation_date") or "").strip(),
                "estimated_nav": str(item.get("estimated_nav") or "").strip(),
                "estimated_change_pct": str(item.get("estimated_change_pct") or "").strip(),
                "field_observations": item.get("field_observations") or {},
                "raw_dates": item.get("raw_dates") or {},
                "fetched_at": item.get("request_fetched_at") or item.get("fetched_at"),
                "endpoint": item.get("endpoint"), "response_sha256": item.get("response_sha256"),
                "response_artifact": item.get("response_artifact"),
                "formula_version": item.get("formula_version"),
                "missing_value_policy": "preserve_missing_semantics",
            }
        )
    return rows


def _build_tracker_valuation_source_basis(codes: list[str]) -> dict[str, Any]:
    normalized_codes = _normalize_tracker_valuation_codes(codes)
    cached_codes: list[str] = []
    uncached_codes: list[str] = []
    now = time.time()
    with _valuation_cache_lock:
        for code in normalized_codes:
            cached = _valuation_cache.get(code)
            if _is_tracker_valuation_cache_entry_fresh(cached, now):
                cached_codes.append(code)
            else:
                uncached_codes.append(code)
    basis = {
        "schema_version": _TRACKER_VALUATION_SOURCE_BASIS_SCHEMA_VERSION,
        "requested_codes": normalized_codes,
        "cached_codes": cached_codes,
        "uncached_codes": uncached_codes,
        "source": "tiantian_fund_valuation_last_or_short_lived_memory_cache",
        "upstream_endpoint_family": "Tiantian FundValuationLast",
        "validation_version": "tiantian-valuation-2026-10-10.1",
        "source_time_max_age_seconds": getattr(settings, "valuation_max_age_seconds", 1800),
        "requires_complete_fields": ["GSZ", "GSZZL", "NAV", "PDATE", "GZTIME"],
        "requires_verified_percentage_base": True,
        "independent_supplier_count": 1,
        "cache_scope": "process_memory_only",
        "cache_ttl_seconds": _VALUATION_CACHE_TTL_SECONDS,
        "missing_cache_ttl_seconds": _VALUATION_MISSING_CACHE_TTL_SECONDS,
        "max_request_codes": _get_tracker_valuation_max_request_codes(),
        "effective_batch_size": _get_tracker_valuation_max_request_codes(),
        "quota_reserved_for_automation": 0,
        "display_fields": [
            "实时估值",
            "实时估值时间",
            "实时估算净值",
        ],
        "source_fields": [
            "FCODE",
            "SHORTNAME",
            "GSZZL",
            "GZTIME",
            "GSZ",
            "NAV",
            "PDATE",
        ],
        "missing_value_policy": "preserve_missing_semantics",
        "truth_note": (
            "Tracker valuation values are optional intraday Tiantian FundValuationLast observations. "
            "Funds for which the provider returns null valuation fields must remain unavailable with a reason, "
            "not rendered as zero or success."
        ),
    }
    basis["digest"] = _stable_json_digest(basis)
    return basis


def _build_tracker_valuation_source_contract(source_basis: dict[str, Any]) -> dict[str, Any]:
    basis = dict(source_basis or {})
    return {
        "schema_version": _TRACKER_VALUATION_SOURCE_CONTRACT_SCHEMA_VERSION,
        "source": "tiantian_fund_valuation_last_or_short_lived_memory_cache",
        "truth_policy": "real_tiantian_fund_valuation_last_response_or_short_lived_memory_cache_only",
        "missing_value_policy": "preserve_missing_semantics",
        "business_value_synthesis": "forbidden",
        "official_publication_truth": "false_intraday_valuation_observation_only",
        "frontend_rendered_truth_allowed": False,
        "cache_basis_schema_version": basis.get("schema_version"),
        "cache_basis_digest": basis.get("digest"),
    }


def _build_tracker_valuation_audit_read_only_payload(codes: list[str]) -> dict[str, Any]:
    source_basis = _build_tracker_valuation_source_basis(codes)
    normalized_codes = list(source_basis["requested_codes"])
    valuations = {
        code: _build_tracker_valuation_unavailable_payload(
            "audit_read_only",
            "只读审计不请求天天基金实时估值接口，也不写入估值缓存",
        )
        for code in normalized_codes
    }
    counts = _summarize_tracker_valuation_counts(valuations)
    return {
        "status": "audit_read_only",
        "source": "tiantian_fund_valuation_last",
        "requested_count": len(normalized_codes),
        "returned_count": len(valuations),
        "cached_count": len(source_basis["cached_codes"]),
        "uncached_count": len(source_basis["uncached_codes"]),
        "available_count": counts["available_count"],
        "missing_count": counts["missing_count"],
        "upstream_requested_count": 0,
        "upstream_returned_count": 0,
        "skipped_uncached_count": len(source_basis["uncached_codes"]),
        "batch_count": 0,
        "rate_limit_wait_seconds": 0.0,
        "quota_exhausted": False,
        "quota_unavailable": False,
        "quota_reserved_for_automation": 0,
        "cache_ttl_seconds": _VALUATION_CACHE_TTL_SECONDS,
        "missing_cache_ttl_seconds": _VALUATION_MISSING_CACHE_TTL_SECONDS,
        "max_request_codes": _get_tracker_valuation_max_request_codes(),
        "effective_batch_size": _get_tracker_valuation_max_request_codes(),
        "audit_read_only": True,
        "detail_fields": list(source_basis["display_fields"]),
        "source_basis": source_basis,
        "cache_basis": source_basis,
        "source_contract": _build_tracker_valuation_source_contract(source_basis),
        "valuation_rows": _build_tracker_valuation_rows(valuations),
        "valuations": valuations,
        "truth_note": source_basis["truth_note"],
    }


def _fetch_tracker_valuations(codes: list[str], *, audit_read_only: bool = False) -> dict[str, Any]:
    normalized_codes = _normalize_tracker_valuation_codes(codes)
    source_basis = _build_tracker_valuation_source_basis(normalized_codes)
    if audit_read_only:
        return _build_tracker_valuation_audit_read_only_payload(normalized_codes)
    if not normalized_codes:
        return {
            "status": "empty",
            "source": "tiantian_fund_valuation_last",
            "requested_count": 0,
            "returned_count": 0,
            "cached_count": 0,
            "uncached_count": 0,
            "available_count": 0,
            "missing_count": 0,
            "upstream_requested_count": 0,
            "upstream_returned_count": 0,
            "skipped_uncached_count": 0,
            "batch_count": 0,
            "rate_limit_wait_seconds": 0.0,
            "quota_exhausted": False,
            "quota_unavailable": False,
            "cache_ttl_seconds": _VALUATION_CACHE_TTL_SECONDS,
            "missing_cache_ttl_seconds": _VALUATION_MISSING_CACHE_TTL_SECONDS,
            "max_request_codes": _get_tracker_valuation_max_request_codes(),
            "effective_batch_size": _get_tracker_valuation_max_request_codes(),
            "source_basis": source_basis,
            "cache_basis": source_basis,
            "source_contract": _build_tracker_valuation_source_contract(source_basis),
            "detail_fields": list(source_basis["display_fields"]),
            "valuation_rows": [],
            "valuations": {},
        }

    results: dict[str, dict[str, Any]] = {}
    now = time.time()
    uncached_codes: list[str] = []
    cached_count = 0
    with _valuation_cache_lock:
        for code in normalized_codes:
            cached = _valuation_cache.get(code)
            if _is_tracker_valuation_cache_entry_fresh(cached, now):
                cached_payload = dict(cached["payload"])
                if cached_payload.get("available") and isinstance(cached_payload.get("raw_fields"), dict):
                    validated = _build_tiantian_valuation_payload(cached_payload["raw_fields"])
                    cached_payload.update(validated)
                    if not validated["available"]:
                        for field in ("estimated_nav", "estimated_change_pct", "estimated_change_value"):
                            cached_payload.pop(field, None)
                results[code] = cached_payload
                cached_count += 1
            else:
                uncached_codes.append(code)

    effective_batch_size = _get_tracker_valuation_max_request_codes()
    upstream_requested_count = len(uncached_codes)
    upstream_returned_count = 0
    batch_count = 1 if uncached_codes else 0
    upstream_failed = False
    upstream_error: str | None = None
    upstream_endpoint = ""
    attempted_endpoints: list[str] = []

    if uncached_codes:
        try:
            upstream_rows, upstream_meta = _fetch_tiantian_tracker_valuation_rows(uncached_codes)
            upstream_endpoint = str(upstream_meta.get("endpoint") or "").strip()
            attempted_endpoints = [str(item) for item in upstream_meta.get("attempted_endpoints") or []]
            by_code = {
                _normalize_code_text(item.get("FCODE")): item
                for item in upstream_rows
                if _normalize_code_text(item.get("FCODE"))
            }
            upstream_returned_count = len(by_code)
            fetched_at = time.time()
            with _valuation_cache_lock:
                for code in uncached_codes:
                    row = by_code.get(code)
                    payload = (
                        _build_tiantian_valuation_payload(row)
                        if row
                        else _build_tracker_valuation_unavailable_payload(
                            "upstream_empty" if not upstream_rows else "upstream_missing",
                            "天天基金实时估值接口返回空数据" if not upstream_rows else "天天基金实时估值接口未返回该基金记录",
                        )
                    )
                    payload.setdefault("source", "tiantian_fund_valuation_last")
                    payload.update(endpoint=upstream_endpoint, response_sha256=upstream_meta.get("response_sha256"),
                                   response_artifact=upstream_meta.get("response_artifact"),
                                   request_fetched_at=upstream_meta.get("fetched_at"))
                    _valuation_cache[code] = {
                        "fetched_at": fetched_at,
                        "payload": dict(payload),
                    }
                    results[code] = dict(payload)
        except TrackerValuationFetchError as exc:
            upstream_failed = True
            upstream_error = str(exc)
            for code in uncached_codes:
                results[code] = _build_tracker_valuation_unavailable_payload(
                    "upstream_failed",
                    "天天基金实时估值主备接口均请求失败",
                )
                results[code]["source"] = "tiantian_fund_valuation_last"

    ordered_results = {
        code: results[code]
        for code in normalized_codes
        if code in results
    }
    counts = _summarize_tracker_valuation_counts(ordered_results)
    status = "upstream_failed" if upstream_failed and uncached_codes else "ok"

    return {
        "status": status,
        "source": "tiantian_fund_valuation_last",
        "requested_count": len(normalized_codes),
        "returned_count": len(ordered_results),
        "cached_count": cached_count,
        "uncached_count": len(uncached_codes),
        "available_count": counts["available_count"],
        "missing_count": counts["missing_count"],
        "upstream_requested_count": upstream_requested_count,
        "upstream_returned_count": upstream_returned_count,
        "skipped_uncached_count": 0,
        "batch_count": batch_count,
        "rate_limit_wait_seconds": 0.0,
        "quota_exhausted": False,
        "quota_unavailable": False,
        "quota_recent_call_count": None,
        "quota_remaining": None,
        "quota_reserved_for_automation": 0,
        "max_calls_per_hour": None,
        "cache_ttl_seconds": _VALUATION_CACHE_TTL_SECONDS,
        "missing_cache_ttl_seconds": _VALUATION_MISSING_CACHE_TTL_SECONDS,
        "max_request_codes": _get_tracker_valuation_max_request_codes(),
        "effective_batch_size": effective_batch_size,
        "upstream_error": upstream_error,
        "upstream_endpoint": upstream_endpoint,
        "attempted_endpoints": attempted_endpoints,
        "state_read_error": None,
        "source_basis": source_basis,
        "cache_basis": source_basis,
        "source_contract": _build_tracker_valuation_source_contract(source_basis),
        "detail_fields": list(source_basis["display_fields"]),
        "valuation_rows": _build_tracker_valuation_rows(ordered_results),
        "valuations": ordered_results,
    }


def _build_database_overview_base() -> dict[str, Any]:
    db_path = settings.database_path
    file_stat = db_path.stat() if db_path.exists() else None
    autostock_tail_fill = load_autostock_nav_tail_fill_snapshot(settings)
    autostock_publish_impact = build_missing_response_publish_impact_summary(
        settings,
        snapshot=autostock_tail_fill,
    )
    overview = {
        "database_path": str(db_path),
        "database_size_bytes": int(file_stat.st_size) if file_stat else 0,
        "database_size_text": _format_size(int(file_stat.st_size)) if file_stat else "0 B",
        "database_file_updated_at": (
            datetime.fromtimestamp(file_stat.st_mtime).isoformat(timespec="seconds")
            if file_stat
            else None
        ),
        "stored_fund_count": 0,
        "latest_nav_snapshot_count": 0,
        "nav_history_record_count": 0,
        "nav_history_hot_record_count": 0,
        "nav_history_archive_record_count": 0,
        "nav_history_effective_record_count": 0,
        "nav_history_archive_bound": False,
        "nav_history_archive_cutoff_date": None,
        "risk_metric_record_count": 0,
        "fund_meta_updated_at": None,
        "fund_meta_timestamp_semantics": "funds 主表元数据触达时间，不等同于净值/风险业务日期",
        "nav_data_updated_at": None,
        "risk_data_updated_at": None,
        "latest_nav_business_date": None,
        "risk_1y_business_date": None,
        "risk_1y_raw_business_date": None,
        "freshness_primary_source": "latest_nav_business_date + risk_1y_business_date",
        "freshness_guidance": {
            "primary_fields": [
                "latest_nav_business_date",
                "risk_1y_business_date",
                "nav_data_updated_at",
                "risk_data_updated_at",
            ],
            "meta_field": "fund_meta_updated_at",
            "meta_field_role": "metadata_only",
            "summary": "优先以净值/风险业务日期和对应写入时间判断是否完成日更，fund_meta_updated_at 仅表示 funds 主表元数据最近触达时间。",
        },
        "latest_nav_coverage_ratio": 0.0,
        "latest_nav_coverage_ratio_pct": 0.0,
        "latest_nav_covered_fund_count": 0,
        "effective_nav_fund_count": 0,
        "effective_nav_ready": False,
        "autostock_shadow_latest_batch_run_id": autostock_tail_fill.get("latest_batch_run_id"),
        "autostock_shadow_history_table_exists": bool(autostock_tail_fill.get("history_table_exists")),
        "autostock_shadow_history_reviewed_total": int(autostock_tail_fill.get("history_reviewed_total") or 0),
        "autostock_shadow_history_coverage_start_date": autostock_tail_fill.get("history_coverage_start_date"),
        "autostock_shadow_history_coverage_business_days": int(
            autostock_tail_fill.get("history_coverage_business_days") or 0
        ),
        "autostock_shadow_persistent_tracking_ready": bool(
            autostock_tail_fill.get("persistent_missing_response_tracking_ready")
        ),
        "autostock_shadow_history_bootstrap_attempted": bool(
            autostock_tail_fill.get("history_bootstrap_attempted")
        ),
        "autostock_shadow_history_bootstrap_inserted_total": int(
            autostock_tail_fill.get("history_bootstrap_inserted_total") or 0
        ),
        "autostock_shadow_history_bootstrap_reason": autostock_tail_fill.get("history_bootstrap_reason"),
        "autostock_shadow_fill_ready_total": int(autostock_tail_fill.get("shadow_fill_ready_total") or 0),
        "autostock_shadow_reviewed_total": int(autostock_tail_fill.get("reviewed_total") or 0),
        "autostock_shadow_missing_response_total": int(autostock_tail_fill.get("missing_response_total") or 0),
        "autostock_shadow_last_reviewed_at": autostock_tail_fill.get("last_reviewed_at"),
        "autostock_shadow_cooldown_active_today": bool(autostock_tail_fill.get("cooldown_active_today")),
        "autostock_shadow_next_retry_business_date": autostock_tail_fill.get("next_retry_business_date"),
        "autostock_shadow_first_day_missing_response_total": int(
            autostock_tail_fill.get("first_day_missing_response_total") or 0
        ),
        "autostock_shadow_persistent_missing_response_total": int(
            autostock_tail_fill.get("persistent_missing_response_total") or 0
        ),
        "autostock_shadow_severe_persistent_missing_response_total": int(
            autostock_tail_fill.get("severe_persistent_missing_response_total") or 0
        ),
        "autostock_shadow_persistent_missing_response_max_days": int(
            autostock_tail_fill.get("persistent_missing_response_max_days") or 0
        ),
        "autostock_shadow_missing_response_observation_bucket_summary": list(
            autostock_tail_fill.get("missing_response_observation_bucket_summary") or []
        ),
        "autostock_shadow_missing_response_observation_codes_sample": list(
            autostock_tail_fill.get("missing_response_observation_codes_sample") or []
        ),
        "autostock_shadow_first_day_missing_response_codes_sample": list(
            autostock_tail_fill.get("first_day_missing_response_codes_sample") or []
        ),
        "autostock_shadow_persistent_missing_response_codes_sample": list(
            autostock_tail_fill.get("persistent_missing_response_codes_sample") or []
        ),
        "autostock_shadow_severe_persistent_missing_response_codes_sample": list(
            autostock_tail_fill.get("severe_persistent_missing_response_codes_sample") or []
        ),
        "autostock_shadow_persistent_missing_response_related_publish_count": int(
            autostock_publish_impact.get("persistent_missing_response_related_publish_count") or 0
        ),
        "autostock_shadow_persistent_missing_response_official_count": int(
            autostock_publish_impact.get("persistent_missing_response_official_count") or 0
        ),
        "autostock_shadow_persistent_missing_response_portfolio_count": int(
            autostock_publish_impact.get("persistent_missing_response_portfolio_count") or 0
        ),
        "autostock_shadow_severe_persistent_missing_response_related_publish_count": int(
            autostock_publish_impact.get("severe_persistent_missing_response_related_publish_count") or 0
        ),
        "autostock_shadow_severe_persistent_missing_response_official_count": int(
            autostock_publish_impact.get("severe_persistent_missing_response_official_count") or 0
        ),
        "autostock_shadow_severe_persistent_missing_response_portfolio_count": int(
            autostock_publish_impact.get("severe_persistent_missing_response_portfolio_count") or 0
        ),
        "autostock_shadow_persistent_missing_response_official_codes_sample": list(
            autostock_publish_impact.get("persistent_missing_response_official_codes_sample") or []
        ),
        "autostock_shadow_persistent_missing_response_portfolio_codes_sample": list(
            autostock_publish_impact.get("persistent_missing_response_portfolio_codes_sample") or []
        ),
        "autostock_shadow_severe_persistent_missing_response_official_codes_sample": list(
            autostock_publish_impact.get("severe_persistent_missing_response_official_codes_sample") or []
        ),
        "autostock_shadow_severe_persistent_missing_response_portfolio_codes_sample": list(
            autostock_publish_impact.get("severe_persistent_missing_response_portfolio_codes_sample") or []
        ),
        "autostock_shadow_lifetime_reviewed_total": int(autostock_tail_fill.get("lifetime_reviewed_total") or 0),
        "autostock_shadow_lifetime_fill_ready_total": int(
            autostock_tail_fill.get("lifetime_shadow_fill_ready_total") or 0
        ),
        "autostock_shadow_lifetime_missing_response_total": int(
            autostock_tail_fill.get("lifetime_missing_response_total") or 0
        ),
        "run_id": None,
        "latest_canonical_run_id": None,
        "replay_precision_status": "missing",
        "replay_precision_status_label": "非精确回放",
        "auxiliary_scope": "cross_run_current_db_state",
        "auxiliary_scope_label": "当前库状态 + 当前有效快照",
        "scope_detail": (
            "当前摘要直接读取 live 数据库、当前 effective_nav_snapshot 和当前 AutoStock shadow 状态，"
            "不对应历史 run 级别精确回放。"
        ),
    }
    freshness_source = "sidecar"
    freshness_payload = load_valid_database_freshness_summary(settings)
    if freshness_payload is None:
        freshness_source = "live_build"
        freshness_payload = build_database_freshness_summary(settings)
        if not freshness_payload.get("source_db_changed_during_build"):
            try:
                write_database_freshness_summary(settings, freshness_payload)
            except Exception:
                pass
    overview_fields = dict((freshness_payload or {}).get("overview_fields") or {})
    for key in (
        "stored_fund_count",
        "latest_nav_snapshot_count",
        "nav_history_record_count",
        "nav_history_hot_record_count",
        "nav_history_archive_record_count",
        "nav_history_effective_record_count",
        "nav_history_archive_bound",
        "nav_history_archive_cutoff_date",
        "risk_metric_record_count",
        "fund_meta_updated_at",
        "nav_data_updated_at",
        "risk_data_updated_at",
        "latest_nav_business_date",
        "risk_1y_business_date",
        "risk_1y_raw_business_date",
    ):
        if key in overview_fields and overview_fields.get(key) is not None:
            overview[key] = overview_fields.get(key)
    overview["database_freshness_summary"] = summarize_database_freshness_payload(
        freshness_payload,
        source=freshness_source,
        artifact_path=database_freshness_summary_path(settings),
    )
    return overview


def _load_database_overview_base_cached() -> dict[str, Any]:
    return _cached_payload(
        (
            "database_overview::"
            f"{_sqlite_file_cache_token(settings.database_path)}::"
            f"{_path_cache_token(settings.latest_report_file)}"
        ),
        _DATABASE_OVERVIEW_CACHE_TTL_SECONDS,
        _build_database_overview_base,
    )


def _load_database_overview(effective_nav_snapshot: dict | None = None) -> dict:
    effective = effective_nav_snapshot or {}
    overview = _load_database_overview_base_cached()
    overview["latest_nav_business_date"] = (
        effective.get("candidate_date")
        or overview.get("latest_nav_business_date")
    )
    overview["risk_1y_business_date"] = (
        effective.get("risk_1y_effective_end_date")
        or effective.get("risk_1y_latest_end_date")
        or overview.get("risk_1y_business_date")
    )
    coverage_ratio = float(effective.get("coverage_ratio_vs_total") or 0.0)
    overview["latest_nav_coverage_ratio"] = coverage_ratio
    overview["latest_nav_coverage_ratio_pct"] = round(coverage_ratio * 100, 2)
    overview["latest_nav_covered_fund_count"] = int(effective.get("non_null_latest_nav_count") or 0)
    overview["effective_nav_fund_count"] = int(effective.get("candidate_fund_count") or 0)
    overview["effective_nav_ready"] = bool(effective.get("ready"))
    risk_coverage_semantics = build_risk_coverage_threshold_semantics(
        {
            "coverage_ratio_vs_candidate": effective.get("risk_1y_effective_coverage_ratio_vs_candidate"),
        }
    )
    overview["risk_1y_coverage_ratio_vs_candidate"] = risk_coverage_semantics["coverage_ratio_vs_candidate"]
    overview["risk_1y_publish_minimum_coverage_ratio"] = risk_coverage_semantics[
        "publish_minimum_coverage_ratio"
    ]
    overview["risk_1y_observation_coverage_ratio"] = risk_coverage_semantics[
        "observation_coverage_ratio"
    ]
    overview["risk_1y_coverage_gate_status"] = risk_coverage_semantics["coverage_gate_status"]
    overview["risk_1y_coverage_gate_label"] = risk_coverage_semantics["coverage_gate_label"]
    overview["risk_1y_coverage_gate_explanation"] = risk_coverage_semantics[
        "coverage_gate_explanation"
    ]
    overview["risk_1y_coverage_observation_line_met"] = risk_coverage_semantics[
        "coverage_observation_line_met"
    ]
    overview["risk_coverage_semantics"] = risk_coverage_semantics
    return overview


def _load_bond_universe_snapshot(latest_payload: dict | None = None) -> dict:
    payload = latest_payload or {}
    run_id = str(payload.get("run_id") or "").strip() or None
    snapshot = payload.get("bond_universe_snapshot") or {}
    counts = payload.get("counts") or {}
    project_summary = payload.get("project_publish_summary") or {}
    fallback_snapshot = project_summary.get("bond_universe_snapshot") or {}
    current = snapshot or fallback_snapshot
    return {
        "stored_universe_count": int(current.get("stored_universe_count") or counts.get("stored_universe_count") or 0),
        "active_universe_count": int(current.get("active_universe_count") or counts.get("active_universe_count") or 0),
        "has_latest_nav_count": int(current.get("has_latest_nav_count") or counts.get("has_latest_nav_count") or 0),
        "has_1y_risk_metrics_count": int(current.get("has_1y_risk_metrics_count") or counts.get("has_1y_risk_metrics_count") or 0),
        "screenable_universe_count": int(current.get("screenable_universe_count") or counts.get("screenable_universe_count") or 0),
        "runtime_excluded_count": int(current.get("runtime_excluded_count") or 0),
        "holding_period_blocked_count": int(current.get("holding_period_blocked_count") or 0),
        "stale_or_missing_nav_count": int(current.get("stale_or_missing_nav_count") or 0),
        "missing_1y_risk_count": int(current.get("missing_1y_risk_count") or 0),
        "screenable_definition": str(current.get("screenable_definition") or "").strip(),
        "run_id": run_id,
        "latest_canonical_run_id": run_id,
        "replay_precision_status": "missing",
        "replay_precision_status_label": "非精确回放",
        "auxiliary_scope": (
            "same_run_report_snapshot" if run_id else "latest_report_snapshot"
        ),
        "auxiliary_scope_label": (
            "同 run 报告快照" if run_id else "latest 报告快照"
        ),
        "scope_detail": (
            "当前摘要来自当前报告 payload 内的 bond_universe_snapshot / counts 快照，"
            "用于描述该 run 的候选池覆盖，不属于独立历史回放诊断。"
        ),
    }


def _load_database_integrity_summary() -> dict:
    def _build() -> dict[str, Any]:
        latest_path, latest_payload = load_latest_nav_history_maintenance_payload(settings)
        audit_before = (latest_payload or {}).get("audit_before") or {}
        audit_after = (latest_payload or {}).get("audit_after") or {}
        maintenance_result = (latest_payload or {}).get("maintenance_result") or {}
        live_snapshot: dict[str, Any] = {}

        fallback_probe_keys = (
            "integrity_candidate_total",
            "missing_nav_total",
            "truncated_history_total",
            "truncated_history_actionable_total",
            "truncated_history_cooldown_total",
            "truncated_history_upstream_limited_total",
            "nav_history_lag_total",
            "returns_lag_total",
            "returns_missing_total",
            "recent_tail_lag_total",
            "nav_value_mismatch_total",
            "sync_candidate_total",
            "dominant_old_tail_total",
            "dominant_old_tail_actionable_total",
            "dominant_old_tail_holding_blocked_total",
            "dominant_old_tail_name_restricted_total",
            "dominant_old_tail_closed_total",
        )

        def _has_audit_value(key: str) -> bool:
            for source in (audit_after, audit_before):
                if key in source and source.get(key) is not None:
                    return True
            return False

        if not latest_payload or any(not _has_audit_value(key) for key in fallback_probe_keys):
            live_snapshot = build_nav_history_integrity_snapshot(settings)

        def _live_or_audit_int(key: str) -> int:
            for source in (audit_after, audit_before, live_snapshot):
                if key in source and source.get(key) is not None:
                    return int(source.get(key) or 0)
            return 0

        return {
            "latest_artifact_path": str(latest_path) if latest_path else None,
            "latest_generated_at": (latest_payload or {}).get("generated_at"),
            "candidate_total": _live_or_audit_int("integrity_candidate_total"),
            "missing_nav_total": _live_or_audit_int("missing_nav_total"),
            "truncated_history_total": _live_or_audit_int("truncated_history_total"),
            "truncated_history_actionable_total": _live_or_audit_int("truncated_history_actionable_total"),
            "truncated_history_cooldown_total": _live_or_audit_int("truncated_history_cooldown_total"),
            "truncated_history_upstream_limited_total": _live_or_audit_int("truncated_history_upstream_limited_total"),
            "nav_history_lag_total": _live_or_audit_int("nav_history_lag_total"),
            "returns_lag_total": _live_or_audit_int("returns_lag_total"),
            "returns_missing_total": _live_or_audit_int("returns_missing_total"),
            "recent_tail_lag_total": _live_or_audit_int("recent_tail_lag_total"),
            "nav_value_mismatch_total": _live_or_audit_int("nav_value_mismatch_total"),
            "sync_candidate_total": _live_or_audit_int("sync_candidate_total"),
            "dominant_old_tail_nav_date": (
                audit_after.get("dominant_old_tail_nav_date")
                or audit_before.get("dominant_old_tail_nav_date")
                or live_snapshot.get("dominant_old_tail_nav_date")
            ),
            "dominant_old_tail_total": _live_or_audit_int("dominant_old_tail_total"),
            "dominant_old_tail_actionable_total": _live_or_audit_int("dominant_old_tail_actionable_total"),
            "dominant_old_tail_holding_blocked_total": _live_or_audit_int("dominant_old_tail_holding_blocked_total"),
            "dominant_old_tail_name_restricted_total": _live_or_audit_int("dominant_old_tail_name_restricted_total"),
            "dominant_old_tail_closed_total": _live_or_audit_int("dominant_old_tail_closed_total"),
            "processed_total": int(maintenance_result.get("processed_total") or 0),
            "recovered_total": int(maintenance_result.get("recovered_total") or 0),
            "failed_total": int(maintenance_result.get("failed_total") or 0),
            "nav_records_inserted": int(maintenance_result.get("nav_records_inserted") or 0),
            "run_id": None,
            "latest_canonical_run_id": None,
            "replay_precision_status": "missing",
            "replay_precision_status_label": "非精确回放",
            "auxiliary_scope": "cross_run_current_db_state",
            "auxiliary_scope_label": "最新维护审计 + 当前库状态",
            "scope_detail": "当前摘要结合最新 nav history maintenance 审计产物与当前 live 数据库快照，不提供历史 run 级别精确回放。",
        }

    return _cached_payload(
        (
            "database_integrity_summary::"
            f"{_path_cache_token(settings.database_path)}::"
            f"{_path_cache_token(settings.reports_dir / 'audit' / 'nav_history_maintenance')}"
        ),
        _DATABASE_INTEGRITY_CACHE_TTL_SECONDS,
        _build,
    )


def _normalize_risk_audit_runtime_scope(
    payload: dict[str, Any] | None,
    *,
    scope_detail: str,
    latest_canonical_run_id: str | None = None,
    fallback_status: str = "latest_only",
    fallback_status_label: str = "仅 latest 审计",
    fallback_scope: str = "latest_audit_only",
    fallback_scope_label: str = "当前最新审计",
) -> dict[str, Any]:
    summary = _extract_audit_runtime_scope_summary(payload)
    if summary.get("replay_precision_status") == "exact":
        summary["replay_precision_status"] = "snapshot_frozen"
        summary["replay_precision_status_label"] = "已保存当期审计快照"
        summary["baseline_reproduction_verified"] = False
        summary["scope_detail"] = "当前读取已保存的当期审计快照，尚未验证历史风险重算结果完全一致。"
    if summary.get("replay_precision_status"):
        if latest_canonical_run_id and not summary.get("latest_canonical_run_id"):
            summary["latest_canonical_run_id"] = latest_canonical_run_id
        summary["scope_detail"] = summary.get("scope_detail") or scope_detail
        return summary
    summary.update(
        {
            "run_id": None,
            "latest_canonical_run_id": latest_canonical_run_id,
            "replay_precision_status": fallback_status,
            "replay_precision_status_label": fallback_status_label,
            "auxiliary_scope": fallback_scope,
            "auxiliary_scope_label": fallback_scope_label,
            "scope_detail": scope_detail,
        }
    )
    return summary


def _load_latest_audit_payload_from_directory(
    artifact_dir: Path,
    filename_glob: str,
) -> tuple[Path | None, dict[str, Any] | None]:
    from backend.task_audit_storage import glob_json_artifacts

    if not artifact_dir.exists():
        return (None, None)
    files = sorted(glob_json_artifacts(artifact_dir, filename_glob), reverse=True)
    for path in files:
        payload = _safe_json_load(path)
        if payload:
            return (path, payload)
    return (None, None)


def _resolve_run_report_path(path_value: str, *, run_id: str | None = None) -> Path:
    raw = str(path_value or "").strip()
    if not raw:
        return Path()

    candidate = Path(raw)
    if candidate.exists():
        return candidate

    project_root = getattr(settings, "project_root", None)
    if project_root and not candidate.is_absolute():
        project_candidate = (Path(project_root) / candidate).resolve()
        if project_candidate.exists():
            return project_candidate

    runs_dir = getattr(settings, "runs_dir", None)
    if runs_dir is None and project_root:
        runs_dir = Path(project_root) / "reports" / "runs"
    if run_id and runs_dir:
        marker = ("reports", "runs", run_id)
        parts = list(candidate.parts)
        for index in range(len(parts) - len(marker) + 1):
            if tuple(parts[index:index + len(marker)]) != marker:
                continue
            suffix_parts = parts[index + len(marker):]
            rebased = Path(runs_dir) / run_id
            if suffix_parts:
                rebased = rebased / Path(*suffix_parts)
            if rebased.exists():
                return rebased
        fallback = Path(runs_dir) / run_id / candidate.name
        if fallback.exists():
            return fallback

    return candidate


def _load_same_run_risk_audit_payload_from_latest_report(
    artifact_key: str,
    *,
    scope_detail: str,
) -> tuple[Path | None, dict[str, Any] | None]:
    publication = _load_publication_registry_cached() or _load_publication_registry_snapshot_cached() or {}
    latest_payload = _load_latest_report_cached() or _load_latest_report_snapshot_cached() or {}
    latest_successful_run = ((publication.get("automation_layers") or {}).get("latest_successful_run") or {})
    latest_canonical_run_id = str(publication.get("latest_canonical_run_id") or "").strip() or None

    report_payload_candidates: list[dict[str, Any]] = []
    if latest_payload:
        report_payload_candidates.append(dict(latest_payload))
    latest_successful_run_id = str(latest_successful_run.get("run_id") or "").strip() or None
    latest_successful_report_path = str(latest_successful_run.get("report_path") or "").strip()
    if latest_successful_report_path:
        latest_successful_report_path_obj = _resolve_run_report_path(
            latest_successful_report_path,
            run_id=latest_successful_run_id,
        )
        latest_successful_payload = _safe_json_load(latest_successful_report_path_obj)
        if latest_successful_payload:
            current_run_id = str(latest_payload.get("run_id") or "").strip()
            if not current_run_id or latest_successful_run_id != current_run_id:
                report_payload_candidates.append(dict(latest_successful_payload))

    for report_payload in report_payload_candidates:
        artifacts = dict((report_payload.get("audit_files") or {}))
        artifact_value = str(artifacts.get(artifact_key) or "").strip()
        if not artifact_value:
            continue
        artifact_path = settings.project_root / artifact_value
        payload = _safe_json_load(artifact_path)
        if not payload:
            return (artifact_path, None)
        run_id = str(report_payload.get("run_id") or "").strip() or None
        payload = {
            **payload,
            "run_id": payload.get("run_id") or run_id,
            "latest_canonical_run_id": latest_canonical_run_id or payload.get("latest_canonical_run_id") or run_id,
            "replay_precision_status": "snapshot_frozen",
            "replay_precision_status_label": "已保存当期审计快照",
            "baseline_reproduction_verified": False,
            "auxiliary_scope": payload.get("auxiliary_scope") or "same_run_report_artifact",
            "auxiliary_scope_label": payload.get("auxiliary_scope_label") or "同 run 风险审计产物",
            "scope_detail": scope_detail,
        }
        return (artifact_path, payload)
    return (None, None)


def _select_contextual_risk_audit_payload(
    artifact_dir: Path,
    *,
    filename_glob: str,
    business_date_keys: tuple[str, ...],
    label: str,
    latest_loader: Callable[[], tuple[Path | None, dict[str, Any] | None]] | None = None,
) -> tuple[Path | None, dict[str, Any] | None]:
    from backend.task_audit_storage import glob_json_artifacts

    run_context = _resolve_current_canonical_run_context()
    current_generated_at = _parse_local_datetime(run_context.get("generated_at"))
    current_candidate_date = str(run_context.get("candidate_date") or "").strip()
    current_generated_day = str(run_context.get("generated_at") or "").strip()[:10]

    if artifact_dir.exists():
        candidates: list[tuple[int, float, float, Path, dict[str, Any]]] = []
        for path in sorted(glob_json_artifacts(artifact_dir, filename_glob), reverse=True):
            payload = _safe_json_load(path)
            if not payload:
                continue
            payload_business_date = ""
            for key in business_date_keys:
                candidate = str((payload or {}).get(key) or "").strip()
                if candidate:
                    payload_business_date = candidate
                    break
            same_business_date = bool(
                current_candidate_date
                and payload_business_date
                and payload_business_date == current_candidate_date
            )
            generated_at_dt = _parse_local_datetime(payload.get("generated_at"))
            payload_generated_day = str(payload.get("generated_at") or "").strip()[:10]
            same_generated_day = bool(
                current_generated_day
                and payload_generated_day
                and payload_generated_day == current_generated_day
            )
            future_penalty = 0.0
            distance_seconds = 0.0
            if current_generated_at is not None and generated_at_dt is not None:
                delta_seconds = (generated_at_dt - current_generated_at).total_seconds()
                if delta_seconds > 0:
                    future_penalty = delta_seconds + 86400.0
                distance_seconds = abs(delta_seconds)
            elif generated_at_dt is None:
                future_penalty = 10**12
                distance_seconds = 10**12
            match_rank = 2 if same_business_date else 1 if same_generated_day else 0
            candidates.append((match_rank, future_penalty, distance_seconds, path, payload))

        if candidates:
            candidates.sort(key=lambda item: (-item[0], item[1], item[2], str(item[3])))
            selected_match_rank, _future_penalty, _distance_seconds, selected_path, selected_payload = candidates[0]
            if selected_match_rank > 0:
                fallback_scope = "same_day_closest_audit"
                fallback_scope_label = "同日最近审计"
                if selected_match_rank >= 2:
                    selected_scope_detail = (
                        f"当前摘要优先匹配当前 canonical run 的同业务日 {label} 审计产物；"
                        "该审计文件未携带 run_id，故仅保证业务日与时点尽量贴近，不宣称精确 run 级回放。"
                    )
                else:
                    selected_scope_detail = (
                        f"当前摘要未找到可确认同业务日的 {label} 审计产物，"
                        "已回退为同自然日且最接近当前 canonical run 生成时点的审计文件，"
                        "属于保守近似对齐。"
                    )
                selected_scope_detail = (
                    selected_scope_detail
                )
                scope_summary = _normalize_risk_audit_runtime_scope(
                    selected_payload,
                    scope_detail=selected_scope_detail,
                    latest_canonical_run_id=run_context.get("latest_canonical_run_id"),
                    fallback_status="partial",
                    fallback_status_label="同日近似对齐",
                    fallback_scope=fallback_scope,
                    fallback_scope_label=fallback_scope_label,
                )
                selected_payload = {
                    **dict(selected_payload or {}),
                    **scope_summary,
                }
                return (selected_path, selected_payload)

    if latest_loader is not None:
        latest_path, latest_payload = latest_loader()
    else:
        latest_path, latest_payload = _load_latest_audit_payload_from_directory(artifact_dir, filename_glob)
    return (latest_path, latest_payload)


def _load_nav_lag_summary() -> dict:
    latest_path, latest_payload = load_latest_nav_lag_audit_payload(settings)
    payload = latest_payload or {}
    summary = {
        "latest_artifact_path": str(latest_path) if latest_path else None,
        "latest_generated_at": payload.get("generated_at"),
        "latest_nav_date": payload.get("latest_nav_date"),
        "previous_nav_date": payload.get("previous_nav_date"),
        "latest_nav_fund_count": int(payload.get("latest_nav_fund_count") or 0),
        "previous_nav_fund_count": int(payload.get("previous_nav_fund_count") or 0),
        "fresh_window_fund_count": int(payload.get("fresh_window_fund_count") or 0),
        "coverage_ratio_vs_fresh_window": payload.get("coverage_ratio_vs_fresh_window"),
        "previous_nav_ftype_counts": payload.get("previous_nav_ftype_counts") or {},
        "previous_nav_samples": (payload.get("previous_nav_samples") or [])[:10],
    }
    summary.update(
        {
            "run_id": None,
            "latest_canonical_run_id": None,
            "replay_precision_status": "latest_only",
            "replay_precision_status_label": "仅 latest 审计",
            "auxiliary_scope": "latest_audit_only",
            "auxiliary_scope_label": "当前最新审计",
            "scope_detail": "当前摘要来自最新 nav lag 审计产物，不提供历史 run 级别回放。",
        }
    )
    return summary


def _risk_lag_summary_report_artifact_token(report_payload: dict[str, Any] | None) -> str:
    artifacts = dict((report_payload or {}).get("audit_files") or {})
    artifact_value = str(artifacts.get("risk_lag_audit_json") or "").strip()
    if not artifact_value:
        return "missing"
    project_root = getattr(settings, "project_root", None)
    artifact_path = Path(project_root) / artifact_value if project_root else Path(artifact_value)
    return _path_cache_token(artifact_path)


def _risk_lag_summary_cache_key() -> str:
    latest_report_file = getattr(settings, "latest_report_file", None)
    publication_registry_file = getattr(settings, "publication_registry_file", None)
    reports_dir = getattr(settings, "reports_dir", None)
    latest_payload = _safe_json_load(Path(latest_report_file)) if latest_report_file else {}
    publication = _load_publication_registry_snapshot_cached() if publication_registry_file else {}
    latest_successful_run = ((publication.get("automation_layers") or {}).get("latest_successful_run") or {})
    latest_successful_run_id = str(latest_successful_run.get("run_id") or "").strip() or None
    latest_successful_report_path = str(latest_successful_run.get("report_path") or "").strip()
    latest_successful_report_path_obj: Path | None = None
    latest_successful_payload: dict[str, Any] = {}
    if latest_successful_report_path:
        latest_successful_report_path_obj = _resolve_run_report_path(
            latest_successful_report_path,
            run_id=latest_successful_run_id,
        )
        latest_successful_payload = _safe_json_load(latest_successful_report_path_obj)

    risk_lag_audit_dir = (
        Path(reports_dir) / "audit" / "risk_lag_audit"
        if reports_dir
        else None
    )
    return "::".join(
        [
            "risk_lag_summary",
            _path_cache_token(latest_report_file),
            _path_cache_token(publication_registry_file),
            _risk_lag_summary_report_artifact_token(latest_payload),
            _path_cache_token(latest_successful_report_path_obj),
            _risk_lag_summary_report_artifact_token(latest_successful_payload),
            _glob_cache_token(risk_lag_audit_dir, "risk_lag_audit_*.json"),
        ]
    )


def _load_risk_lag_summary_uncached() -> dict:
    latest_path, latest_payload = _load_same_run_risk_audit_payload_from_latest_report(
        "risk_lag_audit_json",
        scope_detail="当前摘要来自同一 run 报告绑定的 risk lag 审计快照，尚未验证历史风险重算一致性。",
    )
    if latest_payload is None:
        latest_path, latest_payload = _select_contextual_risk_audit_payload(
            settings.reports_dir / "audit" / "risk_lag_audit",
            filename_glob="risk_lag_audit_*.json",
            business_date_keys=("candidate_date",),
            label="risk lag",
            latest_loader=lambda: load_latest_risk_lag_audit_payload(settings),
        )
    payload = latest_payload or {}
    threshold_semantics = build_risk_coverage_threshold_semantics(payload)
    summary = {
        "latest_artifact_path": str(latest_path) if latest_path else None,
        "latest_generated_at": payload.get("generated_at"),
        "candidate_date": payload.get("candidate_date"),
        "risk_1y_latest_end_date": payload.get("risk_1y_latest_end_date"),
        "risk_1y_effective_end_date": payload.get("risk_1y_effective_end_date"),
        "lag_days_candidate_vs_latest": payload.get("lag_days_candidate_vs_latest"),
        "lag_days_candidate_vs_effective": payload.get("lag_days_candidate_vs_effective"),
        "candidate_fund_count": int(payload.get("candidate_fund_count") or 0),
        "risk_1y_latest_fund_count": int(payload.get("risk_1y_latest_fund_count") or 0),
        "risk_1y_effective_fund_count": int(payload.get("risk_1y_effective_fund_count") or 0),
        "coverage_ratio_vs_candidate": threshold_semantics["coverage_ratio_vs_candidate"],
        "minimum_coverage_ratio": threshold_semantics["minimum_coverage_ratio"],
        "publish_minimum_coverage_ratio": threshold_semantics["publish_minimum_coverage_ratio"],
        "observation_coverage_ratio": threshold_semantics["observation_coverage_ratio"],
        "coverage_observation_line_met": threshold_semantics["coverage_observation_line_met"],
        "coverage_gate_status": threshold_semantics["coverage_gate_status"],
        "coverage_gate_label": threshold_semantics["coverage_gate_label"],
        "coverage_gate_explanation": threshold_semantics["coverage_gate_explanation"],
        "coverage_threshold_role": threshold_semantics["coverage_threshold_role"],
        "observation_threshold_role": threshold_semantics["observation_threshold_role"],
        "dates_aligned": bool(payload.get("dates_aligned")),
        "coverage_sufficient": bool(threshold_semantics["coverage_sufficient"]),
        "aligned": bool(payload.get("aligned")),
        "effective_risk_uncovered_candidate_total": int(payload.get("effective_risk_uncovered_candidate_total") or 0),
        "effective_risk_uncovered_candidate_ftype_counts": payload.get("effective_risk_uncovered_candidate_ftype_counts") or {},
        "effective_risk_uncovered_candidate_samples": (payload.get("effective_risk_uncovered_candidate_samples") or [])[:10],
    }
    summary.update(
        _normalize_risk_audit_runtime_scope(
            payload,
            scope_detail="当前摘要来自最新 risk lag 审计产物，不提供历史 run 级别回放。",
            latest_canonical_run_id=_resolve_current_canonical_run_context().get("latest_canonical_run_id"),
        )
    )
    return summary


def _build_risk_lag_summary_from_report_freshness(
    report_payload: dict[str, Any] | None,
) -> dict[str, Any]:
    current = dict(report_payload or {})
    freshness = (current.get("database_freshness") or {}).get("risk_1y_freshness") or {}
    if not isinstance(freshness, dict) or not freshness:
        return {}
    required_keys = (
        "candidate_date",
        "risk_1y_effective_end_date",
        "coverage_ratio_vs_candidate",
        "dates_aligned",
        "coverage_sufficient",
    )
    if any(freshness.get(key) is None for key in required_keys):
        return {}

    threshold_semantics = build_risk_coverage_threshold_semantics(freshness)
    candidate_fund_count = _to_int_or_none(freshness.get("candidate_fund_count"))
    risk_1y_latest_fund_count = _to_int_or_none(freshness.get("risk_1y_latest_fund_count"))
    risk_1y_effective_fund_count = _to_int_or_none(freshness.get("risk_1y_effective_fund_count"))
    uncovered_total = _to_int_or_none(freshness.get("effective_risk_uncovered_candidate_total"))
    if uncovered_total is None and candidate_fund_count is not None and risk_1y_effective_fund_count is not None:
        uncovered_total = max(0, candidate_fund_count - risk_1y_effective_fund_count)

    run_id = str(current.get("run_id") or "").strip() or None
    generated_at = str(current.get("generated_at") or "").strip() or None
    return {
        "latest_artifact_path": None,
        "latest_generated_at": generated_at,
        "candidate_date": freshness.get("candidate_date"),
        "risk_1y_latest_end_date": freshness.get("risk_1y_latest_end_date"),
        "risk_1y_effective_end_date": freshness.get("risk_1y_effective_end_date"),
        "lag_days_candidate_vs_latest": freshness.get("lag_days_candidate_vs_latest"),
        "lag_days_candidate_vs_effective": freshness.get("lag_days_candidate_vs_effective"),
        "candidate_fund_count": candidate_fund_count,
        "risk_1y_latest_fund_count": risk_1y_latest_fund_count,
        "risk_1y_effective_fund_count": risk_1y_effective_fund_count,
        "coverage_ratio_vs_candidate": threshold_semantics["coverage_ratio_vs_candidate"],
        "minimum_coverage_ratio": threshold_semantics["minimum_coverage_ratio"],
        "publish_minimum_coverage_ratio": threshold_semantics["publish_minimum_coverage_ratio"],
        "observation_coverage_ratio": threshold_semantics["observation_coverage_ratio"],
        "coverage_observation_line_met": threshold_semantics["coverage_observation_line_met"],
        "coverage_gate_status": threshold_semantics["coverage_gate_status"],
        "coverage_gate_label": threshold_semantics["coverage_gate_label"],
        "coverage_gate_explanation": threshold_semantics["coverage_gate_explanation"],
        "coverage_threshold_role": threshold_semantics["coverage_threshold_role"],
        "observation_threshold_role": threshold_semantics["observation_threshold_role"],
        "dates_aligned": bool(freshness.get("dates_aligned")),
        "coverage_sufficient": bool(threshold_semantics["coverage_sufficient"]),
        "aligned": bool(
            freshness.get("aligned")
            if freshness.get("aligned") is not None
            else bool(freshness.get("dates_aligned")) and bool(threshold_semantics["coverage_sufficient"])
        ),
        "effective_risk_uncovered_candidate_total": uncovered_total,
        "run_id": run_id,
        "latest_canonical_run_id": run_id,
        "replay_precision_status": "snapshot_frozen",
        "replay_precision_status_label": "同 run 报告快照",
        "baseline_reproduction_verified": False,
        "auxiliary_scope": "same_run_report_freshness_snapshot",
        "auxiliary_scope_label": "同 run 风险新鲜度快照",
        "scope_detail": (
            "当前摘要来自同一 run 报告内 database_freshness.risk_1y_freshness 轻量投影；"
            "默认状态入口不携带完整 risk lag 样本，完整审计请使用 full/detail 入口。"
        ),
        "summary_source": "latest_report.database_freshness.risk_1y_freshness",
        "sample_scope": "omitted",
    }


def _load_default_risk_lag_summary(report_payload: dict[str, Any] | None) -> dict[str, Any]:
    return _build_risk_lag_summary_from_report_freshness(report_payload) or _load_risk_lag_summary()


def _summarize_risk_lag_summary_for_status_summary(summary: dict[str, Any] | None) -> dict[str, Any]:
    return _compact_dict(
        summary,
        (
            "latest_artifact_path",
            "latest_generated_at",
            "candidate_date",
            "risk_1y_latest_end_date",
            "risk_1y_effective_end_date",
            "lag_days_candidate_vs_latest",
            "lag_days_candidate_vs_effective",
            "candidate_fund_count",
            "risk_1y_latest_fund_count",
            "risk_1y_effective_fund_count",
            "coverage_ratio_vs_candidate",
            "minimum_coverage_ratio",
            "publish_minimum_coverage_ratio",
            "observation_coverage_ratio",
            "coverage_observation_line_met",
            "coverage_gate_status",
            "coverage_gate_label",
            "coverage_gate_explanation",
            "dates_aligned",
            "coverage_sufficient",
            "aligned",
            "effective_risk_uncovered_candidate_total",
            "run_id",
            "latest_canonical_run_id",
            "replay_precision_status",
            "replay_precision_status_label",
            "auxiliary_scope",
            "auxiliary_scope_label",
            "scope_detail",
            "summary_source",
            "sample_scope",
        ),
    )


def _load_risk_lag_summary() -> dict:
    return _cached_payload(
        _risk_lag_summary_cache_key(),
        _RISK_LAG_SUMMARY_CACHE_TTL_SECONDS,
        _load_risk_lag_summary_uncached,
    )


def _load_risk_sync_summary() -> dict:
    return _cached_payload(
        _risk_sync_summary_cache_key(),
        _RISK_SYNC_SUMMARY_CACHE_TTL_SECONDS,
        _load_risk_sync_summary_uncached,
    )


def _risk_sync_summary_report_artifact_token(report_payload: dict[str, Any] | None) -> str:
    artifacts = dict((report_payload or {}).get("audit_files") or {})
    artifact_value = str(artifacts.get("risk_sync_audit_json") or "").strip()
    if not artifact_value:
        return "missing"
    project_root = getattr(settings, "project_root", None)
    artifact_path = Path(project_root) / artifact_value if project_root else Path(artifact_value)
    return _path_cache_token(artifact_path)


def _risk_sync_summary_cache_key() -> str:
    latest_report_file = getattr(settings, "latest_report_file", None)
    publication_registry_file = getattr(settings, "publication_registry_file", None)
    reports_dir = getattr(settings, "reports_dir", None)
    latest_payload = _safe_json_load(Path(latest_report_file)) if latest_report_file else {}
    publication = _load_publication_registry_snapshot_cached() if publication_registry_file else {}
    latest_successful_run = ((publication.get("automation_layers") or {}).get("latest_successful_run") or {})
    latest_successful_run_id = str(latest_successful_run.get("run_id") or "").strip() or None
    latest_successful_report_path = str(latest_successful_run.get("report_path") or "").strip()
    latest_successful_report_path_obj: Path | None = None
    latest_successful_payload: dict[str, Any] = {}
    if latest_successful_report_path:
        latest_successful_report_path_obj = _resolve_run_report_path(
            latest_successful_report_path,
            run_id=latest_successful_run_id,
        )
        latest_successful_payload = _safe_json_load(latest_successful_report_path_obj)

    risk_sync_audit_dir = (
        Path(reports_dir) / "audit" / "risk_sync_audit"
        if reports_dir
        else None
    )
    return "::".join(
        [
            "risk_sync_summary",
            _path_cache_token(latest_report_file),
            _path_cache_token(publication_registry_file),
            _risk_sync_summary_report_artifact_token(latest_payload),
            _path_cache_token(latest_successful_report_path_obj),
            _risk_sync_summary_report_artifact_token(latest_successful_payload),
            _glob_cache_token(risk_sync_audit_dir, "risk_sync_audit_*.json"),
        ]
    )


def _load_risk_sync_summary_uncached() -> dict:
    latest_path, latest_payload = _load_same_run_risk_audit_payload_from_latest_report(
        "risk_sync_audit_json",
        scope_detail="当前摘要来自同一 run 报告绑定的 risk sync 审计快照，尚未验证历史风险重算一致性。",
    )
    if latest_payload is None:
        latest_path, latest_payload = _select_contextual_risk_audit_payload(
            settings.reports_dir / "audit" / "risk_sync_audit",
            filename_glob="risk_sync_audit_*.json",
            business_date_keys=("candidate_date", "effective_candidate_date"),
            label="risk sync",
            latest_loader=lambda: load_latest_risk_sync_audit_payload(settings),
        )
    payload = latest_payload or {}
    summary = {
        "latest_artifact_path": str(latest_path) if latest_path else None,
        "latest_generated_at": payload.get("generated_at"),
        "configured_periods": payload.get("configured_periods") or [],
        "period_coverage_grace_days": payload.get("period_coverage_grace_days"),
        "changed_nav_candidate_total": int(payload.get("changed_nav_candidate_total") or 0),
        "period_eligible_gap_candidate_total": int(payload.get("period_eligible_gap_candidate_total") or 0),
        "actionable_candidate_total": int(payload.get("actionable_candidate_total") or 0),
        "blocked_by_nav_history_total": int(payload.get("blocked_by_nav_history_total") or 0),
        "blocked_by_nav_history_lag_total": int(payload.get("blocked_by_nav_history_lag_total") or 0),
        "blocked_by_returns_lag_total": int(payload.get("blocked_by_returns_lag_total") or 0),
        "low_priority_cadence_tail_total": int(payload.get("low_priority_cadence_tail_total") or 0),
        "period_gap_counts": payload.get("period_gap_counts") or {},
        "nav_span_bucket_counts": payload.get("nav_span_bucket_counts") or {},
        "ftype_counts": payload.get("ftype_counts") or {},
        "blocked_by_nav_history_ftype_counts": payload.get("blocked_by_nav_history_ftype_counts") or {},
        "candidate_samples": (payload.get("candidate_samples") or [])[:10],
        "actionable_candidate_samples": (payload.get("actionable_candidate_samples") or [])[:10],
        "low_priority_cadence_tail_samples": (payload.get("low_priority_cadence_tail_samples") or [])[:10],
    }
    summary.update(
        _normalize_risk_audit_runtime_scope(
            payload,
            scope_detail="当前摘要来自最新 risk sync 审计产物，不提供历史 run 级别回放。",
            latest_canonical_run_id=_resolve_current_canonical_run_context().get("latest_canonical_run_id"),
        )
    )
    return summary


def _load_risk_coverage_gap_summary() -> dict:
    latest_path, latest_payload = _load_same_run_risk_audit_payload_from_latest_report(
        "risk_coverage_gap_audit_json",
        scope_detail="当前摘要来自同一 run 报告绑定的 risk coverage gap 审计快照，尚未验证历史风险重算一致性。",
    )
    if latest_payload is None:
        latest_path, latest_payload = _select_contextual_risk_audit_payload(
            settings.reports_dir / "audit" / "risk_coverage_gap_audit",
            filename_glob="risk_coverage_gap_audit_*.json",
            business_date_keys=("effective_candidate_date", "candidate_date"),
            label="risk coverage gap",
            latest_loader=lambda: load_latest_risk_coverage_gap_audit_payload(settings),
        )
    payload = latest_payload or {}
    formal_pool_impact_summary = payload.get("formal_pool_impact_summary") or {}
    summary = {
        "latest_artifact_path": str(latest_path) if latest_path else None,
        "latest_generated_at": payload.get("generated_at"),
        "effective_candidate_date": payload.get("effective_candidate_date"),
        "effective_risk_end_date": payload.get("effective_risk_end_date"),
        "active_universe_total": int(payload.get("active_universe_total") or 0),
        "risk_ready_total": int(payload.get("risk_ready_total") or 0),
        "missing_1y_risk_total": int(payload.get("missing_1y_risk_total") or 0),
        "screening_relevant_gap_total": int(payload.get("screening_relevant_gap_total") or 0),
        "actionable_gap_total": int(payload.get("actionable_gap_total") or 0),
        "natural_short_history_gap_total": int(payload.get("natural_short_history_gap_total") or 0),
        "low_priority_blocked_gap_total": int(payload.get("low_priority_blocked_gap_total") or 0),
        "cadence_tail_low_priority_gap_total": int(payload.get("cadence_tail_low_priority_gap_total") or 0),
        "effective_snapshot_gap_total": int(payload.get("effective_snapshot_gap_total") or 0),
        "effective_candidate_snapshot_gap_reason_counts": payload.get("effective_candidate_snapshot_gap_reason_counts") or {},
        "non_candidate_tail_total": int(payload.get("non_candidate_tail_total") or 0),
        "non_candidate_tail_reason_counts": payload.get("non_candidate_tail_reason_counts") or {},
        "non_candidate_tail_category_counts": payload.get("non_candidate_tail_category_counts") or {},
        "formal_pool_blocking_total": int(formal_pool_impact_summary.get("formal_pool_blocking_total") or 0),
        "formal_pool_actionable_total": int(formal_pool_impact_summary.get("formal_pool_actionable_total") or 0),
        "formal_pool_natural_short_history_total": int(formal_pool_impact_summary.get("formal_pool_natural_short_history_total") or 0),
        "formal_pool_non_blocking_relevant_total": int(formal_pool_impact_summary.get("formal_pool_non_blocking_relevant_total") or 0),
        "formal_pool_blocking_reason_counts": formal_pool_impact_summary.get("formal_pool_blocking_reason_counts") or {},
        "formal_pool_non_blocking_reason_counts": formal_pool_impact_summary.get("formal_pool_non_blocking_reason_counts") or {},
        "current_publish_blocker_total": int(formal_pool_impact_summary.get("current_publish_blocker_total") or 0),
        "current_publish_blocker_reason_counts": formal_pool_impact_summary.get("current_publish_blocker_reason_counts") or {},
        "formal_pool_impact_summary": formal_pool_impact_summary,
        "new_or_short_history_gap_total": int(payload.get("new_or_short_history_gap_total") or 0),
        "priority_bucket_counts": payload.get("priority_bucket_counts") or {},
        "priority_bucket_reason_counts": payload.get("priority_bucket_reason_counts") or {},
        "primary_reason_counts": payload.get("primary_reason_counts") or {},
        "screening_relevant_reason_counts": payload.get("screening_relevant_reason_counts") or {},
    }
    summary.update(
        _normalize_risk_audit_runtime_scope(
            payload,
            scope_detail="当前摘要来自最新 risk coverage gap 审计产物，不提供历史 run 级别回放。",
            latest_canonical_run_id=_resolve_current_canonical_run_context().get("latest_canonical_run_id"),
        )
    )
    return summary


def _resolve_decision_ledger_path(latest_payload: dict | None) -> Path | None:
    payload = latest_payload or {}
    artifacts = payload.get("artifacts") or {}
    core_artifacts = artifacts.get("core") or {}
    candidates = [
        core_artifacts.get("final_screening_decision_ledger_file"),
        artifacts.get("final_screening_decision_ledger_json"),
    ]
    report_json = artifacts.get("report_json")
    if report_json:
        report_path = settings.project_root / str(report_json)
        if report_path.exists():
            try:
                report_payload = json.loads(report_path.read_text(encoding="utf-8"))
            except Exception:
                report_payload = {}
            report_artifacts = report_payload.get("artifacts") or {}
            report_core = report_artifacts.get("core") or {}
            audit_files = report_payload.get("audit_files") or {}
            internal_audit = (report_payload.get("internal_audit") or {}).get("core_sidecar_paths") or {}
            candidates.extend(
                [
                    report_core.get("final_screening_decision_ledger_file"),
                    audit_files.get("final_screening_decision_ledger_json"),
                    internal_audit.get("final_screening_decision_ledger_file"),
                ]
            )
    for candidate in candidates:
        if not candidate:
            continue
        path = settings.project_root / str(candidate)
        if path.exists():
            return path
    return None


def _load_exclusion_reason_summary(latest_payload: dict | None) -> dict:
    payload = latest_payload or {}
    run_id = str(payload.get("run_id") or "").strip() or None
    runtime_scope_summary = {
        "run_id": run_id,
        "latest_canonical_run_id": run_id,
        "replay_precision_status": "missing",
        "replay_precision_status_label": "非精确回放",
        "auxiliary_scope": (
            "same_run_report_artifact_rebuild"
            if run_id
            else "latest_report_artifact_rebuild"
        ),
        "auxiliary_scope_label": (
            "同 run 报告 + 侧车补建"
            if run_id
            else "latest 报告 + 侧车补建"
        ),
        "scope_detail": (
            "当前摘要优先复用报告内的筛选审计块；缺失时会回退到同批报告产物中的 "
            "stage_b reason / screening audit workbook / annual_return_gate_shadow 侧车结果补建，"
            "不属于独立历史回放结果。"
        ),
    }
    project_summary = payload.get("project_publish_summary") or {}
    ledger_summary = project_summary.get("high_yield_exclusion_ledger") or {}
    standard_screening_audit = project_summary.get("standard_screening_audit") or {}
    score_cluster_explainability = standard_screening_audit.get("score_cluster_explainability") or {}
    annual_return_score_sharpe_coupling = (
        standard_screening_audit.get("annual_return_score_sharpe_coupling") or {}
    )
    annual_return_coupling_needs_refresh = (
        not annual_return_score_sharpe_coupling
        or not bool(annual_return_score_sharpe_coupling.get("available"))
        or "annual_return_only_style_preview" not in annual_return_score_sharpe_coupling
        or "annual_return_only_score_band_preview" not in annual_return_score_sharpe_coupling
        or "annual_return_only_style_band_preview" not in annual_return_score_sharpe_coupling
        or "annual_return_only_local_review_priority_preview"
        not in annual_return_score_sharpe_coupling
        or "dominant_local_boundary_review" not in annual_return_score_sharpe_coupling
    )
    annual_return_gate_shadow: dict[str, Any] = {}
    high_score_friction_gate_review = (
        standard_screening_audit.get("high_score_friction_gate_review") or {}
    )
    risk_adjusted_metric_stack_review = (
        standard_screening_audit.get("risk_adjusted_metric_stack_review") or {}
    )
    if (
        not score_cluster_explainability
        or annual_return_coupling_needs_refresh
        or not high_score_friction_gate_review
        or not risk_adjusted_metric_stack_review
    ):
        stage_b_reason_csv = str((payload.get("artifacts") or {}).get("stage_b_reason_csv") or "").strip()
        if stage_b_reason_csv:
            stage_b_reason_path = settings.project_root / stage_b_reason_csv
            if artifact_is_file(stage_b_reason_path):
                try:
                    ledger_df = read_csv_artifact(stage_b_reason_path)
                    if not score_cluster_explainability:
                        score_cluster_explainability = _build_score_cluster_explainability_audit(
                            ledger_df
                        )
                    if annual_return_coupling_needs_refresh:
                        style_threshold_rows = (
                            (((payload.get("project_publish_summary") or {}).get("threshold_comparison") or {}).get("style_thresholds"))
                            or []
                        )
                        annual_return_score_sharpe_coupling = (
                            _build_annual_return_score_sharpe_coupling_audit(
                                ledger_df,
                                style_threshold_rows=style_threshold_rows,
                            )
                        )
                except CoreCsvStorageError:
                    raise
                except Exception:
                    if not score_cluster_explainability:
                        score_cluster_explainability = {}
                    if not annual_return_score_sharpe_coupling:
                        annual_return_score_sharpe_coupling = {}
        if not high_score_friction_gate_review:
            screening_audit_excel = str(
                (payload.get("artifacts") or {}).get("screening_audit_excel") or ""
            ).strip()
            if screening_audit_excel:
                screening_audit_path = settings.project_root / screening_audit_excel
                if screening_audit_path.exists():
                    try:
                        high_score_friction_gate_review = (
                            load_high_score_friction_gate_review_from_workbook(
                                screening_audit_path,
                                high_score_threshold=float(
                                    standard_screening_audit.get("high_score_threshold") or 75.0
                                ),
                            )
                        )
                    except Exception:
                        high_score_friction_gate_review = {}
        if not risk_adjusted_metric_stack_review:
            screening_audit_excel = str(
                (payload.get("artifacts") or {}).get("screening_audit_excel") or ""
            ).strip()
            if screening_audit_excel:
                screening_audit_path = settings.project_root / screening_audit_excel
                if screening_audit_path.exists():
                    try:
                        risk_adjusted_metric_stack_review = (
                            load_risk_adjusted_metric_stack_review_from_workbook(
                                screening_audit_path,
                                positive_ratio_semantics=project_summary.get(
                                    "positive_ratio_semantics"
                                )
                                or {},
                                low_vol_positive_ratio_shadow_watch=project_summary.get(
                                    "low_vol_positive_ratio_shadow_watch"
                                )
                                or {},
                            )
                        )
                    except Exception:
                        risk_adjusted_metric_stack_review = {}
    verified_low_vol_watch = normalize_low_vol_positive_ratio_shadow_watch(
        project_summary, report_identity=payload, settings=settings,
    )
    risk_adjusted_metric_stack_review = normalize_positive_ratio_review_evidence(
        risk_adjusted_metric_stack_review, build_positive_ratio_semantics_summary(project_summary, verified_low_vol_watch),
    )
    latest_shadow_path, latest_shadow_payload = load_latest_annual_return_gate_shadow_payload(
        settings,
        report=payload,
    )
    if latest_shadow_payload:
        annual_return_gate_shadow = dict(latest_shadow_payload or {})
        if latest_shadow_path is not None:
            annual_return_gate_shadow.setdefault(
                "artifact_project_path",
                str(latest_shadow_path.relative_to(settings.project_root)),
            )
    if not ledger_summary:
        return {
            **runtime_scope_summary,
            "available": False,
            "ledger_path": None,
            "excluded_candidate_total": 0,
            "high_score_candidate_total": 0,
            "high_yield_thresholds": [],
            "bucket_counts": [],
            "high_score_bucket_counts": [],
            "top_reasons": [],
            "top_high_score_excluded": [],
            "reason_group_preview": [],
            "quality_factor_group_preview": [],
            "quality_factor_detail_preview": [],
            "quality_overlap_combo_preview": [],
            "annual_return_score_sharpe_coupling": annual_return_score_sharpe_coupling or {},
            "annual_return_gate_shadow": annual_return_gate_shadow or {},
            "score_cluster_explainability": score_cluster_explainability or {},
            "high_score_friction_gate_review": high_score_friction_gate_review or {},
            "risk_adjusted_metric_stack_review": risk_adjusted_metric_stack_review or {},
            "high_score_friction_count": 0,
            "high_score_friction_reason_preview": [],
            "high_score_friction_style_preview": [],
            "status_split_preview": [],
        }
    high_yield_thresholds = ledger_summary.get("high_yield_thresholds") or []
    annual_ge_5 = next(
        (item for item in high_yield_thresholds if float(item.get("threshold") or 0.0) == 5.0),
        {},
    )
    return {
        **runtime_scope_summary,
        "available": True,
        "ledger_path": str((payload.get("artifacts") or {}).get("stage_b_reason_excel") or ""),
        "excluded_candidate_total": int(ledger_summary.get("excluded_total") or 0),
        "high_score_candidate_total": int(annual_ge_5.get("count") or 0),
        "high_yield_thresholds": high_yield_thresholds,
        "bucket_counts": [],
        "high_score_bucket_counts": [
            {
                "bucket": "annual_ge_5",
                "label": "年化>=5%未入榜",
                "count": int(annual_ge_5.get("count") or 0),
            }
        ],
        "top_reasons": [
            {"reason": str(item.get("reason") or ""), "count": int(item.get("count") or 0)}
            for item in (annual_ge_5.get("top_primary_reasons") or [])[:5]
        ],
        "top_high_score_excluded": (annual_ge_5.get("sample_preview") or [])[:5],
        "reason_group_preview": standard_screening_audit.get("reason_group_preview") or [],
        "quality_factor_group_preview": standard_screening_audit.get("quality_factor_group_preview") or [],
        "quality_factor_detail_preview": standard_screening_audit.get("quality_factor_detail_preview") or [],
        "quality_overlap_combo_preview": standard_screening_audit.get("quality_overlap_combo_preview") or [],
        "annual_return_score_sharpe_coupling": annual_return_score_sharpe_coupling or {},
        "annual_return_gate_shadow": annual_return_gate_shadow or {},
        "score_cluster_explainability": score_cluster_explainability or {},
        "high_score_friction_gate_review": high_score_friction_gate_review or {},
        "risk_adjusted_metric_stack_review": risk_adjusted_metric_stack_review or {},
        "high_score_friction_count": int(standard_screening_audit.get("high_score_friction_count") or 0),
        "high_score_friction_reason_preview": standard_screening_audit.get("high_score_friction_reason_preview") or [],
        "high_score_friction_style_preview": standard_screening_audit.get("high_score_friction_style_preview") or [],
        "status_split_preview": standard_screening_audit.get("status_split_preview") or [],
    }


@app.get("/")
def index() -> Response:
    return send_from_directory(FRONTEND_DIR, "index.html")


@app.get("/tracker")
def tracker_page() -> Response:
    return send_from_directory(FRONTEND_DIR, "tracker.html")


@app.get("/recommendations")
def recommendations_page() -> Response:
    return send_from_directory(FRONTEND_DIR, "recommendations.html")


@app.get("/favorites")
def favorites_page() -> Response:
    return send_from_directory(FRONTEND_DIR, "favorites.html")


@app.get("/reports")
def reports_page() -> Response:
    return send_from_directory(FRONTEND_DIR, "reports.html")


@app.get("/task-summary")
def task_summary_page() -> Response:
    return send_from_directory(FRONTEND_DIR, "task-summary.html")


@app.get("/operations")
def operations_page() -> Response:
    return send_from_directory(FRONTEND_DIR, "operations.html")


@app.get("/settings")
def settings_page() -> Response:
    return send_from_directory(FRONTEND_DIR, "settings.html")


@app.get("/api-settings")
def api_settings_page_alias() -> Response:
    return send_from_directory(FRONTEND_DIR, "settings.html")


@app.get("/excel-viewer")
def excel_viewer() -> Response:
    return send_from_directory(FRONTEND_DIR, "excel_viewer.html")


@app.get("/app.js")
def app_js() -> Response:
    return send_from_directory(FRONTEND_DIR, "app.js")


@app.get("/shared.js")
def shared_js() -> Response:
    return send_from_directory(FRONTEND_DIR, "shared.js")


@app.get("/scheduler-shared.js")
def scheduler_shared_js() -> Response:
    return send_from_directory(FRONTEND_DIR, "scheduler-shared.js")


@app.get("/operations-scheduler-detail.js")
def operations_scheduler_detail_js() -> Response:
    return send_from_directory(FRONTEND_DIR, "operations-scheduler-detail.js")


@app.get("/operations-regression-smoke.js")
def operations_regression_smoke_js() -> Response:
    return send_from_directory(FRONTEND_DIR, "operations-regression-smoke.js")


@app.get("/operations-workflow-status.js")
def operations_workflow_status_js() -> Response:
    return send_from_directory(FRONTEND_DIR, "operations-workflow-status.js")


@app.get("/operations-scheduler-status.js")
def operations_scheduler_status_js() -> Response:
    return send_from_directory(FRONTEND_DIR, "operations-scheduler-status.js")


@app.get("/operations-automation-layers.js")
def operations_automation_layers_js() -> Response:
    return send_from_directory(FRONTEND_DIR, "operations-automation-layers.js")


@app.get("/operations-governance-summary.js")
def operations_governance_summary_js() -> Response:
    return send_from_directory(FRONTEND_DIR, "operations-governance-summary.js")


@app.get("/operations-dashboard-watch.js")
def operations_dashboard_watch_js() -> Response:
    return send_from_directory(FRONTEND_DIR, "operations-dashboard-watch.js")


@app.get("/version-widget.js")
def version_widget_js() -> Response:
    return send_from_directory(FRONTEND_DIR, "version-widget.js")


@app.get("/tracker.js")
def tracker_js() -> Response:
    return send_from_directory(FRONTEND_DIR, "tracker.js")


@app.get("/recommendations.js")
def recommendations_js() -> Response:
    return send_from_directory(FRONTEND_DIR, "recommendations.js")


@app.get("/favorites.js")
def favorites_js() -> Response:
    return send_from_directory(FRONTEND_DIR, "favorites.js")


@app.get("/favorites-export.js")
def favorites_export_js() -> Response:
    return send_from_directory(FRONTEND_DIR, "favorites-export.js")


@app.get("/reports.js")
def reports_js() -> Response:
    return send_from_directory(FRONTEND_DIR, "reports.js")


@app.get("/task-summary.js")
def task_summary_js() -> Response:
    return send_from_directory(FRONTEND_DIR, "task-summary.js")


@app.get("/operations.js")
def operations_js() -> Response:
    return send_from_directory(FRONTEND_DIR, "operations.js")


@app.get("/settings.js")
def settings_js() -> Response:
    return send_from_directory(FRONTEND_DIR, "settings.js")


@app.get("/excel-viewer.js")
def excel_viewer_js() -> Response:
    return send_from_directory(FRONTEND_DIR, "excel_viewer.js")


@app.get("/styles.css")
def styles_css() -> Response:
    return send_from_directory(FRONTEND_DIR, "styles.css")


@app.get("/favicon.svg")
def favicon_svg() -> Response:
    return send_from_directory(FRONTEND_DIR, "favicon.svg")


def _build_api_info_payload() -> dict[str, Any]:
    payload = info_payloads.build_api_info_payload(
        settings=settings,
        version=__version__,
        version_info=VERSION_INFO,
        changelog=CHANGELOG,
        runtime_started_at=_API_RUNTIME_STARTED_AT,
        manual_operations_enabled=MANUAL_OPERATION_MODE,
        publication=_load_publication_registry_snapshot_cached(),
        latest_report_snapshot=_load_latest_report_snapshot_cached(),
        scheduler_state_file=_runtime_scheduler_settings().scheduler_state_file,
    )
    payload["api_runtime"] = _build_api_settings_payload()
    return payload


def _build_health_payload() -> dict[str, Any]:
    database_source_audit = build_database_source_audit(settings)
    disk_usage = _cached_payload(
        "disk_usage_snapshot",
        _DISK_USAGE_CACHE_TTL_SECONDS,
        _build_disk_usage_snapshot,
    )
    runtime_scheduler_settings = _runtime_scheduler_settings()
    scheduler_state = _scheduler_state_with_runtime_evidence(runtime_scheduler_settings)
    publication = _load_publication_registry_cached() or _load_publication_registry_snapshot_cached()
    scheduler_status_payload = _build_health_scheduler_payload(
        scheduler_state,
        publication=publication,
    )
    scheduler_status_summary = _build_health_scheduler_status_summary(scheduler_status_payload)
    return {
        "status": "ok",
        "tiantian_base_url": settings.tiantian_base_url,
        "autostock_base_url": settings.autostock_base_url,
        "api_runtime": _build_api_settings_payload(),
        "database_source_audit": database_source_audit,
        "manual_operations_enabled": MANUAL_OPERATION_MODE,
        "scheduler_status": scheduler_status_summary,
        "automation_autonomy": scheduler_status_payload.get("automation_autonomy") or {},
        "scheduler_path_governance": scheduler_status_payload.get("scheduler_path_governance") or {},
        "schedule_stability_summary": scheduler_status_payload.get("schedule_stability_summary") or {},
        # P0-02.4 / B4: 暴露 archive / logs / 限速状态等的磁盘占用，
        # 供前端被动监控与运维脚本拉取告警（无需进入容器 du）。
        "disk_usage": disk_usage,
        "autostock_rate_limit": {
            "max_calls_per_hour": int(settings.autostock_rate_limit_max_calls_per_hour),
            "min_interval_seconds": float(settings.autostock_rate_limit_min_interval_seconds),
            "shared_state_path": str(settings.autostock_rate_limit_shared_state_path),
        },
        "autostock_directory_source_summary": _build_autostock_directory_source_health_summary(),
    }


def _read_json_file_silent(path: Path | str | None) -> dict[str, Any]:
    if not path:
        return {}
    return _safe_json_load(Path(path))


def _project_path_from_value(value: Any) -> Path | None:
    text = str(value or "").strip()
    if not text:
        return None
    path = Path(text)
    if path.is_absolute():
        return path
    return settings.project_root / path


def _read_fund_codes_from_csv(path: Path | None) -> list[str]:
    if not path or not artifact_is_file(path):
        return []
    codes: list[str] = []
    seen: set[str] = set()
    try:
        with open_csv_artifact(path) as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                if not isinstance(row, dict):
                    continue
                code = _normalize_code_text(
                    row.get("基金代码") or row.get("fcode") or row.get("code")
                )
                if code and code not in seen:
                    seen.add(code)
                    codes.append(code)
    except OSError:
        return []
    return codes


def _load_latest_bond_universe_sync_artifact() -> tuple[Path | None, dict[str, Any]]:
    audit_dir = settings.reports_dir / "audit" / "bond_universe_sync"
    try:
        candidates = sorted(
            audit_dir.glob("bond_universe_sync_*.json"),
            key=lambda path: path.stat().st_mtime_ns,
            reverse=True,
        )
    except OSError:
        candidates = []
    if not candidates:
        return None, {}
    artifact_path = candidates[0]
    return artifact_path, _read_json_file_silent(artifact_path)


def _load_latest_data_truthfulness_audit_artifact() -> tuple[Path | None, dict[str, Any]]:
    from backend.task_audit_storage import glob_json_artifacts

    audit_dir = settings.reports_dir / "audit" / "data_truthfulness"
    try:
        # The producer's timestamped identity survives compression and restore;
        # a storage operation's mtime must not make an old audit the newest.
        candidates = sorted(
            glob_json_artifacts(audit_dir, "data_truthfulness_audit_*.json"),
            key=lambda path: path.name,
            reverse=True,
        )
    except OSError:
        candidates = []
    if not candidates:
        return None, {}
    artifact_path = candidates[0]
    return artifact_path, _read_json_file_silent(artifact_path)


def _autostock_directory_parse_error_codes_from_samples(
    samples: list[Any],
    explicit_codes: list[Any] | None = None,
) -> list[str]:
    codes: list[str] = []
    seen: set[str] = set()
    for raw_code in explicit_codes or []:
        code = _normalize_code_text(raw_code)
        if code and code not in seen:
            seen.add(code)
            codes.append(code)
    for sample in samples:
        if not isinstance(sample, dict):
            continue
        code = _normalize_code_text(sample.get("code"))
        row_preview = sample.get("row_preview")
        if not code and isinstance(row_preview, list) and row_preview:
            code = _normalize_code_text(row_preview[0])
        if code and code not in seen:
            seen.add(code)
            codes.append(code)
    return codes


def _read_autostock_directory_parse_error_local_classification_scope(
    parse_error_codes: list[str],
    final_code_set: set[str],
    payload: dict[str, Any],
    evidence: dict[str, Any],
) -> dict[str, Any]:
    def coerce_int(value: Any) -> int:
        if value in (None, ""):
            return 0
        try:
            return int(value)
        except (TypeError, ValueError):
            try:
                return int(float(value))
            except (TypeError, ValueError):
                return 0

    warnings: list[str] = []
    db_path = Path(getattr(settings, "database_path", settings.project_root / "backend" / "data" / "bond_funds.db"))
    local_rows_by_code: dict[str, dict[str, Any]] = {}
    if parse_error_codes and not db_path.exists():
        warnings.append("directory_parse_error_impact_database_missing")
    elif parse_error_codes:
        try:
            with sqlite3.connect(str(db_path)) as conn:
                conn.row_factory = sqlite3.Row
                table_exists = bool(
                    conn.execute(
                        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
                        ("funds",),
                    ).fetchone()
                )
                if not table_exists:
                    warnings.append("directory_parse_error_impact_funds_table_missing")
                else:
                    columns = {
                        str(row["name"])
                        for row in conn.execute("PRAGMA table_info(funds)").fetchall()
                        if row["name"]
                    }
                    if "fcode" not in columns:
                        warnings.append("directory_parse_error_impact_funds_fcode_missing")
                    else:
                        optional_fields = (
                            "shortname",
                            "ftype",
                            "sub_type",
                            "type_code",
                            "type_category",
                            "data_source",
                            "last_updated_at",
                        )
                        select_exprs = ["fcode"]
                        for field in optional_fields:
                            select_exprs.append(field if field in columns else f"NULL AS {field}")
                        placeholders = ",".join("?" for _ in parse_error_codes)
                        rows = conn.execute(
                            f"""
                            SELECT {', '.join(select_exprs)}
                            FROM funds
                            WHERE fcode IN ({placeholders})
                            """,
                            parse_error_codes,
                        ).fetchall()
                        for row in rows:
                            code = _normalize_code_text(row["fcode"])
                            if not code:
                                continue
                            type_code = str(row["type_code"] or "").strip()
                            type_category = str(row["type_category"] or "").strip()
                            data_source = str(row["data_source"] or "").strip()
                            classification_supported = bool(
                                data_source
                                and (
                                    type_code in {"31", "32"}
                                    or type_category == "fixed_income"
                                )
                            )
                            local_rows_by_code[code] = {
                                "fcode": code,
                                "shortname": str(row["shortname"] or "").strip(),
                                "ftype": str(row["ftype"] or "").strip(),
                                "sub_type": str(row["sub_type"] or "").strip(),
                                "type_code": type_code,
                                "type_category": type_category,
                                "data_source": data_source,
                                "last_updated_at": str(row["last_updated_at"] or "").strip(),
                                "classification_supported": classification_supported,
                            }
        except sqlite3.Error as exc:
            warnings.append(f"directory_parse_error_impact_database_read_failed:{exc}")

    local_missing_codes = [code for code in parse_error_codes if code not in local_rows_by_code]
    local_supported_codes = [
        code
        for code in parse_error_codes
        if local_rows_by_code.get(code, {}).get("classification_supported")
    ]
    local_unsupported_codes = [
        code
        for code in parse_error_codes
        if code in local_rows_by_code and not local_rows_by_code[code].get("classification_supported")
    ]
    formal_overlap_codes = [code for code in parse_error_codes if code in final_code_set]
    formal_overlap_unsupported_codes = [
        code
        for code in formal_overlap_codes
        if code not in local_rows_by_code or not local_rows_by_code[code].get("classification_supported")
    ]
    diffs_after_sync = payload.get("diffs_after_sync") if isinstance(payload.get("diffs_after_sync"), dict) else {}
    missing_after = coerce_int(
        evidence.get("missing_in_local_after_count")
        if evidence.get("missing_in_local_after_count") is not None
        else diffs_after_sync.get("missing_in_local_vs_autostock_total")
        or 0
    )
    missing_active_after = coerce_int(
        diffs_after_sync.get("missing_in_local_active_vs_autostock_total") or 0
    )
    if formal_overlap_unsupported_codes:
        publication_impact_status = "needs_review_final_overlap_without_local_classification"
    elif formal_overlap_codes:
        publication_impact_status = "watch_final_overlap_with_local_classification"
    elif missing_after > 0 or missing_active_after > 0:
        publication_impact_status = "watch_sync_missing_after_parse_errors"
    elif warnings:
        publication_impact_status = "watch_impact_scope_evidence_incomplete"
    else:
        publication_impact_status = "ok_no_formal_final_overlap"
    return {
        "database_path": str(db_path),
        "database_exists": db_path.exists(),
        "publication_impact_status": publication_impact_status if parse_error_codes else "not_applicable",
        "local_funds_match_count": len(local_rows_by_code),
        "local_funds_missing_count": len(local_missing_codes),
        "local_funds_missing_codes": local_missing_codes[:30],
        "local_funds_supported_classification_count": len(local_supported_codes),
        "local_funds_supported_classification_codes": local_supported_codes[:30],
        "local_funds_unsupported_classification_count": len(local_unsupported_codes),
        "local_funds_unsupported_classification_codes": local_unsupported_codes[:30],
        "local_funds_match_samples": [
            local_rows_by_code[code] for code in parse_error_codes if code in local_rows_by_code
        ][:20],
        "formal_final_overlap_unsupported_count": len(formal_overlap_unsupported_codes),
        "formal_final_overlap_unsupported_codes": formal_overlap_unsupported_codes[:30],
        "sync_missing_in_local_after_count": missing_after,
        "sync_missing_active_in_local_after_count": missing_active_after,
        "sync_newly_synced_codes_total": coerce_int(evidence.get("newly_synced_codes_total") or 0),
        "impact_scope_warnings": warnings,
    }


def _build_autostock_directory_source_health_summary() -> dict[str, Any]:
    def coerce_int(value: Any) -> int:
        if value in (None, ""):
            return 0
        try:
            return int(value)
        except (TypeError, ValueError):
            try:
                return int(float(value))
            except (TypeError, ValueError):
                return 0

    artifact_path, payload = _load_latest_bond_universe_sync_artifact()
    evidence = (
        payload.get("autostock_directory_source_evidence")
        if isinstance(payload.get("autostock_directory_source_evidence"), dict)
        else {}
    )
    if not artifact_path or not evidence:
        return {
            "available": False,
            "status": "not_available",
            "artifact_path": str(artifact_path) if artifact_path else "",
            "source": "AutoStock /fund/all",
            "endpoint_path": "/fund/all",
            "truth_policy": "preserve_missing_semantics",
            "business_value_handling": (
                "Missing AutoStock directory evidence is not treated as successful data and is not used "
                "to synthesize fund type or bond membership."
            ),
        }

    parse_error_count = int(
        evidence.get("parse_error_count")
        if evidence.get("parse_error_count") is not None
        else evidence.get("parse_errors")
        or 0
    )
    parse_error_samples = (
        evidence.get("parse_error_samples")
        if isinstance(evidence.get("parse_error_samples"), list)
        else []
    )
    explicit_parse_error_codes = (
        evidence.get("parse_error_codes")
        if isinstance(evidence.get("parse_error_codes"), list)
        else []
    )
    parse_error_codes = _autostock_directory_parse_error_codes_from_samples(
        parse_error_samples,
        explicit_parse_error_codes,
    )
    explicit_parse_error_code_count = coerce_int(
        evidence.get("parse_error_code_count")
        if evidence.get("parse_error_code_count") is not None
        else len(explicit_parse_error_codes)
    )
    latest_report = _load_latest_report_snapshot_cached() or _read_json_file_silent(settings.latest_report_file)
    artifacts = latest_report.get("artifacts") if isinstance(latest_report.get("artifacts"), dict) else {}
    filtered_csv_path = _project_path_from_value(artifacts.get("filtered_csv")) if artifacts else None
    final_codes = _read_fund_codes_from_csv(filtered_csv_path)
    final_code_set = set(final_codes)
    formal_overlap_codes = [code for code in parse_error_codes if code in final_code_set]
    impact_scope = _read_autostock_directory_parse_error_local_classification_scope(
        parse_error_codes,
        final_code_set,
        payload,
        evidence,
    )
    parse_error_reason_counts = (
        evidence.get("parse_error_reason_counts")
        if isinstance(evidence.get("parse_error_reason_counts"), dict)
        else {}
    )
    source_complete = bool(evidence.get("source_complete"))
    warnings: list[str] = []
    blockers: list[str] = []
    warnings.extend(str(item) for item in impact_scope.get("impact_scope_warnings") or [])
    publication_impact_status = str(impact_scope.get("publication_impact_status") or "").strip()
    parse_error_publication_impact_clear = bool(
        parse_error_count > 0
        and publication_impact_status == "ok_no_formal_final_overlap"
        and coerce_int(impact_scope.get("formal_final_overlap_unsupported_count") or 0) == 0
        and coerce_int(impact_scope.get("sync_missing_in_local_after_count") or 0) == 0
        and coerce_int(impact_scope.get("sync_missing_active_in_local_after_count") or 0) == 0
        and coerce_int(evidence.get("database_failed_record_count") or 0) == 0
        and coerce_int(evidence.get("failed_response_count") or 0) == 0
        and coerce_int(evidence.get("transport_exception_count") or 0) == 0
        and coerce_int(evidence.get("failed_batch_count") or 0) == 0
    )
    if parse_error_count > 0:
        warnings.append("directory_parse_errors_present")
    if not source_complete and parse_error_count > 0:
        if not parse_error_publication_impact_clear:
            warnings.append("directory_source_incomplete_due_to_parse_errors")
    elif not source_complete:
        blockers.append("directory_source_incomplete_without_explicit_parse_error_reason")
    formal_overlap_unsupported_codes = impact_scope["formal_final_overlap_unsupported_codes"]
    if formal_overlap_unsupported_codes:
        blockers.append(
            "directory_parse_error_final_overlap_without_local_classification:"
            + ",".join(formal_overlap_unsupported_codes[:20])
        )
    return {
        "available": True,
        "status": "needs_review" if blockers else "watch" if warnings else "ok",
        "artifact_path": str(artifact_path),
        "generated_at": payload.get("generated_at"),
        "source": str(evidence.get("source") or "AutoStock /fund/all"),
        "source_type": str(evidence.get("source_type") or ""),
        "endpoint_path": str(evidence.get("endpoint_path") or "/fund/all"),
        "business_date": evidence.get("business_date"),
        "business_date_semantics": evidence.get("business_date_semantics"),
        "source_complete": source_complete,
        "request_count": int(evidence.get("request_count") or 0),
        "response_count": int(evidence.get("response_count") or 0),
        "returned_record_count": int(evidence.get("returned_record_count") or 0),
        "returned_unique_count": int(evidence.get("returned_unique_count") or 0),
        "raw_total": int(evidence.get("raw_total") or 0),
        "unique_total": int(evidence.get("unique_total") or 0),
        "bond_like_total": int(evidence.get("bond_like_total") or 0),
        "parse_error_count": parse_error_count,
        "source_payload_status": "watch_parse_errors_preserved" if parse_error_count > 0 else "ok",
        "publication_impact_status": (
            publication_impact_status if parse_error_count > 0 else "not_applicable"
        ),
        "source_incomplete_publication_impact_clear": parse_error_publication_impact_clear,
        "parse_error_reason_counts": parse_error_reason_counts,
        "parse_error_code_count": len(parse_error_codes),
        "parse_error_codes": parse_error_codes[:30],
        "parse_error_explicit_code_count": explicit_parse_error_code_count,
        "parse_error_explicit_codes": explicit_parse_error_codes[:30],
        "parse_error_code_truncated": bool(evidence.get("parse_error_code_truncated")),
        "parse_error_sample_count": len(parse_error_samples),
        "parse_error_sample_truncated": bool(evidence.get("parse_error_sample_truncated")),
        "formal_final_overlap_count": len(formal_overlap_codes),
        "formal_final_overlap_codes": formal_overlap_codes[:30],
        "formal_final_overlap_unsupported_count": impact_scope["formal_final_overlap_unsupported_count"],
        "formal_final_overlap_unsupported_codes": impact_scope["formal_final_overlap_unsupported_codes"],
        "parse_error_impact_scope": {
            "truth_policy": "preserve_missing_semantics",
            "business_value_handling": (
                "Read-only impact scope only; missing AutoStock directory type is not inferred from fund name "
                "and is not used to synthesize fund type, bond membership, or valid directory records."
            ),
            **impact_scope,
        },
        "local_funds_match_count": impact_scope["local_funds_match_count"],
        "local_funds_missing_count": impact_scope["local_funds_missing_count"],
        "local_funds_missing_codes": impact_scope["local_funds_missing_codes"],
        "local_funds_supported_classification_count": impact_scope[
            "local_funds_supported_classification_count"
        ],
        "local_funds_supported_classification_codes": impact_scope[
            "local_funds_supported_classification_codes"
        ],
        "local_funds_unsupported_classification_count": impact_scope[
            "local_funds_unsupported_classification_count"
        ],
        "local_funds_unsupported_classification_codes": impact_scope[
            "local_funds_unsupported_classification_codes"
        ],
        "sync_missing_in_local_after_count": impact_scope["sync_missing_in_local_after_count"],
        "sync_missing_active_in_local_after_count": impact_scope[
            "sync_missing_active_in_local_after_count"
        ],
        "sync_newly_synced_codes_total": impact_scope["sync_newly_synced_codes_total"],
        "latest_report_run_id": str(latest_report.get("run_id") or "") or None,
        "latest_report_final_count": int(latest_report.get("final_count") or 0),
        "filtered_csv_path": str(filtered_csv_path) if filtered_csv_path else "",
        "truth_policy": "preserve_missing_semantics",
        "business_value_handling": (
            "Rows missing required AutoStock directory fields are source evidence only; the project does not "
            "infer fund type from fund name and does not synthesize bond membership or valid directory records."
        ),
        "blockers": blockers,
        "warnings": warnings,
    }


def _weekly_task_from_truth(weekly_truth: dict[str, Any], task_name: str) -> dict[str, Any]:
    subprocess_index = (
        weekly_truth.get("maintenance_subprocess_index")
        if isinstance(weekly_truth.get("maintenance_subprocess_index"), dict)
        else {}
    )
    for task in subprocess_index.get("tasks") or []:
        if isinstance(task, dict) and str(task.get("task_name") or "").strip() == task_name:
            return task
    return {}


def _weekly_task_first_api_evidence(task: dict[str, Any], source: str) -> dict[str, Any]:
    stdout = task.get("stdout_result_semantics") if isinstance(task.get("stdout_result_semantics"), dict) else {}
    for evidence in stdout.get("api_source_evidence") or []:
        if isinstance(evidence, dict) and str(evidence.get("source") or "").strip() == source:
            return evidence
    return {}


def _build_weekly_maintenance_warning_truth_summary(
    scheduler_status_payload: dict[str, Any],
) -> dict[str, Any]:
    def coerce_int(value: Any) -> int:
        if value in (None, ""):
            return 0
        try:
            return int(value)
        except (TypeError, ValueError):
            try:
                return int(float(value))
            except (TypeError, ValueError):
                return 0

    artifact_path, payload = _load_latest_data_truthfulness_audit_artifact()
    sections = payload.get("sections") if isinstance(payload.get("sections"), dict) else {}
    automation_truth = (
        sections.get("automation_script_run_truth")
        if isinstance(sections.get("automation_script_run_truth"), dict)
        else {}
    )
    weekly_truth = (
        automation_truth.get("weekly_maintenance_warning_truth")
        if isinstance(automation_truth.get("weekly_maintenance_warning_truth"), dict)
        else {}
    )
    automation_autonomy = (
        scheduler_status_payload.get("automation_autonomy")
        if isinstance(scheduler_status_payload.get("automation_autonomy"), dict)
        else {}
    )
    warning_issue_details = (
        automation_autonomy.get("warning_issue_details")
        if isinstance(automation_autonomy.get("warning_issue_details"), dict)
        else {}
    )
    resolved_warning_issue_details = (
        automation_autonomy.get("resolved_warning_issue_details")
        if isinstance(automation_autonomy.get("resolved_warning_issue_details"), dict)
        else {}
    )
    weekly_detail = (
        warning_issue_details.get("weekly_maintenance_degraded")
        if isinstance(warning_issue_details.get("weekly_maintenance_degraded"), dict)
        else resolved_warning_issue_details.get("weekly_maintenance_degraded")
        if isinstance(resolved_warning_issue_details.get("weekly_maintenance_degraded"), dict)
        else {}
    )
    if not artifact_path or not weekly_truth:
        return {
            "available": False,
            "status": "not_available",
            "artifact_path": str(artifact_path) if artifact_path else "",
            "source": "data_truthfulness_automation_script_run_truth",
            "truth_policy": "read_only_runtime_artifact_evidence_no_state_mutation",
            "business_value_handling": (
                "Missing weekly maintenance truth summary is not treated as successful maintenance evidence "
                "and must not be used to synthesize task success, API data, or manual-intervention state."
            ),
        }

    collect_funds_task = _weekly_task_from_truth(weekly_truth, "collect-funds")
    collect_details_task = _weekly_task_from_truth(weekly_truth, "collect-details")
    collect_managers_task = _weekly_task_from_truth(weekly_truth, "collect-managers")
    collect_funds_evidence = _weekly_task_first_api_evidence(collect_funds_task, "fundMNRank")
    collect_details_evidence = _weekly_task_first_api_evidence(
        collect_details_task,
        "fundMNDetailInformation",
    )
    collect_managers_evidence = _weekly_task_first_api_evidence(
        collect_managers_task,
        "fundMNMangerList",
    )
    collect_details_stderr = (
        collect_details_task.get("stderr_signal_summary")
        if isinstance(collect_details_task.get("stderr_signal_summary"), dict)
        else {}
    )
    collect_details_stdout = (
        collect_details_task.get("stdout_result_semantics")
        if isinstance(collect_details_task.get("stdout_result_semantics"), dict)
        else {}
    )
    collect_managers_stderr = (
        collect_managers_task.get("stderr_signal_summary")
        if isinstance(collect_managers_task.get("stderr_signal_summary"), dict)
        else {}
    )
    collect_managers_stdout = (
        collect_managers_task.get("stdout_result_semantics")
        if isinstance(collect_managers_task.get("stdout_result_semantics"), dict)
        else {}
    )
    blocker_sample_count = len(
        [item for item in collect_details_stderr.get("blocker_samples") or [] if str(item or "").strip()]
    )
    manager_blocker_sample_count = len(
        [item for item in collect_managers_stderr.get("blocker_samples") or [] if str(item or "").strip()]
    )
    empty_detail_count = coerce_int(collect_details_evidence.get("empty_detail_count"))
    failed_response_count = coerce_int(collect_details_evidence.get("failed_response_count"))
    manager_empty_response_count = coerce_int(collect_managers_evidence.get("empty_response_count"))
    manager_failed_response_count = coerce_int(collect_managers_evidence.get("failed_response_count"))
    manager_transport_exception_count = coerce_int(
        collect_managers_evidence.get("transport_exception_count")
    )
    manager_stderr_blockers_explained_by_failed_response = bool(
        collect_managers_stderr.get("classification") == "blocker_signals"
        and manager_blocker_sample_count > 0
        and manager_failed_response_count >= manager_blocker_sample_count
        and manager_empty_response_count >= manager_failed_response_count
        and manager_transport_exception_count == 0
    )
    stderr_blockers_explained_by_empty_detail = bool(
        collect_details_stderr.get("classification") == "blocker_signals"
        and blocker_sample_count > 0
        and empty_detail_count >= blocker_sample_count
        and failed_response_count <= empty_detail_count
        and str(collect_details_evidence.get("stop_watch_fallback_scope") or "").strip()
        == "not_counted_as_detail_success"
    )
    warnings: list[str] = []
    blockers: list[str] = []
    if coerce_int(collect_funds_evidence.get("total_missing_nav_date_count")) > 0:
        warnings.append("weekly_collect_funds_missing_nav_date_present")
    if coerce_int(collect_funds_evidence.get("total_missing_nav_value_count")) > 0:
        warnings.append("weekly_collect_funds_missing_nav_value_present")
    if empty_detail_count > 0:
        warnings.append("weekly_collect_details_empty_detail_responses_present")
    if manager_failed_response_count > 0:
        warnings.append("weekly_collect_managers_failed_responses_present")
    if collect_details_stderr.get("classification") == "blocker_signals":
        if stderr_blockers_explained_by_empty_detail:
            warnings.append("weekly_collect_details_stderr_blockers_explained_by_empty_detail")
        else:
            blockers.append("weekly_collect_details_stderr_blockers_without_empty_detail_evidence")
    if collect_managers_stderr.get("classification") == "blocker_signals":
        if manager_stderr_blockers_explained_by_failed_response:
            warnings.append("weekly_collect_managers_stderr_blockers_explained_by_failed_responses")
        else:
            blockers.append("weekly_collect_managers_stderr_blockers_without_failed_response_evidence")
    if weekly_detail and weekly_detail.get("root_cause_resolved") is not True:
        latest_evidence = (
            weekly_detail.get("latest_resolution_evidence")
            if isinstance(weekly_detail.get("latest_resolution_evidence"), dict)
            else {}
        )
        if latest_evidence:
            warnings.append("weekly_warning_root_cause_unresolved_with_read_only_evidence")
        else:
            blockers.append("weekly_warning_root_cause_not_resolved")
    stage_contract_errors = None
    if weekly_detail.get("root_cause_status") == UNRESOLVED_STAGE_STATUS:
        stage_contract_errors = unresolved_stage_detail_errors(weekly_detail)
        blockers.extend(f"weekly_warning_{error}" for error in stage_contract_errors)
    elif weekly_detail and str(weekly_detail.get("truth_policy") or "").strip() != (
        "read_only_runtime_artifact_evidence_no_state_mutation"
    ):
        blockers.append("weekly_warning_truth_policy_mismatch")

    return {
        "available": True,
        "status": "needs_review" if blockers else "watch" if warnings else "ok",
        "artifact_path": str(artifact_path),
        "generated_at": payload.get("generated_at"),
        "source": "data_truthfulness_automation_script_run_truth",
        "task_name": weekly_truth.get("task_name"),
        "task_id": (weekly_truth.get("row") or {}).get("id")
        if isinstance(weekly_truth.get("row"), dict)
        else None,
        "task_status": (weekly_truth.get("row") or {}).get("status")
        if isinstance(weekly_truth.get("row"), dict)
        else None,
        "manual_intervention_required": bool(
            (weekly_truth.get("interpretation") or {}).get("current_manual_intervention_required_by_state")
        )
        if isinstance(weekly_truth.get("interpretation"), dict)
        else None,
        "manual_intervention_hint_is_historical_only": bool(
            (weekly_truth.get("interpretation") or {}).get("manual_intervention_hint_is_historical_only")
        )
        if isinstance(weekly_truth.get("interpretation"), dict)
        else None,
        "scheduler_warning_root_cause_resolved": weekly_detail.get("root_cause_resolved"),
        "scheduler_warning_root_cause_status": weekly_detail.get("root_cause_status"),
        "scheduler_warning_truth_policy": weekly_detail.get("truth_policy"),
        "scheduler_warning_primary_degraded_stage": weekly_detail.get("primary_degraded_stage"),
        "scheduler_warning_detail_contract_errors": stage_contract_errors,
        "scheduler_warning_resolution_artifact": (
            (weekly_detail.get("latest_resolution_evidence") or {}).get("artifact_path")
            if isinstance(weekly_detail.get("latest_resolution_evidence"), dict)
            else None
        ),
        "maintenance_subprocess_index_path": (
            (weekly_truth.get("maintenance_subprocess_index") or {}).get("index_file") or {}
        ).get("path")
        if isinstance(weekly_truth.get("maintenance_subprocess_index"), dict)
        else None,
        "collect_funds_source": collect_funds_evidence.get("source") or None,
        "collect_funds_total_missing_nav_date_count": coerce_int(
            collect_funds_evidence.get("total_missing_nav_date_count")
        ),
        "collect_funds_total_missing_nav_value_count": coerce_int(
            collect_funds_evidence.get("total_missing_nav_value_count")
        ),
        "collect_details_source": collect_details_evidence.get("source") or None,
        "collect_details_target_count": coerce_int(collect_details_evidence.get("target_count")),
        "collect_details_success_fund_count": coerce_int(
            (collect_details_stdout.get("business_summary") or {}).get("success_fund_count") or 0
        )
        if isinstance(collect_details_stdout.get("business_summary"), dict)
        else 0,
        "collect_details_empty_detail_count": empty_detail_count,
        "collect_details_empty_detail_reason_counts": (
            collect_details_evidence.get("empty_detail_reason_counts")
            if isinstance(collect_details_evidence.get("empty_detail_reason_counts"), dict)
            else {}
        ),
        "collect_details_failed_response_count": failed_response_count,
        "collect_details_stop_watch_fallback_scope": collect_details_evidence.get(
            "stop_watch_fallback_scope"
        ),
        "collect_details_stderr_classification": collect_details_stderr.get("classification"),
        "collect_details_stderr_signal_summary_source": collect_details_task.get(
            "stderr_signal_summary_source"
        ),
        "collect_details_stderr_blocker_sample_count": blocker_sample_count,
        "collect_details_stderr_blockers_explained_by_empty_detail": (
            stderr_blockers_explained_by_empty_detail
        ),
        "collect_managers_source": collect_managers_evidence.get("source") or None,
        "collect_managers_target_count": coerce_int(collect_managers_evidence.get("target_count")),
        "collect_managers_success_manager_count": coerce_int(
            (collect_managers_stdout.get("business_summary") or {}).get("success_manager_count") or 0
        )
        if isinstance(collect_managers_stdout.get("business_summary"), dict)
        else 0,
        "collect_managers_failed_response_count": manager_failed_response_count,
        "collect_managers_empty_response_count": manager_empty_response_count,
        "collect_managers_quality_status_counts": (
            collect_managers_evidence.get("quality_status_counts")
            if isinstance(collect_managers_evidence.get("quality_status_counts"), dict)
            else {}
        ),
        "collect_managers_quality_payload_type_counts": (
            collect_managers_evidence.get("quality_payload_type_counts")
            if isinstance(collect_managers_evidence.get("quality_payload_type_counts"), dict)
            else {}
        ),
        "collect_managers_stderr_classification": collect_managers_stderr.get("classification"),
        "collect_managers_stderr_signal_summary_source": collect_managers_task.get(
            "stderr_signal_summary_source"
        ),
        "collect_managers_stderr_blocker_sample_count": manager_blocker_sample_count,
        "collect_managers_stderr_blockers_explained_by_failed_responses": (
            manager_stderr_blockers_explained_by_failed_response
        ),
        "truth_policy": "read_only_runtime_artifact_evidence_no_state_mutation",
        "business_value_handling": (
            "Weekly maintenance warning truth is summarized from data truthfulness audit artifacts and "
            "current scheduler state only; it does not mutate task_history, hide upstream missing NAV "
            "snapshot fields, or synthesize fund details for empty Tiantian responses."
        ),
        "blockers": blockers,
        "warnings": warnings,
    }


def _build_health_summary_payload() -> dict[str, Any]:
    runtime_scheduler_settings = _runtime_scheduler_settings()
    scheduler_state = _scheduler_state_with_runtime_evidence(runtime_scheduler_settings)
    publication = _load_publication_registry_cached() or _load_publication_registry_snapshot_cached()
    scheduler_status_payload = _build_health_scheduler_payload(
        scheduler_state,
        publication=publication,
    )
    scheduler_status_payload["automation_autonomy"] = dashboard_payloads._build_automation_autonomy_runtime_scope(
        scheduler_status_payload.get("automation_autonomy") or {}
    )
    scheduler_status_payload["publication_summary"] = (
        _build_publication_summary_payload(publication) if publication is not None else {}
    )
    scheduler_status_payload.update(_derive_scheduler_header_summary(scheduler_status_payload))
    payload = {
        "status": "ok",
        "tiantian_base_url": settings.tiantian_base_url,
        "autostock_base_url": settings.autostock_base_url,
        "api_runtime": _build_api_settings_payload(),
        "manual_operations_enabled": MANUAL_OPERATION_MODE,
        "automation_autonomy": scheduler_status_payload.get("automation_autonomy") or {},
        "daily_run": scheduler_status_payload.get("daily_run") or {},
        "intraday_run": scheduler_status_payload.get("intraday_run") or {},
        "weekly_run": scheduler_status_payload.get("weekly_run") or {},
        "snapshot_watchdog_run": scheduler_status_payload.get("snapshot_watchdog_run") or {},
        "publication_summary": scheduler_status_payload.get("publication_summary") or {},
        "automation_mode": scheduler_status_payload.get("automation_mode"),
        "automation_status": scheduler_status_payload.get("automation_status"),
        "manual_intervention_required": scheduler_status_payload.get("manual_intervention_required"),
        "scheduler_running": scheduler_status_payload.get("scheduler_running"),
        "latest_canonical_run_id": scheduler_status_payload.get("latest_canonical_run_id"),
        "current_published_run_id": scheduler_status_payload.get("current_published_run_id"),
        "latest_successful_run_id": scheduler_status_payload.get("latest_successful_run_id"),
        "daily_screening_run_id": scheduler_status_payload.get("daily_screening_run_id"),
        "blocking_issue_codes": list(scheduler_status_payload.get("blocking_issue_codes") or []),
        "warning_issue_codes": list(scheduler_status_payload.get("warning_issue_codes") or []),
        "resolved_warning_issue_codes": list(
            scheduler_status_payload.get("resolved_warning_issue_codes") or []
        ),
        "host_mode": scheduler_status_payload.get("host_mode"),
        "current_task_name": scheduler_status_payload.get("current_task_name"),
        "current_task_started_at": scheduler_status_payload.get("current_task_started_at"),
        "next_run_at": scheduler_status_payload.get("next_run_at"),
        "next_run_label": scheduler_status_payload.get("next_run_label"),
        "autostock_directory_source_summary": _build_autostock_directory_source_health_summary(),
        "weekly_maintenance_warning_truth_summary": (
            _build_weekly_maintenance_warning_truth_summary(scheduler_status_payload)
        ),
    }
    return _attach_lightweight_status_source_contract(
        payload,
        source="health_summary_scheduler_publication_runtime_artifacts",
        payload_mode="health_summary",
        official_publication_truth="false_health_diagnostics_only",
        basis_extra={
            "status": payload.get("status"),
            "automation_status": payload.get("automation_status"),
            "current_task_name": payload.get("current_task_name"),
            "daily_screening_run_id": (payload.get("daily_run") or {}).get("screening_run_id")
            if isinstance(payload.get("daily_run"), dict)
            else None,
            "autostock_directory_source_status": (
                payload.get("autostock_directory_source_summary") or {}
            ).get("status")
            if isinstance(payload.get("autostock_directory_source_summary"), dict)
            else None,
            "weekly_maintenance_warning_truth_status": (
                payload.get("weekly_maintenance_warning_truth_summary") or {}
            ).get("status")
            if isinstance(payload.get("weekly_maintenance_warning_truth_summary"), dict)
            else None,
        },
        truth_note=(
            "/api/health/summary is a lightweight diagnostic summary assembled from real scheduler "
            "state, publication registry, API runtime settings, and audit artifacts. It must not "
            "synthesize fund business values or hide upstream missing semantics."
        ),
    )


def _build_bootstrap_summary_payload() -> dict[str, Any]:
    payload = {
        "status": "ok",
        "health": _build_health_summary_payload(),
        "workflow": _workflow_header_summary(),
    }
    health = payload.get("health") if isinstance(payload.get("health"), dict) else {}
    return _attach_lightweight_status_source_contract(
        payload,
        source="bootstrap_summary_health_workflow_runtime_artifacts",
        payload_mode="bootstrap_summary",
        official_publication_truth="false_bootstrap_diagnostics_only",
        basis_extra={
            "status": payload.get("status"),
            "health_source_basis_digest": (health.get("source_basis") or {}).get("digest")
            if isinstance(health.get("source_basis"), dict)
            else None,
            "workflow_status": (payload.get("workflow") or {}).get("status")
            if isinstance(payload.get("workflow"), dict)
            else None,
            "workflow_run_id": (payload.get("workflow") or {}).get("run_id")
            if isinstance(payload.get("workflow"), dict)
            else None,
        },
        truth_note=(
            "/api/bootstrap/summary combines the real health summary and workflow runtime header for "
            "frontend bootstrap only. It must not synthesize fund business values or publication truth."
        ),
    )


def _build_disk_usage_snapshot() -> dict[str, Any]:
    from backend.storage_observation import load_storage_usage

    return load_storage_usage(settings)


def _build_scheduler_last_runs_summary(scheduler_overview: dict | None) -> dict[str, dict[str, Any]]:
    return scheduler_diagnostics.build_scheduler_last_runs_summary(scheduler_overview)


def _summarize_scheduler_snapshot_for_api(snapshot: dict | None) -> dict[str, Any]:
    return scheduler_diagnostics.summarize_scheduler_snapshot_for_api(snapshot)


def _summarize_scheduler_run_for_api(run: dict | None) -> dict[str, Any]:
    from backend.collection_count_correction import apply_to_weekly_payload

    if (run or {}).get("task_name") == "scheduled_weekly_bond_maintenance":
        run = restore_weekly_maintenance_metrics(run, settings)
    summary = scheduler_diagnostics.summarize_scheduler_run_for_api(
        run,
        project_root=settings.project_root,
    )
    return apply_to_weekly_payload(settings, summary)


def _build_scheduler_overview_api_summary(scheduler_overview: dict | None) -> dict[str, Any]:
    from backend.collection_count_correction import apply_to_weekly_payload

    summary = scheduler_diagnostics.build_scheduler_overview_api_summary(scheduler_overview)
    summary["publication_dependency"] = configured_publication_blocker(_runtime_scheduler_settings())
    if summary.get("weekly_run"):
        summary["weekly_run"] = apply_to_weekly_payload(settings, summary["weekly_run"])
    return summary


def _build_scheduler_status_payload(
    scheduler_state: dict | None,
    *,
    publication: dict[str, Any] | None = None,
    include_detail: bool = True,
) -> dict[str, Any]:
    payload = scheduler_diagnostics.build_scheduler_status_payload(
        scheduler_state,
        publication=publication,
        include_detail=include_detail,
        build_scheduler_overview_summary=_build_scheduler_overview_summary,
        build_scheduler_overview_api_summary_fn=_build_scheduler_overview_api_summary,
        build_scheduler_chain_audit=_build_scheduler_chain_audit,
        build_automation_autonomy_summary=lambda full_overview, chain_audit: _build_automation_autonomy_summary(
            full_overview,
            chain_audit,
            manual_operations_enabled=MANUAL_OPERATION_MODE,
        ),
        build_scheduler_last_runs_summary_fn=_build_scheduler_last_runs_summary,
        restore_weekly_maintenance_fn=lambda run: restore_weekly_maintenance_metrics(run, settings),
    )
    payload["enabled"] = _resolve_scheduler_enabled_state(scheduler_state or payload)
    payload["publication_dependency"] = configured_publication_blocker(_runtime_scheduler_settings())
    from backend.collection_count_correction import apply_to_weekly_payload

    if payload.get("weekly_run"):
        payload["weekly_run"] = apply_to_weekly_payload(settings, payload["weekly_run"])
    if not include_detail:
        lightweight_overview = _build_scheduler_overview_summary(scheduler_state)
        if publication is not None:
            lightweight_overview["publication_layers"] = deepcopy(publication.get("automation_layers") or {})
            lightweight_overview["publication_context"] = {
                "latest_canonical_run_id": publication.get("latest_canonical_run_id"),
                "current_published_run_id": publication.get("current_published_run_id"),
                "latest_successful_run_id": publication.get("latest_successful_run_id"),
            }
        lightweight_api_overview = _build_scheduler_overview_api_summary(lightweight_overview)
        for run_key in ("daily_run", "intraday_run", "weekly_run", "snapshot_watchdog_run"):
            base_run = dict(payload.get(run_key) or {})
            overview_run = dict(lightweight_api_overview.get(run_key) or {})
            for key in (
                "scheduled_for",
                "start_delay_seconds",
                "started_late",
                "startup_catch_up_suspected",
                "startup_catch_up_origin",
                "scheduler_start_gap_seconds",
            ):
                if key in overview_run:
                    base_run[key] = overview_run.get(key)
            payload[run_key] = base_run
        lightweight_chain_audit = _build_scheduler_chain_audit(lightweight_overview)
        payload["scheduler_chain_audit"] = scheduler_diagnostics.summarize_scheduler_chain_audit_for_status_summary(
            lightweight_chain_audit
        )
        payload["automation_autonomy"] = _build_automation_autonomy_summary(
            lightweight_overview,
            lightweight_chain_audit,
            manual_operations_enabled=MANUAL_OPERATION_MODE,
        )
        payload["jobs"] = list((scheduler_state or {}).get("jobs") or [])
    payload["automation_autonomy"] = dashboard_payloads._build_automation_autonomy_runtime_scope(
        payload.get("automation_autonomy") or {}
    )
    if publication is not None:
        payload["publication_summary"] = _build_publication_summary_payload(publication)
    payload.update(_derive_scheduler_header_summary(payload))
    if not include_detail:
        return payload
    scheduler_chain_audit = payload.get("scheduler_chain_audit") or {}
    nav_risk_governance_summary = dashboard_payloads._build_nav_risk_governance_summary(
        {
            "database_freshness": {
                "effective_nav_snapshot": (scheduler_state or {}).get("effective_nav_snapshot") or {},
            }
        },
        publication or {},
        payload,
        scheduler_chain_audit,
        _load_risk_lag_summary(),
        _load_risk_sync_summary(),
        _load_risk_coverage_gap_summary(),
    )
    payload["nav_risk_governance_summary"] = nav_risk_governance_summary
    latest_report = _load_latest_report_cached() or _load_latest_report_snapshot_cached() or {}
    long_term_snapshot = ((latest_report.get("database_freshness") or {}).get("long_term_history_snapshot") or {})
    if long_term_snapshot:
        maintenance_detail = build_long_term_maintenance_detail(
            {
                "maintenance_summary": payload.pop("_long_term_maintenance_summary_full", None)
                or payload.get("maintenance_summary")
                or {},
                "task_closures": payload.pop("_long_term_task_closures_full", None)
                or payload.get("task_closures")
                or {},
            }
        )
        maintenance_detail = _merge_live_long_term_priority_blocked_summary(maintenance_detail)
        long_term_governance_summary = build_long_term_governance_summary(
            long_term_snapshot,
            maintenance_detail,
        )
        payload["long_term_governance_summary"] = (
            long_term_governance_summary
            if include_detail
            else scheduler_diagnostics.summarize_long_term_governance_for_status_summary(
                long_term_governance_summary
            )
        )
    payload["current_final_selection_governance_queue"] = (
        _load_current_final_selection_governance_queue_summary_cached()
    )
    return payload


def _derive_scheduler_header_summary(
    scheduler_payload: dict[str, Any] | None,
) -> dict[str, Any]:
    payload = dict(scheduler_payload or {})
    jobs = list(payload.get("jobs") or [])
    next_job: dict[str, Any] | None = None
    next_run_at: str | None = None
    for item in jobs:
        if not isinstance(item, dict):
            continue
        candidate = str(item.get("next_run_time") or "").strip()
        if not candidate:
            continue
        if next_run_at is None or candidate < next_run_at:
            next_run_at = candidate
            next_job = item

    next_run_label = None
    if isinstance(next_job, dict):
        job_id = str(next_job.get("id") or next_job.get("name") or "").strip()
        next_run_label = {
            "snapshot_watchdog": "快照看门狗",
            "daily_refresh": "日更刷新",
            "intraday_refresh": "盘中补刷",
            "weekly_maintenance": "周维护",
        }.get(job_id, job_id or None)

    scheduler_path_governance = dict(payload.get("scheduler_path_governance") or {})
    automation_autonomy = dict(payload.get("automation_autonomy") or {})
    current_task = (
        dict(payload.get("current_task") or {})
        if isinstance(payload.get("current_task"), dict)
        else {}
    )
    current_task_name = (
        str(current_task.get("task_name") or "").strip()
        or str(automation_autonomy.get("current_task_name") or "").strip()
        or str(payload.get("current_task_name") or "").strip()
        or None
    )
    current_task_started_at = (
        str(current_task.get("started_at") or "").strip()
        or str(payload.get("current_task_started_at") or "").strip()
        or str(automation_autonomy.get("current_task_started_at") or "").strip()
        or str(automation_autonomy.get("current_recovery_started_at") or "").strip()
        or None
    )
    automation_host_mode = str(payload.get("automation_host_mode") or "").strip()
    host_mode = (
        str(scheduler_path_governance.get("deployment_mode") or "").strip()
        or (
            "external_process"
            if automation_host_mode == "external_scheduler_process"
            else "embedded_web"
            if automation_host_mode == "embedded_web_scheduler"
            else ""
        )
    ) or None
    automation_mode = (
        str(automation_autonomy.get("status") or "").strip()
        or ("automatic" if bool(payload.get("scheduler_running")) else "unknown")
    ) or None
    manual_intervention_required = automation_autonomy.get("manual_intervention_required")
    if manual_intervention_required is None and "manual_intervention_required" in payload:
        manual_intervention_required = payload.get("manual_intervention_required")
    scheduler_running = automation_autonomy.get("scheduler_running")
    if scheduler_running is None and "scheduler_running" in payload:
        scheduler_running = bool(payload.get("scheduler_running"))

    def _issue_code_list(key: str) -> list[Any]:
        value = automation_autonomy.get(key)
        if isinstance(value, list):
            return list(value)
        value = payload.get(key)
        if isinstance(value, list):
            return list(value)
        return []

    def _clean_text_value(key: str) -> str | None:
        value = str(automation_autonomy.get(key) or payload.get(key) or "").strip()
        return value or None

    return {
        "automation_mode": automation_mode,
        "automation_status": automation_mode,
        "manual_intervention_required": manual_intervention_required,
        "scheduler_running": scheduler_running,
        "latest_canonical_run_id": _clean_text_value("latest_canonical_run_id"),
        "current_published_run_id": _clean_text_value("current_published_run_id"),
        "latest_successful_run_id": _clean_text_value("latest_successful_run_id"),
        "daily_screening_run_id": _clean_text_value("daily_screening_run_id"),
        "blocking_issue_codes": _issue_code_list("blocking_issue_codes"),
        "warning_issue_codes": _issue_code_list("warning_issue_codes"),
        "resolved_warning_issue_codes": _issue_code_list("resolved_warning_issue_codes"),
        "host_mode": host_mode,
        "current_task_name": current_task_name,
        "current_task_started_at": current_task_started_at,
        "next_run_at": next_run_at,
        "next_run_label": next_run_label,
    }


def _build_health_scheduler_payload(
    scheduler_state: dict | None,
    *,
    publication: dict[str, Any] | None = None,
) -> dict[str, Any]:
    state = dict(scheduler_state or {})
    busy_skip_summary = scheduler_diagnostics.summarize_busy_skips(state.get("recent_busy_skips"))
    scheduler_enabled = _resolve_scheduler_enabled_state(state)
    last_runs = dict(state.get("last_runs") or {})
    daily_run = dict(last_runs.get("scheduled_daily_bond_refresh") or {})
    intraday_run = dict(last_runs.get("scheduled_intraday_bond_refresh") or {})
    weekly_run = dict(last_runs.get("scheduled_weekly_bond_maintenance") or {})
    weekly_run = scheduler_diagnostics.apply_weekly_collection_subprocess_overlay(
        weekly_run,
        project_root=settings.project_root,
    )
    snapshot_watchdog_run = dict(last_runs.get("scheduled_snapshot_watchdog_refresh") or {})
    publication_layers = dict((publication or {}).get("automation_layers") or {})
    publication_context = {
        "latest_canonical_run_id": (publication or {}).get("latest_canonical_run_id"),
        "current_published_run_id": (publication or {}).get("current_published_run_id"),
        "latest_successful_run_id": (publication or {}).get("latest_successful_run_id"),
    }
    lightweight_overview = {
        "busy_skip_summary": busy_skip_summary,
        "enabled": scheduler_enabled,
        "scheduler_running": bool(state.get("scheduler_running")),
        "jobs": state.get("jobs") or [],
        "automation_host_mode": state.get("automation_host_mode"),
        "host_context": state.get("host_context"),
        "configured_host_context": state.get("configured_host_context"),
        "host_context_mismatch": bool(state.get("host_context_mismatch")),
        "scheduler_service_runtime": state.get("scheduler_service_runtime") or {},
        "web_service_runtime": state.get("web_service_runtime") or {},
        "started_at": state.get("started_at"),
        "stopped_at": state.get("stopped_at"),
        "last_heartbeat_at": state.get("last_heartbeat_at"),
        "last_error": state.get("last_error"),
        "screening_backlog_due": bool(state.get("screening_backlog_due")),
        "screening_backlog_reason": state.get("screening_backlog_reason"),
        "startup_catch_up_pending": bool(state.get("startup_catch_up_pending")),
        "startup_catch_up_due_jobs": state.get("startup_catch_up_due_jobs") or [],
        "failed_screening_retry_cooldown": state.get(
            "failed_screening_retry_cooldown"
        )
        or {},
        "configured_job_ids": state.get("configured_job_ids") or [],
        "missing_job_ids": state.get("missing_job_ids") or [],
        "unexpected_job_ids": state.get("unexpected_job_ids") or [],
        "job_drift_detected": bool(state.get("job_drift_detected")),
        "effective_nav_snapshot": state.get("effective_nav_snapshot") or {},
        "latest_screened_snapshot": state.get("latest_screened_snapshot") or {},
        "nav_freshness": state.get("nav_freshness") or {},
        "current_task": state.get("current_task") or {},
        "manual_run_history": state.get("manual_run_history") or [],
        "pending_manual_run_requests": state.get("pending_manual_run_requests") or [],
        "schedule_stability_summary": state.get("schedule_stability_summary") or {"summary": {}, "tasks": []},
        "automation_closure_digest": state.get("automation_closure_digest")
        or {"summary": {}, "alerts": [], "ordered_tasks": []},
        "recent_failure_recovery_summary": state.get("recent_failure_recovery_summary") or {},
        "daily_run": daily_run,
        "intraday_run": intraday_run,
        "weekly_run": weekly_run,
        "snapshot_watchdog_run": snapshot_watchdog_run,
        "publication_layers": publication_layers,
        "publication_context": publication_context,
    }
    (
        schedule_stability_summary_source,
        automation_closure_digest_source,
    ) = _resolve_lightweight_scheduler_runtime_digest_sources(
        scheduler_state,
        schedule_stability_summary=lightweight_overview.get("schedule_stability_summary") or {},
        automation_closure_digest=lightweight_overview.get("automation_closure_digest") or {},
    )
    scheduler_chain_audit = _build_scheduler_chain_audit(lightweight_overview)
    automation_autonomy = _build_automation_autonomy_summary(
        lightweight_overview,
        scheduler_chain_audit,
        manual_operations_enabled=MANUAL_OPERATION_MODE,
        load_database_overview_fn=_load_database_overview,
    )
    scheduler_path_governance = scheduler_diagnostics.build_scheduler_path_governance_summary(
        lightweight_overview
    )
    daily_run_api = _summarize_scheduler_run_for_api(daily_run)
    intraday_run_api = _summarize_scheduler_run_for_api(intraday_run)
    weekly_run_api = _summarize_scheduler_run_for_api(weekly_run)
    snapshot_watchdog_run_api = _summarize_scheduler_run_for_api(snapshot_watchdog_run)
    schedule_stability_summary = scheduler_diagnostics.summarize_schedule_stability_for_status_summary(
        schedule_stability_summary_source
    )
    response_payload = scheduler_diagnostics.apply_current_recovery_overlay_to_scheduler_payload({
        "busy_skip_summary": busy_skip_summary,
        "publication_dependency": configured_publication_blocker(_runtime_scheduler_settings()),
        "enabled": scheduler_enabled,
        "scheduler_running": bool(state.get("scheduler_running")),
        "automation_host_mode": state.get("automation_host_mode"),
        "jobs": state.get("jobs") or [],
        "host_context": state.get("host_context"),
        "configured_host_context": state.get("configured_host_context"),
        "host_context_mismatch": bool(state.get("host_context_mismatch")),
        "scheduler_service_runtime": state.get("scheduler_service_runtime") or {},
        "web_service_runtime": state.get("web_service_runtime") or {},
        "last_heartbeat_at": state.get("last_heartbeat_at"),
        "last_error": state.get("last_error"),
        "screening_backlog_due": bool(state.get("screening_backlog_due")),
        "screening_backlog_reason": state.get("screening_backlog_reason"),
        "startup_catch_up_pending": bool(state.get("startup_catch_up_pending")),
        "startup_catch_up_due_jobs": state.get("startup_catch_up_due_jobs") or [],
        "failed_screening_retry_cooldown": state.get(
            "failed_screening_retry_cooldown"
        )
        or {},
        "configured_job_ids": state.get("configured_job_ids") or [],
        "missing_job_ids": state.get("missing_job_ids") or [],
        "unexpected_job_ids": state.get("unexpected_job_ids") or [],
        "job_drift_detected": bool(state.get("job_drift_detected")),
        "current_task": state.get("current_task") or {},
        "daily_run": daily_run_api,
        "intraday_run": intraday_run_api,
        "weekly_run": weekly_run_api,
        "snapshot_watchdog_run": snapshot_watchdog_run_api,
        "scheduler_chain_audit": scheduler_diagnostics.summarize_scheduler_chain_audit_for_status_summary(
            scheduler_chain_audit
        ),
        "automation_autonomy": automation_autonomy,
        "scheduler_path_governance": scheduler_path_governance,
        "schedule_stability_summary": schedule_stability_summary,
    })
    response_payload.update(_derive_scheduler_header_summary(response_payload))
    return _attach_lightweight_status_source_contract(
        response_payload,
        source="scheduler_status_compact_runtime_state",
        payload_mode="scheduler_status_compact",
        official_publication_truth="false_scheduler_diagnostics_only",
        basis_extra={
            "automation_status": response_payload.get("automation_status"),
            "current_task_name": response_payload.get("current_task_name"),
            "daily_screening_run_id": (response_payload.get("daily_run") or {}).get("screening_run_id")
            if isinstance(response_payload.get("daily_run"), dict)
            else None,
            "scheduler_summary_build_profile_schema_version": (
                response_payload.get("scheduler_summary_build_profile") or {}
            ).get("schema_version")
            if isinstance(response_payload.get("scheduler_summary_build_profile"), dict)
            else None,
        },
        truth_note=(
            "/api/scheduler/status is a compact scheduler diagnostic view assembled from real scheduler "
            "state, publication registry, database/runtime artifact cache tokens, and scheduler diagnostics. "
            "It must not synthesize fund business values or use frontend-rendered state as automation truth."
        ),
    )


def _build_scheduler_status_summary_payload(
    scheduler_state: dict | None,
    *,
    publication: dict[str, Any] | None = None,
) -> dict[str, Any]:
    profile_started_at = time.perf_counter()
    build_profile: dict[str, Any] = {
        "schema_version": SCHEDULER_SUMMARY_BUILD_PROFILE_SCHEMA_VERSION,
        "detail_scope": "compact",
        "timing_unit": "seconds",
        "steps": [],
        "fragment_cache": [],
    }

    def _record_summary_step(name: str, started_at: float) -> None:
        build_profile.setdefault("steps", []).append(
            {
                "name": str(name or "").strip() or "unknown",
                "seconds": round(max(0.0, time.perf_counter() - started_at), 6),
            }
        )

    def _record_fragment_cache(status: dict[str, Any] | None) -> None:
        current = dict(status or {})
        if not current:
            return
        build_profile.setdefault("fragment_cache", []).append(
            {
                "fragment": current.get("fragment"),
                "status": current.get("status"),
                "hit": bool(current.get("hit")),
                "seconds": current.get("seconds"),
                "ttl_seconds": current.get("ttl_seconds"),
                "basis_digest": current.get("basis_digest"),
            }
        )

    state = dict(scheduler_state or {})
    busy_skip_summary = scheduler_diagnostics.summarize_busy_skips(state.get("recent_busy_skips"))
    runtime_overview: dict[str, Any] | None = None

    def _runtime_overview() -> dict[str, Any]:
        nonlocal runtime_overview
        if runtime_overview is None:
            runtime_overview = _build_scheduler_overview_summary(state)
            if publication is not None and not runtime_overview.get("publication_layers"):
                runtime_overview["publication_layers"] = dict(publication.get("automation_layers") or {})
            if publication is not None and not runtime_overview.get("publication_context"):
                runtime_overview["publication_context"] = {
                    "latest_canonical_run_id": publication.get("latest_canonical_run_id"),
                    "current_published_run_id": publication.get("current_published_run_id"),
                    "latest_successful_run_id": publication.get("latest_successful_run_id"),
                }
        return runtime_overview

    needs_runtime_snapshot_alignment = bool(
        state.get("screening_backlog_due")
        or state.get("startup_catch_up_pending")
        or not (state.get("effective_nav_snapshot") or {})
        or not (state.get("latest_screened_snapshot") or {})
    )
    runtime_snapshot_source = _runtime_overview() if needs_runtime_snapshot_alignment else {}
    runtime_effective_nav_snapshot = (
        runtime_snapshot_source.get("effective_nav_snapshot") or state.get("effective_nav_snapshot") or {}
    )
    runtime_latest_screened_snapshot = (
        runtime_snapshot_source.get("latest_screened_snapshot") or state.get("latest_screened_snapshot") or {}
    )
    runtime_screening_backlog_due = bool(
        runtime_snapshot_source.get("screening_backlog_due", state.get("screening_backlog_due"))
    )
    runtime_screening_backlog_reason = (
        runtime_snapshot_source.get("screening_backlog_reason")
        or state.get("screening_backlog_reason")
    )
    if not runtime_screening_backlog_due:
        runtime_screening_backlog_reason = None
    runtime_startup_catch_up_pending = bool(
        runtime_snapshot_source.get("startup_catch_up_pending", state.get("startup_catch_up_pending"))
    )
    runtime_startup_catch_up_due_jobs = (
        runtime_snapshot_source.get("startup_catch_up_due_jobs")
        or state.get("startup_catch_up_due_jobs")
        or []
    )
    step_started_at = time.perf_counter()
    scheduler_enabled = _resolve_scheduler_enabled_state(state)
    last_runs = dict(state.get("last_runs") or {})
    daily_run = _summarize_scheduler_run_for_api(last_runs.get("scheduled_daily_bond_refresh") or {})
    intraday_run = _summarize_scheduler_run_for_api(last_runs.get("scheduled_intraday_bond_refresh") or {})
    raw_weekly_run = scheduler_diagnostics.apply_weekly_collection_subprocess_overlay(
        last_runs.get("scheduled_weekly_bond_maintenance") or {},
        project_root=settings.project_root,
    )
    weekly_run = _summarize_scheduler_run_for_api(raw_weekly_run)
    if weekly_run:
        weekly_run["degradation_summary"] = scheduler_diagnostics.build_scheduler_run_degradation_summary(
            raw_weekly_run or weekly_run,
            task_key="weekly_maintenance",
            project_root=settings.project_root,
        )
    snapshot_watchdog_run = _summarize_scheduler_run_for_api(
        last_runs.get("scheduled_snapshot_watchdog_refresh") or {}
    )
    _record_summary_step("run_headers", step_started_at)

    step_started_at = time.perf_counter()
    lightweight_overview = {
        "busy_skip_summary": busy_skip_summary,
        "enabled": scheduler_enabled,
        "scheduler_running": bool(state.get("scheduler_running")),
        "automation_host_mode": state.get("automation_host_mode"),
        "host_context": state.get("host_context"),
        "configured_host_context": state.get("configured_host_context"),
        "host_context_mismatch": bool(state.get("host_context_mismatch")),
        "scheduler_service_runtime": state.get("scheduler_service_runtime") or {},
        "web_service_runtime": state.get("web_service_runtime") or {},
        "started_at": state.get("started_at"),
        "stopped_at": state.get("stopped_at"),
        "last_heartbeat_at": state.get("last_heartbeat_at"),
        "last_error": state.get("last_error"),
        "publication_dependency": configured_publication_blocker(_runtime_scheduler_settings()),
        "screening_backlog_due": runtime_screening_backlog_due,
        "screening_backlog_reason": runtime_screening_backlog_reason,
        "startup_catch_up_pending": runtime_startup_catch_up_pending,
        "startup_catch_up_due_jobs": runtime_startup_catch_up_due_jobs,
        "failed_screening_retry_cooldown": (
            (runtime_overview or {}).get("failed_screening_retry_cooldown")
            or state.get("failed_screening_retry_cooldown")
            or {}
        ),
        "configured_job_ids": state.get("configured_job_ids") or [],
        "missing_job_ids": state.get("missing_job_ids") or [],
        "unexpected_job_ids": state.get("unexpected_job_ids") or [],
        "job_drift_detected": bool(state.get("job_drift_detected")),
        "effective_nav_snapshot": runtime_effective_nav_snapshot,
        "latest_screened_snapshot": runtime_latest_screened_snapshot,
        "nav_freshness": state.get("nav_freshness") or {},
        "current_task": state.get("current_task") or {},
        "manual_run_history": state.get("manual_run_history") or [],
        "pending_manual_run_requests": state.get("pending_manual_run_requests") or [],
        "task_closures": state.get("task_closures") or {},
        "child_task_idle_streaks": state.get("child_task_idle_streaks") or {},
        "schedule_stability_summary": state.get("schedule_stability_summary") or {"summary": {}, "tasks": []},
        "automation_closure_digest": state.get("automation_closure_digest")
        or {"summary": {}, "alerts": [], "ordered_tasks": []},
        "recent_failure_recovery_summary": state.get("recent_failure_recovery_summary") or {},
        "daily_run": daily_run,
        "intraday_run": intraday_run,
        "weekly_run": weekly_run,
        "snapshot_watchdog_run": snapshot_watchdog_run,
    }
    if publication is not None:
        lightweight_overview["publication_layers"] = dict(publication.get("automation_layers") or {})
        lightweight_overview["publication_context"] = {
            "latest_canonical_run_id": publication.get("latest_canonical_run_id"),
            "current_published_run_id": publication.get("current_published_run_id"),
            "latest_successful_run_id": publication.get("latest_successful_run_id"),
        }
    _record_summary_step("lightweight_overview", step_started_at)

    step_started_at = time.perf_counter()
    (
        schedule_stability_summary_source,
        automation_closure_digest_source,
    ) = _resolve_lightweight_scheduler_runtime_digest_sources(
        scheduler_state,
        schedule_stability_summary=lightweight_overview.get("schedule_stability_summary") or {},
        automation_closure_digest=lightweight_overview.get("automation_closure_digest") or {},
        allow_runtime_overview_fallback=False,
    )
    if needs_runtime_snapshot_alignment and runtime_overview is not None:
        if _scheduler_schedule_stability_digest_incomplete(schedule_stability_summary_source):
            schedule_stability_summary_source = dict(
                runtime_overview.get("schedule_stability_summary") or schedule_stability_summary_source
            )
        if _scheduler_automation_closure_digest_incomplete(automation_closure_digest_source):
            automation_closure_digest_source = dict(
                runtime_overview.get("automation_closure_digest") or automation_closure_digest_source
            )
    _record_summary_step("runtime_digest_sources", step_started_at)

    step_started_at = time.perf_counter()
    latest_report_loader = _test_overridden_callable(
        "_load_latest_report_cached",
        _ORIGINAL_LOAD_LATEST_REPORT_CACHED,
    )
    if latest_report_loader is not None:
        latest_report = latest_report_loader() or {}
    else:
        latest_report = _load_latest_report_snapshot_cached() or _load_latest_report_cached() or {}
    long_term_snapshot = ((latest_report.get("database_freshness") or {}).get("long_term_history_snapshot") or {})
    _record_summary_step("latest_report_snapshot", step_started_at)

    step_started_at = time.perf_counter()
    maintenance_summary = state.get("maintenance_summary") or {}
    task_closures = state.get("task_closures") or {}
    if long_term_snapshot and not maintenance_summary and not task_closures:
        maintenance_summary = _runtime_overview().get("maintenance_summary") or {}
    _record_summary_step("maintenance_sources", step_started_at)

    summary_common_cache_basis = _scheduler_summary_common_file_cache_basis()
    lightweight_overview_digest = _stable_json_digest(
        _strip_scheduler_state_cache_volatile_fields(lightweight_overview)
    )

    step_started_at = time.perf_counter()
    (
        automation_and_path_governance,
        automation_cache_status,
    ) = _load_scheduler_summary_fragment_cached(
        "automation_and_path_governance",
        {
            **summary_common_cache_basis,
            "manual_operations_enabled": MANUAL_OPERATION_MODE,
            "lightweight_overview_digest": lightweight_overview_digest,
        },
        lambda: (
            lambda chain_audit: {
                "scheduler_chain_audit": chain_audit,
                "automation_autonomy": dashboard_payloads._build_automation_autonomy_runtime_scope(
                    _build_automation_autonomy_summary(
                        lightweight_overview,
                        chain_audit,
                        manual_operations_enabled=MANUAL_OPERATION_MODE,
                        load_database_overview_fn=_load_database_overview,
                    )
                ),
                "scheduler_path_governance": scheduler_diagnostics.build_scheduler_path_governance_summary(
                    lightweight_overview
                ),
            }
        )(_build_scheduler_chain_audit(lightweight_overview)),
    )
    _record_fragment_cache(automation_cache_status)
    scheduler_chain_audit = dict(
        (automation_and_path_governance or {}).get("scheduler_chain_audit") or {}
    )
    automation_autonomy = dict(
        (automation_and_path_governance or {}).get("automation_autonomy") or {}
    )
    scheduler_path_governance = dict(
        (automation_and_path_governance or {}).get("scheduler_path_governance") or {}
    )
    if not scheduler_chain_audit:
        scheduler_chain_audit = _build_scheduler_chain_audit(lightweight_overview)
    if not automation_autonomy:
        automation_autonomy = dashboard_payloads._build_automation_autonomy_runtime_scope(
            _build_automation_autonomy_summary(
                lightweight_overview,
                scheduler_chain_audit,
                manual_operations_enabled=MANUAL_OPERATION_MODE,
                load_database_overview_fn=_load_database_overview,
            )
        )
    if not scheduler_path_governance:
        scheduler_path_governance = scheduler_diagnostics.build_scheduler_path_governance_summary(
            lightweight_overview
        )
    _record_summary_step("automation_and_path_governance", step_started_at)

    step_started_at = time.perf_counter()
    automation_closure_digest = scheduler_diagnostics.summarize_automation_closure_digest_for_status_summary(
        automation_closure_digest_source
    )
    _record_summary_step("automation_closure_digest", step_started_at)

    maintenance_summary_for_response = maintenance_summary
    long_term_governance_summary = {}
    if long_term_snapshot:
        step_started_at = time.perf_counter()
        maintenance_detail = build_long_term_maintenance_detail(
            {
                "maintenance_summary": maintenance_summary,
                "task_closures": task_closures,
            }
        )
        maintenance_detail = _merge_live_long_term_priority_blocked_summary(maintenance_detail)
        long_term_governance_summary = build_long_term_governance_summary(
            long_term_snapshot,
            maintenance_detail,
            include_semantic_null_snapshot_audit=False,
        )
        _record_summary_step("long_term_governance", step_started_at)
    else:
        _record_summary_step("long_term_governance", time.perf_counter())

    step_started_at = time.perf_counter()
    risk_lag_summary = _load_default_risk_lag_summary(latest_report)
    _record_summary_step("risk_lag_summary", step_started_at)

    step_started_at = time.perf_counter()
    risk_sync_summary = _load_risk_sync_summary()
    _record_summary_step("risk_sync_summary", step_started_at)

    step_started_at = time.perf_counter()
    risk_coverage_gap_summary, risk_gap_cache_status = _load_scheduler_summary_fragment_cached(
        "risk_coverage_gap_summary",
        {
            **summary_common_cache_basis,
            "latest_report_run_id": latest_report.get("run_id"),
        },
        _load_risk_coverage_gap_summary,
    )
    risk_coverage_gap_summary = dict(risk_coverage_gap_summary or {})
    _record_fragment_cache(risk_gap_cache_status)
    _record_summary_step("risk_coverage_gap_summary", step_started_at)

    step_started_at = time.perf_counter()
    current_final_selection_governance_queue = _load_current_final_selection_governance_queue_summary_cached()
    _record_summary_step("current_final_selection_governance_queue", step_started_at)

    step_started_at = time.perf_counter()
    nav_risk_payload = {
        "database_freshness": {
            "effective_nav_snapshot": state.get("effective_nav_snapshot") or {},
            "risk_1y_freshness": ((latest_report.get("database_freshness") or {}).get("risk_1y_freshness") or {}),
        }
    }
    nav_risk_scheduler_runs = {
        "daily_run": daily_run,
        "intraday_run": intraday_run,
        "weekly_run": weekly_run,
        "snapshot_watchdog_run": snapshot_watchdog_run,
    }
    nav_risk_governance_summary, nav_risk_cache_status = _load_scheduler_summary_fragment_cached(
        "nav_risk_governance",
        {
            **summary_common_cache_basis,
            "nav_risk_payload_digest": _stable_json_digest(nav_risk_payload),
            "publication_digest": _stable_json_digest(publication or {}),
            "scheduler_runs_digest": _stable_json_digest(nav_risk_scheduler_runs),
            "scheduler_chain_audit_digest": _stable_json_digest(scheduler_chain_audit),
            "risk_lag_summary_digest": _stable_json_digest(risk_lag_summary),
            "risk_sync_summary_digest": _stable_json_digest(risk_sync_summary),
            "risk_coverage_gap_summary_digest": _stable_json_digest(risk_coverage_gap_summary),
        },
        lambda: dashboard_payloads._build_nav_risk_governance_summary(
            nav_risk_payload,
            publication or {},
            nav_risk_scheduler_runs,
            scheduler_chain_audit,
            risk_lag_summary,
            risk_sync_summary,
            risk_coverage_gap_summary,
        ),
    )
    nav_risk_governance_summary = dict(nav_risk_governance_summary or {})
    _record_fragment_cache(nav_risk_cache_status)
    _record_summary_step("nav_risk_governance", step_started_at)

    steps = list(build_profile.get("steps") or [])
    build_profile["total_seconds"] = round(max(0.0, time.perf_counter() - profile_started_at), 6)
    build_profile["step_count"] = len(steps)
    fragment_cache_entries = list(build_profile.get("fragment_cache") or [])
    build_profile["fragment_cache_hit_count"] = sum(
        1 for item in fragment_cache_entries if bool((item or {}).get("hit"))
    )
    build_profile["fragment_cache_miss_count"] = sum(
        1 for item in fragment_cache_entries if not bool((item or {}).get("hit"))
    )
    build_profile["slowest_steps"] = [
        {
            "name": step.get("name"),
            "seconds": step.get("seconds"),
        }
        for step in sorted(
            steps,
            key=lambda item: float(item.get("seconds") or 0.0),
            reverse=True,
        )[:5]
    ]

    response_payload = scheduler_diagnostics.apply_current_recovery_overlay_to_scheduler_payload({
        "busy_skip_summary": busy_skip_summary,
        "enabled": scheduler_enabled,
        "scheduler_running": bool(state.get("scheduler_running")),
        "automation_host_mode": state.get("automation_host_mode"),
        "jobs": state.get("jobs") or [],
        "host_context": state.get("host_context"),
        "configured_host_context": state.get("configured_host_context"),
        "host_context_mismatch": bool(state.get("host_context_mismatch")),
        "scheduler_service_runtime": state.get("scheduler_service_runtime") or {},
        "web_service_runtime": state.get("web_service_runtime") or {},
        "last_heartbeat_at": state.get("last_heartbeat_at"),
        "last_error": state.get("last_error"),
        "screening_backlog_due": runtime_screening_backlog_due,
        "screening_backlog_reason": runtime_screening_backlog_reason,
        "startup_catch_up_pending": runtime_startup_catch_up_pending,
        "startup_catch_up_due_jobs": runtime_startup_catch_up_due_jobs,
        "failed_screening_retry_cooldown": (
            (runtime_overview or {}).get("failed_screening_retry_cooldown")
            or state.get("failed_screening_retry_cooldown")
            or {}
        ),
        "configured_job_ids": state.get("configured_job_ids") or [],
        "missing_job_ids": state.get("missing_job_ids") or [],
        "unexpected_job_ids": state.get("unexpected_job_ids") or [],
        "job_drift_detected": bool(state.get("job_drift_detected")),
        "effective_nav_snapshot": runtime_effective_nav_snapshot,
        "latest_screened_snapshot": runtime_latest_screened_snapshot,
        "nav_freshness": state.get("nav_freshness") or {},
        "current_task": state.get("current_task") or {},
        "daily_run": daily_run,
        "intraday_run": intraday_run,
        "weekly_run": weekly_run,
        "snapshot_watchdog_run": snapshot_watchdog_run,
        "scheduler_chain_audit": scheduler_diagnostics.summarize_scheduler_chain_audit_for_status_summary(
            scheduler_chain_audit
        ),
        "automation_autonomy": automation_autonomy,
        "scheduler_path_governance": scheduler_path_governance,
        "maintenance_summary": scheduler_diagnostics.summarize_maintenance_summary_for_status_summary(
            maintenance_summary_for_response
        ),
        "automation_closure_digest": automation_closure_digest,
        "recent_failure_recovery_summary": (
            (runtime_overview or {}).get("recent_failure_recovery_summary")
            or state.get("recent_failure_recovery_summary")
            or {}
        ),
        "risk_lag_summary": _summarize_risk_lag_summary_for_status_summary(risk_lag_summary),
        "nav_risk_governance_summary": nav_risk_governance_summary,
        "long_term_governance_summary": scheduler_diagnostics.summarize_long_term_governance_for_status_summary(
            long_term_governance_summary
        ),
        "current_final_selection_governance_queue": current_final_selection_governance_queue,
        "schedule_stability_summary": scheduler_diagnostics.summarize_schedule_stability_for_status_summary(
            schedule_stability_summary_source
        ),
        "publication_summary": (_build_publication_summary_payload(publication) if publication is not None else {}),
        "manual_run_history_count": len(state.get("manual_run_history") or []),
        "pending_manual_run_request_count": len(state.get("pending_manual_run_requests") or []),
        "scheduler_summary_build_profile": build_profile,
        "publication_dependency": configured_publication_blocker(_runtime_scheduler_settings()),
    })
    response_payload.update(_derive_scheduler_header_summary(response_payload))
    return _attach_lightweight_status_source_contract(
        response_payload,
        source="scheduler_status_compact_runtime_state",
        payload_mode="scheduler_status_compact",
        official_publication_truth="false_scheduler_diagnostics_only",
        basis_extra={
            "automation_status": response_payload.get("automation_status"),
            "current_task_name": response_payload.get("current_task_name"),
            "daily_screening_run_id": (response_payload.get("daily_run") or {}).get("screening_run_id")
            if isinstance(response_payload.get("daily_run"), dict)
            else None,
            "scheduler_summary_build_profile_schema_version": (
                response_payload.get("scheduler_summary_build_profile") or {}
            ).get("schema_version")
            if isinstance(response_payload.get("scheduler_summary_build_profile"), dict)
            else None,
        },
        truth_note=(
            "/api/scheduler/status is a compact scheduler diagnostic view assembled from real scheduler "
            "state, publication registry, database/runtime artifact cache tokens, and scheduler diagnostics. "
            "It must not synthesize fund business values or use frontend-rendered state as automation truth."
        ),
    )


def _build_health_scheduler_status_summary(
    scheduler_status_payload: dict | None,
) -> dict[str, Any]:
    payload = dict(scheduler_status_payload or {})
    automation_autonomy = dict(payload.get("automation_autonomy") or {})
    screening_backlog_due_raw = bool(payload.get("screening_backlog_due"))
    screening_backlog_due = bool(automation_autonomy.get("screening_backlog_due", screening_backlog_due_raw))
    screening_backlog_reason = payload.get("screening_backlog_reason") if screening_backlog_due else None
    return {
        "busy_skip_summary": deepcopy(payload.get("busy_skip_summary") or {}),
        "enabled": _resolve_scheduler_enabled_state(payload),
        "scheduler_running": bool(payload.get("scheduler_running")),
        "automation_host_mode": payload.get("automation_host_mode"),
        "host_context": payload.get("host_context"),
        "configured_host_context": payload.get("configured_host_context"),
        "host_context_mismatch": bool(payload.get("host_context_mismatch")),
        "last_heartbeat_at": payload.get("last_heartbeat_at"),
        "last_error": payload.get("last_error"),
        "screening_backlog_due": screening_backlog_due,
        "screening_backlog_due_raw": screening_backlog_due_raw,
        "screening_backlog_reason": screening_backlog_reason,
        "startup_catch_up_pending": bool(payload.get("startup_catch_up_pending")),
        "startup_catch_up_due_jobs": payload.get("startup_catch_up_due_jobs") or [],
        "configured_job_ids": payload.get("configured_job_ids") or [],
        "missing_job_ids": payload.get("missing_job_ids") or [],
        "unexpected_job_ids": payload.get("unexpected_job_ids") or [],
        "job_drift_detected": bool(payload.get("job_drift_detected")),
        "current_task": payload.get("current_task") or {},
        "daily_run": payload.get("daily_run") or {},
        "intraday_run": payload.get("intraday_run") or {},
        "weekly_run": payload.get("weekly_run") or {},
        "snapshot_watchdog_run": payload.get("snapshot_watchdog_run") or {},
        **_derive_scheduler_header_summary(payload),
    }


def _build_publication_summary_payload(
    publication: dict[str, Any] | None,
    *,
    tracker_summary: dict[str, Any] | None = None,
    run_id: str | None = None,
) -> dict[str, Any]:
    publication_status_summary = build_publication_status_summary(
        publication,
        tracker_summary=tracker_summary,
    )
    publication_status_summary = dashboard_payloads._build_publication_status_runtime_scope(
        publication_status_summary,
        run_id=run_id,
    )
    publication_summary = build_publication_summary(
        publication,
        tracker_summary=tracker_summary,
    )
    publication_summary["status_summary"] = publication_status_summary
    return publication_summary


def _load_latest_report_cached() -> dict[str, Any] | None:
    view = _committed_publication_view()
    if view is not None:
        return deepcopy(view["report"])
    return _cached_payload(
        f"latest_report_payload::{_file_content_cache_token(settings.latest_report_file)}::{_file_content_cache_token(settings.publication_registry_file)}",
        _DASHBOARD_LATEST_REPORT_CACHE_TTL_SECONDS,
        lambda: manager.latest_report(),
    )


def _load_latest_report_snapshot_cached() -> dict[str, Any] | None:
    view = _committed_publication_view()
    if view is not None:
        return deepcopy(view["report"])
    return _cached_payload(
        f"latest_report_snapshot::{_file_content_cache_token(settings.latest_report_file)}",
        _DASHBOARD_LATEST_REPORT_CACHE_TTL_SECONDS,
        lambda: (_safe_json_load(settings.latest_report_file) or None),
    )


def _load_publication_registry_cached() -> dict[str, Any]:
    from backend.runtime_replay_cache import refresh_registry_replay_precision

    loader = _test_overridden_callable(
        "load_publication_registry",
        _ORIGINAL_LOAD_PUBLICATION_REGISTRY_FUNCTION,
    )
    if loader is not None:
        return loader(settings) or {}
    view = _committed_publication_view()
    if view is not None:
        return deepcopy(view["registry"])
    payload = _cached_payload(
        f"publication_registry_payload::{_file_content_cache_token(settings.publication_registry_file)}",
        _PUBLICATION_REGISTRY_CACHE_TTL_SECONDS,
        lambda: (_safe_json_load(settings.publication_registry_file) or load_publication_registry(settings)),
    )
    return refresh_registry_replay_precision(payload or {}, project_root=settings.project_root)


def _load_publication_registry_snapshot_cached() -> dict[str, Any]:
    from backend.runtime_replay_cache import refresh_registry_replay_precision

    view = _committed_publication_view()
    if view is not None:
        return deepcopy(view["registry"])
    payload = _cached_payload(
        f"publication_registry_snapshot::{_file_content_cache_token(settings.publication_registry_file)}",
        _PUBLICATION_REGISTRY_CACHE_TTL_SECONDS,
        lambda: _safe_json_load(settings.publication_registry_file),
    )
    return refresh_registry_replay_precision(payload or {}, project_root=settings.project_root)


def _summarize_current_final_selection_governance_queue(
    queue_payload: dict[str, Any] | None,
) -> dict[str, Any]:
    queue = dict(queue_payload or {})
    codes_by_action = {
        str(action_key or "").strip(): list(codes or [])
        for action_key, codes in (queue.get("codes_by_action") or {}).items()
        if str(action_key or "").strip()
    }
    action_counts = {
        action_key: len(
            {
                str(code or "").strip()
                for code in codes
                if str(code or "").strip()
            }
        )
        for action_key, codes in codes_by_action.items()
    }
    queue_total = _to_int_or_none(queue.get("queue_total"))
    candidate_code_total = _to_int_or_none(queue.get("candidate_code_total"))
    return {
        "available": bool(queue.get("available")),
        "reason": queue.get("reason"),
        "run_id": queue.get("run_id"),
        "source_report_run_id": queue.get("source_report_run_id"),
        "target_report_run_id": queue.get("target_report_run_id"),
        "target_report_run_source": queue.get("target_report_run_source"),
        "current_report_run_id": queue.get("current_report_run_id"),
        "artifact_project_path": queue.get("artifact_project_path"),
        "generated_at": queue.get("generated_at"),
        "queue_total": int(queue_total or 0),
        "candidate_code_total": int(candidate_code_total or 0),
        "codes_by_action_counts": action_counts,
        "action_key_count": len(action_counts),
        "scope": "current_runtime_advisory_queue",
        "truth_policy": "real_final_selection_contraction_shadow_artifact_summary_only",
        "missing_value_policy": "preserve_missing_semantics",
        "business_value_synthesis": "forbidden",
        "publication_truth": False,
        "official_publication_truth": "false_advisory_governance_only",
        "frontend_rendered_truth_allowed": False,
    }


def _load_current_final_selection_governance_queue_summary_cached() -> dict[str, Any]:
    cache_basis = {
        "publication_registry_file": _file_content_cache_token(settings.publication_registry_file),
        "latest_report_file": _file_content_cache_token(settings.latest_report_file),
        "final_selection_contraction_shadow": _final_selection_contraction_shadow_cache_token(),
    }
    return _cached_payload(
        f"current_final_selection_governance_queue::{_stable_json_digest(cache_basis)}",
        _CURRENT_FINAL_SELECTION_GOVERNANCE_QUEUE_CACHE_TTL_SECONDS,
        lambda: _summarize_current_final_selection_governance_queue(
            load_final_selection_governance_queue(settings)
        ),
    )


def _committed_publication_view() -> dict[str, Any] | None:
    """Pin report and publication metadata to one verified request generation."""
    from backend.publication_transaction import load_publication_head, publication_io_lock, resolve_committed_path
    from backend.runtime_replay_cache import refresh_registry_replay_precision

    if has_request_context() and getattr(g, "fund_ana_publication_settings", None) is settings:
        return g.fund_ana_publication_view
    with publication_io_lock(settings):
        head = load_publication_head(settings)
        if not head:
            return None
        registry = _safe_json_load(settings.publication_registry_file)
        if (registry.get("commit_generation") != head["generation"]
                or registry.get("current_published_run_id") != head["current_published_run_id"]):
            registry = load_publication_registry(settings)
        entry = head["committed_runs"][head["current_published_run_id"]]
        report = _safe_json_load(resolve_committed_path(settings, entry["report_path"]))
        if not report or report.get("run_id") != head["current_published_run_id"]:
            raise RuntimeError("committed_publication_report_unreadable")
        registry = refresh_registry_replay_precision(registry, project_root=settings.project_root)
        view = {"report": report, "registry": registry, "generation": head["generation"]}
    if has_request_context():
        g.fund_ana_publication_settings = settings
        g.fund_ana_publication_view = view
    return view


def _load_current_published_report_cached() -> dict[str, Any] | None:
    view = _committed_publication_view()
    if view is not None:
        return deepcopy(view["report"])
    return _cached_payload(
        f"current_published_report::{_file_content_cache_token(settings.publication_registry_file)}::{_file_content_cache_token(settings.latest_report_file)}",
        _DASHBOARD_LATEST_REPORT_CACHE_TTL_SECONDS,
        lambda: (
            lambda latest_payload, publication: (
                latest_payload
                if latest_payload
                and str(latest_payload.get("run_id") or "").strip()
                and str(latest_payload.get("run_id") or "").strip()
                == str((publication or {}).get("current_published_run_id") or "").strip()
                else load_current_published_report_payload(settings)
            )
        )(
            _safe_json_load(settings.latest_report_file),
            _load_publication_registry_cached() or _load_publication_registry_snapshot_cached(),
        ),
    )


def _attach_lightweight_status_source_contract(
    payload: dict[str, Any],
    *,
    source: str,
    payload_mode: str,
    official_publication_truth: str,
    basis_extra: dict[str, Any] | None = None,
    truth_note: str = "",
) -> dict[str, Any]:
    response_payload = dict(payload or {})
    publication = _load_publication_registry_cached() or _load_publication_registry_snapshot_cached()
    latest_report = _load_current_published_report_cached() or _load_latest_report_snapshot_cached() or _load_latest_report_cached() or {}
    automation = response_payload.get("automation_autonomy")
    automation = automation if isinstance(automation, dict) else {}
    automation_lineage = automation.get("automation_lineage")
    automation_lineage = automation_lineage if isinstance(automation_lineage, dict) else {}
    source_text = str(source or "").strip() or "lightweight_status_runtime_summary"
    payload_mode_text = str(payload_mode or "").strip() or "summary"
    scheduler_state_file = _runtime_scheduler_settings().scheduler_state_file
    basis: dict[str, Any] = {
        "schema_version": _LIGHTWEIGHT_STATUS_SOURCE_BASIS_SCHEMA_VERSION,
        "source": source_text,
        "payload_mode": payload_mode_text,
        "run_id": str(
            response_payload.get("run_id")
            or response_payload.get("latest_report_run_id")
            or (latest_report or {}).get("run_id")
            or ""
        ).strip(),
        "latest_report_run_id": str((latest_report or {}).get("run_id") or "").strip(),
        "latest_report_file": _file_content_cache_token(settings.latest_report_file),
        "latest_report_file_path": _project_relative_path_text(settings.latest_report_file),
        "publication_registry_file": _file_content_cache_token(settings.publication_registry_file),
        "publication_registry_file_path": _project_relative_path_text(settings.publication_registry_file),
        "scheduler_state_file": _scheduler_state_semantic_cache_token(scheduler_state_file),
        "scheduler_state_file_path": _project_relative_path_text(scheduler_state_file),
        "database_file": _sqlite_file_cache_token(settings.database_path),
        "database_file_path": _project_relative_path_text(settings.database_path),
        "latest_canonical_run_id": str(
            automation_lineage.get("latest_canonical_run_id")
            or automation.get("latest_canonical_run_id")
            or (publication or {}).get("latest_canonical_run_id")
            or ""
        ).strip(),
        "current_published_run_id": str(
            automation_lineage.get("current_published_run_id")
            or automation.get("current_published_run_id")
            or (publication or {}).get("current_published_run_id")
            or ""
        ).strip(),
        "latest_successful_run_id": str(
            automation_lineage.get("latest_successful_run_id")
            or automation.get("latest_successful_run_id")
            or (publication or {}).get("latest_successful_run_id")
            or ""
        ).strip(),
        "daily_screening_run_id": str(
            automation_lineage.get("daily_screening_run_id")
            or automation.get("daily_screening_run_id")
            or ((response_payload.get("daily_run") or {}).get("screening_run_id")
                if isinstance(response_payload.get("daily_run"), dict)
                else "")
            or ""
        ).strip() or None,
        "daily_screening_evidence": dict(automation_lineage.get("daily_screening_evidence") or {}),
        "scheduler_running": response_payload.get("scheduler_running"),
        "manual_intervention_required": response_payload.get("manual_intervention_required"),
        "missing_value_policy": "preserve_missing_semantics",
        "business_value_synthesis": "forbidden",
        "frontend_rendered_truth_allowed": False,
        "truth_note": truth_note or (
            "This lightweight status response is assembled from runtime state, publication registry, "
            "database/file cache tokens, and real local artifacts only. It must not synthesize fund "
            "business values or treat frontend-rendered state as truth."
        ),
    }
    basis.update(dict(basis_extra or {}))
    basis["digest"] = _stable_json_digest(basis)
    contract = {
        "schema_version": _LIGHTWEIGHT_STATUS_SOURCE_CONTRACT_SCHEMA_VERSION,
        "source": source_text,
        "truth_policy": "real_runtime_state_publication_registry_database_and_runtime_artifacts_only",
        "missing_value_policy": "preserve_missing_semantics",
        "business_value_synthesis": "forbidden",
        "official_publication_truth": str(official_publication_truth or "").strip()
        or "false_status_diagnostics_only",
        "frontend_rendered_truth_allowed": False,
        "cache_basis_schema_version": basis["schema_version"],
        "cache_basis_digest": basis["digest"],
    }
    response_payload["source_basis"] = basis
    response_payload["cache_basis"] = dict(basis)
    response_payload["source_contract"] = contract
    return response_payload


def _build_publication_summary_source_metadata(
    publication: dict[str, Any],
    summary_payload: dict[str, Any],
) -> dict[str, Any]:
    publication_payload = publication if isinstance(publication, dict) else {}
    summary = summary_payload if isinstance(summary_payload, dict) else {}
    canonical_runs = summary.get("canonical_runs") if isinstance(summary.get("canonical_runs"), list) else []
    latest_report_path_text = str(
        publication_payload.get("latest_canonical_report_path")
        or summary.get("latest_canonical_report_path")
        or ""
    ).strip()
    latest_report_path = _resolve_existing_report_artifact_path(latest_report_path_text)
    canonical_run_ids = [
        str(item.get("run_id") or "").strip()
        for item in canonical_runs
        if isinstance(item, dict) and str(item.get("run_id") or "").strip()
    ]
    latest_canonical_run_id = str(
        summary.get("latest_canonical_run_id")
        or publication_payload.get("latest_canonical_run_id")
        or ""
    ).strip()
    current_published_run_id = str(
        summary.get("current_published_run_id")
        or publication_payload.get("current_published_run_id")
        or ""
    ).strip()
    basis: dict[str, Any] = {
        "schema_version": _PUBLICATION_SUMMARY_SOURCE_BASIS_SCHEMA_VERSION,
        "source": "publication_summary_publication_registry_artifact",
        "payload_mode": "summary",
        "latest_canonical_run_id": latest_canonical_run_id,
        "current_published_run_id": current_published_run_id,
        "latest_canonical_report_path": latest_report_path_text,
        "canonical_run_count": summary.get("canonical_run_count"),
        "archived_superseded_run_count": summary.get("archived_superseded_run_count"),
        "canonical_run_ids": canonical_run_ids,
        "latest_canonical_run_present": bool(
            latest_canonical_run_id and latest_canonical_run_id in canonical_run_ids
        ),
        "current_published_run_present": bool(
            current_published_run_id and current_published_run_id in canonical_run_ids
        ),
        "publication_registry_file": _file_content_cache_token(settings.publication_registry_file),
        "publication_registry_file_path": _project_relative_path_text(settings.publication_registry_file),
        "latest_report_file": _file_content_cache_token(settings.latest_report_file),
        "latest_report_file_path": _project_relative_path_text(settings.latest_report_file),
        "latest_canonical_report_token": _path_cache_token(latest_report_path),
        "latest_canonical_report_file_path": _project_relative_path_text(latest_report_path),
        "missing_value_policy": "preserve_missing_semantics",
        "business_value_synthesis": "forbidden",
        "frontend_rendered_truth_allowed": False,
        "truth_note": (
            "/api/publication/summary is a compact read-only view of the publication registry "
            "artifact and declared latest report pointer. Frontend-rendered tables are never "
            "publication truth."
        ),
    }
    basis["digest"] = _stable_json_digest(basis)
    contract = {
        "schema_version": _PUBLICATION_SUMMARY_SOURCE_CONTRACT_SCHEMA_VERSION,
        "source": "publication_summary_publication_registry_artifact",
        "truth_policy": "real_publication_registry_artifact_only",
        "missing_value_policy": "preserve_missing_semantics",
        "business_value_synthesis": "forbidden",
        "official_publication_truth": "publication_registry",
        "frontend_rendered_truth_allowed": False,
        "cache_basis_schema_version": basis["schema_version"],
        "cache_basis_digest": basis["digest"],
    }
    return {
        "source_basis": basis,
        "cache_basis": dict(basis),
        "source_contract": contract,
    }


def _build_latest_report_source_metadata(
    payload: dict[str, Any],
    *,
    publication: dict[str, Any] | None,
    payload_source: str,
) -> dict[str, Any]:
    artifacts = payload.get("artifacts") if isinstance(payload.get("artifacts"), dict) else {}
    report_json_path = _resolve_existing_report_artifact_path(artifacts.get("report_json"))
    basis: dict[str, Any] = {
        "schema_version": _LATEST_REPORT_SOURCE_BASIS_SCHEMA_VERSION,
        "source": "reports_latest_current_published_or_latest_report",
        "payload_source": str(payload_source or "").strip() or "unknown",
        "run_id": str(payload.get("run_id") or "").strip(),
        "generated_at": str(payload.get("generated_at") or "").strip(),
        "final_count": payload.get("final_count")
        if payload.get("final_count") is not None
        else (payload.get("counts") or {}).get("final_count")
        if isinstance(payload.get("counts"), dict)
        else None,
        "latest_report_file": _file_content_cache_token(settings.latest_report_file),
        "latest_report_file_path": _project_relative_path_text(settings.latest_report_file),
        "publication_registry_file": _file_content_cache_token(settings.publication_registry_file),
        "publication_registry_file_path": _project_relative_path_text(settings.publication_registry_file),
        "latest_canonical_run_id": str((publication or {}).get("latest_canonical_run_id") or "").strip(),
        "current_published_run_id": str((publication or {}).get("current_published_run_id") or "").strip(),
        "latest_canonical_report_path": str((publication or {}).get("latest_canonical_report_path") or "").strip(),
        "current_published_report_path": str((publication or {}).get("current_published_report_path") or "").strip(),
        "report_json_artifact_path": _project_relative_path_text(report_json_path),
        "report_json_artifact_token": _path_cache_token(report_json_path),
        "artifact_keys": sorted(str(key) for key in artifacts.keys()),
        "truth_note": (
            "/api/reports/latest only exposes the current published/latest report artifact plus "
            "publication registry metadata. It must not synthesize fund business values or use "
            "frontend-rendered tables as publication truth."
        ),
    }
    basis["digest"] = _stable_json_digest(basis)
    contract = {
        "schema_version": 1,
        "source": "reports_latest_current_published_or_latest_report",
        "truth_policy": "real_publication_registry_and_report_artifact_only",
        "missing_value_policy": "preserve_missing_semantics",
        "business_value_synthesis": "forbidden",
        "official_publication_truth": "publication_registry_and_current_published_report",
        "frontend_rendered_truth_allowed": False,
        "cache_basis_schema_version": basis["schema_version"],
        "cache_basis_digest": basis["digest"],
    }
    return {
        "source_basis": basis,
        "cache_basis": dict(basis),
        "source_contract": contract,
    }


def _iter_report_artifact_paths(
    artifacts: dict[str, Any],
    *,
    prefix: str = "",
) -> list[tuple[str, str]]:
    entries: list[tuple[str, str]] = []
    for raw_key, raw_value in artifacts.items():
        key = str(raw_key or "").strip()
        if not key:
            continue
        artifact_key = f"{prefix}.{key}" if prefix else key
        if isinstance(raw_value, dict):
            entries.extend(_iter_report_artifact_paths(raw_value, prefix=artifact_key))
            continue
        value_text = str(raw_value or "").strip()
        if value_text:
            entries.append((artifact_key, value_text))
    return entries


def _resolve_project_file_artifact_source(file_path: Path) -> dict[str, Any]:
    report_payload = _load_current_published_report_cached() or _load_latest_report_cached() or {}
    publication = _load_publication_registry_cached()
    artifacts = report_payload.get("artifacts") if isinstance(report_payload.get("artifacts"), dict) else {}
    resolved_path = Path(file_path).resolve()
    artifact_key = ""
    artifact_path = ""
    for candidate_key, candidate_path in _iter_report_artifact_paths(artifacts):
        candidate = (settings.project_root / candidate_path).resolve()
        if candidate == resolved_path:
            artifact_key = candidate_key
            artifact_path = candidate_path
            break
    return {
        "report_payload": report_payload,
        "publication": publication,
        "resolved_path": resolved_path,
        "artifact_key": artifact_key,
        "artifact_path": artifact_path,
    }


def _build_excel_preview_source_metadata(
    file_path: Path,
    *,
    relative_path: str,
    selected_sheet: str | None,
    sheets: list[dict[str, Any]],
    workbook_sheet_names: list[str],
) -> dict[str, Any]:
    source = _resolve_project_file_artifact_source(file_path)
    report_payload = source["report_payload"]
    publication = source["publication"]
    resolved_path = source["resolved_path"]
    artifact_key = source["artifact_key"]
    artifact_path = source["artifact_path"]

    sheet_summaries = [
        {
            "name": str(sheet.get("name") or ""),
            "row_count": int(sheet.get("row_count") or 0),
            "column_count": int(sheet.get("column_count") or 0),
        }
        for sheet in sheets
        if isinstance(sheet, dict)
    ]
    basis: dict[str, Any] = {
        "schema_version": _EXCEL_PREVIEW_SOURCE_BASIS_SCHEMA_VERSION,
        "source": "project_excel_artifact_preview",
        "run_id": str(report_payload.get("run_id") or "").strip(),
        "generated_at": str(report_payload.get("generated_at") or "").strip(),
        "requested_path": str(relative_path or "").strip(),
        "resolved_path": _project_relative_path_text(resolved_path),
        "file_token": _path_cache_token(resolved_path),
        "file_content_token": _file_content_cache_token(resolved_path),
        "artifact_key": artifact_key,
        "artifact_path": artifact_path,
        "artifact_declared_in_latest_report": bool(artifact_key),
        "latest_report_file": _file_content_cache_token(settings.latest_report_file),
        "latest_report_file_path": _project_relative_path_text(settings.latest_report_file),
        "publication_registry_file": _file_content_cache_token(settings.publication_registry_file),
        "publication_registry_file_path": _project_relative_path_text(settings.publication_registry_file),
        "latest_canonical_run_id": str((publication or {}).get("latest_canonical_run_id") or "").strip(),
        "current_published_run_id": str((publication or {}).get("current_published_run_id") or "").strip(),
        "selected_sheet": selected_sheet or None,
        "sheet_count": int(len(sheet_summaries)),
        "workbook_sheet_count": int(len(workbook_sheet_names)),
        "sheet_summaries": sheet_summaries,
        "truth_note": (
            "/api/preview-excel exposes a read-only view of a real project Excel artifact. "
            "It must not synthesize fund business values or treat frontend filtering/sorting as publication truth."
        ),
    }
    basis["digest"] = _stable_json_digest(basis)
    contract = {
        "schema_version": 1,
        "source": "project_excel_artifact_preview",
        "truth_policy": "real_project_excel_artifact_only",
        "missing_value_policy": "preserve_missing_semantics",
        "business_value_synthesis": "forbidden",
        "official_publication_truth": "false_preview_only",
        "frontend_rendered_truth_allowed": False,
        "cache_basis_schema_version": basis["schema_version"],
        "cache_basis_digest": basis["digest"],
    }
    return {
        "source_basis": basis,
        "cache_basis": dict(basis),
        "source_contract": contract,
    }


def _build_project_file_source_headers(
    file_path: Path,
    *,
    relative_path: str,
    download: bool,
) -> dict[str, str]:
    source = _resolve_project_file_artifact_source(file_path)
    report_payload = source["report_payload"]
    publication = source["publication"]
    resolved_path = source["resolved_path"]
    artifact_key = source["artifact_key"]
    artifact_path = source["artifact_path"]
    file_token = _path_cache_token(resolved_path)
    if not resolved_path.exists() and has_stored_csv(resolved_path):
        original = original_artifact_stat(resolved_path)
        file_token = f"{original.st_mtime_ns}:{original.st_size}"
    basis: dict[str, Any] = {
        "schema_version": _PROJECT_FILE_SOURCE_HEADER_SCHEMA_VERSION,
        "source": "project_file_artifact_binary",
        "run_id": str(report_payload.get("run_id") or "").strip(),
        "generated_at": str(report_payload.get("generated_at") or "").strip(),
        "requested_path": str(relative_path or "").strip(),
        "resolved_path": _project_relative_path_text(resolved_path),
        "file_token": file_token,
        "file_content_token": _file_content_cache_token(resolved_path),
        "artifact_key": artifact_key,
        "artifact_path": artifact_path,
        "artifact_declared_in_latest_report": bool(artifact_key),
        "latest_report_file": _file_content_cache_token(settings.latest_report_file),
        "publication_registry_file": _file_content_cache_token(settings.publication_registry_file),
        "latest_canonical_run_id": str((publication or {}).get("latest_canonical_run_id") or "").strip(),
        "current_published_run_id": str((publication or {}).get("current_published_run_id") or "").strip(),
        "download": bool(download),
    }
    basis["digest"] = _stable_json_digest(basis)
    return {
        "X-Fund-ANA-Source": "project_file_artifact_binary",
        "X-Fund-ANA-Truth-Policy": "real_project_file_artifact_only",
        "X-Fund-ANA-Missing-Value-Policy": "preserve_missing_semantics",
        "X-Fund-ANA-Business-Value-Synthesis": "forbidden",
        "X-Fund-ANA-Frontend-Rendered-Truth-Allowed": "false",
        "X-Fund-ANA-Official-Publication-Truth": "false_binary_transfer_only",
        "X-Fund-ANA-Run-ID": str(basis["run_id"]),
        "X-Fund-ANA-Artifact-Key": str(artifact_key),
        "X-Fund-ANA-Artifact-Declared": "true" if artifact_key else "false",
        "X-Fund-ANA-File-Token": str(basis["file_token"]),
        "X-Fund-ANA-File-Content-Token": str(basis["file_content_token"]),
        "X-Fund-ANA-Latest-Canonical-Run-ID": str(basis["latest_canonical_run_id"]),
        "X-Fund-ANA-Current-Published-Run-ID": str(basis["current_published_run_id"]),
        "X-Fund-ANA-Source-Basis-Digest": str(basis["digest"]),
        "X-Fund-ANA-Source-Basis-Schema-Version": str(basis["schema_version"]),
    }


def _build_scheduler_detail_source_metadata(
    payload: dict[str, Any],
    *,
    profile: dict[str, Any],
) -> dict[str, Any]:
    response_payload = dict(payload or {})
    detail_profile = dict(profile or {})
    publication = _load_publication_registry_cached() or _load_publication_registry_snapshot_cached()
    latest_report = _load_current_published_report_cached() or _load_latest_report_snapshot_cached() or _load_latest_report_cached() or {}
    automation = response_payload.get("automation_autonomy")
    automation = automation if isinstance(automation, dict) else {}
    automation_lineage = automation.get("automation_lineage")
    automation_lineage = automation_lineage if isinstance(automation_lineage, dict) else {}
    publication_summary = response_payload.get("publication_summary")
    publication_summary = publication_summary if isinstance(publication_summary, dict) else {}
    scheduler_state_file = _runtime_scheduler_settings().scheduler_state_file
    reports_dir = Path(settings.reports_dir)
    audit_dir = reports_dir / "audit"

    basis: dict[str, Any] = {
        "schema_version": _SCHEDULER_DETAIL_SOURCE_BASIS_SCHEMA_VERSION,
        "source": "scheduler_status_detail_runtime_state",
        "detail_scope": str(detail_profile.get("detail_scope") or response_payload.get("detail_scope") or "").strip()
        or "compact",
        "section": detail_profile.get("section") or response_payload.get("section"),
        "sample_limit": detail_profile.get("sample_limit"),
        "include_full_samples": bool(detail_profile.get("include_full_samples")),
        "latest_report_file": _file_content_cache_token(settings.latest_report_file),
        "latest_report_file_path": _project_relative_path_text(settings.latest_report_file),
        "publication_registry_file": _file_content_cache_token(settings.publication_registry_file),
        "publication_registry_file_path": _project_relative_path_text(settings.publication_registry_file),
        "scheduler_state_file": _scheduler_state_semantic_cache_token(scheduler_state_file),
        "scheduler_state_file_path": _project_relative_path_text(scheduler_state_file),
        "database_file": _sqlite_file_cache_token(settings.database_path),
        "database_file_path": _project_relative_path_text(settings.database_path),
        "risk_coverage_gap_audit": _glob_content_cache_token(
            audit_dir / "risk_coverage_gap_audit",
            "risk_coverage_gap_audit_*.json",
        ),
        "final_selection_contraction_shadow": _glob_content_cache_token(
            audit_dir / "final_selection_contraction_shadow",
            "final_selection_contraction_shadow_*.json",
        ),
        "run_id": str((latest_report or {}).get("run_id") or "").strip(),
        "latest_report_run_id": str((latest_report or {}).get("run_id") or "").strip(),
        "latest_canonical_run_id": str(
            automation_lineage.get("latest_canonical_run_id")
            or automation.get("latest_canonical_run_id")
            or publication_summary.get("latest_canonical_run_id")
            or (publication or {}).get("latest_canonical_run_id")
            or ""
        ).strip(),
        "current_published_run_id": str(
            automation_lineage.get("current_published_run_id")
            or automation.get("current_published_run_id")
            or publication_summary.get("current_published_run_id")
            or (publication or {}).get("current_published_run_id")
            or ""
        ).strip(),
        "latest_successful_run_id": str(
            automation_lineage.get("latest_successful_run_id")
            or automation.get("latest_successful_run_id")
            or publication_summary.get("latest_successful_run_id")
            or (publication or {}).get("latest_successful_run_id")
            or ""
        ).strip(),
        "daily_screening_run_id": str(
            automation_lineage.get("daily_screening_run_id")
            or automation.get("daily_screening_run_id")
            or ((response_payload.get("daily_run") or {}).get("screening_run_id")
                if isinstance(response_payload.get("daily_run"), dict)
                else "")
            or ""
        ).strip() or None,
        "daily_screening_evidence": dict(automation_lineage.get("daily_screening_evidence") or {}),
        "automation_lineage_source": str(automation_lineage.get("source") or "").strip(),
        "automation_lineage_truth_policy": str(automation_lineage.get("truth_policy") or "").strip(),
        "scheduler_running": response_payload.get("scheduler_running"),
        "manual_intervention_required": response_payload.get("manual_intervention_required"),
        "truth_note": (
            "/api/scheduler/status/detail is assembled from scheduler state, publication registry, "
            "database/runtime artifact cache tokens, and real scheduler diagnostics. It must not synthesize "
            "fund business values or treat frontend-rendered state as automation truth."
        ),
    }
    basis["digest"] = _stable_json_digest(basis)
    contract = {
        "schema_version": 1,
        "source": "scheduler_status_detail_runtime_state",
        "truth_policy": "real_scheduler_state_publication_registry_database_and_runtime_artifacts_only",
        "missing_value_policy": "preserve_missing_semantics",
        "business_value_synthesis": "forbidden",
        "official_publication_truth": "false_scheduler_diagnostics_only",
        "frontend_rendered_truth_allowed": False,
        "cache_basis_schema_version": basis["schema_version"],
        "cache_basis_digest": basis["digest"],
    }
    return {
        "source_basis": basis,
        "cache_basis": dict(basis),
        "source_contract": contract,
    }


def _load_scheduler_status_cached() -> dict[str, Any]:
    return _cached_payload(
        "scheduler_status_payload",
        _SCHEDULER_STATUS_CACHE_TTL_SECONDS,
        lambda: load_scheduler_status(_runtime_scheduler_settings()),
    )


def _load_scheduler_state_snapshot_cached() -> dict[str, Any]:
    runtime_settings = _runtime_scheduler_settings()
    return _cached_payload(
        f"scheduler_state_snapshot::{_path_cache_token(runtime_settings.scheduler_state_file)}",
        _SCHEDULER_STATUS_CACHE_TTL_SECONDS,
        lambda: _scheduler_state_with_runtime_evidence(runtime_settings),
    )


def _build_scheduler_status_summary_cached() -> dict[str, Any]:
    runtime_settings = _runtime_scheduler_settings()
    return _cached_payload(
        (
            "scheduler_status_summary_payload::"
            f"{_scheduler_state_semantic_cache_token(runtime_settings.scheduler_state_file)}::"
            f"{_file_content_cache_token(settings.publication_registry_file)}::"
            f"{_final_selection_contraction_shadow_cache_token()}"
        ),
        _SCHEDULER_STATUS_CACHE_TTL_SECONDS,
        lambda: _build_scheduler_status_summary_payload(
            _load_scheduler_state_snapshot_cached(),
            publication=_load_publication_registry_cached() or _load_publication_registry_snapshot_cached(),
        ),
    )


def _build_scheduler_status_detail_cached() -> dict[str, Any]:
    return _build_scheduler_status_detail_variant_cached(
        include_detail=True,
        include_full_samples=False,
        sample_limit=scheduler_diagnostics.DEFAULT_SCHEDULER_DETAIL_SAMPLE_LIMIT,
    )


def _start_scheduler_detail_build_profile(
    detail_scope: str,
    *,
    section: str | None = None,
    sample_limit: Any = None,
    include_full_samples: bool = False,
) -> tuple[float, dict[str, Any]]:
    started_at = time.perf_counter()
    normalized_scope = str(detail_scope or "").strip() or "compact"
    profile: dict[str, Any] = {
        "schema_version": SCHEDULER_DETAIL_BUILD_PROFILE_SCHEMA_VERSION,
        "detail_scope": normalized_scope,
        "section": str(section or "").strip() or None,
        "timing_unit": "seconds",
        "cache_ttl_seconds": _SCHEDULER_STATUS_DETAIL_CACHE_TTL_SECONDS,
        "sample_limit": scheduler_diagnostics.normalize_scheduler_detail_sample_limit(sample_limit),
        "include_full_samples": bool(include_full_samples),
        "steps": [],
    }
    return started_at, profile


def _record_scheduler_detail_build_step(
    profile: dict[str, Any],
    name: str,
    started_at: float,
    **extra: Any,
) -> None:
    step = {
        "name": str(name or "").strip() or "unknown",
        "seconds": round(max(0.0, time.perf_counter() - started_at), 6),
    }
    for key, value in extra.items():
        if value is not None:
            step[str(key)] = value
    profile.setdefault("steps", []).append(step)


def _finish_scheduler_detail_build_profile(
    profile_started_at: float,
    profile: dict[str, Any],
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = dict(payload or {})
    summary_profile = dict(payload.get("scheduler_summary_build_profile") or {})
    steps = list(profile.get("steps") or [])
    profile["total_seconds"] = round(max(0.0, time.perf_counter() - profile_started_at), 6)
    profile["step_count"] = len(steps)
    profile["slowest_steps"] = [
        {
            "name": step.get("name"),
            "seconds": step.get("seconds"),
        }
        for step in sorted(
            steps,
            key=lambda item: float(item.get("seconds") or 0.0),
            reverse=True,
        )[:5]
    ]
    if summary_profile:
        profile["summary_profile"] = {
            "schema_version": summary_profile.get("schema_version"),
            "detail_scope": summary_profile.get("detail_scope"),
            "total_seconds": summary_profile.get("total_seconds"),
            "slowest_steps": list(summary_profile.get("slowest_steps") or [])[:5],
        }
    return profile


def _with_scheduler_detail_build_profile(
    payload: dict[str, Any],
    profile_started_at: float,
    profile: dict[str, Any],
) -> dict[str, Any]:
    response_payload = dict(payload or {})
    response_payload["scheduler_detail_build_profile"] = _finish_scheduler_detail_build_profile(
        profile_started_at,
        profile,
        response_payload,
    )
    response_payload.update(
        _build_scheduler_detail_source_metadata(
            response_payload,
            profile=response_payload["scheduler_detail_build_profile"],
        )
    )
    return response_payload


def _scheduler_detail_section_manifest() -> list[dict[str, Any]]:
    return [
        {
            "section": "runs",
            "label": "recent scheduler runs",
            "endpoint": "/api/scheduler/status/detail?section=runs",
            "heavy": False,
        },
        {
            "section": "manual_history",
            "label": "manual request queue",
            "endpoint": "/api/scheduler/status/detail?section=manual_history",
            "heavy": False,
        },
        {
            "section": "task_closures",
            "label": "task closures",
            "endpoint": "/api/scheduler/status/detail?section=task_closures",
            "heavy": False,
        },
        {
            "section": "maintenance",
            "label": "maintenance summary",
            "endpoint": "/api/scheduler/status/detail?section=maintenance",
            "heavy": False,
        },
        {
            "section": "schedule_stability",
            "label": "schedule stability",
            "endpoint": "/api/scheduler/status/detail?section=schedule_stability",
            "heavy": False,
        },
        {
            "section": "long_term_governance",
            "label": "long-term governance summary",
            "endpoint": "/api/scheduler/status/detail?section=long_term_governance",
            "heavy": False,
        },
        {
            "section": "nav_risk_governance",
            "label": "NAV/risk governance summary",
            "endpoint": "/api/scheduler/status/detail?section=nav_risk_governance",
            "heavy": False,
        },
        {
            "section": "full_audit",
            "label": "legacy full scheduler detail",
            "endpoint": "/api/scheduler/status/detail?full=1",
            "heavy": True,
        },
    ]


def _build_scheduler_detail_index_payload(
    summary_payload: dict[str, Any] | None,
    scheduler_state: dict[str, Any] | None,
    *,
    sample_limit: Any = scheduler_diagnostics.DEFAULT_SCHEDULER_DETAIL_SAMPLE_LIMIT,
) -> dict[str, Any]:
    normalized_sample_limit = scheduler_diagnostics.normalize_scheduler_detail_sample_limit(sample_limit)
    summary = deepcopy(summary_payload or {})
    state = dict(scheduler_state or {})
    latest_runs = scheduler_diagnostics.build_scheduler_last_runs_summary(
        {
            "daily_run": summary.get("daily_run") or {},
            "intraday_run": summary.get("intraday_run") or {},
            "weekly_run": summary.get("weekly_run") or {},
            "snapshot_watchdog_run": summary.get("snapshot_watchdog_run") or {},
        }
    )
    current_final_selection_governance_queue = (
        summary.get("current_final_selection_governance_queue")
        if isinstance(summary.get("current_final_selection_governance_queue"), dict)
        else {}
    ) or _load_current_final_selection_governance_queue_summary_cached()
    payload = {
        **summary,
        "state_schema_version": state.get("state_schema_version"),
        "detail_scope": "detail_index",
        "detail_index_schema_version": _SCHEDULER_DETAIL_INDEX_SCHEMA_VERSION,
        "detail_endpoint": "/api/scheduler/status/detail",
        "heavy_compatibility_endpoint": "/api/scheduler/status/detail?full=1",
        "detail_sections": _scheduler_detail_section_manifest(),
        "detail_sample_policy": {
            "mode": "sample_omitted" if normalized_sample_limit <= 0 else "sample_limited",
            "include_full_samples": False,
            "sample_limit": normalized_sample_limit,
            "max_sample_limit": scheduler_diagnostics.MAX_SCHEDULER_DETAIL_SAMPLE_LIMIT,
        },
        "manual_run_history": list(state.get("manual_run_history") or [])[:10],
        "pending_manual_run_requests": list(state.get("pending_manual_run_requests") or [])[:10],
        "manual_run_history_count": len(state.get("manual_run_history") or []),
        "pending_manual_run_request_count": len(state.get("pending_manual_run_requests") or []),
        "jobs": list(state.get("jobs") or []),
        "task_closures": _build_scheduler_detail_task_closures_summary(
            state.get("task_closures") or {}
        ),
        "child_task_idle_streaks": state.get("child_task_idle_streaks") or {},
        "last_runs": deepcopy(latest_runs),
        "latest_runs": latest_runs,
        "task_run_artifacts": scheduler_diagnostics.build_task_run_artifacts_summary(state),
        "current_final_selection_governance_queue": current_final_selection_governance_queue,
    }
    payload["scheduler_detail_split"] = {
        "enabled": True,
        "mode": "section_index",
        "default_detail_heavy": False,
        "full_audit_requires_full_param": True,
        "summary": "detail=1 returns a lightweight index; use section=... or full=1 for deeper diagnostics.",
    }
    payload.update(_derive_scheduler_header_summary(payload))
    return payload


def _build_scheduler_status_detail_index_cached(
    *,
    sample_limit: Any = scheduler_diagnostics.DEFAULT_SCHEDULER_DETAIL_SAMPLE_LIMIT,
) -> dict[str, Any]:
    runtime_settings = _runtime_scheduler_settings()
    normalized_sample_limit = scheduler_diagnostics.normalize_scheduler_detail_sample_limit(sample_limit)
    return _cached_payload(
        (
            "scheduler_status_detail_index_payload::"
            f"{_path_cache_token(runtime_settings.scheduler_state_file)}::"
            f"{_path_cache_token(settings.publication_registry_file)}::"
            f"{_final_selection_contraction_shadow_cache_token()}::"
            f"limit_{normalized_sample_limit}"
        ),
        _SCHEDULER_STATUS_DETAIL_CACHE_TTL_SECONDS,
        lambda: _build_scheduler_detail_index_payload(
            _build_scheduler_status_summary_cached(),
            _load_scheduler_state_snapshot_cached(),
            sample_limit=normalized_sample_limit,
        ),
    )


def _build_scheduler_status_detail_section_cached(
    section: str,
    *,
    sample_limit: Any = scheduler_diagnostics.DEFAULT_SCHEDULER_DETAIL_SAMPLE_LIMIT,
) -> dict[str, Any]:
    normalized_section = str(section or "").strip().lower().replace("-", "_")
    index_payload = _build_scheduler_status_detail_index_cached(sample_limit=sample_limit)
    if normalized_section in {"", "index", "summary"}:
        return index_payload
    section_payloads = {
        "runs": {
            "daily_run": index_payload.get("daily_run") or {},
            "intraday_run": index_payload.get("intraday_run") or {},
            "weekly_run": index_payload.get("weekly_run") or {},
            "snapshot_watchdog_run": index_payload.get("snapshot_watchdog_run") or {},
            "last_runs": index_payload.get("last_runs") or {},
            "latest_runs": index_payload.get("latest_runs") or {},
        },
        "manual_history": {
            "manual_run_history": index_payload.get("manual_run_history") or [],
            "pending_manual_run_requests": index_payload.get("pending_manual_run_requests") or [],
            "manual_run_history_count": index_payload.get("manual_run_history_count"),
            "pending_manual_run_request_count": index_payload.get("pending_manual_run_request_count"),
            "automation_autonomy": index_payload.get("automation_autonomy") or {},
        },
        "task_closures": {
            "task_closures": index_payload.get("task_closures") or {},
            "child_task_idle_streaks": index_payload.get("child_task_idle_streaks") or {},
            "automation_closure_digest": index_payload.get("automation_closure_digest") or {},
        },
        "maintenance": {
            "maintenance_summary": index_payload.get("maintenance_summary") or {},
            "weekly_run": index_payload.get("weekly_run") or {},
            "task_run_artifacts": index_payload.get("task_run_artifacts") or {},
            "current_final_selection_governance_queue": (
                index_payload.get("current_final_selection_governance_queue") or {}
            ),
        },
        "schedule_stability": {
            "schedule_stability_summary": index_payload.get("schedule_stability_summary") or {},
            "jobs": index_payload.get("jobs") or [],
            "next_run_at": index_payload.get("next_run_at"),
            "next_run_label": index_payload.get("next_run_label"),
        },
        "long_term_governance": {
            "long_term_governance_summary": index_payload.get("long_term_governance_summary") or {},
            "maintenance_summary": index_payload.get("maintenance_summary") or {},
            "current_final_selection_governance_queue": (
                index_payload.get("current_final_selection_governance_queue") or {}
            ),
        },
        "nav_risk_governance": {
            "nav_risk_governance_summary": index_payload.get("nav_risk_governance_summary") or {},
            "risk_lag_summary": index_payload.get("risk_lag_summary") or {},
            "effective_nav_snapshot": index_payload.get("effective_nav_snapshot") or {},
            "latest_screened_snapshot": index_payload.get("latest_screened_snapshot") or {},
        },
    }
    if normalized_section == "full_audit":
        return _build_scheduler_status_detail_variant_cached(
            include_detail=True,
            include_full_samples=False,
            sample_limit=sample_limit,
        )
    if normalized_section not in section_payloads:
        return {
            "error": "unknown_scheduler_detail_section",
            "section": normalized_section,
            "available_sections": [
                item["section"]
                for item in _scheduler_detail_section_manifest()
            ],
        }
    return {
        "detail_scope": "section",
        "section": normalized_section,
        "section_schema_version": _SCHEDULER_DETAIL_INDEX_SCHEMA_VERSION,
        "generated_from": "scheduler_status_detail_index",
        "detail_sample_policy": index_payload.get("detail_sample_policy") or {},
        "scheduler_detail_split": index_payload.get("scheduler_detail_split") or {},
        **section_payloads[normalized_section],
    }


def _build_scheduler_status_detail_variant_cached(
    *,
    include_detail: bool = True,
    include_full_samples: bool = False,
    sample_limit: Any = scheduler_diagnostics.DEFAULT_SCHEDULER_DETAIL_SAMPLE_LIMIT,
) -> dict[str, Any]:
    runtime_settings = _runtime_scheduler_settings()
    normalized_sample_limit = scheduler_diagnostics.normalize_scheduler_detail_sample_limit(sample_limit)
    detail_mode = "detail" if include_detail else "compact"
    sample_mode = "full" if include_full_samples else f"limit_{normalized_sample_limit}"
    return _cached_payload(
        (
            "scheduler_status_detail_payload::"
            f"{_path_cache_token(runtime_settings.scheduler_state_file)}::"
            f"{_path_cache_token(settings.publication_registry_file)}::"
            f"{_path_cache_token(settings.latest_report_file)}::"
            f"{_final_selection_contraction_shadow_cache_token()}::"
            f"{detail_mode}::"
            f"{sample_mode}"
        ),
        _SCHEDULER_STATUS_DETAIL_CACHE_TTL_SECONDS,
        lambda: (
            scheduler_diagnostics.reduce_scheduler_detail_samples(
                _build_scheduler_status_payload(
                    _load_scheduler_state_snapshot_cached(),
                    publication=_load_publication_registry_cached() or _load_publication_registry_snapshot_cached(),
                    include_detail=True,
                ),
                sample_limit=normalized_sample_limit,
                include_full_samples=include_full_samples,
            )
            if include_detail
            else {
                **_build_scheduler_status_summary_cached(),
                "detail_scope": "compact",
                "detail_sample_policy": {
                    "mode": "compact",
                    "include_full_samples": False,
                    "sample_limit": normalized_sample_limit,
                    "max_sample_limit": scheduler_diagnostics.MAX_SCHEDULER_DETAIL_SAMPLE_LIMIT,
                },
            }
        ),
    )


def _load_long_term_gap_audit_bundle_cached() -> dict[str, dict[str, Any]]:
    maintenance_state_file = _maintenance_state_path(settings)
    return _cached_payload(
        (
            "long_term_gap_audit_bundle::"
            f"{_path_cache_token(settings.database_path)}::"
            f"{_path_cache_token(maintenance_state_file)}"
        ),
        _LONG_TERM_NO_STALE_AUDIT_CACHE_TTL_SECONDS,
        lambda: (
            lambda full_universe, no_stale_universe, failure_backoff, full_candidates, full_stats, no_stale_candidates, no_stale_stats: {
                "full": {
                    **(
                        build_long_term_history_gap_audit(
                            settings,
                            include_stale_candidates=True,
                            universe=full_universe,
                            candidates=full_candidates,
                            stats=full_stats,
                            failure_backoff=failure_backoff,
                            write_artifact=False,
                        )
                        or {}
                    ),
                },
                "no_stale": {
                    **(
                        build_long_term_history_gap_audit(
                            settings,
                            include_stale_candidates=False,
                            universe=no_stale_universe,
                            candidates=no_stale_candidates,
                            stats=no_stale_stats,
                            failure_backoff=failure_backoff,
                            write_artifact=False,
                        )
                        or {}
                    ),
                },
            }
        )(
            *_load_long_term_gap_audit_bundle_inputs(),
        ),
    )


def _load_long_term_gap_audit_bundle_inputs() -> tuple[
    list[dict[str, Any]],
    list[dict[str, Any]],
    dict[str, dict[str, Any]],
    list[dict[str, Any]],
    dict[str, int],
    list[dict[str, Any]],
    dict[str, int],
]:
    universe, full_candidates, full_stats = _read_bond_universe(
        settings,
        settings.database_path,
        annual_enabled=True,
        rank_enabled=True,
        include_stale_candidates=True,
    )
    state_payload = _load_maintenance_state(settings)
    failure_backoff = _hydrate_failure_backoff_policy(
        state_payload.get("failure_backoff") or {},
        empty_payload_retry_days=_resolve_empty_payload_retry_days(settings),
        stale_refresh_retry_days=_resolve_stale_refresh_retry_days(settings),
        empty_payload_upstream_limited_failures=_resolve_empty_payload_upstream_limited_failures(settings),
        stale_refresh_upstream_limited_failures=_resolve_stale_refresh_upstream_limited_failures(settings),
    )

    def _clone_stats(source: dict[str, Any]) -> dict[str, int]:
        return {str(key): int(value or 0) for key, value in dict(source or {}).items()}

    no_stale_universe: list[dict[str, Any]] = []
    no_stale_candidates: list[dict[str, Any]] = []
    for raw_row in universe:
        row = dict(raw_row or {})
        row["stale_annual_returns"] = False
        row["stale_rank_history"] = False
        issue_types = [
            issue
            for issue in list(row.get("issue_types") or [])
            if issue not in {"stale_annual_returns", "stale_rank_history"}
        ]
        row["issue_types"] = issue_types
        no_stale_universe.append(row)
        if issue_types:
            no_stale_candidates.append(row)

    no_stale_stats = _clone_stats(full_stats)
    no_stale_stats["annual_history_stale_total"] = 0
    no_stale_stats["rank_history_stale_total"] = 0
    no_stale_stats["screenable_candidate_total"] = sum(
        1 for row in no_stale_candidates if bool(row.get("screenable_universe"))
    )
    no_stale_stats["buyable_candidate_total"] = sum(
        1 for row in no_stale_candidates if bool(row.get("buyable"))
    )

    return (
        universe,
        no_stale_universe,
        failure_backoff,
        full_candidates,
        _clone_stats(full_stats),
        no_stale_candidates,
        no_stale_stats,
    )


def _load_long_term_no_stale_audit_cached() -> dict[str, Any]:
    payload = dict((_load_long_term_gap_audit_bundle_cached().get("no_stale") or {}))
    return {
        **payload,
        **_extract_audit_runtime_scope_summary(payload),
    }


def _load_latest_long_term_maintenance_payload_cached() -> dict[str, Any]:
    return _cached_payload(
        f"long_term_history_maintenance_latest::{_path_cache_token(settings.reports_dir / 'audit' / 'long_term_history_maintenance')}",
        _LONG_TERM_NO_STALE_AUDIT_CACHE_TTL_SECONDS,
        lambda: (
            lambda path, payload: {
                "artifact_path": str(path) if path else None,
                **(payload if isinstance(payload, dict) else {}),
            }
        )(*load_latest_long_term_history_maintenance_payload(settings)),
    )


def _load_long_term_gap_audit_cached() -> dict[str, Any]:
    payload = dict((_load_long_term_gap_audit_bundle_cached().get("full") or {}))
    return {
        **payload,
        **_extract_audit_runtime_scope_summary(payload),
    }


def _long_term_priority_blocked_stale_scope_schema_version(summary: dict[str, Any] | None) -> int:
    try:
        return int(
            (
                (dict(summary or {}).get("stale_scope_not_ready_full_summary") or {}).get(
                    "nav_bucket_schema_version"
                )
            )
            or 0
        )
    except (TypeError, ValueError):
        return 0


def _long_term_priority_blocked_summary_has_complete_live_shape(
    summary: dict[str, Any] | None,
) -> bool:
    payload = dict(summary or {})
    if not payload:
        return False
    if not dict(payload.get("counts_by_priority") or {}):
        return False
    return (
        _long_term_priority_blocked_stale_scope_schema_version(payload)
        >= STALE_SCOPE_NAV_BUCKET_SCHEMA_VERSION
    )


def _first_nonempty_dict(*values: Any) -> dict[str, Any]:
    for value in values:
        if isinstance(value, dict) and value:
            return dict(value)
    return {}


def _summarize_long_term_gap_audit_for_dashboard(
    audit: dict[str, Any] | None,
) -> dict[str, Any]:
    payload = dict(audit or {})
    if not payload:
        return {}
    return {
        "total_gap_fund_count": int(payload.get("total_gap_fund_count") or 0),
        "candidate_total": int(payload.get("candidate_total") or 0),
        "screenable_candidate_total": int(payload.get("screenable_candidate_total") or 0),
        "buyable_candidate_total": int(payload.get("buyable_candidate_total") or 0),
        "action_counts": dict(payload.get("action_counts") or {}),
        "source_counts": dict(payload.get("source_counts") or {}),
        "blocker_counts": dict(payload.get("blocker_counts") or {}),
        "action_scope_matrix": dict(payload.get("action_scope_matrix") or {}),
        "scope_filtered_no_retry_summary": dict(
            payload.get("scope_filtered_no_retry_summary") or {}
        ),
        "samples_by_action": dict(payload.get("samples_by_action") or {}),
        "upstream_observation_summary": dict(payload.get("upstream_observation_summary") or {}),
    }


def _merge_long_term_upstream_observation_summaries(
    primary_summary: dict[str, Any] | None,
    incoming_summary: dict[str, Any] | None,
) -> dict[str, Any]:
    merged = dict(primary_summary or {})

    def _numeric_value(value: Any) -> int | None:
        if isinstance(value, bool):
            return None
        try:
            if value is None or str(value).strip() == "":
                return None
            return int(value)
        except (TypeError, ValueError):
            return None

    for key, incoming_value in dict(incoming_summary or {}).items():
        if incoming_value in (None, "", [], {}):
            continue
        current_value = merged.get(key)
        incoming_number = _numeric_value(incoming_value)
        current_number = _numeric_value(current_value)
        if incoming_number is not None and current_number is not None:
            merged[key] = max(current_number, incoming_number)
            continue
        if isinstance(current_value, dict) and isinstance(incoming_value, dict):
            nested = dict(current_value)
            for nested_key, nested_incoming_value in dict(incoming_value).items():
                if nested_incoming_value in (None, "", [], {}):
                    continue
                nested_current_value = nested.get(nested_key)
                nested_incoming_number = _numeric_value(nested_incoming_value)
                nested_current_number = _numeric_value(nested_current_value)
                if nested_incoming_number is not None and nested_current_number is not None:
                    nested[nested_key] = max(nested_current_number, nested_incoming_number)
                elif nested_current_value in (None, "", [], {}):
                    nested[nested_key] = nested_incoming_value
            merged[key] = nested
            continue
        if current_value in (None, "", [], {}):
            merged[key] = incoming_value
    incoming_watchlist_total = _numeric_value(
        (incoming_summary or {}).get("upstream_limited_watchlist_total")
    )
    incoming_has_watchlist_breakdown = any(
        key in (incoming_summary or {})
        for key in (
            "stale_upstream_limited_watchlist_total",
            "terminal_upstream_limited_watchlist_total",
        )
    )
    if incoming_watchlist_total is not None and not incoming_has_watchlist_breakdown:
        primary_active_total = _numeric_value(
            (primary_summary or {}).get("upstream_limited_watchlist_total")
        ) or 0
        primary_stale_total = _numeric_value(
            (primary_summary or {}).get("stale_upstream_limited_watchlist_total")
        ) or 0
        primary_terminal_total = _numeric_value(
            (primary_summary or {}).get("terminal_upstream_limited_watchlist_total")
        ) or 0
        if incoming_watchlist_total >= (
            primary_active_total + primary_stale_total + primary_terminal_total
        ):
            merged["upstream_limited_watchlist_total"] = incoming_watchlist_total
            merged["stale_upstream_limited_watchlist_total"] = 0
            merged["terminal_upstream_limited_watchlist_total"] = 0
            for prefix in ("stale", "terminal"):
                for suffix in ("reason_counts", "subtype_counts", "blocker_counts"):
                    merged[f"{prefix}_upstream_limited_{suffix}"] = {}
    return merged


def _merge_live_long_term_priority_blocked_summary(
    maintenance_detail: dict[str, Any] | None,
) -> dict[str, Any]:
    detail = dict(maintenance_detail or {})
    incoming_upstream_observation_summary = dict(
        detail.get("upstream_observation_summary") or {}
    )
    incoming_priority_blocked_summary = dict(
        detail.get("priority_blocked_summary") or {}
    )
    live_audit: dict[str, Any] = {}
    live_no_stale_audit: dict[str, Any] = {}

    def _summarize_gap_audit_with_incoming_upstream(
        audit: dict[str, Any] | None,
    ) -> dict[str, Any]:
        summarized = _summarize_long_term_gap_audit_for_dashboard(audit)
        if incoming_upstream_observation_summary:
            summarized["upstream_observation_summary"] = (
                _merge_long_term_upstream_observation_summaries(
                    summarized.get("upstream_observation_summary"),
                    incoming_upstream_observation_summary,
                )
            )
        return summarized

    try:
        latest_maintenance = _load_latest_long_term_maintenance_payload_cached()
    except Exception:
        latest_maintenance = {}
    maintenance_result = dict((latest_maintenance.get("maintenance_result") or {}))
    live_gap_audit = dict((latest_maintenance.get("long_term_gap_audit") or {}))
    effective_live_gap_audit = dict(live_gap_audit or {})

    if maintenance_result or live_gap_audit:
        if not detail.get("status"):
            detail["status"] = maintenance_result.get("status")
        if not detail.get("no_action_category"):
            detail["no_action_category"] = maintenance_result.get("no_action_category")
        if not detail.get("idle_reason"):
            detail["idle_reason"] = maintenance_result.get("idle_reason")
        if not detail.get("processed_total"):
            detail["processed_total"] = maintenance_result.get("processed_total")
        if not detail.get("changed_total"):
            detail["changed_total"] = maintenance_result.get("changed_total")
        if not detail.get("coverage_progress") and isinstance(
            maintenance_result.get("coverage_progress"),
            dict,
        ):
            detail["coverage_progress"] = dict(maintenance_result.get("coverage_progress") or {})
        if not detail.get("upstream_observation_summary") and isinstance(
            maintenance_result.get("upstream_observation_summary"),
            dict,
        ):
            detail["upstream_observation_summary"] = dict(
                maintenance_result.get("upstream_observation_summary") or {}
            )
        if not detail.get("failed_reason_governance_summary") and isinstance(
            maintenance_result.get("failed_reason_governance_summary"),
            dict,
        ):
            detail["failed_reason_governance_summary"] = dict(
                maintenance_result.get("failed_reason_governance_summary") or {}
            )
        if not detail.get("failed_reason_summary") and isinstance(
            maintenance_result.get("failed_reason_summary"),
            dict,
        ):
            detail["failed_reason_summary"] = dict(
                maintenance_result.get("failed_reason_summary") or {}
            )
        if effective_live_gap_audit:
            detail["live_gap_audit"] = _summarize_gap_audit_with_incoming_upstream(
                effective_live_gap_audit
            )

        live_priority_blocked_candidate = _first_nonempty_dict(
            effective_live_gap_audit.get("priority_blocked_summary"),
        )
        priority_blocked_summary = _first_nonempty_dict(
            live_priority_blocked_candidate,
            detail.get("priority_blocked_summary"),
            maintenance_result.get("priority_blocked_summary"),
        )
        merged_priority_blocked_summary = dict(priority_blocked_summary)
        if merged_priority_blocked_summary:
            current_stale_schema_version = _long_term_priority_blocked_stale_scope_schema_version(
                merged_priority_blocked_summary
            )
            live_stale_schema_version = _long_term_priority_blocked_stale_scope_schema_version(
                live_priority_blocked_candidate
            )
            if (
                current_stale_schema_version < STALE_SCOPE_NAV_BUCKET_SCHEMA_VERSION
                and live_stale_schema_version >= STALE_SCOPE_NAV_BUCKET_SCHEMA_VERSION
            ):
                merged_priority_blocked_summary = dict(live_priority_blocked_candidate)

        needs_live_priority_merge = not _long_term_priority_blocked_summary_has_complete_live_shape(
            merged_priority_blocked_summary
        )
        if needs_live_priority_merge:
            try:
                live_audit = _load_long_term_gap_audit_cached()
                live_no_stale_audit = _load_long_term_no_stale_audit_cached()
            except Exception:
                live_audit = {}
                live_no_stale_audit = {}
            if live_audit:
                detail["live_gap_audit"] = _summarize_gap_audit_with_incoming_upstream(live_audit)
                live_priority_blocked_candidate = _first_nonempty_dict(
                    live_audit.get("priority_blocked_summary"),
                    live_priority_blocked_candidate,
                )
                merged_priority_blocked_summary = _first_nonempty_dict(
                    live_priority_blocked_candidate,
                    merged_priority_blocked_summary,
                )
            if not _long_term_priority_blocked_summary_has_complete_live_shape(
                merged_priority_blocked_summary
            ):
                live_priority_blocked_summary = _merge_live_priority_blocked_summaries(
                    live_priority_blocked_candidate,
                    live_no_stale_audit.get("priority_blocked_summary"),
                )
                if live_priority_blocked_summary:
                    merged_priority_blocked_summary = dict(live_priority_blocked_summary)
        if merged_priority_blocked_summary:
            should_merge_no_stale_priority = (
                bool(incoming_priority_blocked_summary)
                or not _long_term_priority_blocked_summary_has_complete_live_shape(
                    merged_priority_blocked_summary
                )
            )
            if should_merge_no_stale_priority:
                if not live_no_stale_audit:
                    try:
                        live_no_stale_audit = _load_long_term_no_stale_audit_cached()
                    except Exception:
                        live_no_stale_audit = {}
                live_no_stale_priority = dict(
                    live_no_stale_audit.get("priority_blocked_summary") or {}
                )
                if live_no_stale_priority:
                    merged_priority_blocked_summary = _merge_live_priority_blocked_summaries(
                        merged_priority_blocked_summary,
                        live_no_stale_priority,
                    )
            detail["priority_blocked_summary"] = merged_priority_blocked_summary
        return detail
    try:
        live_audit = _load_long_term_gap_audit_cached()
        live_no_stale_audit = _load_long_term_no_stale_audit_cached()
    except Exception:
        return detail
    if not detail.get("upstream_observation_summary") and isinstance(
        live_audit.get("upstream_observation_summary"),
        dict,
    ):
        detail["upstream_observation_summary"] = dict(
            live_audit.get("upstream_observation_summary") or {}
        )
    if not detail.get("failed_reason_governance_summary") and isinstance(
        live_audit.get("failed_reason_governance_summary"),
        dict,
    ):
        detail["failed_reason_governance_summary"] = dict(
            live_audit.get("failed_reason_governance_summary") or {}
        )
    if not detail.get("failed_reason_summary") and isinstance(
        live_audit.get("failed_reason_summary"),
        dict,
    ):
        detail["failed_reason_summary"] = dict(
            live_audit.get("failed_reason_summary") or {}
        )
    detail["live_gap_audit"] = _summarize_gap_audit_with_incoming_upstream(live_audit)
    live_priority_blocked_summary = _merge_live_priority_blocked_summaries(
        live_audit.get("priority_blocked_summary"),
        live_no_stale_audit.get("priority_blocked_summary"),
    )
    if live_priority_blocked_summary:
        detail["priority_blocked_summary"] = live_priority_blocked_summary
    return detail


def _build_scheduler_detail_task_closures_summary(task_closures: dict[str, Any] | None) -> dict[str, Any]:
    summarized = summarize_task_closures_for_state(task_closures)
    weekly_closure = dict(summarized.get("weekly_maintenance") or {})
    weekly_child_tasks = dict(weekly_closure.get("child_tasks") or {})
    long_term_child = dict(weekly_child_tasks.get("long_term_history_maintenance") or {})
    if long_term_child:
        weekly_child_tasks["long_term_history_maintenance"] = scheduler_diagnostics._normalize_long_term_child_task_payload(
            long_term_child
        )
        weekly_closure["child_tasks"] = weekly_child_tasks
        summarized["weekly_maintenance"] = weekly_closure
    return summarized


def _merge_live_priority_blocked_summaries(
    full_summary: dict[str, Any] | None,
    no_stale_summary: dict[str, Any] | None,
) -> dict[str, Any]:
    normalized_full = dict(full_summary or {})
    normalized_no_stale = dict(no_stale_summary or {})
    if not normalized_full:
        return normalized_no_stale
    if not normalized_no_stale:
        return normalized_full

    bucket_keys = (
        "buyable_active_metadata_blocked",
        "buyable_active_scope_not_ready",
        "other_metadata_blocked",
        "other_scope_not_ready",
    )

    def _unique_samples(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        deduped: list[dict[str, Any]] = []
        seen: set[str] = set()
        for raw_item in items:
            item = dict(raw_item or {})
            code = _normalize_code_text(item.get("fcode"))
            key = code or json.dumps(item, ensure_ascii=False, sort_keys=True)
            if key in seen:
                continue
            seen.add(key)
            deduped.append(item)
        return deduped

    def _bucket_source_counts(summary: dict[str, Any], bucket: str) -> dict[str, int]:
        payload = (summary.get("source_counts_by_bucket") or {}).get(bucket) or {}
        return {
            str(source or "").strip() or "unknown": int(value or 0)
            for source, value in dict(payload).items()
        }

    def _bucket_total(summary: dict[str, Any], bucket: str) -> int:
        return int(
            ((summary.get("counts_by_bucket") or {}).get(bucket))
            or summary.get(f"{bucket}_total")
            or 0
        )

    merged_samples_by_bucket: dict[str, list[dict[str, Any]]] = {}
    merged_source_counts_by_bucket: dict[str, dict[str, int]] = {}
    merged_counts_by_bucket: dict[str, int] = {}

    for bucket in bucket_keys:
        full_samples = list(((normalized_full.get("samples_by_bucket") or {}).get(bucket)) or [])
        no_stale_samples = list(((normalized_no_stale.get("samples_by_bucket") or {}).get(bucket)) or [])
        no_stale_bucket_total = _bucket_total(normalized_no_stale, bucket)
        full_bucket_total = _bucket_total(normalized_full, bucket)
        full_bucket_source_counts = _bucket_source_counts(normalized_full, bucket)
        no_stale_bucket_source_counts = _bucket_source_counts(normalized_no_stale, bucket)
        stale_full_codes = {
            _normalize_code_text((item or {}).get("fcode"))
            for item in full_samples
            if str((item or {}).get("source_category") or "").strip() == "local_stale_history_gap"
            if _normalize_code_text((item or {}).get("fcode"))
        }
        removed_no_stale_samples = [
            item
            for item in no_stale_samples
            if _normalize_code_text((item or {}).get("fcode")) in stale_full_codes
        ]
        filtered_no_stale_samples = [
            item
            for item in no_stale_samples
            if _normalize_code_text((item or {}).get("fcode")) not in stale_full_codes
        ]
        filtered_no_stale_codes = {
            _normalize_code_text((sample or {}).get("fcode"))
            for sample in filtered_no_stale_samples
            if _normalize_code_text((sample or {}).get("fcode"))
        }
        stale_full_samples = [
            item
            for item in full_samples
            if str((item or {}).get("source_category") or "").strip() == "local_stale_history_gap"
        ]
        non_stale_full_samples = [
            item
            for item in full_samples
            if str((item or {}).get("source_category") or "").strip() != "local_stale_history_gap"
            and _normalize_code_text((item or {}).get("fcode")) not in filtered_no_stale_codes
        ]
        merged_bucket_samples = _unique_samples([*filtered_no_stale_samples, *stale_full_samples, *non_stale_full_samples])
        if merged_bucket_samples:
            merged_samples_by_bucket[bucket] = merged_bucket_samples

        merged_source_counts = dict(no_stale_bucket_source_counts)
        removed_no_stale_source_counts: dict[str, int] = {}
        for item in removed_no_stale_samples:
            source_key = str((item or {}).get("source_category") or "").strip() or "unknown"
            removed_no_stale_source_counts[source_key] = int(removed_no_stale_source_counts.get(source_key) or 0) + 1
        for source_key, removed_count in removed_no_stale_source_counts.items():
            adjusted = int(merged_source_counts.get(source_key) or 0) - int(removed_count or 0)
            if adjusted > 0:
                merged_source_counts[source_key] = adjusted
            else:
                merged_source_counts.pop(source_key, None)
        for source_key, count in full_bucket_source_counts.items():
            if source_key == "local_stale_history_gap":
                merged_source_counts[source_key] = int(count or 0)
            else:
                merged_source_counts.setdefault(source_key, int(count or 0))
        if merged_source_counts:
            merged_source_counts_by_bucket[bucket] = merged_source_counts
        if bucket.endswith("scope_not_ready"):
            stale_full_count = int(full_bucket_source_counts.get("local_stale_history_gap") or 0)
            removed_overlap_count = len(removed_no_stale_samples)
            merged_counts_by_bucket[bucket] = max(
                0,
                no_stale_bucket_total + stale_full_count - removed_overlap_count,
            )
        else:
            merged_counts_by_bucket[bucket] = max(no_stale_bucket_total, full_bucket_total)

    merged_counts_by_priority = dict(normalized_full.get("counts_by_priority") or {})
    if not merged_counts_by_priority:
        buyable_active_total = int(merged_counts_by_bucket.get("buyable_active_metadata_blocked") or 0) + int(
            merged_counts_by_bucket.get("buyable_active_scope_not_ready") or 0
        )
        other_total = int(merged_counts_by_bucket.get("other_metadata_blocked") or 0) + int(
            merged_counts_by_bucket.get("other_scope_not_ready") or 0
        )
        if buyable_active_total > 0:
            merged_counts_by_priority["buyable_active_priority"] = buyable_active_total
        if other_total > 0:
            merged_counts_by_priority["active_priority"] = other_total

    stale_buyable_active_scope_not_ready_total = int(
        (merged_source_counts_by_bucket.get("buyable_active_scope_not_ready") or {}).get("local_stale_history_gap") or 0
    )
    stale_other_scope_not_ready_total = int(
        (merged_source_counts_by_bucket.get("other_scope_not_ready") or {}).get("local_stale_history_gap") or 0
    )
    stale_scope_not_ready_total = stale_buyable_active_scope_not_ready_total + stale_other_scope_not_ready_total
    total = int(sum(merged_counts_by_bucket.values()))
    scope_not_ready_total = int(merged_counts_by_bucket.get("buyable_active_scope_not_ready") or 0) + int(
        merged_counts_by_bucket.get("other_scope_not_ready") or 0
    )
    no_stale_raw_total = int(
        normalized_no_stale.get("total_raw")
        or normalized_no_stale.get("total")
        or 0
    )
    full_raw_total = int(
        normalized_full.get("total_raw")
        or normalized_full.get("total")
        or 0
    )
    total_excluding_stale_scope_not_ready = max(0, total - stale_scope_not_ready_total)
    merged_raw_total = no_stale_raw_total or total_excluding_stale_scope_not_ready or full_raw_total

    merged = dict(normalized_no_stale)
    merged.update(
        {
            "total": total,
            "total_raw": merged_raw_total,
            "buyable_active_metadata_blocked_total": int(
                merged_counts_by_bucket.get("buyable_active_metadata_blocked") or 0
            ),
            "buyable_active_scope_not_ready_total": int(
                merged_counts_by_bucket.get("buyable_active_scope_not_ready") or 0
            ),
            "other_metadata_blocked_total": int(merged_counts_by_bucket.get("other_metadata_blocked") or 0),
            "other_scope_not_ready_total": int(merged_counts_by_bucket.get("other_scope_not_ready") or 0),
            "scope_not_ready_total": scope_not_ready_total,
            "stale_buyable_active_scope_not_ready_total": stale_buyable_active_scope_not_ready_total,
            "stale_other_scope_not_ready_total": stale_other_scope_not_ready_total,
            "stale_scope_not_ready_total": stale_scope_not_ready_total,
            "total_excluding_stale_scope_not_ready": total_excluding_stale_scope_not_ready,
            "counts_by_bucket": merged_counts_by_bucket,
            "counts_by_priority": merged_counts_by_priority,
            "source_counts_by_bucket": merged_source_counts_by_bucket,
            "stale_scope_not_ready_full_summary": dict(
                normalized_full.get("stale_scope_not_ready_full_summary") or {}
            ),
            "samples_by_bucket": merged_samples_by_bucket,
        }
    )
    return merged


def _build_latest_dashboard_compact_cached() -> dict[str, Any]:
    def _build() -> dict[str, Any]:
        payload = _load_current_published_report_cached() or _load_latest_report_snapshot_cached() or _load_latest_report_cached()
        if not payload:
            return {"error": "暂无报告"}
        scheduler_state = _load_scheduler_state_snapshot_cached()
        publication = _load_publication_registry_snapshot_cached() or _load_publication_registry_cached()
        return dashboard_payloads.build_latest_dashboard_response(
            payload,
            publication,
            scheduler_state,
            settings=settings,
            manual_operation_mode=MANUAL_OPERATION_MODE,
            normalize_portfolio_groups=_normalize_portfolio_groups,
            build_portfolio_group_brief=_build_portfolio_group_brief,
            build_database_source_audit=build_database_source_audit,
            load_database_overview=_load_database_overview,
            load_database_integrity_summary=_load_database_integrity_summary,
            load_nav_lag_summary=_load_nav_lag_summary,
            load_risk_lag_summary=_load_risk_lag_summary,
            load_risk_sync_summary=_load_risk_sync_summary,
            load_risk_coverage_gap_summary=_load_risk_coverage_gap_summary,
            load_bond_universe_snapshot=_load_bond_universe_snapshot,
            load_exclusion_reason_summary=_load_exclusion_reason_summary,
            normalize_coverage_layers=_normalize_coverage_layers,
            build_coverage_layers_from_snapshots=_build_coverage_layers_from_snapshots,
            build_publish_funnel_summary=_build_publish_funnel_summary,
            build_scheduler_overview_summary=lambda state: _build_dashboard_scheduler_lightweight_overview(
                state,
                publication=publication,
                include_recent_failure_recovery=False,
                allow_runtime_digest_fallback=False,
            ),
            build_scheduler_overview_api_summary=_build_scheduler_overview_api_summary,
            build_scheduler_chain_audit=_build_scheduler_chain_audit,
            build_automation_autonomy_summary=_build_automation_autonomy_summary,
            # 首页 compact 响应只消费 scheduler/report 中已提交的治理快照。
            # live gap audit 会重新扫描完整长期历史全集，冷构建可达十几秒；
            # 诊断详情页仍保留 live merge，避免把重治理扫描放进首屏路径。
            merge_live_long_term_priority_blocked_summary=None,
            read_excel_cached=_read_excel_cached,
            read_tracker_workbook_sheet=_read_tracker_workbook_sheet,
            compact=True,
        )

    cache_basis = {
        "latest_report_file": _file_content_cache_token(settings.latest_report_file),
        "publication_registry_file": _file_content_cache_token(settings.publication_registry_file),
        "scheduler_state_file": _scheduler_state_semantic_cache_token(
            _runtime_scheduler_settings().scheduler_state_file
        ),
        "final_selection_contraction_shadow": _final_selection_contraction_shadow_cache_token(),
    }
    return _cached_payload(
        f"dashboard_latest_compact_response::{_stable_json_digest(cache_basis)}::"
        f"{_path_cache_token(settings.reports_dir / 'audit' / 'annual_return_gate_shadow')}::"
        f"{_low_vol_overlap_audit_cache_token()}",
        _DASHBOARD_COMPACT_CACHE_TTL_SECONDS,
        _build,
    )


def _build_latest_dashboard_scoped_detail_cached(detail_scope: str) -> dict[str, Any]:
    def _build() -> dict[str, Any]:
        scheduler_overview_builder = _build_scheduler_overview_summary
        if detail_scope in {"reports", "task_summary"}:
            payload = _load_current_published_report_cached() or _load_latest_report_snapshot_cached() or _load_latest_report_cached()
            publication = _load_publication_registry_snapshot_cached() or _load_publication_registry_cached()
            scheduler_state = _load_scheduler_state_snapshot_cached()
        elif detail_scope == "diagnostic_summary":
            payload = _load_current_published_report_cached() or _load_latest_report_snapshot_cached() or _load_latest_report_cached()
            publication = _load_publication_registry_snapshot_cached() or _load_publication_registry_cached()
            scheduler_state = _load_scheduler_state_snapshot_cached()
            scheduler_overview_builder = lambda state: _build_dashboard_scheduler_lightweight_overview(
                state,
                publication=publication,
            )
        else:
            payload = _load_current_published_report_cached() or _load_latest_report_cached()
            publication = _load_publication_registry_cached()
            scheduler_state = _load_scheduler_state_snapshot_cached()
        if not payload:
            return {"error": "暂无报告"}
        scoped_merge = _merge_live_long_term_priority_blocked_summary
        if detail_scope in {"reports", "task_summary"}:
            scoped_merge = None
        risk_lag_loader = (
            (lambda: _load_default_risk_lag_summary(payload))
            if detail_scope == "diagnostic_summary"
            else _load_risk_lag_summary
        )
        return dashboard_payloads.build_latest_dashboard_response(
            payload,
            publication,
            scheduler_state,
            settings=settings,
            manual_operation_mode=MANUAL_OPERATION_MODE,
            normalize_portfolio_groups=_normalize_portfolio_groups,
            build_portfolio_group_brief=_build_portfolio_group_brief,
            build_database_source_audit=build_database_source_audit,
            load_database_overview=_load_database_overview,
            load_database_integrity_summary=_load_database_integrity_summary,
            load_nav_lag_summary=_load_nav_lag_summary,
            load_risk_lag_summary=risk_lag_loader,
            load_risk_sync_summary=_load_risk_sync_summary,
            load_risk_coverage_gap_summary=_load_risk_coverage_gap_summary,
            load_bond_universe_snapshot=_load_bond_universe_snapshot,
            load_exclusion_reason_summary=_load_exclusion_reason_summary,
            normalize_coverage_layers=_normalize_coverage_layers,
            build_coverage_layers_from_snapshots=_build_coverage_layers_from_snapshots,
            build_publish_funnel_summary=_build_publish_funnel_summary,
            build_scheduler_overview_summary=scheduler_overview_builder,
            build_scheduler_overview_api_summary=_build_scheduler_overview_api_summary,
            build_scheduler_chain_audit=_build_scheduler_chain_audit,
            build_automation_autonomy_summary=_build_automation_autonomy_summary,
            merge_live_long_term_priority_blocked_summary=scoped_merge,
            read_excel_cached=_read_excel_cached,
            read_tracker_workbook_sheet=_read_tracker_workbook_sheet,
            compact=False,
            detail_scope=detail_scope,
        )

    cache_basis = {
        "detail_scope": str(detail_scope or "").strip(),
        "latest_report_file": _file_content_cache_token(settings.latest_report_file),
        "publication_registry_file": _file_content_cache_token(settings.publication_registry_file),
        "scheduler_state_file": _scheduler_state_semantic_cache_token(
            _runtime_scheduler_settings().scheduler_state_file
        ),
        "final_selection_contraction_shadow": _final_selection_contraction_shadow_cache_token(),
        "low_vol_overlap_audit": _low_vol_overlap_audit_cache_token(),
    }
    return _cached_payload(
        f"dashboard_latest_scoped_detail::{_stable_json_digest(cache_basis)}::"
        f"{_path_cache_token(settings.reports_dir / 'audit' / 'annual_return_gate_shadow')}",
        _DASHBOARD_SCOPED_DETAIL_CACHE_TTL_SECONDS,
        _build,
    )


def _refresh_database_freshness_summary_sidecar() -> dict[str, Any]:
    payload = build_database_freshness_summary(settings)
    artifact_path: Path | None = None
    if not payload.get("source_db_changed_during_build"):
        artifact_path = write_database_freshness_summary(settings, payload)
    return summarize_database_freshness_payload(
        payload,
        source="api_prewarm",
        artifact_path=artifact_path or database_freshness_summary_path(settings),
    )


def _prewarm_recommendations_detail_sidecar() -> dict[str, Any]:
    payload = _load_current_published_report_cached() or _load_latest_report_snapshot_cached() or _load_latest_report_cached()
    if not payload:
        return {"error": "暂无报告"}
    return _build_recommendations_detail_response(payload)


def _prewarm_tracker_current_derived_fields_sidecar() -> dict[str, Any]:
    payload = _load_current_published_report_cached() or _load_latest_report_snapshot_cached() or _load_latest_report_cached()
    if not payload:
        return {"error": "暂无报告"}
    response = _build_tracker_current_response(payload)
    payload_scope = response.get("payload_scope") if isinstance(response.get("payload_scope"), dict) else {}
    return {
        "run_id": response.get("run_id"),
        "row_count": response.get("row_count"),
        "tracker_current_derived_fields": payload_scope.get("tracker_current_derived_fields"),
    }


_ORIGINAL_LOAD_CURRENT_PUBLISHED_REPORT_CACHED = _load_current_published_report_cached
_ORIGINAL_LOAD_LATEST_REPORT_SNAPSHOT_CACHED = _load_latest_report_snapshot_cached
_ORIGINAL_LOAD_LATEST_REPORT_CACHED = _load_latest_report_cached
_ORIGINAL_LOAD_PUBLICATION_REGISTRY_CACHED = _load_publication_registry_cached
_ORIGINAL_LOAD_PUBLICATION_REGISTRY_SNAPSHOT_CACHED = _load_publication_registry_snapshot_cached
_ORIGINAL_BUILD_LATEST_DASHBOARD_SCOPED_DETAIL_CACHED = _build_latest_dashboard_scoped_detail_cached
_ORIGINAL_LOAD_PUBLICATION_REGISTRY_FUNCTION = load_publication_registry


def _ensure_api_prewarm() -> None:
    global _api_prewarm_started
    if "pytest" in sys.modules or os.environ.get("PYTEST_CURRENT_TEST"):
        return
    if os.environ.get("FUND_ANA_DISABLE_API_PREWARM") == "1":
        return
    with _api_prewarm_lock:
        if _api_prewarm_started:
            return
        _api_prewarm_started = True

    def _run_prewarm_target(target: Callable[[], Any]) -> None:
        try:
            target()
        except Exception:
            pass

    # 默认 dashboard detail 页是用户首访热点；同步预热可把冷启动构建成本前移到 worker 启动阶段。
    _run_prewarm_target(lambda: _build_latest_dashboard_scoped_detail_cached("diagnostic_summary"))

    def runner() -> None:
        # 后台预热按顺序执行，避免多个重型 target 在启动期争抢同一批 cache/锁，
        # 反而拖慢首个真实请求。
        prewarm_targets: list[Callable[[], Any]] = [
            _refresh_database_freshness_summary_sidecar,
            _build_latest_dashboard_compact_cached,
            _build_dashboard_overview_cached,
            _prewarm_recommendations_detail_sidecar,
            _prewarm_tracker_current_derived_fields_sidecar,
            lambda: _build_latest_dashboard_scoped_detail_cached("task_summary"),
            lambda: _build_latest_dashboard_scoped_detail_cached("reports"),
            _load_latest_report_cached,
            _load_publication_registry_cached,
            _build_scheduler_status_summary_cached,
            _load_database_integrity_summary,
            lambda: _cached_payload(
                "disk_usage_snapshot",
                _DISK_USAGE_CACHE_TTL_SECONDS,
                _build_disk_usage_snapshot,
            ),
            # operations/detail 页面所需重型块放到最后，避免与首屏热点竞争。
            _build_scheduler_status_detail_cached,
        ]
        for target in prewarm_targets:
            _run_prewarm_target(target)

    threading.Thread(target=runner, daemon=True).start()


_ensure_api_prewarm()


register_system_routes(
    app,
    ensure_scheduler=lambda: ensure_embedded_scheduler(),
    build_api_info_payload=lambda: _build_api_info_payload(),
    build_health_payload=lambda: annotate_scheduler_payload(settings, _build_health_payload()),
    build_health_summary_payload=lambda: annotate_scheduler_payload(settings, _build_health_summary_payload()),
    build_bootstrap_summary_payload=lambda: annotate_scheduler_payload(settings, _build_bootstrap_summary_payload()),
    build_autostock_missing_response_observation_payload=lambda: build_missing_response_observation_payload(
        settings,
    ),
    build_api_settings_payload=lambda: _build_api_settings_payload(),
    update_api_settings=lambda payload: _update_api_settings(payload),
    probe_api_settings=lambda payload: _probe_api_settings(payload),
    build_storage_usage_payload=lambda: _build_disk_usage_snapshot(),
)


from backend.api_routes.report_history import register_history_routes
register_history_routes(app, settings)


register_reports_routes(
    app,
    ensure_scheduler=lambda: ensure_embedded_scheduler(),
    manager_status=lambda: manager.status(),
    workflow_status_summary=lambda: _workflow_status_summary(),
    manual_operations_enabled=lambda: MANUAL_OPERATION_MODE,
    run_collection_default=lambda: settings.run_collection_by_default,
    run_screening_default=lambda: settings.run_screening_by_default,
    start_workflow=lambda options: manager.start(options),
    list_reports=lambda: manager.list_reports(),
    request_arg_truthy=lambda *names: _request_arg_truthy(*names),
    build_reports_compact_response=lambda publication: _build_reports_compact_response(publication),
    load_publication_registry=lambda: _load_publication_registry_cached(),
    load_current_published_report=lambda: _load_current_published_report_cached(),
    load_latest_report=lambda: _load_latest_report_cached(),
    build_publication_summary_payload=lambda publication, **kwargs: _build_publication_summary_payload(
        publication,
        **kwargs,
    ),
    build_latest_report_source_metadata=lambda payload, **kwargs: _build_latest_report_source_metadata(
        payload,
        **kwargs,
    ),
    build_excel_preview_source_metadata=lambda file_path, **kwargs: _build_excel_preview_source_metadata(
        file_path,
        **kwargs,
    ),
    build_project_file_source_headers=lambda file_path, **kwargs: _build_project_file_source_headers(
        file_path,
        **kwargs,
    ),
    manager_latest_report=lambda: manager.latest_report(),
    resolve_project_file=lambda relative_path: _resolve_project_file(relative_path),
    deduplicate_headers=lambda headers: _deduplicate_headers(headers),
)


register_scheduler_routes(
    app,
    ensure_scheduler=lambda: ensure_embedded_scheduler(),
    build_status_summary_cached=lambda: annotate_scheduler_payload(settings, _build_scheduler_status_summary_cached()),
    default_detail_sample_limit=lambda: scheduler_diagnostics.DEFAULT_SCHEDULER_DETAIL_SAMPLE_LIMIT,
    start_detail_build_profile=lambda *args, **kwargs: _start_scheduler_detail_build_profile(*args, **kwargs),
    record_detail_build_step=lambda *args, **kwargs: _record_scheduler_detail_build_step(*args, **kwargs),
    with_detail_build_profile=lambda payload, started_at, profile: _with_scheduler_detail_build_profile(
        payload,
        started_at,
        profile,
    ),
    build_detail_section_cached=lambda *args, **kwargs: annotate_scheduler_payload(
        settings, _build_scheduler_status_detail_section_cached(*args, **kwargs)),
    build_detail_index_cached=lambda *args, **kwargs: annotate_scheduler_payload(
        settings, _build_scheduler_status_detail_index_cached(*args, **kwargs)),
    build_detail_variant_cached=lambda *args, **kwargs: annotate_scheduler_payload(
        settings, _build_scheduler_status_detail_variant_cached(*args, **kwargs)),
    manual_operations_enabled=lambda: MANUAL_OPERATION_MODE,
    dispatch_run_request=lambda job_id: _dispatch_scheduler_run_request(job_id, requested_by="api"),
    dispatch_internal_run_request=lambda job_id: _dispatch_scheduler_run_request(
        job_id,
        requested_by="scheduler_cli",
    ),
    is_loopback_request=lambda: _is_loopback_request(),
    load_collection_count_correction=lambda task_id: load_collection_count_correction(settings, task_id),
    load_refresh_collection_correction=lambda task_id: load_refresh_collection_correction(settings, task_id),
)


_build_publication_registry_summary = build_publication_registry_summary

register_publication_routes(
    app,
    ensure_scheduler=lambda: ensure_embedded_scheduler(),
    load_publication_registry=lambda: _load_publication_registry_cached(),
    request_arg_truthy=lambda *names: _request_arg_truthy(*names),
    build_publication_summary_source_metadata=lambda publication, summary: (
        _build_publication_summary_source_metadata(publication, summary)
    ),
)


register_dashboard_routes(
    app,
    ensure_scheduler=lambda: ensure_embedded_scheduler(),
    is_test_context=lambda: "pytest" in sys.modules or bool(os.environ.get("PYTEST_CURRENT_TEST")),
    build_overview_response=lambda payload, publication, scheduler_state: annotate_scheduler_payload(settings, _attach_dashboard_overview_source_contract(
        _build_dashboard_overview_response(
            payload,
            publication,
            scheduler_state,
        )
    )),
    load_overview_payload_for_test=lambda: _load_dashboard_overview_payload_for_pytest(),
    load_overview_publication_for_test=lambda: _load_dashboard_overview_publication_for_pytest(),
    load_scheduler_state_snapshot=lambda: _load_scheduler_state_snapshot_cached(),
    build_overview_cached=lambda: annotate_scheduler_payload(settings, _build_dashboard_overview_cached()),
    build_latest_dashboard_detail_response=lambda **kwargs: dashboard_latest_detail(**kwargs),
)


register_tracker_routes(
    app,
    ensure_scheduler=lambda: ensure_embedded_scheduler(),
    load_current_published_report=lambda: _load_current_published_report_cached(),
    load_latest_report=lambda: _load_latest_report_cached(),
    load_latest_report_snapshot=lambda: _load_latest_report_snapshot_cached(),
    load_manager_latest_report=lambda: manager.latest_report(),
    request_arg_truthy=lambda *names: _request_arg_truthy(*names),
    build_tracker_current_response=lambda payload, audit_read_only=False: _build_tracker_current_response(
        payload,
        audit_read_only=audit_read_only,
    ),
    build_tracker_current_compact_response=lambda payload: _build_tracker_current_compact_response(payload),
    normalize_tracker_valuation_codes=lambda codes: _normalize_tracker_valuation_codes(codes),
    get_tracker_valuation_max_request_codes=lambda: _get_tracker_valuation_max_request_codes(),
    fetch_tracker_valuations=lambda codes, **kwargs: _fetch_tracker_valuations(codes, **kwargs),
    tracker_valuation_fetch_error=TrackerValuationFetchError,
    valuation_cache_ttl_seconds=_VALUATION_CACHE_TTL_SECONDS,
    valuation_missing_cache_ttl_seconds=_VALUATION_MISSING_CACHE_TTL_SECONDS,
)


register_recommendations_routes(
    app,
    ensure_scheduler=lambda: ensure_embedded_scheduler(),
    is_test_context=lambda: "pytest" in sys.modules or bool(os.environ.get("PYTEST_CURRENT_TEST")),
    load_recommendations_payload_for_test=lambda: _load_recommendations_payload_for_pytest(),
    load_current_published_report=lambda: _load_current_published_report_cached(),
    load_latest_report=lambda: _load_latest_report_cached(),
    load_latest_report_snapshot=lambda: _load_latest_report_snapshot_cached(),
    load_manager_latest_report=lambda: manager.latest_report(),
    build_recommendations_summary_response=lambda payload: _build_recommendations_summary_response(payload),
    build_recommendations_detail_response=lambda payload, detail_group="", audit_read_only=False: _build_recommendations_detail_response(
        payload,
        detail_group,
        audit_read_only=audit_read_only,
    ),
    build_recommendations_export_workbook=lambda payload: _build_recommendations_export_workbook(payload),
)


register_favorites_routes(
    app,
    ensure_scheduler=lambda: ensure_embedded_scheduler(),
    load_manager_latest_report=lambda: _load_current_published_report_cached(),
    favorite_query_history_snapshot=lambda: _favorite_query_history_snapshot(),
    save_favorite_query_history=lambda codes, **kwargs: _save_favorite_query_history(codes, **kwargs),
    normalize_favorite_history_entry_type=lambda value, **kwargs: _normalize_favorite_history_entry_type(
        value,
        **kwargs,
    ),
    normalize_code_text=lambda value: _normalize_code_text(value),
    request_arg_truthy=lambda *names: _request_arg_truthy(*names),
    is_truthy_text=lambda value: _is_truthy_text(value),
    build_favorite_fund_scores_response=lambda payload, codes, **kwargs: _build_favorite_fund_scores_response(
        payload,
        codes,
        **kwargs,
    ),
    tracker_valuation_fetch_error=TrackerValuationFetchError,
    favorite_fund_query_max_codes=FAVORITE_FUND_QUERY_MAX_CODES,
    favorite_metric_detail_max_codes=_FAVORITE_METRIC_DETAIL_MAX_CODES,
    favorite_metric_detail_fields=[
        _FAVORITE_PERIODIC_WEEKLY_DETAIL_KEY,
        _FAVORITE_PERIODIC_MONTHLY_DETAIL_KEY,
        _FAVORITE_METRIC_DETAIL_ANNUAL_BREAKDOWN_KEY,
        _FAVORITE_METRIC_DETAIL_ANNUAL_VOLATILITY_KEY,
    ],
    load_cached_favorite_metric_detail_payload=lambda codes: _load_cached_favorite_metric_detail_payload(codes),
    build_favorite_metric_detail_source_basis=lambda codes, **kwargs: _build_favorite_metric_detail_source_basis(
        codes,
        **kwargs,
    ),
    build_favorite_metric_detail_source_contract=lambda source_basis: _build_favorite_metric_detail_source_contract(
        source_basis,
    ),
)


def _apply_dashboard_sample_policy(
    payload: dict[str, Any],
    *,
    sample_limit: Any = None,
    include_full_samples: bool = False,
) -> dict[str, Any]:
    if include_full_samples or sample_limit is not None:
        return dashboard_payloads.reduce_dashboard_sample_payload(
            payload,
            sample_limit=(
                dashboard_payloads.DEFAULT_DASHBOARD_SAMPLE_LIMIT
                if sample_limit is None
                else sample_limit
            ),
            include_full_samples=include_full_samples,
        )
    return payload


def dashboard_latest_detail(
    *,
    compact: bool = False,
    detail_scope: str = "diagnostic_summary",
    dashboard_sample_limit: Any = None,
    dashboard_full_samples: bool = False,
) -> Response:
    ensure_embedded_scheduler()
    in_pytest = "pytest" in sys.modules or os.environ.get("PYTEST_CURRENT_TEST")

    def _dashboard_latest_json_response(payload: dict[str, Any]) -> Response:
        if payload.get("error") == "暂无报告":
            return jsonify(payload), 404
        sampled_payload = _apply_dashboard_sample_policy(
            payload,
            sample_limit=dashboard_sample_limit,
            include_full_samples=dashboard_full_samples,
        )
        return jsonify(annotate_scheduler_payload(settings,
            _attach_dashboard_latest_source_contract(
                sampled_payload,
                detail_scope=detail_scope,
                compact=compact,
                sample_limit=dashboard_sample_limit,
                include_full_samples=dashboard_full_samples,
            )
        ))

    if compact:
        if in_pytest:
            publication = _load_dashboard_publication_for_pytest()
            payload = dashboard_payloads.build_latest_dashboard_response(
                _load_dashboard_payload_for_pytest(),
                publication,
                _load_scheduler_state_snapshot_cached(),
                settings=settings,
                manual_operation_mode=MANUAL_OPERATION_MODE,
                normalize_portfolio_groups=_normalize_portfolio_groups,
                build_portfolio_group_brief=_build_portfolio_group_brief,
                build_database_source_audit=build_database_source_audit,
                load_database_overview=_load_database_overview,
                load_database_integrity_summary=_load_database_integrity_summary,
                load_nav_lag_summary=_load_nav_lag_summary,
                load_risk_lag_summary=_load_risk_lag_summary,
                load_risk_sync_summary=_load_risk_sync_summary,
                load_risk_coverage_gap_summary=_load_risk_coverage_gap_summary,
                load_bond_universe_snapshot=_load_bond_universe_snapshot,
                load_exclusion_reason_summary=_load_exclusion_reason_summary,
                normalize_coverage_layers=_normalize_coverage_layers,
                build_coverage_layers_from_snapshots=_build_coverage_layers_from_snapshots,
                build_publish_funnel_summary=_build_publish_funnel_summary,
                build_scheduler_overview_summary=lambda state: _build_dashboard_scheduler_lightweight_overview(
                    state,
                    publication=publication,
                    include_recent_failure_recovery=False,
                    allow_runtime_digest_fallback=False,
                ),
                build_scheduler_overview_api_summary=_build_scheduler_overview_api_summary,
                build_scheduler_chain_audit=_build_scheduler_chain_audit,
                build_automation_autonomy_summary=_build_automation_autonomy_summary,
                merge_live_long_term_priority_blocked_summary=None,
                read_excel_cached=_read_excel_cached,
                read_tracker_workbook_sheet=_read_tracker_workbook_sheet,
                compact=True,
            )
        else:
            payload = _build_latest_dashboard_compact_cached()
        return _dashboard_latest_json_response(payload)

    if detail_scope in {"reports", "task_summary"}:
        if in_pytest:
            if _pytest_prefers_scoped_detail_cache():
                payload = _build_latest_dashboard_scoped_detail_cached(detail_scope)
                return _dashboard_latest_json_response(payload)
            payload = dashboard_payloads.build_latest_dashboard_response(
                _load_dashboard_payload_for_pytest(),
                _load_dashboard_publication_for_pytest(),
                _load_scheduler_state_snapshot_cached(),
                settings=settings,
                manual_operation_mode=MANUAL_OPERATION_MODE,
                normalize_portfolio_groups=_normalize_portfolio_groups,
                build_portfolio_group_brief=_build_portfolio_group_brief,
                build_database_source_audit=build_database_source_audit,
                load_database_overview=_load_database_overview,
                load_database_integrity_summary=_load_database_integrity_summary,
                load_nav_lag_summary=_load_nav_lag_summary,
                load_risk_lag_summary=_load_risk_lag_summary,
                load_risk_sync_summary=_load_risk_sync_summary,
                load_risk_coverage_gap_summary=_load_risk_coverage_gap_summary,
                load_bond_universe_snapshot=_load_bond_universe_snapshot,
                load_exclusion_reason_summary=_load_exclusion_reason_summary,
                normalize_coverage_layers=_normalize_coverage_layers,
                build_coverage_layers_from_snapshots=_build_coverage_layers_from_snapshots,
                build_publish_funnel_summary=_build_publish_funnel_summary,
                build_scheduler_overview_summary=_build_scheduler_overview_summary,
                build_scheduler_overview_api_summary=_build_scheduler_overview_api_summary,
                build_scheduler_chain_audit=_build_scheduler_chain_audit,
                build_automation_autonomy_summary=_build_automation_autonomy_summary,
                merge_live_long_term_priority_blocked_summary=None,
                read_excel_cached=_read_excel_cached,
                read_tracker_workbook_sheet=_read_tracker_workbook_sheet,
                compact=False,
                detail_scope=detail_scope,
            )
        else:
            payload = _build_latest_dashboard_scoped_detail_cached(detail_scope)
        return _dashboard_latest_json_response(payload)

    if detail_scope in {"diagnostic_summary", "full", "audit_full"}:
        if in_pytest:
            if _pytest_prefers_scoped_detail_cache():
                payload = _build_latest_dashboard_scoped_detail_cached(detail_scope)
                return _dashboard_latest_json_response(payload)
            scheduler_overview_builder = _build_scheduler_overview_summary
            scoped_merge = _merge_live_long_term_priority_blocked_summary
            payload_loader = _load_dashboard_payload_for_pytest
            publication_loader = _load_dashboard_publication_for_pytest
            if detail_scope == "diagnostic_summary":
                scheduler_overview_builder = lambda state: _build_dashboard_scheduler_lightweight_overview(
                    state,
                    publication=_load_dashboard_publication_for_pytest(),
                )
            else:
                payload_loader = _load_dashboard_detail_payload_for_pytest
                publication_loader = _load_dashboard_detail_publication_for_pytest
            selected_payload = payload_loader()
            selected_publication = publication_loader()
            risk_lag_loader = (
                (lambda: _load_default_risk_lag_summary(selected_payload))
                if detail_scope == "diagnostic_summary"
                else _load_risk_lag_summary
            )
            payload = dashboard_payloads.build_latest_dashboard_response(
                selected_payload,
                selected_publication,
                _load_scheduler_state_snapshot_cached(),
                settings=settings,
                manual_operation_mode=MANUAL_OPERATION_MODE,
                normalize_portfolio_groups=_normalize_portfolio_groups,
                build_portfolio_group_brief=_build_portfolio_group_brief,
                build_database_source_audit=build_database_source_audit,
                load_database_overview=_load_database_overview,
                load_database_integrity_summary=_load_database_integrity_summary,
                load_nav_lag_summary=_load_nav_lag_summary,
                load_risk_lag_summary=risk_lag_loader,
                load_risk_sync_summary=_load_risk_sync_summary,
                load_risk_coverage_gap_summary=_load_risk_coverage_gap_summary,
                load_bond_universe_snapshot=_load_bond_universe_snapshot,
                load_exclusion_reason_summary=_load_exclusion_reason_summary,
                normalize_coverage_layers=_normalize_coverage_layers,
                build_coverage_layers_from_snapshots=_build_coverage_layers_from_snapshots,
                build_publish_funnel_summary=_build_publish_funnel_summary,
                build_scheduler_overview_summary=scheduler_overview_builder,
                build_scheduler_overview_api_summary=_build_scheduler_overview_api_summary,
                build_scheduler_chain_audit=_build_scheduler_chain_audit,
                build_automation_autonomy_summary=_build_automation_autonomy_summary,
                merge_live_long_term_priority_blocked_summary=scoped_merge,
                read_excel_cached=_read_excel_cached,
                read_tracker_workbook_sheet=_read_tracker_workbook_sheet,
                compact=False,
                detail_scope=detail_scope,
            )
        else:
            payload = _build_latest_dashboard_scoped_detail_cached(detail_scope)
        return _dashboard_latest_json_response(payload)

    payload = _load_latest_report_cached()
    if not payload:
        return jsonify({"error": "暂无报告"}), 404

    scheduler_state = _load_scheduler_state_snapshot_cached()
    publication = _load_publication_registry_cached()
    return _dashboard_latest_json_response(
        dashboard_payloads.build_latest_dashboard_response(
            payload,
            publication,
            scheduler_state,
            settings=settings,
            manual_operation_mode=MANUAL_OPERATION_MODE,
            normalize_portfolio_groups=_normalize_portfolio_groups,
            build_portfolio_group_brief=_build_portfolio_group_brief,
            build_database_source_audit=build_database_source_audit,
            load_database_overview=_load_database_overview,
            load_database_integrity_summary=_load_database_integrity_summary,
            load_nav_lag_summary=_load_nav_lag_summary,
            load_risk_lag_summary=_load_risk_lag_summary,
            load_risk_sync_summary=_load_risk_sync_summary,
            load_risk_coverage_gap_summary=_load_risk_coverage_gap_summary,
            load_bond_universe_snapshot=_load_bond_universe_snapshot,
            load_exclusion_reason_summary=_load_exclusion_reason_summary,
            normalize_coverage_layers=_normalize_coverage_layers,
            build_coverage_layers_from_snapshots=_build_coverage_layers_from_snapshots,
            build_publish_funnel_summary=_build_publish_funnel_summary,
            build_scheduler_overview_summary=_build_scheduler_overview_summary,
            build_scheduler_overview_api_summary=_build_scheduler_overview_api_summary,
            build_scheduler_chain_audit=_build_scheduler_chain_audit,
            build_automation_autonomy_summary=_build_automation_autonomy_summary,
            merge_live_long_term_priority_blocked_summary=_merge_live_long_term_priority_blocked_summary,
            read_excel_cached=_read_excel_cached,
            read_tracker_workbook_sheet=_read_tracker_workbook_sheet,
            compact=False,
            detail_scope=detail_scope,
        )
    )


if __name__ == "__main__":
    ensure_embedded_scheduler()
    app.run(host=settings.web_host, port=settings.web_port, debug=False)
