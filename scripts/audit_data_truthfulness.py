#!/usr/bin/env python3
from __future__ import annotations

import argparse
import ast
import csv
import hashlib
import json
import math
import re
import sqlite3
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Sequence

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
CORE_ROOT = PROJECT_ROOT / "core"
if str(CORE_ROOT) not in sys.path:
    sys.path.insert(0, str(CORE_ROOT))

from backend.config import Settings, load_settings  # noqa: E402
from core.utils.core_csv_storage import open_csv_artifact
from backend.task_runtime import SCHEDULER_EXECUTION_ROW_SQL
from backend.task_audit_storage import (
    artifact_exists, is_legacy_task_audit_path, is_maintenance_audit_path, original_artifact_size, read_task_audit,
    read_artifact_text_tail, read_original_artifact_bytes, write_task_audit,
)
from backend.scheduler_lineage import (
    daily_screening_evidence, is_busy_skip_without_screening, is_failed_without_completed_screening,
)
from backend.annual_return_gate_shadow import load_incomplete_annual_shadow_terminal
from backend.artifact_paths import project_relative_artifact_path, resolve_project_artifact_path
from backend.database_freshness_summary import build_database_freshness_summary  # noqa: E402
from backend.effective_nav import (  # noqa: E402
    _latest_risk_rowid_predicate,
    _risk_metric_ready_predicate,
    load_effective_nav_snapshot,
)
from backend.metric_units import DECIMAL_RATIO_PERCENT_FIELDS, metric_unit_metadata  # noqa: E402
from backend.publication_status import build_publication_status_summary  # noqa: E402
from backend.publication_transaction import (  # noqa: E402
    PublicationIntegrityError,
    load_publication_head,
    resolve_committed_path,
)
from backend.risk_metric_contract import risk_metric_formula_contract  # noqa: E402
from backend.collection_log_summary import classify_collection_log_severity  # noqa: E402
from backend.weekly_warning_contract import (  # noqa: E402
    PERSISTED_STAGE_TRUTH_POLICY,
    UNRESOLVED_STAGE_STATUS,
    unresolved_stage_detail_errors,
)
from backend.risk_coverage_semantics import build_risk_coverage_threshold_semantics  # noqa: E402
from utils.project_runtime_config import load_runtime_config  # noqa: E402
from scripts.audit_runtime_utils import build_audit_artifact_path, load_json  # noqa: E402
from analyzers.risk_analyzer import RiskAnalyzer  # noqa: E402
from database.screening_pick_payloads import PickPayloadError, decode_snapshot  # noqa: E402
from utils import build_nav_snapshot_signature  # noqa: E402
from utils.period_return_snapshot import (  # noqa: E402
    RETURN_PROVENANCE_FIELDS, has_dated_period_snapshot, period_collection_blocking_count,
)
from utils.manager_collection_summary import (  # noqa: E402
    manager_collection_source_errors,
    parse_manager_collection_summary,
)
from utils.detail_collection_summary import (  # noqa: E402
    detail_collection_source_errors,
    parse_detail_collection_summary,
)
from bond_fund_screening.tasks.subprocess_logging import classify_stderr_signals  # noqa: E402


SCRIPT_NAME = "audit_data_truthfulness"
SCHEMA_VERSION = 1
SOURCE_SCAN_KEYWORDS = (
    "mock",
    "fake",
    "dummy",
    "fabricat",
    "placeholder",
    "模拟",
    "伪造",
    "编造",
    "假数据",
    "示例数据",
)
SOURCE_SCAN_ALLOWED_HINTS = (
    "unittest.mock",
    "pytest",
    "shadow",
    "模拟仅用于审计",
    "不代表已建议",
    "不伪造",
    "避免伪造",
)
SOURCE_SCAN_DIRS = ("backend", "core", "scripts", "frontend")
SOURCE_SCAN_SUFFIXES = {".py", ".js", ".html", ".css"}
SOURCE_SCAN_EXCLUDED_DIRS = {
    "__pycache__",
    ".pytest_cache",
    ".venv",
    "data",
    "output",
    "reports",
    "logs",
}
SOURCE_SCAN_EXCLUDED_DIRS_SORTED = tuple(sorted(SOURCE_SCAN_EXCLUDED_DIRS))
LIVE_API_ENDPOINTS = (
    "/api/info",
    "/api/settings/apis",
    "/api/health/summary",
    "/api/bootstrap/summary",
    "/api/status/summary",
    "/api/scheduler/status",
    "/api/dashboard/overview",
    "/api/dashboard/latest?sample_limit=0",
    "/api/dashboard/latest/reports?sample_limit=0",
    "/api/reports/latest",
    "/api/publication/summary",
    "/api/scheduler/status/detail?sample_limit=0",
)
LIVE_API_REQUIRED_TIANTIAN_ENDPOINTS = (
    "/fundMNRank",
    "/fundSearch",
    "/fundMNHisNetList",
    "/fundMNDetailInformation",
    "/fundMNMangerList",
    "/fundMSNMangerInfo",
    "/fundMNPeriodIncrease",
    "/fundRankDiagram",
)
LIVE_API_REQUIRED_AUTOSTOCK_ENDPOINTS = (
    "/fund/all",
    "/fund",
    "/fund/detail",
    "/fund/detail/list",
)
AUTOMATION_SCHEDULED_REFRESH_TASK_NAME = "scheduled_daily_bond_refresh"
AUTOMATION_INTRADAY_REFRESH_TASK_NAME = "scheduled_intraday_bond_refresh"
AUTOMATION_WEEKLY_MAINTENANCE_TASK_NAME = "scheduled_weekly_bond_maintenance"
AUTOMATION_REQUIRED_SUBPROCESS_TASKS = (
    "daily-screening-master",
    "long-1year",
    "long-2year",
    "long-3year",
    "long-4year",
    "long-5year",
    "任务1: 长期筛选",
    "任务2: 短期筛选",
    "short-1week",
    "short-1month",
    "short-3month",
    "short-6month",
    "任务3: 最终筛选",
    "任务4: 累积跟踪",
)
AUTOMATION_OPTIONAL_SUBPROCESS_TASKS = (
    "annual-return-gate-shadow",
)
AUTOMATION_REQUIRED_REFRESH_SUBPROCESS_TASKS = (
    "collect-funds",
    "collect-nav",
    "calculate-risk",
)
AUTOMATION_REQUIRED_INTRADAY_SUBPROCESS_TASKS = ("collect-nav", "calculate-risk")
AUTOMATION_SUCCESS_STATUSES = {"success", "succeeded", "ok"}
AUTOMATION_DEGRADED_STATUSES = {"degraded", "warning", "warn"}
AUTOMATION_SUBPROCESS_INDEX_PATTERN = re.compile(
    r"subprocess_index=(?P<path>\S+)\s+count=(?P<count>\d+)\s+failed=(?P<failed>\d+)"
)
ANSI_ESCAPE_PATTERN = re.compile(r"\x1b\[[0-9;]*m")
COLLECT_FUNDS_SUMMARY_PATTERN = re.compile(r"bond基金列表采集完成:\s*(?P<count>[\d,]+)\s*只")
COLLECT_NAV_SUMMARY_PATTERN = re.compile(r"增量更新完成:\s*(?P<count>[\d,]+)\s*条净值记录")
COLLECT_DETAILS_SUMMARY_PATTERN = re.compile(r"基金详情采集完成:\s*(?P<count>[\d,]+)\s*只基金")
COLLECT_MANAGERS_SUMMARY_PATTERN = re.compile(r"基金经理采集完成:\s*(?P<count>[\d,]+)\s*位经理")
CALCULATE_RISK_SUMMARY_PATTERN = re.compile(
    r"风险指标同步完成:\s*目标基金(?P<target>[\d,]+)只,\s*生成(?P<periods>[\d,]+)个周期"
)
CALCULATE_RISK_FULL_SUMMARY_PATTERN = re.compile(
    r"风险指标计算完成:\s*(?P<periods>[\d,]+)个指标"
)
API_SOURCE_EVIDENCE_MARKER = "api_source_evidence="
TASK_COMPLETION_STAT_PATTERN = re.compile(
    r"总任务数:\s*(?P<total>[\d,]+).*?"
    r"已处理:\s*(?P<processed>[\d,]+).*?"
    r"成功:\s*(?P<success>[\d,]+).*?"
    r"跳过:\s*(?P<skipped>[\d,]+).*?"
    r"失败:\s*(?P<failed>[\d,]+).*?"
    r"成功率:\s*(?P<success_rate>[\d.]+)%",
    re.DOTALL,
)
COLLECTOR_STAT_PATTERN = re.compile(
    r"(?P<label>FundCollector|NavCollector)\s+统计.*?"
    r"总数:\s*(?P<total>[\d,]+).*?"
    r"成功:\s*(?P<success>[\d,]+).*?"
    r"失败:\s*(?P<failed>[\d,]+).*?"
    r"跳过:\s*(?P<skipped>[\d,]+).*?"
    r"成功率:\s*(?P<success_rate>[\d.]+)%",
    re.DOTALL,
)
STDERR_BLOCKER_SIGNAL_PATTERN = re.compile(
    r"\bTraceback\b|\bCRITICAL\b|\[CRITICAL\]|\bERROR\b|\[ERROR\]|\bException\b|异常|错误",
    re.IGNORECASE,
)
STDERR_WARNING_SIGNAL_PATTERN = re.compile(r"\bWARNING\b|\[WARNING\]|\bWARN\b|\[WARN\]|警告", re.IGNORECASE)
STDERR_FAILURE_SIGNAL_PATTERN = re.compile(r"\bfailed\b|失败", re.IGNORECASE)
STDERR_ZERO_FAILURE_PATTERN = re.compile(
    r"(?:failed|failed_count|records_failed|total_failed|失败)\s*(?:[:：=]|为)\s*0\b",
    re.IGNORECASE,
)
FRONTEND_FIELD_LINEAGE_FIELDS = (
    "基金代码",
    "基金名称",
    "正式投资评分",
    "正式榜分组",
    "债基风格标签",
    "持有期状态",
    "项目质量信心",
    "AutoStock原始校验状态",
    "基金规模(亿)",
    "费率合计(%)",
    "最新净值日期",
    "1年-年化收益(%)",
    "1年-夏普比率",
    "1年-最大回撤(%)",
    "1年-波动率",
    "实时估值",
    "实时估值时间",
    "实时估算净值",
    "平台成立以来收益年化(%)",
    "成立以来年度收益明细",
    "数据来源",
)
FRONTEND_FIELD_PAGE_REFERENCES = {
    "recommendations.js": (
        "基金代码",
        "基金名称",
        "正式投资评分",
        "正式榜分组",
        "债基风格标签",
        "基金规模(亿)",
        "费率合计(%)",
        "最新净值日期",
        "1年-年化收益(%)",
        "1年-夏普比率",
        "1年-最大回撤(%)",
        "1年-波动率",
        "平台成立以来收益年化(%)",
        "成立以来年度收益明细",
        "实时估值",
        "实时估值时间",
        "实时估算净值",
    ),
    "tracker.js": (
        "基金代码",
        "基金名称",
        "正式投资评分",
        "正式榜分组",
        "债基风格标签",
        "基金规模(亿)",
        "最新净值日期",
        "1年-年化收益(%)",
        "1年-夏普比率",
        "1年-最大回撤(%)",
        "1年-波动率",
        "平台成立以来收益年化(%)",
        "实时估值",
        "实时估值时间",
        "实时估算净值",
    ),
    "favorites.js": (
        "基金代码",
        "基金名称",
        "正式投资评分",
        "正式榜分组",
        "债基风格标签",
        "基金规模(亿)",
        "费率合计(%)",
        "最新净值日期",
        "1年-年化收益(%)",
        "1年-夏普比率",
        "1年-最大回撤(%)",
        "1年-波动率",
        "平台成立以来收益年化(%)",
        "实时估值",
        "实时估值时间",
        "实时估算净值",
    ),
}
FILTERED_CSV_REQUIRED_FIELDS = (
    "基金代码",
    "基金名称",
    "正式投资评分",
    "正式榜分组",
    "基金规模(亿)",
    "最新净值日期",
    "1年-年化收益(%)",
    "1年-夏普比率",
    "1年-最大回撤(%)",
    "1年-波动率",
)
TRACKER_SIDECAR_REQUIRED_FIELDS = (
    "基金代码",
    "基金名称",
    "正式投资评分",
    "正式榜分组",
    "债基风格标签",
    "持有期状态",
    "项目质量信心",
    "AutoStock原始校验状态",
    "长期稳定性标签",
    "费率合计(%)",
)
RECOMMENDATIONS_SIDECAR_REQUIRED_FIELDS = (
    "基金代码",
    "基金名称",
    "正式投资评分",
    "正式榜分组",
    "债基风格标签",
    "基金规模(亿)",
    "费率合计(%)",
    "最新净值日期",
    "1年-年化收益(%)",
    "1年-夏普比率",
    "1年-最大回撤(%)",
    "1年-波动率",
    "平台成立以来收益年化(%)",
    "成立以来年度收益明细",
    "数据来源",
)
TRACKER_DERIVED_REQUIRED_MAPS = (
    "data_source_by_code",
    "annualized_return_by_code",
    "annual_return_breakdown_by_code",
)
TRACKER_DERIVED_CACHE_BASIS_REQUIRED_KEYS = (
    "schema_version",
    "run_id",
    "generated_at",
    "database_file",
    "tracker_rows_signature",
)
RECOMMENDATIONS_CACHE_BASIS_REQUIRED_KEYS = (
    "schema_version",
    "run_id",
    "generated_at",
    "latest_report_file",
    "database_file",
    "project_tracker_excel",
)
RECOMMENDATIONS_FROZEN_BASIS_REQUIRED_KEYS = (
    "schema_version",
    "basis_kind",
    "run_id",
    "generated_at",
    "revision",
    "project_tracker_excel",
    "project_tracker_current_detail_json",
    "publication_basis_json",
)
RECOMMENDATIONS_DETAIL_SOURCE_CONTRACT_EXPECTED = {
    "source": "run_local_recommendations_detail",
    "truth_policy": "real_api_database_runtime_artifact_and_publication_registry_only",
    "missing_value_policy": "preserve_missing_semantics",
    "business_value_synthesis": "forbidden",
}
RECOMMENDATIONS_DETAIL_SIDECAR_SOURCE_CONTRACT_EXPECTED = {
    "source": "run_local_recommendations_detail",
    "truth_policy": "real_api_database_runtime_artifact_and_publication_registry_only",
    "missing_value_policy": "preserve_missing_semantics",
    "business_value_synthesis": "forbidden",
}
RECOMMENDATIONS_DETAIL_LEGACY_TRUTH_POLICY = "real_api_database_or_runtime_artifact_only"
RECOMMENDATIONS_SUMMARY_SOURCE_BASIS_REQUIRED_KEYS = (
    "schema_version",
    "source",
    "payload_mode",
    "run_id",
    "generated_at",
    "latest_report_file",
    "publication_registry_file",
    "current_published_run_id",
    "final_count",
    "portfolio_group_count",
    "portfolio_fund_count",
    "summary_rows_omitted",
    "portfolio_group_funds_omitted",
    "report_artifacts",
    "missing_value_policy",
    "business_value_synthesis",
    "digest",
)
RECOMMENDATIONS_SUMMARY_SOURCE_CONTRACT_EXPECTED = {
    "source": "recommendations_summary_current_published_report_artifact",
    "truth_policy": "real_publication_registry_and_report_artifact_summary_only",
    "missing_value_policy": "preserve_missing_semantics",
    "business_value_synthesis": "forbidden",
}
REPORTS_LATEST_SOURCE_BASIS_REQUIRED_KEYS = (
    "schema_version",
    "source",
    "payload_source",
    "run_id",
    "generated_at",
    "final_count",
    "latest_report_file",
    "publication_registry_file",
    "latest_canonical_run_id",
    "current_published_run_id",
    "report_json_artifact_token",
    "digest",
)
PUBLICATION_SUMMARY_SOURCE_BASIS_REQUIRED_KEYS = (
    "schema_version",
    "source",
    "payload_mode",
    "latest_canonical_run_id",
    "current_published_run_id",
    "latest_canonical_report_path",
    "canonical_run_count",
    "archived_superseded_run_count",
    "canonical_run_ids",
    "latest_canonical_run_present",
    "current_published_run_present",
    "publication_registry_file",
    "latest_report_file",
    "latest_canonical_report_token",
    "missing_value_policy",
    "business_value_synthesis",
    "digest",
)
DASHBOARD_LATEST_SOURCE_BASIS_REQUIRED_KEYS = (
    "schema_version",
    "source",
    "payload_mode",
    "detail_scope",
    "compact",
    "sample_limit",
    "include_full_samples",
    "sample_policy_mode",
    "run_id",
    "latest_report_run_id",
    "generated_at",
    "latest_report_file",
    "publication_registry_file",
    "scheduler_state_file",
    "database_file",
    "latest_canonical_run_id",
    "current_published_run_id",
    "latest_successful_run_id",
    "publication_status",
    "publication_status_source",
    "daily_screening_run_id",
    "final_count",
    "scope_contract_digest",
    "dashboard_build_profile_schema_version",
    "dashboard_build_detail_scope",
    "dashboard_build_step_count",
    "missing_value_policy",
    "business_value_synthesis",
    "digest",
)
EXCEL_PREVIEW_SOURCE_BASIS_REQUIRED_KEYS = (
    "schema_version",
    "source",
    "run_id",
    "generated_at",
    "requested_path",
    "resolved_path",
    "file_token",
    "file_content_token",
    "artifact_key",
    "artifact_path",
    "artifact_declared_in_latest_report",
    "latest_report_file",
    "publication_registry_file",
    "latest_canonical_run_id",
    "current_published_run_id",
    "sheet_count",
    "workbook_sheet_count",
    "sheet_summaries",
    "digest",
)
SCHEDULER_DETAIL_SOURCE_BASIS_REQUIRED_KEYS = (
    "schema_version",
    "source",
    "detail_scope",
    "sample_limit",
    "include_full_samples",
    "run_id",
    "latest_report_run_id",
    "latest_report_file",
    "publication_registry_file",
    "scheduler_state_file",
    "database_file",
    "risk_coverage_gap_audit",
    "latest_canonical_run_id",
    "current_published_run_id",
    "daily_screening_run_id",
    "automation_lineage_source",
    "automation_lineage_truth_policy",
    "digest",
)
DASHBOARD_OVERVIEW_SOURCE_BASIS_REQUIRED_KEYS = (
    "schema_version",
    "source",
    "payload_mode",
    "detail_scope",
    "run_id",
    "generated_at",
    "latest_report_file",
    "publication_registry_file",
    "scheduler_state_file",
    "database_file",
    "nav_history_maintenance",
    "nav_lag_audit",
    "risk_sync_audit",
    "risk_coverage_gap_audit",
    "latest_canonical_run_id",
    "current_published_run_id",
    "final_count",
    "stable_bond_final_count",
    "enhanced_bond_final_count",
    "dynamic_final_count",
    "scope_contract_digest",
    "cache_basis_schema_version",
    "cache_basis_digest",
    "missing_value_policy",
    "business_value_synthesis",
    "digest",
)
LIGHTWEIGHT_STATUS_SOURCE_BASIS_REQUIRED_KEYS = (
    "schema_version",
    "source",
    "payload_mode",
    "run_id",
    "latest_report_run_id",
    "latest_report_file",
    "publication_registry_file",
    "scheduler_state_file",
    "database_file",
    "latest_canonical_run_id",
    "current_published_run_id",
    "missing_value_policy",
    "business_value_synthesis",
    "digest",
)
REPORTS_LATEST_SOURCE_CONTRACT_EXPECTED = {
    "source": "reports_latest_current_published_or_latest_report",
    "truth_policy": "real_publication_registry_and_report_artifact_only",
    "missing_value_policy": "preserve_missing_semantics",
    "business_value_synthesis": "forbidden",
    "official_publication_truth": "publication_registry_and_current_published_report",
}
PUBLICATION_SUMMARY_SOURCE_CONTRACT_EXPECTED = {
    "source": "publication_summary_publication_registry_artifact",
    "truth_policy": "real_publication_registry_artifact_only",
    "missing_value_policy": "preserve_missing_semantics",
    "business_value_synthesis": "forbidden",
    "official_publication_truth": "publication_registry",
}
DASHBOARD_LATEST_SOURCE_CONTRACT_EXPECTED = {
    "source": "dashboard_latest_current_published_report_scheduler_database_artifacts",
    "truth_policy": "real_publication_registry_report_artifact_scheduler_state_database_and_runtime_artifacts_only",
    "missing_value_policy": "preserve_missing_semantics",
    "business_value_synthesis": "forbidden",
    "official_publication_truth": "publication_registry_and_current_published_report",
    "publication_status_source": "publication_registry_runtime_overlay",
}
EXCEL_PREVIEW_SOURCE_CONTRACT_EXPECTED = {
    "source": "project_excel_artifact_preview",
    "truth_policy": "real_project_excel_artifact_only",
    "missing_value_policy": "preserve_missing_semantics",
    "business_value_synthesis": "forbidden",
    "official_publication_truth": "false_preview_only",
}
SCHEDULER_DETAIL_SOURCE_CONTRACT_EXPECTED = {
    "source": "scheduler_status_detail_runtime_state",
    "truth_policy": "real_scheduler_state_publication_registry_database_and_runtime_artifacts_only",
    "missing_value_policy": "preserve_missing_semantics",
    "business_value_synthesis": "forbidden",
    "official_publication_truth": "false_scheduler_diagnostics_only",
}
DASHBOARD_OVERVIEW_SOURCE_CONTRACT_EXPECTED = {
    "source": "dashboard_overview_current_published_report_scheduler_database_artifacts",
    "truth_policy": "real_publication_registry_report_artifact_scheduler_state_database_and_runtime_artifacts_only",
    "missing_value_policy": "preserve_missing_semantics",
    "business_value_synthesis": "forbidden",
    "official_publication_truth": "publication_registry_and_current_published_report",
}
LIGHTWEIGHT_STATUS_SOURCE_CONTRACT_EXPECTED = {
    "truth_policy": "real_runtime_state_publication_registry_database_and_runtime_artifacts_only",
    "missing_value_policy": "preserve_missing_semantics",
    "business_value_synthesis": "forbidden",
}
FAVORITES_SCORE_SOURCE_CONTRACT_EXPECTED = {
    "source": "favorite_fund_score_runtime_query",
    "truth_policy": "real_report_artifact_sqlite_database_and_optional_provider_snapshot_only",
    "missing_value_policy": "preserve_missing_semantics",
    "business_value_synthesis": "forbidden",
    "official_publication_truth": "false_favorites_diagnostics_only",
}
FAVORITE_METRIC_DETAILS_SOURCE_CONTRACT_EXPECTED = {
    "source": "favorite_metric_details_runtime_query",
    "truth_policy": "real_sqlite_nav_annual_return_and_local_calculation_only",
    "missing_value_policy": "preserve_missing_semantics",
    "business_value_synthesis": "forbidden",
    "official_publication_truth": "false_favorites_detail_diagnostics_only",
}
TRACKER_CURRENT_SOURCE_CONTRACT_EXPECTED = {
    "source": "tracker_current_published_report_tracker_artifact",
    "truth_policy": "real_publication_registry_report_artifact_tracker_artifact_sqlite_and_local_calculation_only",
    "missing_value_policy": "preserve_missing_semantics",
    "business_value_synthesis": "forbidden",
    "official_publication_truth": "current_published_report_tracker_artifact",
}
TRACKER_VALUATIONS_SOURCE_CONTRACT_EXPECTED = {
    "source": "tiantian_fund_valuation_last_or_short_lived_memory_cache",
    "truth_policy": "real_tiantian_fund_valuation_last_response_or_short_lived_memory_cache_only",
    "missing_value_policy": "preserve_missing_semantics",
    "business_value_synthesis": "forbidden",
    "official_publication_truth": "false_intraday_valuation_observation_only",
}
FAVORITES_SCORE_SOURCE_BASIS_REQUIRED_KEYS = (
    "schema_version",
    "source",
    "run_id",
    "generated_at",
    "latest_report_file",
    "database_file",
    "requested_codes",
    "response_codes",
    "payload_mode",
    "save_history",
    "digest",
)
FAVORITE_METRIC_DETAILS_SOURCE_BASIS_REQUIRED_KEYS = (
    "schema_version",
    "source",
    "requested_codes",
    "effective_codes",
    "returned_codes",
    "database_file",
    "cache_ttl_seconds",
    "detail_fields",
    "source_tables",
    "missing_value_policy",
    "digest",
)
TRACKER_CURRENT_SOURCE_BASIS_REQUIRED_KEYS = (
    "schema_version",
    "source",
    "payload_mode",
    "dataset_source",
    "run_id",
    "generated_at",
    "latest_report_file",
    "publication_registry_file",
    "database_file",
    "report_artifacts",
    "audit_read_only",
    "row_count",
    "returned_count",
    "detail_diagnostics",
    "missing_value_policy",
    "business_value_synthesis",
    "digest",
)
TRACKER_VALUATIONS_SOURCE_BASIS_REQUIRED_KEYS = (
    "schema_version",
    "requested_codes",
    "cached_codes",
    "uncached_codes",
    "source",
    "upstream_endpoint_family",
    "cache_scope",
    "cache_ttl_seconds",
    "max_request_codes",
    "effective_batch_size",
    "quota_reserved_for_automation",
    "display_fields",
    "source_fields",
    "missing_value_policy",
    "digest",
)
REAL_FUND_DIRECTORY_SOURCE_LABELS = {
    "fundMNRank",
    "fundSearch",
    "autostock_directory_fallback",
}
GOVERNED_BLANK_DATA_SOURCE_FLAGS = {
    "api_unreconciled_historical_residual",
}
ANNUAL_RANK_PERCENTILE_TOLERANCE = 0.000001
ANNUAL_RETURN_LONG_TERM_WINDOW_YEARS = 5
RANK_HISTORY_WINDOW_DAYS = 365
RANK_HISTORY_MIN_OBSERVATIONS = 20
RANK_HISTORY_STALE_DAYS_DEFAULT = 30
LIVE_FIELD_ENDPOINTS = (
    {
        "key": "tracker_detail",
        "method": "GET",
        "endpoint": "/api/tracker/current?detail=1&audit_read_only=1",
        "row_path": ("rows",),
        "expected_diagnostics": {
            "path": ("payload_scope", "tracker_current_detail_diagnostics"),
            "audit_read_only": True,
            "write_status_field": "derived_sidecar_write_status",
            "allowed_write_statuses": ("hit", "audit_read_only"),
            "forbidden_write_statuses": ("written",),
        },
        "required_fields": (
            "基金代码",
            "基金名称",
            "正式投资评分",
            "正式榜分组",
            "债基风格标签",
            "基金规模(亿)",
            "最新净值日期",
            "1年-年化收益(%)",
            "1年-夏普比率",
            "1年-最大回撤(%)",
            "1年-波动率",
            "平台成立以来收益年化(%)",
        ),
    },
    {
        "key": "recommendations_detail",
        "method": "GET",
        "endpoint": "/api/recommendations/latest/detail?audit_read_only=1",
        "row_path": ("rows",),
        "expected_diagnostics": {
            "path": ("payload_scope", "recommendations_detail_diagnostics"),
            "audit_read_only": True,
            "write_status_field": "sidecar_write_status",
            "allowed_write_statuses": ("hit", "audit_read_only"),
            "forbidden_write_statuses": ("written",),
        },
        "required_fields": (
            "基金代码",
            "基金名称",
            "正式投资评分",
            "正式榜分组",
            "债基风格标签",
            "基金规模(亿)",
            "费率合计(%)",
            "最新净值日期",
            "1年-年化收益(%)",
            "1年-夏普比率",
            "1年-最大回撤(%)",
            "1年-波动率",
            "平台成立以来收益年化(%)",
        ),
    },
    {
        "key": "recommendations_summary",
        "method": "GET",
        "endpoint": "/api/recommendations/latest",
        "row_path": ("rows",),
        "required_fields": (),
    },
    {
        "key": "favorites_score",
        "method": "POST",
        "endpoint": "/api/favorites/score",
        "post_payload": {
            "save_history": False,
            "response_mode": "compact",
            "include_recommendation_pool": False,
            "include_autostock": False,
        },
        "row_path": ("rows",),
        "expected_payload_scope": "compact",
        "expected_detail_included": False,
        "expected_omitted_detail_fields": (
            "成立以来年度收益明细",
            "__style_metric_references",
        ),
        "required_fields": (
            "基金代码",
            "基金名称",
            "正式投资评分",
            "正式榜分组",
            "债基风格标签",
            "基金规模(亿)",
            "费率合计(%)",
            "最新净值日期",
            "1年-年化收益(%)",
            "1年-夏普比率",
            "1年-最大回撤(%)",
            "1年-波动率",
            "平台成立以来收益年化(%)",
        ),
    },
    {
        "key": "tracker_valuations",
        "method": "GET",
        "endpoint": "/api/tracker/valuations?audit_read_only=1",
        "use_sample_codes_query": True,
        "row_path": ("valuation_rows",),
        "expected_status": "audit_read_only",
        "required_fields": (
            "fcode",
            "available",
            "source",
            "reason",
            "valuation_date",
            "estimated_nav",
            "estimated_change_pct",
            "missing_value_policy",
        ),
        "expected_detail_fields": (
            "实时估值",
            "实时估值时间",
            "实时估算净值",
        ),
    },
    {
        "key": "favorite_metric_details",
        "method": "POST",
        "endpoint": "/api/favorites/metric-details",
        "post_payload": {},
        "row_path": ("__unused_rows__",),
        "expected_detail_fields": (
            "weekly_recent",
            "monthly_ytd",
            "since_inception_annual_return_breakdown",
            "since_inception_annual_return_volatility_pct",
        ),
    },
)
SOURCE_TRUTH_DOMAINS = (
    {
        "key": "fund_directory",
        "label": "基金列表与基础分类",
        "upstream_sources": (
            "Tiantian /fundMNRank FundType=31/32",
            "Tiantian /fundSearch exact-code fallback",
            "AutoStock /fund/all directory reconciliation",
        ),
        "truth_rule": "Only real upstream API payloads may create/update fund identity, type, buyable flag and source fields.",
        "tables": (
            {
                "name": "funds",
                "required": True,
                "min_row_count": 1,
                "required_fields": (
                    "fcode",
                    "shortname",
                    "ftype",
                    "type_code",
                    "type_category",
                    "is_buyable",
                    "purchase_status",
                    "data_source",
                    "data_source_governance_flag",
                    "last_updated_at",
                ),
                "identity_fields": ("fcode", "shortname"),
            },
        ),
        "required_artifacts": (),
    },
    {
        "key": "nav_and_return_snapshots",
        "label": "净值与收益快照",
        "upstream_sources": (
            "Tiantian /fundMNRank latest NAV and period returns",
            "Tiantian /fundSearch exact-code fallback latest NAV",
            "Tiantian /fundMNHisNetList historical NAV",
        ),
        "truth_rule": "NAV and return values must come from upstream API payloads or existing SQLite history; missing NAV remains missing.",
        "tables": (
            {
                "name": "fund_returns",
                "required": True,
                "min_row_count": 1,
                "required_fields": (
                    "fcode",
                    "latest_nav",
                    "latest_nav_date",
                    "daily_growth",
                    "return_1w",
                    "return_1m",
                    "return_3m",
                    "return_6m",
                    "return_1y",
                    "return_ytd",
                    "last_updated_at",
                ),
                "identity_fields": ("fcode", "latest_nav_date"),
            },
            {
                "name": "fund_nav_history",
                "required": True,
                "min_row_count": 1,
                "required_fields": ("fcode", "nav_date", "unit_nav", "accum_nav", "daily_growth"),
                "identity_fields": ("fcode", "nav_date"),
            },
        ),
        "required_artifacts": (),
    },
    {
        "key": "risk_metrics",
        "label": "风险指标",
        "upstream_sources": (
            "Local RiskAnalyzer calculation from real fund_nav_history rows",
        ),
        "truth_rule": "Risk metrics are reproducible local calculations over real NAV history, not upstream literals or frontend estimates.",
        "tables": (
            {
                "name": "fund_risk_metrics",
                "required": True,
                "min_row_count": 1,
                "required_fields": (
                    "fcode",
                    "period",
                    "volatility",
                    "max_drawdown",
                    "annual_return",
                    "sharpe_ratio",
                    "sortino_ratio",
                    "calmar_ratio",
                    "var_95",
                    "var_99",
                    "positive_ratio",
                    "data_points",
                    "start_date",
                    "end_date",
                    "calculated_at",
                    "nav_snapshot_signature",
                ),
                "identity_fields": ("fcode", "period", "end_date"),
            },
        ),
        "required_artifacts": (
            {
                "key_path": "artifacts.core.risk_lag_audit_json",
                "required": True,
                "artifact_type": "risk_lag_audit",
            },
            {
                "key_path": "artifacts.core.risk_sync_audit_json",
                "required": True,
                "artifact_type": "risk_sync_audit",
            },
        ),
    },
    {
        "key": "holding_period_gate",
        "label": "持有期门禁",
        "upstream_sources": (
            "Tiantian /fundMNDetailInformation CYCLE/PRSVPERIOD/PRSVDATE/PRSVTYPE/BUYTIME",
            "Tiantian /fundMNRank CYCLE/FEATURE/BFUNDTYPE supplemental fields",
            "Local structured/name parsing for holding_days and source labels",
        ),
        "truth_rule": "Formal publication must keep structured holding_days > threshold excluded; unknown/no-signal states are not fabricated passes.",
        "tables": (
            {
                "name": "funds",
                "required": True,
                "min_row_count": 1,
                "required_fields": (
                    "fcode",
                    "has_holding_period",
                    "holding_days",
                    "holding_keywords",
                    "holding_period_source",
                    "cycle",
                    "preserve_period",
                    "preserve_date",
                    "preserve_type",
                    "buy_time",
                ),
                "identity_fields": ("fcode", "holding_days"),
            },
        ),
        "required_artifacts": (
            {
                "key_path": "artifacts.excluded_csv",
                "required": True,
                "artifact_type": "holding_period_excluded_csv",
            },
        ),
    },
    {
        "key": "trade_fee_scale",
        "label": "申购状态、费率与规模",
        "upstream_sources": (
            "Tiantian /fundMNRank BUY/ISBUY/ENDNAV",
            "Tiantian /fundMNDetailInformation MGREXP/TRUSTEXP/SALESEXP/SGZT/SHZT/ENDNAV/ZJZC/BUYTIME",
        ),
        "truth_rule": "Trade status only accepts explicit upstream status fields; BUYTIME is minimum purchase info, not a fake suspension status.",
        "tables": (
            {
                "name": "funds",
                "required": True,
                "min_row_count": 1,
                "required_fields": (
                    "fcode",
                    "scale_billion",
                    "net_asset",
                    "scale_report_date",
                    "is_buyable",
                    "purchase_status",
                    "redemption_status",
                    "min_purchase",
                    "mgr_fee_rate",
                    "trust_fee_rate",
                    "sales_fee_rate",
                    "last_updated_at",
                ),
                "identity_fields": ("fcode", "scale_billion", "purchase_status"),
            },
            {
                "name": "fund_scale",
                "required": False,
                "min_row_count": 0,
                "required_fields": ("fcode", "report_date", "total_asset", "net_asset", "total_share"),
                "identity_fields": ("fcode", "report_date"),
            },
        ),
        "required_artifacts": (),
    },
    {
        "key": "annual_returns_and_rank",
        "label": "年度收益与同类排名",
        "upstream_sources": (
            "Tiantian /fundMNPeriodIncrease RANGE=n annual returns",
            "Tiantian /fundRankDiagram rank history",
        ),
        "truth_rule": "Annual return and rank history are upstream-derived tables; rank/percentile calculations preserve missing upstream data.",
        "tables": (
            {
                "name": "fund_annual_returns",
                "required": True,
                "min_row_count": 1,
                "required_fields": ("fcode", "year", "annual_return", "rank_in_category", "category_count", "percentile"),
                "identity_fields": ("fcode", "year"),
            },
            {
                "name": "fund_rank_history",
                "required": True,
                "min_row_count": 1,
                "required_fields": ("fcode", "rank_date", "period", "rank_position", "category_total", "percentile"),
                "identity_fields": ("fcode", "rank_date", "period"),
            },
        ),
        "required_artifacts": (),
    },
    {
        "key": "manager_info",
        "label": "基金经理信息",
        "upstream_sources": (
            "Tiantian /fundMNMangerList fund manager list",
            "Tiantian /fundMSNMangerInfo manager detail",
        ),
        "truth_rule": "Manager ids/names and manager detail metrics must come from upstream manager APIs or remain missing.",
        "tables": (
            {
                "name": "funds",
                "required": True,
                "min_row_count": 1,
                "required_fields": ("fcode", "manager_ids", "manager_names"),
                "identity_fields": ("fcode", "manager_ids", "manager_names"),
            },
            {
                "name": "fund_managers",
                "required": True,
                "min_row_count": 1,
                "required_fields": (
                    "mgr_id",
                    "name",
                    "total_days",
                    "manage_scale",
                    "fund_count",
                    "annual_return",
                    "max_profit",
                    "max_drawdown",
                    "score_overall",
                    "last_updated_at",
                ),
                "identity_fields": ("mgr_id", "name"),
            },
        ),
        "required_artifacts": (),
    },
    {
        "key": "autostock_validation_and_valuation",
        "label": "AutoStock 交叉验证与估值补充",
        "upstream_sources": (
            "AutoStock /fund/all bond directory reconciliation",
            "AutoStock /fund and /fund/detail/list batch validation",
            "AutoStock valuation fields used only as realtime/validation supplements",
        ),
        "truth_rule": "AutoStock may validate or supplement current valuation state, but frontend must not render realtime valuation as formal NAV/risk truth.",
        "tables": (
            {
                "name": "autostock_nav_tail_fill",
                "required": False,
                "min_row_count": 0,
                "required_fields": (
                    "fcode",
                    "local_latest_nav",
                    "local_latest_nav_date",
                    "autostock_latest_nav",
                    "autostock_latest_nav_date",
                    "autostock_estimated_nav",
                    "autostock_valuation_date",
                    "autostock_found",
                    "review_status",
                    "source_name",
                    "batch_run_id",
                    "updated_at",
                ),
                "identity_fields": ("fcode", "batch_run_id", "review_status"),
            },
        ),
        "required_artifacts": (
            {
                "key_path": "artifacts.core.autostock_validation_audit_file",
                "required": True,
                "artifact_type": "autostock_validation_audit",
            },
        ),
    },
    {
        "key": "screening_and_publication",
        "label": "筛选决策与正式发布",
        "upstream_sources": (
            "Local screening engine over real SQLite/API-derived fields",
            "Run-local final decision ledger and publication registry",
        ),
        "truth_rule": "Formal list truth is the canonical run and publication registry, not archived exports or frontend tables.",
        "tables": (
            {
                "name": "screening_decisions",
                "required": True,
                "min_row_count": 1,
                "required_fields": ("run_id", "fcode", "step_id", "step_name", "passed", "reason_code", "metrics_json"),
                "identity_fields": ("run_id", "fcode", "step_id"),
            },
            {
                "name": "screening_picks",
                "required": True,
                "min_row_count": 1,
                "required_fields": ("run_id", "fcode", "rank", "fund_type", "period_years", "combo_score", "snapshot_json"),
                "identity_fields": ("run_id", "fcode", "rank"),
            },
        ),
        "required_artifacts": (
            {
                "key_path": "artifacts.filtered_csv",
                "required": True,
                "artifact_type": "formal_filtered_csv",
            },
            {
                "key_path": "artifacts.core.final_screening_decision_ledger_file",
                "required": True,
                "artifact_type": "final_screening_decision_ledger",
            },
            {
                "key_path": "artifacts.core.bond_mainlist_quality_audit_json_file",
                "required": True,
                "artifact_type": "bond_mainlist_quality_audit",
            },
        ),
    },
)


def _safe_int(value: Any) -> int:
    if value in (None, ""):
        return 0
    try:
        return int(value)
    except (TypeError, ValueError):
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return 0


def _safe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_optional_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        try:
            return int(float(text))
        except (TypeError, ValueError):
            return None


def _format_number_text(value: Any, digits: int = 4) -> str:
    if value in (None, ""):
        return ""
    try:
        number = float(value)
    except (TypeError, ValueError):
        return str(value).strip()
    normalized = f"{number:.{digits}f}"
    if digits > 0:
        normalized = normalized.rstrip("0").rstrip(".")
    return normalized if normalized != "-0" else "0"


def _safe_ratio(numerator: int, denominator: int) -> float | None:
    if numerator <= 0 or denominator <= 0:
        return None
    return round(float(numerator) / float(denominator), 6)


def _lag_days(left_date: str, right_date: str) -> int | None:
    left_text = str(left_date or "").strip()
    right_text = str(right_date or "").strip()
    if not left_text or not right_text:
        return None
    try:
        left = datetime.strptime(left_text, "%Y-%m-%d").date()
        right = datetime.strptime(right_text, "%Y-%m-%d").date()
    except ValueError:
        return None
    return max(0, (left - right).days)


def _classify_status(blockers: Sequence[str], warnings: Sequence[str]) -> str:
    if blockers:
        return "needs_review"
    if warnings:
        return "watch"
    return "ok"


def _read_json(path: Path) -> dict[str, Any]:
    try:
        if path.name.endswith((".json.gz", ".json.legacy.gz")) or is_legacy_task_audit_path(path) or is_maintenance_audit_path(path):
            return read_task_audit(path)
        return load_json(path)
    except (OSError, ValueError):
        return {}


def _resolve_project_path(settings: Settings, value: Any) -> Path | None:
    text = str(value or "").strip()
    if not text:
        return None
    path = Path(text)
    if not path.is_absolute():
        path = settings.project_root / path
    return path


def _clean_business_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() in {"", "nan", "none", "null"}:
        return ""
    return text


def _normalize_fund_code(value: Any) -> str:
    text = _clean_business_text(value)
    if not text:
        return ""
    if re.fullmatch(r"\d+(?:\.0+)?", text):
        try:
            text = str(int(float(text)))
        except (TypeError, ValueError):
            pass
    digits = re.sub(r"\D", "", text)
    if digits:
        return digits.zfill(6)[-6:]
    return text


def _read_csv_header_and_sample_codes(path: Path, sample_size: int = 5) -> tuple[list[str], list[str], int]:
    try:
        with open_csv_artifact(path) as fh:
            reader = csv.DictReader(fh)
            fieldnames = list(reader.fieldnames or [])
            codes: list[str] = []
            row_count = 0
            for row in reader:
                row_count += 1
                code = _normalize_fund_code(row.get("基金代码"))
                if code and len(codes) < sample_size:
                    codes.append(code)
    except OSError:
        return [], [], 0
    return fieldnames, codes, row_count


def _read_csv_codes(path: Path, code_field: str = "基金代码") -> list[str]:
    codes: list[str] = []
    seen: set[str] = set()
    try:
        with open_csv_artifact(path) as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                code = _normalize_fund_code(row.get(code_field))
                if code and code not in seen:
                    seen.add(code)
                    codes.append(code)
    except OSError:
        return []
    return codes


def _read_csv_rows_by_code(path: Path, code_field: str = "基金代码") -> dict[str, dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    try:
        with open_csv_artifact(path) as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                code = _normalize_fund_code(row.get(code_field))
                if code and code not in rows:
                    rows[code] = dict(row)
    except OSError:
        return {}
    return rows


def _normalize_business_date(value: Any) -> str:
    text = _clean_business_text(value)
    if not text:
        return ""
    try:
        return datetime.fromisoformat(text[:10]).date().isoformat()
    except ValueError:
        return ""


def _safe_date_gt(left: Any, right: Any) -> bool:
    left_text = _normalize_business_date(left)
    right_text = _normalize_business_date(right)
    return bool(left_text and right_text and left_text > right_text)


def _safe_date_le(left: Any, right: Any) -> bool:
    left_text = _normalize_business_date(left)
    right_text = _normalize_business_date(right)
    return bool(left_text and right_text and left_text <= right_text)


def _load_tracker_sidecar_rows_by_code(settings: Settings, latest: dict[str, Any]) -> dict[str, dict[str, Any]]:
    artifacts = latest.get("artifacts") if isinstance(latest.get("artifacts"), dict) else {}
    tracker_path = _resolve_project_path(settings, artifacts.get("project_tracker_current_detail_json"))
    if not tracker_path or not tracker_path.exists():
        tracker_path = _resolve_project_path(settings, artifacts.get("run_local_project_tracker_current_detail_json"))
    if not tracker_path or not tracker_path.exists():
        return {}
    payload = _read_json(tracker_path)
    rows: dict[str, dict[str, Any]] = {}
    for item in payload.get("rows") or []:
        if not isinstance(item, dict):
            continue
        code = _normalize_fund_code(item.get("基金代码") or item.get("fcode") or item.get("code"))
        if code and code not in rows:
            rows[code] = dict(item)
    return rows


def _formal_artifact_snapshot_alignment(
    *,
    settings: Settings,
    latest: dict[str, Any],
    final_codes: Sequence[str],
    conn: sqlite3.Connection,
) -> dict[str, Any]:
    """Check whether published artifacts still expose the canonical run snapshot during live writes."""
    clean_final_codes = [_normalize_fund_code(code) for code in final_codes if _normalize_fund_code(code)]
    artifacts = latest.get("artifacts") if isinstance(latest.get("artifacts"), dict) else {}
    filtered_csv_path = _resolve_project_path(settings, artifacts.get("filtered_csv"))
    source_rows: dict[str, dict[str, dict[str, Any]]] = {}
    if filtered_csv_path and filtered_csv_path.exists():
        source_rows["filtered_csv"] = _read_csv_rows_by_code(filtered_csv_path)
    tracker_rows = _load_tracker_sidecar_rows_by_code(settings, latest)
    if tracker_rows:
        source_rows["project_tracker_current_detail_json"] = tracker_rows

    db_snapshot: dict[str, dict[str, Any]] = {}
    risk_table_available = _table_exists(conn, "fund_risk_metrics")
    risk_ready_predicate = _risk_metric_ready_predicate(conn, "risk") if risk_table_available else ""
    latest_risk_rowid_predicate = (
        _latest_risk_rowid_predicate("risk", "latest") if risk_table_available else ""
    )
    for offset in range(0, len(clean_final_codes), 500):
        chunk = clean_final_codes[offset : offset + 500]
        if not chunk:
            continue
        values_clause = ",".join(["(?)"] * len(chunk))
        risk_cte = (
            f"""
            risk_1y AS (
                SELECT
                    risk.fcode,
                    risk.end_date AS risk_1y_end_date
                FROM fund_risk_metrics risk
                WHERE risk.period = '1y'
                  AND COALESCE(TRIM(risk.end_date), '') <> ''
                  AND {risk_ready_predicate}
                  AND {latest_risk_rowid_predicate}
            )
            """
            if risk_table_available
            else ""
        )
        risk_select = "risk_1y.risk_1y_end_date" if risk_table_available else "NULL AS risk_1y_end_date"
        risk_join = "LEFT JOIN risk_1y ON risk_1y.fcode = c.fcode" if risk_table_available else ""
        cte_separator = "," if risk_table_available else ""
        rows = conn.execute(
            f"""
            WITH final_codes(fcode) AS (
                VALUES {values_clause}
            ),
            nav_latest AS (
                SELECT fcode, MAX(nav_date) AS hist_latest_nav_date
                FROM fund_nav_history
                GROUP BY fcode
            )
            {cte_separator}
            {risk_cte}
            SELECT
                c.fcode,
                r.latest_nav_date,
                h.hist_latest_nav_date,
                {risk_select}
            FROM final_codes c
            LEFT JOIN fund_returns r ON r.fcode = c.fcode
            LEFT JOIN nav_latest h ON h.fcode = c.fcode
            {risk_join}
            """,
            chunk,
        ).fetchall()
        for row in rows:
            db_snapshot[str(row["fcode"])] = {key: row[key] for key in row.keys()}

    source_summaries: dict[str, Any] = {}
    clean_for_nav = False
    clean_for_risk = False
    for source_name, rows_by_code in source_rows.items():
        missing_row_codes: list[str] = []
        missing_display_date_codes: list[str] = []
        missing_risk_snapshot_codes: list[str] = []
        display_date_after_history_codes: list[str] = []
        display_date_after_risk_codes: list[str] = []
        display_date_matches_unbacked_returns_codes: list[str] = []
        canonical_lagged_codes: list[str] = []

        for code in clean_final_codes:
            row = rows_by_code.get(code)
            if row is None:
                missing_row_codes.append(code)
                continue
            display_date = _normalize_business_date(row.get("最新净值日期"))
            if not display_date:
                missing_display_date_codes.append(code)
                continue
            db_row = db_snapshot.get(code) or {}
            latest_nav_date = _normalize_business_date(db_row.get("latest_nav_date"))
            hist_latest_nav_date = _normalize_business_date(db_row.get("hist_latest_nav_date"))
            risk_1y_end_date = _normalize_business_date(db_row.get("risk_1y_end_date"))
            if risk_table_available and not risk_1y_end_date:
                missing_risk_snapshot_codes.append(code)
            current_returns_unbacked = bool(
                latest_nav_date
                and hist_latest_nav_date
                and latest_nav_date > hist_latest_nav_date
            )
            if hist_latest_nav_date and _safe_date_gt(display_date, hist_latest_nav_date):
                display_date_after_history_codes.append(code)
            if risk_1y_end_date and _safe_date_gt(display_date, risk_1y_end_date):
                display_date_after_risk_codes.append(code)
            if current_returns_unbacked and display_date == latest_nav_date:
                display_date_matches_unbacked_returns_codes.append(code)
            if current_returns_unbacked and _safe_date_le(display_date, hist_latest_nav_date):
                canonical_lagged_codes.append(code)

        source_clean_for_nav = bool(
            clean_final_codes
            and not missing_row_codes
            and not missing_display_date_codes
            and not display_date_after_history_codes
            and not display_date_matches_unbacked_returns_codes
        )
        source_clean_for_risk = bool(
            source_clean_for_nav
            and not missing_risk_snapshot_codes
            and not display_date_after_risk_codes
        )
        clean_for_nav = clean_for_nav or source_clean_for_nav
        clean_for_risk = clean_for_risk or source_clean_for_risk
        source_summaries[source_name] = {
            "row_count": len(rows_by_code),
            "final_code_count": len(clean_final_codes),
            "clean_for_nav_during_live_write": source_clean_for_nav,
            "clean_for_risk_during_live_write": source_clean_for_risk,
            "missing_row_count": len(missing_row_codes),
            "missing_display_date_count": len(missing_display_date_codes),
            "missing_ready_risk_snapshot_count": len(missing_risk_snapshot_codes),
            "display_date_after_history_count": len(display_date_after_history_codes),
            "display_date_after_risk_count": len(display_date_after_risk_codes),
            "display_date_matches_unbacked_returns_count": len(display_date_matches_unbacked_returns_codes),
            "canonical_lagged_count": len(canonical_lagged_codes),
            "samples": {
                "missing_row_codes": missing_row_codes[:10],
                "missing_display_date_codes": missing_display_date_codes[:10],
                "missing_ready_risk_snapshot_codes": missing_risk_snapshot_codes[:10],
                "display_date_after_history_codes": display_date_after_history_codes[:10],
                "display_date_after_risk_codes": display_date_after_risk_codes[:10],
                "display_date_matches_unbacked_returns_codes": display_date_matches_unbacked_returns_codes[:10],
                "canonical_lagged_codes": canonical_lagged_codes[:10],
            },
        }

    return {
        "available": bool(source_summaries),
        "final_code_count": len(clean_final_codes),
        "clean_for_nav_during_live_write": clean_for_nav,
        "clean_for_risk_during_live_write": clean_for_risk,
        "sources": source_summaries,
        "truth_note": (
            "During weekly maintenance live writes, formal publication truth may remain pinned to canonical run "
            "artifacts. This check only downgrades current-DB snapshot mismatches when at least one formal artifact "
            "covers every final code and does not expose an unbacked current fund_returns latest_nav_date."
        ),
    }


def _summarize_field_presence(rows: list[dict[str, Any]], fields: Sequence[str]) -> dict[str, Any]:
    row_count = len(rows)
    field_summary: dict[str, Any] = {}
    all_row_keys = {str(key) for row in rows for key in row.keys()}
    for field in fields:
        present_count = sum(1 for row in rows if field in row)
        non_empty_count = sum(1 for row in rows if _clean_business_text(row.get(field)))
        field_summary[field] = {
            "present": field in all_row_keys,
            "present_count": int(present_count),
            "non_empty_count": int(non_empty_count),
            "missing_or_empty_count": int(row_count - non_empty_count),
        }
    return {
        "row_count": int(row_count),
        "fields": field_summary,
    }


def _summarize_frontend_cache_basis_contract(
    cache_basis: Any,
    *,
    artifact_key: str,
    required_keys: Sequence[str],
    expected_run_id: str,
    allowed_run_ids: Sequence[str] | None = None,
    nullable_keys: Sequence[str] = (),
) -> tuple[dict[str, Any], list[str]]:
    def _has_required_cache_value(value: Any) -> bool:
        if isinstance(value, bool):
            return True
        if isinstance(value, (int, float)):
            return True
        if isinstance(value, (list, tuple, set, dict)):
            return bool(value)
        return bool(_clean_business_text(value))

    blockers: list[str] = []
    if not isinstance(cache_basis, dict) or not cache_basis:
        return (
            {
                "present": False,
                "schema_version": None,
                "run_id": None,
                "required_keys": list(required_keys),
                "missing_required_keys": list(required_keys),
            },
            [f"{artifact_key}_cache_basis_missing"],
        )

    missing_required_keys = [
        key
        for key in required_keys
        if key not in cache_basis or (
            not _has_required_cache_value(cache_basis.get(key))
            and not (key in nullable_keys and cache_basis[key] is None)
        )
    ]
    if missing_required_keys:
        blockers.append(
            f"{artifact_key}_cache_basis_required_keys_missing:"
            + ",".join(missing_required_keys)
        )

    observed_run_id = str(cache_basis.get("run_id") or "").strip()
    accepted_run_ids = {
        str(item or "").strip()
        for item in [expected_run_id, *(allowed_run_ids or [])]
        if str(item or "").strip()
    }
    if expected_run_id and observed_run_id not in accepted_run_ids:
        blockers.append(f"{artifact_key}_cache_basis_run_id_mismatch")

    return (
        {
            "present": True,
            "schema_version": cache_basis.get("schema_version"),
            "run_id": observed_run_id or None,
            "accepted_run_ids": sorted(accepted_run_ids),
            "required_keys": list(required_keys),
            "missing_required_keys": missing_required_keys,
            "generated_at": cache_basis.get("generated_at"),
        },
        blockers,
    )


def _summarize_recommendations_cache_basis_contract(
    cache_basis: Any,
    *,
    settings: Settings,
    report: dict[str, Any],
    artifact_key: str,
) -> tuple[dict[str, Any], list[str]]:
    """Audit dependencies of the report protocol, never of a claimed cache kind.

    Frozen values depend on committed files, not on the current mutable database.
    A cache cannot opt into the frozen contract without a matching publication.
    """
    control = report.get("publication_control") or {}
    frozen = bool(control.get("commit_protocol"))
    required_keys = (
        RECOMMENDATIONS_FROZEN_BASIS_REQUIRED_KEYS
        if frozen else RECOMMENDATIONS_CACHE_BASIS_REQUIRED_KEYS
    )
    run_id = str(report.get("run_id") or "").strip()
    summary, blockers = _summarize_frontend_cache_basis_contract(
        cache_basis, artifact_key=artifact_key,
        required_keys=required_keys, expected_run_id=run_id,
    )
    basis = cache_basis if isinstance(cache_basis, dict) else {}
    summary["basis_kind"] = basis.get("basis_kind")
    summary["expected_basis_kind"] = "immutable_publication_revision" if frozen else "legacy_mutable_inputs"
    prefix = f"{artifact_key}_cache_basis_"
    if not frozen:
        if basis.get("basis_kind"):
            blockers.append(prefix + "unexpected_basis_kind")
        return summary, blockers

    if control.get("commit_protocol") != 1:
        blockers.append(prefix + "unsupported_publication_protocol")
    if basis.get("basis_kind") != "immutable_publication_revision":
        blockers.append(prefix + "basis_kind_mismatch")
    # Both frozen sidecar versions use the same committed-file dependency keys.
    if basis.get("schema_version") not in (2, 3):
        blockers.append(prefix + "schema_version_mismatch")
    for key, expected in (
        ("revision", control.get("revision")),
        ("generated_at", report.get("generated_at")),
    ):
        if not expected or basis.get(key) != expected:
            blockers.append(prefix + key + "_mismatch")

    # Verify SHA-256 from the committed descriptor before trusting any token.
    # Loading the head is read-only; no alias recovery or sidecar writes occur.
    try:
        head = load_publication_head(settings)
        entry = (head.get("committed_runs") or {}).get(run_id) or {}
        if (
            head.get("current_published_run_id") != run_id
            or entry.get("revision") != control.get("revision")
            or not entry.get("manifest_path")
        ):
            raise PublicationIntegrityError("frozen_basis_publication_revision_not_current")
        committed_report_path = resolve_committed_path(settings, entry.get("report_path"))
        if _read_json(committed_report_path) != report:
            raise PublicationIntegrityError("frozen_basis_report_not_committed_report")
        revision_dir = committed_report_path.parent.resolve()
        expected_parent = (Path(settings.runs_dir) / run_id / "revisions").resolve()
        if revision_dir.parent != expected_parent or revision_dir.name != control.get("revision"):
            raise PublicationIntegrityError("frozen_basis_revision_directory_mismatch")
        manifest_path = resolve_committed_path(settings, entry["manifest_path"])
        manifest = _read_json(manifest_path)
        if (
            manifest_path.parent.resolve() != revision_dir
            or manifest.get("run_id") != run_id
            or manifest.get("revision") != control.get("revision")
        ):
            raise PublicationIntegrityError("frozen_basis_manifest_revision_mismatch")
    except (OSError, ValueError, TypeError, PublicationIntegrityError) as exc:
        blockers.append(prefix + "publication_evidence_invalid")
        summary["publication_evidence_error"] = str(exc)
        return summary, blockers

    summary["publication_evidence"] = {
        "generation": head["generation"], "run_id": run_id,
        "revision": entry["revision"], "manifest_sha256": entry["manifest_sha256"],
        "report_sha256": entry["report_sha256"],
    }
    artifacts = report.get("artifacts") or {}
    files = manifest["files"]
    evidence: dict[str, Any] = {}
    for key in (
        "project_tracker_excel", "project_tracker_current_detail_json", "publication_basis_json",
    ):
        item: dict[str, Any] = {"path": artifacts.get(key)}
        evidence[key] = item
        try:
            if not artifacts.get(key):
                raise ValueError("frozen_basis_artifact_not_declared")
            path = resolve_committed_path(settings, artifacts[key])
            if not path.resolve().is_relative_to(revision_dir):
                raise ValueError("frozen_basis_artifact_outside_revision")
            contents = path.read_bytes()
            token = f"{len(contents)}:{hashlib.sha1(contents).hexdigest()}"
            digest = hashlib.sha256(contents).hexdigest()
            item.update({
                "content_token": token, "sha256": digest,
                "cache_token_matches": basis.get(key) == token,
                "committed_sha256_matches": files.get(str(path.resolve().relative_to(revision_dir))) == digest,
            })
            if not item["cache_token_matches"]:
                blockers.append(prefix + f"{key}_fingerprint_mismatch")
            if not item["committed_sha256_matches"]:
                blockers.append(prefix + f"{key}_not_bound_to_commit")
        except (OSError, ValueError, PublicationIntegrityError) as exc:
            item["error"] = str(exc)
            blockers.append(prefix + f"{key}_unverifiable")
    summary["frozen_artifacts"] = evidence
    return summary, blockers


def _summarize_recommendations_detail_source_contract(
    payload: Any,
    *,
    artifact_key: str,
    expected_contract: dict[str, str] | None = None,
    allowed_values: dict[str, set[str]] | None = None,
) -> tuple[dict[str, Any], list[str]]:
    expected_values = dict(expected_contract or RECOMMENDATIONS_DETAIL_SOURCE_CONTRACT_EXPECTED)
    allowed_values = allowed_values or {}
    blockers: list[str] = []
    if not isinstance(payload, dict) or not payload:
        return (
            {
                "present": False,
                "expected": expected_values,
                "observed": {},
            },
            [f"{artifact_key}_source_contract_missing"],
        )

    for key, expected in expected_values.items():
        observed = str(payload.get(key) or "").strip()
        accepted = {expected, *(allowed_values.get(key) or set())}
        if observed not in accepted:
            suffix = {
                "source": "source_mismatch",
                "truth_policy": "truth_policy_mismatch",
                "missing_value_policy": "missing_value_policy_mismatch",
                "business_value_synthesis": "synthesis_policy_mismatch",
            }[key]
            blockers.append(f"{artifact_key}_source_contract_{suffix}")

    return (
        {
            "present": True,
            "schema_version": payload.get("schema_version"),
            "expected": expected_values,
            "accepted_values": {
                key: sorted({expected_values[key], *(allowed_values.get(key) or set())})
                for key in expected_values
            },
            "observed": {
                key: payload.get(key)
                for key in expected_values
            },
            "official_publication_truth": payload.get("official_publication_truth"),
            "publication_status_source": payload.get("publication_status_source"),
            "frontend_rendered_truth_allowed": payload.get("frontend_rendered_truth_allowed"),
            "cache_basis_digest": payload.get("cache_basis_digest"),
            "cache_basis_schema_version": payload.get("cache_basis_schema_version"),
        },
        blockers,
    )


def _summarize_live_endpoint_source_contract(
    payload: Any,
    *,
    artifact_key: str,
    expected: dict[str, str],
    source_basis: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], list[str]]:
    blockers: list[str] = []
    if not isinstance(payload, dict) or not payload:
        return (
            {
                "present": False,
                "expected": dict(expected),
                "observed": {},
            },
            [f"{artifact_key}_source_contract_missing"],
        )

    for key, expected_value in expected.items():
        if _clean_live_text(payload.get(key)) != expected_value:
            blockers.append(f"{artifact_key}_source_contract_{key}_mismatch")
    if payload.get("frontend_rendered_truth_allowed") is not False:
        blockers.append(f"{artifact_key}_source_contract_frontend_truth_allowed")
    if _safe_optional_int(payload.get("cache_basis_schema_version")) is None:
        blockers.append(f"{artifact_key}_source_contract_cache_schema_missing")
    basis = source_basis if isinstance(source_basis, dict) else {}
    if basis and _clean_live_text(payload.get("cache_basis_digest")) != _clean_live_text(basis.get("digest")):
        blockers.append(f"{artifact_key}_source_contract_digest_mismatch")

    return (
        {
            "present": True,
            "schema_version": payload.get("schema_version"),
            "expected": dict(expected),
            "observed": {
                key: payload.get(key)
                for key in expected
            },
            "frontend_rendered_truth_allowed": payload.get("frontend_rendered_truth_allowed"),
            "cache_basis_schema_version": payload.get("cache_basis_schema_version"),
        },
        blockers,
    )


def _rows_from_path(payload: dict[str, Any], row_path: Sequence[str]) -> list[dict[str, Any]]:
    current: Any = payload
    for key in row_path:
        if not isinstance(current, dict):
            return []
        current = current.get(key)
    if not isinstance(current, list):
        return []
    return [dict(item) for item in current if isinstance(item, dict)]


def _read_db_stat_token(path: Path) -> str:
    try:
        stat = path.stat()
    except OSError:
        return "missing"
    return f"{int(stat.st_mtime_ns)}:{int(stat.st_size)}"


def _read_file_integrity_token(path: Path | str | None) -> dict[str, Any]:
    if path is None:
        return {
            "path": "",
            "exists": False,
            "mtime_ns": None,
            "size_bytes": None,
            "sha256": None,
            "token": "unresolved",
        }
    path = Path(path)
    try:
        stat = path.stat()
        content = path.read_bytes()
    except OSError:
        return {
            "path": str(path),
            "exists": False,
            "mtime_ns": None,
            "size_bytes": None,
            "sha256": None,
            "token": "missing",
        }
    digest = hashlib.sha256(content).hexdigest()
    return {
        "path": str(path),
        "exists": True,
        "mtime_ns": int(stat.st_mtime_ns),
        "size_bytes": int(stat.st_size),
        "sha256": digest,
        "token": f"{int(stat.st_mtime_ns)}:{int(stat.st_size)}:{digest}",
    }


def _connect_read_only(database_path: Path) -> sqlite3.Connection:
    uri = f"file:{database_path.resolve()}?mode=ro"
    conn = sqlite3.connect(uri, uri=True, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only = ON")
    return conn


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table_name,),
    ).fetchone()
    return bool(row)


def _table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    try:
        rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    except sqlite3.Error:
        return set()
    return {str(row["name"] or "") for row in rows if str(row["name"] or "").strip()}


def _attach_nav_history_archive_for_audit(
    conn: sqlite3.Connection,
    settings: Settings,
    *,
    include_daily_growth: bool = False,
    columns: Sequence[str] = ("fcode", "nav_date"),
) -> tuple[dict[str, Any], Any | None, str]:
    """Return the NAV source SQL that matches the active hot/cold split."""
    selected_columns = tuple(str(column or "").strip() for column in columns if str(column or "").strip())
    if not selected_columns:
        selected_columns = ("fcode", "nav_date")
    safe_columns = [
        column
        for column in selected_columns
        if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", column)
    ]
    if not safe_columns:
        safe_columns = ["fcode", "nav_date"]
    hot_source_sql = f"SELECT {', '.join(safe_columns)} FROM fund_nav_history"
    try:
        from database.nav_history_queries import (  # type: ignore
            attach_nav_history_archive_if_available,
            build_nav_history_union_source_sql,
            detach_nav_history_archive,
            load_latest_nav_history_archive_binding,
            nav_history_archive_binding_matches_database,
        )

        binding = load_latest_nav_history_archive_binding(
            project_root=getattr(settings, "project_root", PROJECT_ROOT),
        )
        if not nav_history_archive_binding_matches_database(
            binding,
            getattr(settings, "database_path", ""),
            project_root=getattr(settings, "project_root", PROJECT_ROOT),
        ):
            binding = {}
        attach = attach_nav_history_archive_if_available(
            conn,
            archive_path=binding.get("archive_path"),
            archive_table_name=binding.get("archive_table_name") or "fund_nav_history_archive",
            archive_cutoff_date=binding.get("archive_cutoff_date") or "",
        )
        if attach.get("attached"):
            return (
                dict(attach),
                detach_nav_history_archive,
                build_nav_history_union_source_sql(
                    attach,
                    include_daily_growth=include_daily_growth,
                    columns=tuple(safe_columns),
                ),
            )
        return dict(attach or {}), None, hot_source_sql
    except Exception as exc:
        return {"attached": False, "reason": f"attach_failed:{exc}"}, None, hot_source_sql


def _table_row_count(conn: sqlite3.Connection, table_name: str) -> int | None:
    try:
        row = conn.execute(f"SELECT COUNT(*) AS row_count FROM {table_name}").fetchone()
    except sqlite3.Error:
        return None
    return _safe_int(row["row_count"] if row else 0)


def _artifact_value_from_key_path(payload: dict[str, Any], key_path: str) -> Any:
    current: Any = payload
    for key in str(key_path or "").split("."):
        if not key:
            continue
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _audit_pick_snapshot_storage(conn: sqlite3.Connection, *, sample_rows: int = 200) -> dict[str, Any]:
    """Bounded storage checks, explicitly not a proof of unsampled history."""
    first, last = conn.execute("SELECT MIN(id),MAX(id) FROM screening_picks").fetchone()
    samples = {}
    if first is not None:
        for sql, parameters in (
            ("SELECT id,snapshot_json FROM screening_picks ORDER BY id LIMIT ?", (sample_rows,)),
            ("SELECT id,snapshot_json FROM screening_picks WHERE id>=? ORDER BY id LIMIT ?",
             ((first + last) // 2, sample_rows)),
            ("SELECT id,snapshot_json FROM screening_picks ORDER BY id DESC LIMIT ?", (sample_rows,)),
        ):
            samples.update((row[0], row[1]) for row in conn.execute(sql, parameters))
    counts = {"text": 0, "compressed_blob": 0, "null": 0, "invalid": 0}
    failures = []
    for row_id, stored in samples.items():
        try:
            decode_snapshot(stored)
            counts["null" if stored is None else "text" if isinstance(stored, str) else "compressed_blob"] += 1
        except PickPayloadError as error:
            counts["invalid"] += 1
            if len(failures) < 10:
                failures.append({"pick_id": row_id, "error": str(error)})
    return {
        "status": "needs_review" if failures else "sampled_storage_valid" if samples else "empty",
        "scope": "deduplicated_first_id_midpoint_id_and_latest_id_windows_only",
        "per_window_limit": sample_rows, "sampled_row_count": len(samples),
        "sampled_ids": sorted(samples), "storage_counts": counts, "failures": failures,
        "all_sampled_payloads_valid": not failures if samples else None,
        "full_history_verified": False,
        "truth_note": "Compressed samples are checked for format, bounded decode, byte length and SHA256; original TEXT/NULL are preserved. No business formula validation is implied.",
    }


def _audit_source_truth_manifest_table(
    conn: sqlite3.Connection,
    table_spec: dict[str, Any],
) -> tuple[dict[str, Any], list[str], list[str]]:
    table_name = str(table_spec.get("name") or "").strip()
    required = bool(table_spec.get("required"))
    min_row_count = _safe_int(table_spec.get("min_row_count"))
    required_fields = tuple(str(item) for item in table_spec.get("required_fields") or ())
    identity_fields = tuple(str(item) for item in table_spec.get("identity_fields") or ())
    blockers: list[str] = []
    warnings: list[str] = []

    exists = bool(table_name and _table_exists(conn, table_name))
    columns: set[str] = set()
    row_count: int | None = None
    missing_fields = list(required_fields)
    if not exists:
        if required:
            blockers.append(f"source_truth_table_missing:{table_name}")
        else:
            warnings.append(f"source_truth_optional_table_missing:{table_name}")
    else:
        columns = _table_columns(conn, table_name)
        row_count = _table_row_count(conn, table_name)
        missing_fields = [field for field in required_fields if field not in columns]
        if missing_fields:
            issue = f"source_truth_table_fields_missing:{table_name}:{','.join(missing_fields)}"
            if required:
                blockers.append(issue)
            else:
                warnings.append(issue)
        if required and row_count is not None and row_count < min_row_count:
            blockers.append(f"source_truth_table_empty:{table_name}")

    storage_check = None
    if table_name == "screening_picks" and {"id", "snapshot_json"} <= columns:
        storage_check = _audit_pick_snapshot_storage(conn)
        if storage_check["status"] == "needs_review":
            blockers.append("source_truth_pick_snapshot_storage_invalid")

    return (
        {
            "name": table_name,
            "required": required,
            "exists": exists,
            "row_count": row_count,
            "min_row_count": min_row_count,
            "required_fields": list(required_fields),
            "missing_required_fields": missing_fields,
            "identity_fields": list(identity_fields),
            **({"snapshot_storage_check": storage_check} if storage_check is not None else {}),
            "truth_note": "Schema and row counts are read from SQLite in read-only mode; values are not generated by the audit.",
        },
        blockers,
        warnings,
    )


def _audit_source_truth_manifest_artifact(
    settings: Settings,
    latest_report: dict[str, Any],
    artifact_spec: dict[str, Any],
) -> tuple[dict[str, Any], list[str], list[str]]:
    key_path = str(artifact_spec.get("key_path") or "").strip()
    required = bool(artifact_spec.get("required"))
    artifact_type = str(artifact_spec.get("artifact_type") or "").strip()
    raw_value = _artifact_value_from_key_path(latest_report, key_path)
    artifact_path = _resolve_project_path(settings, raw_value)
    exists = bool(artifact_path and artifact_path.exists())
    blockers: list[str] = []
    warnings: list[str] = []

    if not exists:
        issue = f"source_truth_artifact_missing:{key_path}"
        if required:
            blockers.append(issue)
        else:
            warnings.append(issue)

    return (
        {
            "key_path": key_path,
            "artifact_type": artifact_type,
            "required": required,
            "configured_value": raw_value,
            "path": str(artifact_path) if artifact_path else "",
            "exists": exists,
        },
        blockers,
        warnings,
    )


def _source_truth_manifest(settings: Settings) -> dict[str, Any]:
    blockers: list[str] = []
    warnings: list[str] = []
    latest = _read_json(settings.latest_report_file)
    run_id = str(latest.get("run_id") or "").strip()
    domain_results: dict[str, Any] = {}
    table_check_total = 0
    artifact_check_total = 0
    required_table_total = 0
    required_artifact_total = 0

    if not settings.database_path.exists():
        return {
            "status": "needs_review",
            "blockers": ["database_missing"],
            "warnings": [],
            "domains": {},
        }

    with _connect_read_only(settings.database_path) as conn:
        for domain in SOURCE_TRUTH_DOMAINS:
            domain_key = str(domain.get("key") or "").strip()
            domain_blockers: list[str] = []
            domain_warnings: list[str] = []
            table_results: list[dict[str, Any]] = []
            artifact_results: list[dict[str, Any]] = []

            for table_spec in domain.get("tables") or ():
                table_check_total += 1
                if bool(table_spec.get("required")):
                    required_table_total += 1
                table_result, table_blockers, table_warnings = _audit_source_truth_manifest_table(
                    conn,
                    dict(table_spec),
                )
                table_results.append(table_result)
                domain_blockers.extend(table_blockers)
                domain_warnings.extend(table_warnings)

            for artifact_spec in domain.get("required_artifacts") or ():
                artifact_check_total += 1
                if bool(artifact_spec.get("required")):
                    required_artifact_total += 1
                artifact_result, artifact_blockers, artifact_warnings = _audit_source_truth_manifest_artifact(
                    settings,
                    latest,
                    dict(artifact_spec),
                )
                artifact_results.append(artifact_result)
                domain_blockers.extend(artifact_blockers)
                domain_warnings.extend(artifact_warnings)

            domain_results[domain_key] = {
                "status": _classify_status(domain_blockers, domain_warnings),
                "label": domain.get("label"),
                "upstream_sources": list(domain.get("upstream_sources") or ()),
                "truth_rule": domain.get("truth_rule"),
                "tables": table_results,
                "artifacts": artifact_results,
                "blockers": domain_blockers,
                "warnings": domain_warnings,
            }
            blockers.extend(f"{domain_key}:{item}" for item in domain_blockers)
            warnings.extend(f"{domain_key}:{item}" for item in domain_warnings)

    return {
        "status": _classify_status(blockers, warnings),
        "blockers": blockers,
        "warnings": warnings,
        "run_id": run_id or None,
        "domain_count": len(SOURCE_TRUTH_DOMAINS),
        "table_check_total": table_check_total,
        "required_table_total": required_table_total,
        "artifact_check_total": artifact_check_total,
        "required_artifact_total": required_artifact_total,
        "domains": domain_results,
        "truth_note": (
            "This manifest documents business data source-of-truth rules and verifies the current SQLite schema, "
            "row presence and canonical run artifacts in read-only mode. It does not call upstream APIs or fabricate values."
        ),
    }


def _query_fund_source_rows_by_codes(
    conn: sqlite3.Connection,
    codes: Sequence[str],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    clean_codes = [code for code in (_normalize_fund_code(item) for item in codes) if code]
    for offset in range(0, len(clean_codes), 500):
        chunk = clean_codes[offset : offset + 500]
        placeholders = ",".join("?" for _ in chunk)
        if not placeholders:
            continue
        try:
            query_rows = conn.execute(
                f"""
                SELECT
                    fcode,
                    COALESCE(data_source, '') AS data_source,
                    COALESCE(data_source_governance_flag, '') AS data_source_governance_flag
                FROM funds
                WHERE fcode IN ({placeholders})
                """,
                chunk,
            ).fetchall()
        except sqlite3.Error:
            return []
        rows.extend(dict(row) for row in query_rows)
    return rows


def _fund_directory_source_quality(settings: Settings) -> dict[str, Any]:
    blockers: list[str] = []
    warnings: list[str] = []
    latest = _read_json(settings.latest_report_file)
    run_id = str(latest.get("run_id") or "").strip()
    final_count = _safe_int(latest.get("final_count"))
    artifacts = latest.get("artifacts") if isinstance(latest.get("artifacts"), dict) else {}
    filtered_csv_path = _resolve_project_path(settings, artifacts.get("filtered_csv"))
    publish_summary = latest.get("project_publish_summary")
    data_source_audit_summary = (
        publish_summary.get("data_source_audit_summary")
        if isinstance(publish_summary, dict)
        else {}
    )
    data_source_audit_summary = (
        dict(data_source_audit_summary)
        if isinstance(data_source_audit_summary, dict)
        else {}
    )

    if not settings.database_path.exists():
        return {
            "status": "needs_review",
            "blockers": ["database_missing"],
            "warnings": [],
            "run_id": run_id or None,
        }

    if not filtered_csv_path or not filtered_csv_path.exists():
        blockers.append("formal_filtered_csv_missing_for_source_quality")
        final_codes: list[str] = []
    else:
        final_codes = _read_csv_codes(filtered_csv_path)
        if final_count and len(final_codes) != final_count:
            blockers.append("formal_filtered_csv_code_count_not_equal_final_count")

    db_summary: dict[str, Any] = {}
    final_summary: dict[str, Any] = {
        "final_code_count": len(final_codes),
        "final_count_from_latest": final_count,
        "final_source_counts": {},
        "final_blank_count": 0,
        "final_fallback_count": 0,
        "final_missing_db_count": 0,
        "final_governed_blank_count": 0,
        "final_unresolved_blank_count": 0,
    }

    with _connect_read_only(settings.database_path) as conn:
        if not _table_exists(conn, "funds"):
            blockers.append("funds_table_missing_for_source_quality")
        else:
            columns = _table_columns(conn, "funds")
            required_columns = {"fcode", "data_source", "data_source_governance_flag", "last_updated_at"}
            missing_columns = sorted(required_columns - columns)
            if missing_columns:
                blockers.append(f"funds_source_quality_fields_missing:{','.join(missing_columns)}")
            else:
                row = conn.execute(
                    """
                    SELECT
                        COUNT(*) AS total,
                        SUM(CASE WHEN TRIM(COALESCE(data_source, '')) = '' THEN 1 ELSE 0 END) AS blank_source_count,
                        SUM(
                            CASE
                                WHEN TRIM(COALESCE(data_source, '')) = ''
                                 AND TRIM(COALESCE(data_source_governance_flag, '')) != ''
                                THEN 1 ELSE 0
                            END
                        ) AS governed_blank_source_count,
                        SUM(
                            CASE
                                WHEN TRIM(COALESCE(data_source, '')) = ''
                                 AND TRIM(COALESCE(data_source_governance_flag, '')) = ''
                                THEN 1 ELSE 0
                            END
                        ) AS unresolved_blank_source_count,
                        SUM(
                            CASE
                                WHEN TRIM(COALESCE(data_source, '')) != ''
                                 AND TRIM(COALESCE(data_source_governance_flag, '')) != ''
                                THEN 1 ELSE 0
                            END
                        ) AS nonblank_source_with_governance_flag_count,
                        SUM(CASE WHEN TRIM(COALESCE(last_updated_at, '')) != '' THEN 1 ELSE 0 END) AS updated_at_present_count,
                        MIN(NULLIF(TRIM(COALESCE(last_updated_at, '')), '')) AS earliest_last_updated_at,
                        MAX(NULLIF(TRIM(COALESCE(last_updated_at, '')), '')) AS latest_last_updated_at
                    FROM funds
                    """
                ).fetchone()
                total = _safe_int(row["total"] if row else 0)
                blank_source_count = _safe_int(row["blank_source_count"] if row else 0)
                governed_blank_count = _safe_int(row["governed_blank_source_count"] if row else 0)
                unresolved_blank_count = _safe_int(row["unresolved_blank_source_count"] if row else 0)
                nonblank_governed_count = _safe_int(
                    row["nonblank_source_with_governance_flag_count"] if row else 0
                )
                updated_at_present_count = _safe_int(row["updated_at_present_count"] if row else 0)
                source_rows = conn.execute(
                    """
                    SELECT
                        CASE
                            WHEN TRIM(COALESCE(data_source, '')) = '' THEN '__blank__'
                            ELSE TRIM(data_source)
                        END AS source_label,
                        COUNT(*) AS row_count
                    FROM funds
                    GROUP BY source_label
                    ORDER BY row_count DESC
                    """
                ).fetchall()
                flag_rows = conn.execute(
                    """
                    SELECT
                        CASE
                            WHEN TRIM(COALESCE(data_source_governance_flag, '')) = '' THEN '__blank__'
                            ELSE TRIM(data_source_governance_flag)
                        END AS governance_flag,
                        COUNT(*) AS row_count
                    FROM funds
                    GROUP BY governance_flag
                    ORDER BY row_count DESC
                    """
                ).fetchall()
                unknown_source_labels = [
                    str(item["source_label"])
                    for item in source_rows
                    if str(item["source_label"]) not in REAL_FUND_DIRECTORY_SOURCE_LABELS
                    and str(item["source_label"]) != "__blank__"
                ]
                unknown_governance_flags = [
                    str(item["governance_flag"])
                    for item in flag_rows
                    if str(item["governance_flag"]) not in GOVERNED_BLANK_DATA_SOURCE_FLAGS
                    and str(item["governance_flag"]) != "__blank__"
                ]
                if unresolved_blank_count:
                    blockers.append("fund_directory_unresolved_blank_data_source_present")
                if nonblank_governed_count:
                    blockers.append("fund_directory_nonblank_source_has_governance_flag")
                if unknown_source_labels:
                    blockers.append(f"fund_directory_unknown_data_source_labels:{','.join(unknown_source_labels)}")
                if unknown_governance_flags:
                    blockers.append(
                        f"fund_directory_unknown_data_source_governance_flags:{','.join(unknown_governance_flags)}"
                    )
                if total and updated_at_present_count < total:
                    blockers.append("fund_directory_last_updated_at_missing")

                db_summary = {
                    "total_fund_count": total,
                    "blank_source_count": blank_source_count,
                    "governed_blank_source_count": governed_blank_count,
                    "unresolved_blank_source_count": unresolved_blank_count,
                    "nonblank_source_with_governance_flag_count": nonblank_governed_count,
                    "updated_at_present_count": updated_at_present_count,
                    "earliest_last_updated_at": row["earliest_last_updated_at"] if row else None,
                    "latest_last_updated_at": row["latest_last_updated_at"] if row else None,
                    "source_counts": {
                        str(item["source_label"]): _safe_int(item["row_count"])
                        for item in source_rows
                    },
                    "governance_flag_counts": {
                        str(item["governance_flag"]): _safe_int(item["row_count"])
                        for item in flag_rows
                    },
                    "known_real_source_labels": sorted(REAL_FUND_DIRECTORY_SOURCE_LABELS),
                    "governed_blank_flags": sorted(GOVERNED_BLANK_DATA_SOURCE_FLAGS),
                    "unknown_source_labels": unknown_source_labels,
                    "unknown_governance_flags": unknown_governance_flags,
                }

                if final_codes:
                    fund_rows = _query_fund_source_rows_by_codes(conn, final_codes)
                    fund_row_by_code = {
                        _normalize_fund_code(row.get("fcode")): row
                        for row in fund_rows
                    }
                    source_counts: dict[str, int] = {}
                    blank_count = 0
                    governed_blank_final_count = 0
                    unresolved_blank_final_count = 0
                    fallback_count = 0
                    missing_codes: list[str] = []
                    for code in final_codes:
                        fund_row = fund_row_by_code.get(code)
                        if not fund_row:
                            missing_codes.append(code)
                            continue
                        source_label = _clean_business_text(fund_row.get("data_source"))
                        governance_flag = _clean_business_text(fund_row.get("data_source_governance_flag"))
                        if not source_label:
                            blank_count += 1
                            source_key = "__BLANK__"
                            if governance_flag in GOVERNED_BLANK_DATA_SOURCE_FLAGS:
                                governed_blank_final_count += 1
                            else:
                                unresolved_blank_final_count += 1
                        else:
                            source_key = source_label
                        if source_label == "autostock_directory_fallback":
                            fallback_count += 1
                        source_counts[source_key] = source_counts.get(source_key, 0) + 1

                    final_summary = {
                        "final_code_count": len(final_codes),
                        "final_count_from_latest": final_count,
                        "final_source_counts": source_counts,
                        "final_blank_count": blank_count,
                        "final_fallback_count": fallback_count,
                        "final_missing_db_count": len(missing_codes),
                        "final_missing_db_sample": missing_codes[:10],
                        "final_governed_blank_count": governed_blank_final_count,
                        "final_unresolved_blank_count": unresolved_blank_final_count,
                    }
                    if missing_codes:
                        blockers.append("formal_list_code_missing_from_funds")
                    if blank_count:
                        blockers.append("formal_list_blank_data_source_present")
                    if fallback_count:
                        blockers.append("formal_list_fallback_data_source_present")

    summary_available = bool(data_source_audit_summary.get("available"))
    expected_final_blank = _safe_int(data_source_audit_summary.get("final_blank_count"))
    expected_final_fallback = _safe_int(data_source_audit_summary.get("final_fallback_count"))
    expected_final_source_counts = {
        str(key): _safe_int(value)
        for key, value in (data_source_audit_summary.get("final_source_counts") or {}).items()
    }
    if final_count and not summary_available:
        blockers.append("publish_data_source_audit_summary_missing")
    if summary_available:
        if expected_final_blank != final_summary.get("final_blank_count"):
            blockers.append("publish_data_source_audit_summary_blank_count_mismatch")
        if expected_final_fallback != final_summary.get("final_fallback_count"):
            blockers.append("publish_data_source_audit_summary_fallback_count_mismatch")
        if expected_final_source_counts != final_summary.get("final_source_counts"):
            blockers.append("publish_data_source_audit_summary_source_counts_mismatch")

    return {
        "status": _classify_status(blockers, warnings),
        "blockers": blockers,
        "warnings": warnings,
        "run_id": run_id or None,
        "database_summary": db_summary,
        "formal_list_summary": final_summary,
        "publish_summary_crosscheck": {
            "available": summary_available,
            "expected_final_blank_count": expected_final_blank,
            "expected_final_fallback_count": expected_final_fallback,
            "expected_final_source_counts": expected_final_source_counts,
            "counts_match": bool(
                summary_available
                and expected_final_blank == final_summary.get("final_blank_count")
                and expected_final_fallback == final_summary.get("final_fallback_count")
                and expected_final_source_counts == final_summary.get("final_source_counts")
            ),
        },
        "truth_note": (
            "This read-only check quantifies funds.data_source traceability and cross-checks the formal list "
            "against the publish-time data_source_audit_summary. Governed historical blanks may remain in the "
            "database, but blank or fallback sources are not acceptable in the formal list."
        ),
    }


def _nav_return_snapshot_consistency(settings: Settings) -> dict[str, Any]:
    blockers: list[str] = []
    warnings: list[str] = []
    latest = _read_json(settings.latest_report_file)
    run_id = str(latest.get("run_id") or "").strip()
    final_count = _safe_int(latest.get("final_count"))
    artifacts = latest.get("artifacts") if isinstance(latest.get("artifacts"), dict) else {}
    filtered_csv_path = _resolve_project_path(settings, artifacts.get("filtered_csv"))
    final_codes = _read_csv_codes(filtered_csv_path) if filtered_csv_path and filtered_csv_path.exists() else []

    if not settings.database_path.exists():
        return {
            "status": "needs_review",
            "blockers": ["database_missing"],
            "warnings": [],
            "run_id": run_id or None,
        }

    with _connect_read_only(settings.database_path) as conn:
        for table_name in ("fund_returns", "fund_nav_history"):
            if not _table_exists(conn, table_name):
                blockers.append(f"nav_return_table_missing:{table_name}")
        if blockers:
            return {
                "status": _classify_status(blockers, warnings),
                "blockers": blockers,
                "warnings": warnings,
                "run_id": run_id or None,
            }

        returns_columns = _table_columns(conn, "fund_returns")
        nav_columns = _table_columns(conn, "fund_nav_history")
        missing_returns_columns = sorted({"fcode", "latest_nav", "latest_nav_date"} - returns_columns)
        missing_nav_columns = sorted({"fcode", "nav_date", "unit_nav"} - nav_columns)
        if missing_returns_columns:
            blockers.append(f"fund_returns_snapshot_fields_missing:{','.join(missing_returns_columns)}")
        if missing_nav_columns:
            blockers.append(f"fund_nav_history_snapshot_fields_missing:{','.join(missing_nav_columns)}")
        if blockers:
            return {
                "status": _classify_status(blockers, warnings),
                "blockers": blockers,
                "warnings": warnings,
                "run_id": run_id or None,
                "missing_returns_columns": missing_returns_columns,
                "missing_nav_columns": missing_nav_columns,
            }

        summary_row = conn.execute(
            """
            WITH nav_latest AS (
                SELECT fcode, MAX(nav_date) AS hist_latest_nav_date
                FROM fund_nav_history
                GROUP BY fcode
            ),
            return_rows AS (
                SELECT
                    r.fcode,
                    NULLIF(TRIM(COALESCE(r.latest_nav_date, '')), '') AS latest_nav_date,
                    r.latest_nav,
                    h.hist_latest_nav_date
                FROM fund_returns r
                LEFT JOIN nav_latest h ON h.fcode = r.fcode
            )
            SELECT
                COUNT(*) AS returns_total,
                SUM(CASE WHEN latest_nav_date IS NOT NULL THEN 1 ELSE 0 END) AS returns_with_latest_date,
                SUM(CASE WHEN latest_nav IS NOT NULL THEN 1 ELSE 0 END) AS returns_with_latest_nav,
                SUM(CASE WHEN latest_nav_date IS NOT NULL AND hist_latest_nav_date IS NULL THEN 1 ELSE 0 END) AS returns_with_date_but_no_history,
                SUM(CASE WHEN latest_nav_date IS NOT NULL AND hist_latest_nav_date IS NOT NULL THEN 1 ELSE 0 END) AS returns_with_history_compared,
                SUM(
                    CASE
                        WHEN latest_nav_date IS NOT NULL
                         AND hist_latest_nav_date IS NOT NULL
                         AND latest_nav_date != hist_latest_nav_date
                        THEN 1 ELSE 0
                    END
                ) AS latest_date_mismatch_count,
                MIN(latest_nav_date) AS min_latest_nav_date,
                MAX(latest_nav_date) AS max_latest_nav_date
            FROM return_rows
            """
        ).fetchone()
        nav_summary_row = conn.execute(
            """
            SELECT
                COUNT(DISTINCT fcode) AS nav_code_count,
                COUNT(*) AS nav_row_count,
                MIN(nav_date) AS min_nav_date,
                MAX(nav_date) AS max_nav_date
            FROM fund_nav_history
            """
        ).fetchone()
        value_row = conn.execute(
            """
            WITH nav_latest_date AS (
                SELECT fcode, MAX(nav_date) AS nav_date
                FROM fund_nav_history
                GROUP BY fcode
            ),
            nav_latest AS (
                SELECT h.fcode, h.nav_date, h.unit_nav
                FROM fund_nav_history h
                JOIN nav_latest_date d
                  ON d.fcode = h.fcode
                 AND d.nav_date = h.nav_date
            )
            SELECT
                COUNT(*) AS compared_value_count,
                SUM(CASE WHEN r.latest_nav IS NULL OR n.unit_nav IS NULL THEN 1 ELSE 0 END) AS missing_value_count,
                SUM(
                    CASE
                        WHEN r.latest_nav IS NOT NULL
                         AND n.unit_nav IS NOT NULL
                         AND ABS(CAST(r.latest_nav AS REAL) - CAST(n.unit_nav AS REAL)) > 0.000001
                        THEN 1 ELSE 0
                    END
                ) AS latest_nav_value_mismatch_count
            FROM fund_returns r
            JOIN nav_latest n ON n.fcode = r.fcode
            WHERE NULLIF(TRIM(COALESCE(r.latest_nav_date, '')), '') = n.nav_date
            """
        ).fetchone()
        orphan_rows = conn.execute(
            """
            WITH nav_latest AS (
                SELECT fcode, MAX(nav_date) AS hist_latest_nav_date
                FROM fund_nav_history
                GROUP BY fcode
            )
            SELECT r.fcode, r.latest_nav_date, r.latest_nav
            FROM fund_returns r
            LEFT JOIN nav_latest h ON h.fcode = r.fcode
            WHERE NULLIF(TRIM(COALESCE(r.latest_nav_date, '')), '') IS NOT NULL
              AND h.fcode IS NULL
            ORDER BY r.fcode
            LIMIT 20
            """
        ).fetchall()
        mismatch_rows = conn.execute(
            """
            WITH nav_latest AS (
                SELECT fcode, MAX(nav_date) AS hist_latest_nav_date
                FROM fund_nav_history
                GROUP BY fcode
            )
            SELECT r.fcode, r.latest_nav_date, h.hist_latest_nav_date
            FROM fund_returns r
            JOIN nav_latest h ON h.fcode = r.fcode
            WHERE NULLIF(TRIM(COALESCE(r.latest_nav_date, '')), '') IS NOT NULL
              AND r.latest_nav_date != h.hist_latest_nav_date
            ORDER BY r.fcode
            LIMIT 20
            """
        ).fetchall()

        final_missing_history_count = 0
        final_mismatch_count = 0
        final_value_mismatch_count = 0
        final_missing_snapshot_date_count = 0
        final_missing_snapshot_value_count = 0
        final_missing_db_count = 0
        if final_codes:
            clean_final_codes = [_normalize_fund_code(code) for code in final_codes if _normalize_fund_code(code)]
            for offset in range(0, len(clean_final_codes), 500):
                chunk = clean_final_codes[offset : offset + 500]
                placeholders = ",".join("?" for _ in chunk)
                if not placeholders:
                    continue
                final_row = conn.execute(
                    f"""
                    WITH nav_latest_date AS (
                        SELECT fcode, MAX(nav_date) AS hist_latest_nav_date
                        FROM fund_nav_history
                        GROUP BY fcode
                    ),
                    nav_latest AS (
                        SELECT h.fcode, h.nav_date, h.unit_nav
                        FROM fund_nav_history h
                        JOIN nav_latest_date d
                          ON d.fcode = h.fcode
                         AND d.hist_latest_nav_date = h.nav_date
                    ),
                    final_codes(fcode) AS (
                        VALUES {','.join(['(?)'] * len(chunk))}
                    )
                    SELECT
                        SUM(CASE WHEN r.fcode IS NULL THEN 1 ELSE 0 END) AS missing_db_count,
                        SUM(
                            CASE
                                WHEN r.fcode IS NOT NULL
                                 AND NULLIF(TRIM(COALESCE(r.latest_nav_date, '')), '') IS NULL
                                THEN 1 ELSE 0
                            END
                        ) AS missing_snapshot_date_count,
                        SUM(
                            CASE
                                WHEN r.fcode IS NOT NULL
                                 AND r.latest_nav IS NULL
                                THEN 1 ELSE 0
                            END
                        ) AS missing_snapshot_value_count,
                        SUM(CASE WHEN r.fcode IS NOT NULL AND n.fcode IS NULL THEN 1 ELSE 0 END) AS missing_history_count,
                        SUM(
                            CASE
                                WHEN r.fcode IS NOT NULL
                                 AND n.fcode IS NOT NULL
                                 AND NULLIF(TRIM(COALESCE(r.latest_nav_date, '')), '') != n.nav_date
                                THEN 1 ELSE 0
                            END
                        ) AS mismatch_count,
                        SUM(
                            CASE
                                WHEN r.fcode IS NOT NULL
                                 AND n.fcode IS NOT NULL
                                 AND NULLIF(TRIM(COALESCE(r.latest_nav_date, '')), '') = n.nav_date
                                 AND r.latest_nav IS NOT NULL
                                 AND n.unit_nav IS NOT NULL
                                 AND ABS(CAST(r.latest_nav AS REAL) - CAST(n.unit_nav AS REAL)) > 0.000001
                                THEN 1 ELSE 0
                            END
                        ) AS value_mismatch_count
                    FROM final_codes c
                    LEFT JOIN fund_returns r ON r.fcode = c.fcode
                    LEFT JOIN nav_latest n ON n.fcode = c.fcode
                    """,
                    chunk,
                ).fetchone()
                final_missing_db_count += _safe_int(final_row["missing_db_count"] if final_row else 0)
                final_missing_snapshot_date_count += _safe_int(
                    final_row["missing_snapshot_date_count"] if final_row else 0
                )
                final_missing_snapshot_value_count += _safe_int(
                    final_row["missing_snapshot_value_count"] if final_row else 0
                )
                final_missing_history_count += _safe_int(final_row["missing_history_count"] if final_row else 0)
                final_mismatch_count += _safe_int(final_row["mismatch_count"] if final_row else 0)
                final_value_mismatch_count += _safe_int(final_row["value_mismatch_count"] if final_row else 0)

        formal_artifact_alignment = _formal_artifact_snapshot_alignment(
            settings=settings,
            latest=latest,
            final_codes=final_codes,
            conn=conn,
        )

    returns_with_date_but_no_history = _safe_int(summary_row["returns_with_date_but_no_history"])
    latest_date_mismatch_count = _safe_int(summary_row["latest_date_mismatch_count"])
    latest_nav_value_mismatch_count = _safe_int(value_row["latest_nav_value_mismatch_count"])
    missing_value_count = _safe_int(value_row["missing_value_count"])
    formal_list_db_nav_clean = not any(
        [
            final_missing_db_count,
            final_missing_snapshot_date_count,
            final_missing_snapshot_value_count,
            final_missing_history_count,
            final_mismatch_count,
            final_value_mismatch_count,
        ]
    )
    formal_artifact_nav_clean = bool(
        formal_artifact_alignment.get("clean_for_nav_during_live_write")
    )
    formal_nav_clean_or_canonical_lagged = formal_list_db_nav_clean or formal_artifact_nav_clean
    weekly_write_window = _weekly_maintenance_live_write_window(settings)
    if latest_date_mismatch_count:
        if formal_nav_clean_or_canonical_lagged and weekly_write_window.get("active"):
            warnings.append("fund_returns_latest_nav_date_mismatch_history_during_weekly_maintenance")
        else:
            blockers.append("fund_returns_latest_nav_date_mismatch_history")
    if latest_nav_value_mismatch_count:
        blockers.append("fund_returns_latest_nav_value_mismatch_history")
    if missing_value_count:
        blockers.append("fund_returns_or_history_latest_nav_value_missing")
    if final_missing_db_count:
        blockers.append("formal_list_fund_returns_missing")
    if final_missing_snapshot_date_count:
        blockers.append("formal_list_latest_nav_date_missing")
    if final_missing_snapshot_value_count:
        blockers.append("formal_list_latest_nav_value_missing")
    if final_missing_history_count:
        if formal_artifact_nav_clean and weekly_write_window.get("active"):
            warnings.append("formal_list_nav_history_mismatch_current_db_during_weekly_maintenance")
        else:
            blockers.append("formal_list_nav_history_missing")
    if final_mismatch_count:
        if formal_artifact_nav_clean and weekly_write_window.get("active"):
            warnings.append("formal_list_latest_nav_date_mismatch_history_during_weekly_maintenance")
        else:
            blockers.append("formal_list_latest_nav_date_mismatch_history")
    if final_value_mismatch_count:
        blockers.append("formal_list_latest_nav_value_mismatch_history")
    if final_count and final_codes and len(final_codes) != final_count:
        blockers.append("formal_filtered_csv_code_count_not_equal_final_count_for_nav_truth")

    return {
        "status": _classify_status(blockers, warnings),
        "blockers": blockers,
        "warnings": warnings,
        "run_id": run_id or None,
        "database_summary": {
            "returns_total": _safe_int(summary_row["returns_total"]),
            "returns_with_latest_date": _safe_int(summary_row["returns_with_latest_date"]),
            "returns_with_latest_nav": _safe_int(summary_row["returns_with_latest_nav"]),
            "returns_with_date_but_no_history": returns_with_date_but_no_history,
            "returns_with_history_compared": _safe_int(summary_row["returns_with_history_compared"]),
            "latest_date_mismatch_count": latest_date_mismatch_count,
            "min_latest_nav_date": summary_row["min_latest_nav_date"],
            "max_latest_nav_date": summary_row["max_latest_nav_date"],
            "nav_code_count": _safe_int(nav_summary_row["nav_code_count"]),
            "nav_row_count": _safe_int(nav_summary_row["nav_row_count"]),
            "min_nav_date": nav_summary_row["min_nav_date"],
            "max_nav_date": nav_summary_row["max_nav_date"],
            "compared_value_count": _safe_int(value_row["compared_value_count"]),
            "missing_value_count": missing_value_count,
            "latest_nav_value_mismatch_count": latest_nav_value_mismatch_count,
            "missing_history_interpretation": (
                "recorded_as_missing_history_not_usable_for_history_based_metrics"
                if returns_with_date_but_no_history
                else "all_return_snapshots_have_history"
            ),
            "returns_without_history_sample": [dict(row) for row in orphan_rows],
            "date_mismatch_sample": [dict(row) for row in mismatch_rows],
        },
        "formal_list_summary": {
            "final_code_count": len(final_codes),
            "final_count_from_latest": final_count,
            "missing_fund_returns_count": final_missing_db_count,
            "missing_latest_nav_date_count": final_missing_snapshot_date_count,
            "missing_latest_nav_value_count": final_missing_snapshot_value_count,
            "missing_nav_history_count": final_missing_history_count,
            "latest_nav_date_mismatch_count": final_mismatch_count,
            "latest_nav_value_mismatch_count": final_value_mismatch_count,
        },
        "formal_artifact_snapshot_alignment": formal_artifact_alignment,
        "weekly_maintenance_live_write_window": weekly_write_window,
        "truth_note": (
            "This read-only check ensures fund_returns latest NAV snapshots are backed by real fund_nav_history rows. "
            "Return rows with latest_nav_date but no history are reported as missing-history semantics and must not enter the formal list."
        ),
    }


def _risk_snapshot_input_consistency(settings: Settings) -> dict[str, Any]:
    blockers: list[str] = []
    warnings: list[str] = []
    latest = _read_json(settings.latest_report_file)
    run_id = str(latest.get("run_id") or "").strip()
    final_count = _safe_int(latest.get("final_count"))
    artifacts = latest.get("artifacts") if isinstance(latest.get("artifacts"), dict) else {}
    filtered_csv_path = _resolve_project_path(settings, artifacts.get("filtered_csv"))
    final_codes = _read_csv_codes(filtered_csv_path) if filtered_csv_path and filtered_csv_path.exists() else []

    if not settings.database_path.exists():
        return {
            "status": "needs_review",
            "blockers": ["database_missing"],
            "warnings": [],
            "run_id": run_id or None,
        }

    with _connect_read_only(settings.database_path) as conn:
        for table_name in ("fund_risk_metrics", "fund_returns", "fund_nav_history"):
            if not _table_exists(conn, table_name):
                blockers.append(f"risk_input_table_missing:{table_name}")
        if blockers:
            return {
                "status": _classify_status(blockers, warnings),
                "blockers": blockers,
                "warnings": warnings,
                "run_id": run_id or None,
            }

        risk_columns = _table_columns(conn, "fund_risk_metrics")
        returns_columns = _table_columns(conn, "fund_returns")
        required_risk_columns = {
            "fcode",
            "period",
            "end_date",
            "nav_snapshot_signature",
        }
        required_returns_columns = {
            "fcode",
            "latest_nav_date",
            "latest_nav",
            "daily_growth",
            "return_1w",
            "return_1m",
            "return_3m",
            "return_6m",
            "return_1y",
            "return_2y",
            "return_3y",
            "return_5y",
            "return_ytd",
            "return_since",
        }
        missing_risk_columns = sorted(required_risk_columns - risk_columns)
        missing_returns_columns = sorted(required_returns_columns - returns_columns)
        if missing_risk_columns:
            blockers.append(f"risk_input_fields_missing:{','.join(missing_risk_columns)}")
        if missing_returns_columns:
            blockers.append(f"fund_returns_signature_fields_missing:{','.join(missing_returns_columns)}")
        if blockers:
            return {
                "status": _classify_status(blockers, warnings),
                "blockers": blockers,
                "warnings": warnings,
                "run_id": run_id or None,
                "missing_risk_columns": missing_risk_columns,
                "missing_returns_columns": missing_returns_columns,
            }

        nav_archive_attach, nav_archive_detach, nav_source_sql = _attach_nav_history_archive_for_audit(
            conn,
            settings,
        )
        summary_rows = conn.execute(
            f"""
            WITH nav_latest AS (
                SELECT fcode, MAX(nav_date) AS latest_hist_nav_date
                FROM ({nav_source_sql}) AS nav_history
                GROUP BY fcode
            )
            SELECT
                r.period,
                COUNT(*) AS row_count,
                COUNT(DISTINCT r.fcode) AS code_count,
                SUM(CASE WHEN TRIM(COALESCE(r.nav_snapshot_signature, '')) = '' THEN 1 ELSE 0 END) AS blank_signature_count,
                SUM(CASE WHEN fr.fcode IS NULL THEN 1 ELSE 0 END) AS missing_returns_count,
                SUM(CASE WHEN h.fcode IS NULL THEN 1 ELSE 0 END) AS missing_nav_history_count,
                SUM(CASE WHEN h.latest_hist_nav_date IS NOT NULL AND r.end_date > h.latest_hist_nav_date THEN 1 ELSE 0 END) AS risk_end_after_nav_count,
                SUM(CASE WHEN h.latest_hist_nav_date IS NOT NULL AND r.end_date < h.latest_hist_nav_date THEN 1 ELSE 0 END) AS risk_end_before_nav_count,
                MIN(r.start_date) AS min_start_date,
                MAX(r.end_date) AS max_end_date,
                MAX(h.latest_hist_nav_date) AS max_hist_nav_date
            FROM fund_risk_metrics r
            LEFT JOIN fund_returns fr ON fr.fcode = r.fcode
            LEFT JOIN nav_latest h ON h.fcode = r.fcode
            GROUP BY r.period
            ORDER BY r.period
            """
        ).fetchall()
        period_date_select = ", fr.period_return_date" if "period_return_date" in returns_columns else ""
        risk_rows = conn.execute(
            f"""
            SELECT
                r.fcode,
                r.period,
                r.end_date,
                COALESCE(r.nav_snapshot_signature, '') AS nav_snapshot_signature,
                fr.latest_nav_date,
                fr.latest_nav,
                fr.daily_growth,
                fr.return_1w,
                fr.return_1m,
                fr.return_3m,
                fr.return_6m,
                fr.return_1y,
                fr.return_2y,
                fr.return_3y,
                fr.return_5y,
                fr.return_ytd,
                fr.return_since{period_date_select}
            FROM fund_risk_metrics r
            JOIN fund_returns fr ON fr.fcode = r.fcode
            """
        ).fetchall()
        signature_mismatch_sample: list[dict[str, Any]] = []
        signature_mismatch_count = 0
        for row in risk_rows:
            expected_signature = build_nav_snapshot_signature(dict(row))
            actual_signature = _clean_business_text(row["nav_snapshot_signature"])
            if actual_signature != expected_signature:
                signature_mismatch_count += 1
                if len(signature_mismatch_sample) < 20:
                    signature_mismatch_sample.append(
                        {
                            "fcode": row["fcode"],
                            "period": row["period"],
                            "stored_signature": actual_signature,
                            "expected_signature": expected_signature,
                            "latest_nav_date": row["latest_nav_date"],
                        }
                    )

        period_summary: list[dict[str, Any]] = []
        total_blank_signature = 0
        total_missing_returns = 0
        total_missing_nav_history = 0
        total_risk_end_after_nav = 0
        for row in summary_rows:
            blank_signature_count = _safe_int(row["blank_signature_count"])
            missing_returns_count = _safe_int(row["missing_returns_count"])
            missing_nav_history_count = _safe_int(row["missing_nav_history_count"])
            risk_end_after_nav_count = _safe_int(row["risk_end_after_nav_count"])
            total_blank_signature += blank_signature_count
            total_missing_returns += missing_returns_count
            total_missing_nav_history += missing_nav_history_count
            total_risk_end_after_nav += risk_end_after_nav_count
            period_summary.append(
                {
                    "period": str(row["period"] or ""),
                    "row_count": _safe_int(row["row_count"]),
                    "code_count": _safe_int(row["code_count"]),
                    "blank_signature_count": blank_signature_count,
                    "missing_returns_count": missing_returns_count,
                    "missing_nav_history_count": missing_nav_history_count,
                    "risk_end_after_nav_count": risk_end_after_nav_count,
                    "risk_end_before_nav_count": _safe_int(row["risk_end_before_nav_count"]),
                    "min_start_date": row["min_start_date"],
                    "max_end_date": row["max_end_date"],
                    "max_hist_nav_date": row["max_hist_nav_date"],
                }
            )
        if total_blank_signature:
            blockers.append("risk_nav_snapshot_signature_blank")
        if total_missing_returns:
            blockers.append("risk_metric_missing_fund_returns_snapshot")
        if total_missing_nav_history:
            blockers.append("risk_metric_missing_nav_history")
        if total_risk_end_after_nav:
            blockers.append("risk_metric_end_date_after_latest_nav_history")

        final_summary = {
            "final_code_count": len(final_codes),
            "final_count_from_latest": final_count,
            "missing_1y_risk_count": 0,
            "blank_signature_count": 0,
            "signature_mismatch_count": 0,
            "missing_returns_count": 0,
            "missing_nav_history_count": 0,
            "risk_end_after_nav_count": 0,
        }
        final_signature_mismatch_count = 0
        if final_codes:
            clean_final_codes = [_normalize_fund_code(code) for code in final_codes if _normalize_fund_code(code)]
            for offset in range(0, len(clean_final_codes), 500):
                chunk = clean_final_codes[offset : offset + 500]
                if not chunk:
                    continue
                values_clause = ",".join(["(?)"] * len(chunk))
                final_rows = conn.execute(
                    f"""
                    WITH final_codes(fcode) AS (
                        VALUES {values_clause}
                    ),
                    nav_latest AS (
                        SELECT fcode, MAX(nav_date) AS latest_hist_nav_date
                        FROM ({nav_source_sql}) AS nav_history
                        GROUP BY fcode
                    )
                    SELECT
                        c.fcode AS final_fcode,
                        r.period,
                        r.end_date,
                        COALESCE(r.nav_snapshot_signature, '') AS nav_snapshot_signature,
                        fr.latest_nav_date,
                        fr.latest_nav,
                        fr.daily_growth,
                        fr.return_1w,
                        fr.return_1m,
                        fr.return_3m,
                        fr.return_6m,
                        fr.return_1y,
                        fr.return_2y,
                        fr.return_3y,
                        fr.return_5y,
                        fr.return_ytd,
                        fr.return_since,
                        h.latest_hist_nav_date{period_date_select}
                    FROM final_codes c
                    LEFT JOIN fund_risk_metrics r
                      ON r.fcode = c.fcode
                     AND r.period = '1y'
                    LEFT JOIN fund_returns fr ON fr.fcode = c.fcode
                    LEFT JOIN nav_latest h ON h.fcode = c.fcode
                    """,
                    chunk,
                ).fetchall()
                for row in final_rows:
                    if not row["period"]:
                        final_summary["missing_1y_risk_count"] += 1
                        continue
                    actual_signature = _clean_business_text(row["nav_snapshot_signature"])
                    if not actual_signature:
                        final_summary["blank_signature_count"] += 1
                    if row["latest_nav_date"] is None:
                        final_summary["missing_returns_count"] += 1
                    if row["latest_hist_nav_date"] is None:
                        final_summary["missing_nav_history_count"] += 1
                    if row["latest_hist_nav_date"] is not None and row["end_date"] is not None and row["end_date"] > row["latest_hist_nav_date"]:
                        final_summary["risk_end_after_nav_count"] += 1
                    if row["latest_nav_date"] is not None and actual_signature:
                        expected_signature = build_nav_snapshot_signature(dict(row))
                        if actual_signature != expected_signature:
                            final_signature_mismatch_count += 1
                final_summary["signature_mismatch_count"] = final_signature_mismatch_count

        formal_artifact_alignment = _formal_artifact_snapshot_alignment(
            settings=settings,
            latest=latest,
            final_codes=final_codes,
            conn=conn,
        )
        if nav_archive_detach is not None:
            nav_archive_detach(conn)

    formal_list_risk_clean = not any(
        [
            final_summary["missing_1y_risk_count"],
            final_summary["blank_signature_count"],
            final_summary["signature_mismatch_count"],
            final_summary["missing_returns_count"],
            final_summary["missing_nav_history_count"],
            final_summary["risk_end_after_nav_count"],
        ]
    )
    formal_artifact_risk_clean = bool(
        formal_artifact_alignment.get("clean_for_risk_during_live_write")
    )
    formal_risk_clean_or_canonical_lagged = formal_list_risk_clean or formal_artifact_risk_clean
    weekly_write_window = _weekly_maintenance_live_write_window(settings)

    if final_count and final_codes and len(final_codes) != final_count:
        blockers.append("formal_filtered_csv_code_count_not_equal_final_count_for_risk_input_truth")
    if final_summary["missing_1y_risk_count"]:
        blockers.append("formal_list_missing_1y_risk_snapshot")
    if final_summary["blank_signature_count"]:
        blockers.append("formal_list_risk_nav_snapshot_signature_blank")
    if final_summary["signature_mismatch_count"]:
        if formal_artifact_risk_clean and weekly_write_window.get("active"):
            warnings.append("formal_list_risk_nav_snapshot_signature_mismatch_during_weekly_maintenance")
        else:
            blockers.append("formal_list_risk_nav_snapshot_signature_mismatch")
    if final_summary["missing_returns_count"]:
        blockers.append("formal_list_risk_missing_returns_snapshot")
    if final_summary["missing_nav_history_count"]:
        if formal_artifact_risk_clean and weekly_write_window.get("active"):
            warnings.append("formal_list_risk_missing_nav_history_during_weekly_maintenance")
        else:
            blockers.append("formal_list_risk_missing_nav_history")
    if final_summary["risk_end_after_nav_count"]:
        blockers.append("formal_list_risk_end_after_nav_history")
    if signature_mismatch_count:
        if formal_risk_clean_or_canonical_lagged and weekly_write_window.get("active"):
            warnings.append("risk_nav_snapshot_signature_mismatch_current_returns_during_weekly_maintenance")
        else:
            blockers.append("risk_nav_snapshot_signature_mismatch_current_returns")

    return {
        "status": _classify_status(blockers, warnings),
        "blockers": blockers,
        "warnings": warnings,
        "run_id": run_id or None,
        "period_summary": period_summary,
        "signature_check": {
            "checked_row_count": len(risk_rows),
            "signature_mismatch_count": signature_mismatch_count,
            "signature_mismatch_sample": signature_mismatch_sample,
        },
        "formal_list_summary": final_summary,
        "formal_artifact_snapshot_alignment": formal_artifact_alignment,
        "weekly_maintenance_live_write_window": weekly_write_window,
        "nav_history_archive_attach": nav_archive_attach,
        "truth_note": (
            "This read-only check verifies fund_risk_metrics are tied to the current real fund_returns snapshot "
            "via nav_snapshot_signature and do not have risk end dates beyond real hot/cold NAV history. It does "
            "not modify thresholds; numerical formula replay is covered by risk_metric_recalculation_truth."
        ),
    }


RISK_RECALCULATION_SAMPLE_LIMIT = 12
RISK_RECALCULATION_PERIODS = ("1y", "3y", "5y")
RISK_RECALCULATION_REQUIRED_PERIOD = "1y"
RISK_RECALCULATION_PERIOD = RISK_RECALCULATION_REQUIRED_PERIOD
RISK_RECALCULATION_LOW_VOL_SAMPLE_LIMIT_PER_PERIOD = 4
RISK_RECALCULATION_BOUNDARY_SAMPLE_LIMIT = 6
RISK_RECALCULATION_SCORE_FIELD = "正式投资评分"
RISK_RECALCULATION_NUMERIC_FIELDS = {
    "annual_return": 4,
    "volatility": 6,
    "max_drawdown": 6,
    "sharpe_ratio": 4,
    "sortino_ratio": 4,
    "raw_sharpe_ratio": 4,
    "raw_sortino_ratio": 4,
    "calmar_ratio": 4,
    "var_95": 6,
    "var_99": 6,
    "positive_ratio": 2,
}
RISK_RECALCULATION_FLAG_FIELDS = ("sharpe_ratio_cap_hit", "sortino_ratio_cap_hit")
RISK_RECALCULATION_NAV_COLUMNS = ("fcode", "nav_date", "unit_nav", "accum_nav", "daily_growth")
RISK_RECALCULATION_RETURN_FIELDS = (
    "latest_nav_date",
    "latest_nav",
    "daily_growth",
    "return_1w",
    "return_1m",
    "return_3m",
    "return_6m",
    "return_1y",
    "return_2y",
    "return_3y",
    "return_5y",
    "return_ytd",
    "return_since",
)


def _risk_recalculation_api_return_field(period: str) -> str:
    normalized_period = str(period or "").strip()
    return {
        "1y": "return_1y",
        "3y": "return_3y",
        "5y": "return_5y",
    }.get(normalized_period, "")


def _risk_recalculation_return_provenance_select(columns: set[str]) -> str:
    fields = [f"fr.{field}" for field in RETURN_PROVENANCE_FIELDS if field in columns]
    return ", " + ", ".join(fields) if fields else ""


def _risk_recalculation_return_snapshot(row: sqlite3.Row) -> dict[str, Any]:
    # Absence in a legacy schema stays absent, preserving its input signature;
    # a migrated NULL date participates in the new signature, like production.
    return {field: row[field] for field in (*RISK_RECALCULATION_RETURN_FIELDS, *RETURN_PROVENANCE_FIELDS)
            if field in row.keys()}


def _dedupe_preserve_order(values: Sequence[Any]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values or []:
        code = _normalize_fund_code(value)
        if code and code not in seen:
            seen.add(code)
            result.append(code)
    return result


def _risk_recalculation_boundary_codes(
    csv_rows_by_code: dict[str, dict[str, Any]],
    final_codes: Sequence[str],
    *,
    limit: int = RISK_RECALCULATION_BOUNDARY_SAMPLE_LIMIT,
) -> list[str]:
    scored: list[tuple[float, int, str]] = []
    for index, code in enumerate(final_codes or []):
        row = csv_rows_by_code.get(code) or {}
        score = _safe_float(row.get(RISK_RECALCULATION_SCORE_FIELD))
        if score is None:
            continue
        scored.append((float(score), index, code))
    scored.sort(key=lambda item: (item[0], item[1]))
    return [code for _, _, code in scored[: max(0, int(limit or 0))]]


def _risk_recalculation_low_vol_codes_by_period(
    conn: sqlite3.Connection,
    final_codes: Sequence[str],
    *,
    periods: Sequence[str] = RISK_RECALCULATION_PERIODS,
    limit_per_period: int = RISK_RECALCULATION_LOW_VOL_SAMPLE_LIMIT_PER_PERIOD,
) -> dict[str, list[str]]:
    normalized_codes = _dedupe_preserve_order(final_codes)
    if not normalized_codes:
        return {str(period): [] for period in periods}

    values_clause = ",".join(["(?)"] * len(normalized_codes))
    low_vol_by_period: dict[str, list[str]] = {}
    for period in periods:
        period_text = str(period or "").strip()
        if not period_text:
            continue
        rows = conn.execute(
            f"""
            WITH final_codes(fcode) AS (
                VALUES {values_clause}
            ),
            latest AS (
                SELECT
                    risk.fcode,
                    risk.volatility,
                    risk.end_date,
                    risk.rowid,
                    ROW_NUMBER() OVER (
                        PARTITION BY risk.fcode, risk.period
                        ORDER BY COALESCE(TRIM(risk.end_date), '') DESC, risk.rowid DESC
                    ) AS rn
                FROM fund_risk_metrics risk
                JOIN final_codes ON final_codes.fcode = risk.fcode
                WHERE risk.period = ?
            )
            SELECT fcode
            FROM latest
            WHERE rn = 1
              AND volatility IS NOT NULL
            ORDER BY volatility ASC, COALESCE(TRIM(end_date), '') DESC, rowid DESC
            LIMIT ?
            """,
            (*normalized_codes, period_text, max(0, int(limit_per_period or 0))),
        ).fetchall()
        low_vol_by_period[period_text] = [_normalize_fund_code(row["fcode"]) for row in rows if row["fcode"]]
    return low_vol_by_period


def _build_risk_recalculation_sample_specs(
    conn: sqlite3.Connection,
    *,
    final_codes: Sequence[str],
    csv_rows_by_code: dict[str, dict[str, Any]],
    sample_limit: int,
) -> list[dict[str, Any]]:
    if int(sample_limit or 0) <= 0:
        return []
    head_codes = _dedupe_preserve_order(list(final_codes or [])[: max(0, int(sample_limit or 0))])
    boundary_codes = _risk_recalculation_boundary_codes(csv_rows_by_code, final_codes)
    low_vol_by_period = _risk_recalculation_low_vol_codes_by_period(conn, final_codes)
    spec_by_key: dict[tuple[str, str], dict[str, Any]] = {}

    def add(code: Any, period: str, reason: str) -> None:
        normalized_code = _normalize_fund_code(code)
        period_text = str(period or "").strip()
        reason_text = str(reason or "").strip()
        if not normalized_code or not period_text or not reason_text:
            return
        key = (normalized_code, period_text)
        spec = spec_by_key.setdefault(
            key,
            {"fcode": normalized_code, "period": period_text, "sample_reasons": []},
        )
        reasons = spec.setdefault("sample_reasons", [])
        if reason_text not in reasons:
            reasons.append(reason_text)

    for code in head_codes:
        for period in RISK_RECALCULATION_PERIODS:
            add(code, period, "formal_head_sample")
    for code in boundary_codes:
        for period in RISK_RECALCULATION_PERIODS:
            add(code, period, "formal_low_score_boundary")
    for period, codes in low_vol_by_period.items():
        for code in codes:
            add(code, period, "formal_low_volatility_focus")

    return list(spec_by_key.values())


def _sql_literal(value: Any) -> str:
    return "'" + str(value or "").replace("'", "''") + "'"


def _table_columns_in_schema(conn: sqlite3.Connection, table_name: str, *, schema: str = "") -> set[str]:
    try:
        if schema:
            safe_schema = str(schema or "").strip()
            safe_table = str(table_name or "").strip()
            if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", safe_schema):
                return set()
            if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", safe_table):
                return set()
            rows = conn.execute(f"PRAGMA {safe_schema}.table_info({safe_table})").fetchall()
        else:
            rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    except sqlite3.Error:
        return set()
    return {str(row["name"] or "") for row in rows if str(row["name"] or "").strip()}


def _nav_source_select_sql(table_ref: str, columns: set[str], *, cutoff_date: str = "") -> str:
    safe_table_ref = str(table_ref or "").strip()
    selected: list[str] = []
    for column in RISK_RECALCULATION_NAV_COLUMNS:
        if column in columns:
            selected.append(column)
        else:
            selected.append(f"NULL AS {column}")
    sql = f"SELECT {', '.join(selected)} FROM {safe_table_ref}"
    if cutoff_date:
        sql += f" WHERE nav_date < {_sql_literal(cutoff_date)}"
    return sql


def _risk_recalculation_nav_source_for_audit(
    conn: sqlite3.Connection,
    settings: Settings,
) -> tuple[dict[str, Any], Any | None, str]:
    hot_columns = _table_columns(conn, "fund_nav_history")
    hot_source_sql = _nav_source_select_sql("fund_nav_history", hot_columns)
    try:
        from database.nav_history_queries import (  # type: ignore
            attach_nav_history_archive_if_available,
            detach_nav_history_archive,
            load_latest_nav_history_archive_binding,
            nav_history_archive_binding_matches_database,
            normalize_archive_table_name,
        )

        binding = load_latest_nav_history_archive_binding(
            project_root=getattr(settings, "project_root", PROJECT_ROOT),
        )
        if not nav_history_archive_binding_matches_database(
            binding,
            getattr(settings, "database_path", ""),
            project_root=getattr(settings, "project_root", PROJECT_ROOT),
        ):
            binding = {}
        attach = attach_nav_history_archive_if_available(
            conn,
            archive_path=binding.get("archive_path"),
            archive_table_name=binding.get("archive_table_name") or "fund_nav_history_archive",
            archive_cutoff_date=binding.get("archive_cutoff_date") or "",
        )
        if not attach.get("attached"):
            return dict(attach or {}), None, hot_source_sql
        alias = str(attach.get("alias") or "nav_archive_db").strip()
        table_name = normalize_archive_table_name(
            attach.get("archive_table_name") or binding.get("archive_table_name") or "fund_nav_history_archive"
        )
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", alias):
            return dict(attach or {}), None, hot_source_sql
        cold_columns = _table_columns_in_schema(conn, table_name, schema=alias)
        cold_source_sql = _nav_source_select_sql(
            f"{alias}.{table_name}",
            cold_columns,
            cutoff_date=str(attach.get("archive_cutoff_date") or "").strip(),
        )
        return dict(attach), detach_nav_history_archive, f"{hot_source_sql}\nUNION ALL\n{cold_source_sql}"
    except Exception as exc:
        return {"attached": False, "reason": f"attach_failed:{exc}"}, None, hot_source_sql


def _metric_round_tolerance(digits: int) -> float:
    return float(10 ** (-max(0, int(digits)))) / 2 + 1e-12


def _numeric_metric_mismatch(stored: Any, expected: Any, digits: int) -> dict[str, Any] | None:
    stored_number = _safe_float(stored)
    expected_number = _safe_float(expected)
    if stored_number is None or expected_number is None:
        if stored_number is None and expected_number is None:
            return None
        return {
            "stored": stored,
            "expected": expected,
            "reason": "missing_numeric_side",
        }
    expected_rounded = round(float(expected_number), int(digits))
    tolerance = _metric_round_tolerance(int(digits))
    if abs(float(stored_number) - expected_rounded) <= tolerance:
        return None
    return {
        "stored": stored_number,
        "expected": expected_rounded,
        "raw_expected": expected_number,
        "tolerance": tolerance,
    }


def _flag_metric_mismatch(stored: Any, expected: Any) -> dict[str, Any] | None:
    if stored is None or expected is None:
        if stored is None and expected is None:
            return None
        return {"stored": stored, "expected": expected, "reason": "missing_flag_side"}
    stored_bool = bool(_safe_int(stored))
    expected_bool = bool(expected)
    if stored_bool == expected_bool:
        return None
    return {
        "stored": stored_bool,
        "expected": expected_bool,
    }


def _read_recalculation_nav_frame(
    conn: sqlite3.Connection,
    nav_source_sql: str,
    *,
    fcode: str,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    rows = conn.execute(
        f"""
        SELECT
            nav_date AS date,
            unit_nav AS nav,
            accum_nav,
            daily_growth
        FROM ({nav_source_sql}) AS nav_history
        WHERE fcode = ?
          AND nav_date >= ?
          AND nav_date <= ?
        ORDER BY nav_date
        """,
        (fcode, start_date, end_date),
    ).fetchall()
    if not rows:
        return pd.DataFrame(columns=["nav", "accum_nav", "daily_growth"])
    frame = pd.DataFrame([dict(row) for row in rows])
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame["nav"] = pd.to_numeric(frame["nav"], errors="coerce")
    frame["accum_nav"] = pd.to_numeric(frame["accum_nav"], errors="coerce")
    frame["daily_growth"] = pd.to_numeric(frame["daily_growth"], errors="coerce")
    frame = frame.dropna(subset=["date"]).set_index("date").sort_index()
    return frame[["nav", "accum_nav", "daily_growth"]]


def _risk_recalculation_query_start_date(end_date: str, period: str) -> str:
    period_days = {"1y": 365, "3y": 365 * 3, "5y": 365 * 5}.get(str(period or "").strip())
    if not period_days:
        return str(end_date or "").strip()
    try:
        return (pd.Timestamp(end_date) - pd.Timedelta(days=int(period_days))).date().isoformat()
    except Exception:
        return str(end_date or "").strip()


def _risk_recalculation_compare_row(
    row: sqlite3.Row,
    recalculated: dict[str, Any],
    *,
    start_date: str,
    end_date: str,
    nav_snapshot_signature: str,
) -> tuple[dict[str, Any], int | None]:
    field_mismatches: dict[str, Any] = {}
    for field, digits in RISK_RECALCULATION_NUMERIC_FIELDS.items():
        mismatch = _numeric_metric_mismatch(row[field], recalculated.get(field), digits)
        if mismatch:
            field_mismatches[field] = mismatch
    for field in RISK_RECALCULATION_FLAG_FIELDS:
        mismatch = _flag_metric_mismatch(row[field], recalculated.get(field))
        if mismatch:
            field_mismatches[field] = mismatch

    stored_data_points = _safe_optional_int(row["data_points"])
    expected_data_points = _safe_optional_int(recalculated.get("data_points"))
    if stored_data_points != expected_data_points:
        field_mismatches["data_points"] = {
            "stored": stored_data_points,
            "expected": expected_data_points,
        }

    expected_start_date = recalculated.get("start_date")
    expected_end_date = recalculated.get("end_date")
    expected_start_text = (
        pd.Timestamp(expected_start_date).date().isoformat()
        if expected_start_date is not None
        else ""
    )
    expected_end_text = (
        pd.Timestamp(expected_end_date).date().isoformat()
        if expected_end_date is not None
        else ""
    )
    if start_date != expected_start_text:
        field_mismatches["start_date"] = {
            "stored": start_date,
            "expected": expected_start_text,
        }
    if end_date != expected_end_text:
        field_mismatches["end_date"] = {
            "stored": end_date,
            "expected": expected_end_text,
        }

    stored_signature = _clean_business_text(row["nav_snapshot_signature"])
    if stored_signature != nav_snapshot_signature:
        field_mismatches["nav_snapshot_signature"] = {
            "stored": stored_signature,
            "expected": nav_snapshot_signature,
        }

    return field_mismatches, stored_data_points


def _risk_recalculation_period_coverage(
    conn: sqlite3.Connection,
    final_codes: Sequence[str],
    *,
    periods: Sequence[str] = RISK_RECALCULATION_PERIODS,
) -> dict[str, int]:
    normalized_codes = _dedupe_preserve_order(final_codes)
    period_values = [str(period or "").strip() for period in periods if str(period or "").strip()]
    if not normalized_codes or not period_values:
        return {period: 0 for period in period_values}
    values_clause = ",".join(["(?)"] * len(normalized_codes))
    period_placeholders = ",".join(["?"] * len(period_values))
    rows = conn.execute(
        f"""
        WITH final_codes(fcode) AS (
            VALUES {values_clause}
        ),
        latest AS (
            SELECT
                risk.fcode,
                risk.period,
                ROW_NUMBER() OVER (
                    PARTITION BY risk.fcode, risk.period
                    ORDER BY COALESCE(TRIM(risk.end_date), '') DESC, risk.rowid DESC
                ) AS rn
            FROM fund_risk_metrics risk
            JOIN final_codes ON final_codes.fcode = risk.fcode
            WHERE risk.period IN ({period_placeholders})
        )
        SELECT period, COUNT(*) AS latest_risk_count
        FROM latest
        WHERE rn = 1
        GROUP BY period
        """,
        (*normalized_codes, *period_values),
    ).fetchall()
    coverage = {period: 0 for period in period_values}
    for row in rows:
        period = str(row["period"] or "").strip()
        if period in coverage:
            coverage[period] = _safe_int(row["latest_risk_count"])
    return coverage


def _risk_metric_recalculation_truth_legacy_1y(
    settings: Settings,
    *,
    sample_limit: int = RISK_RECALCULATION_SAMPLE_LIMIT,
) -> dict[str, Any]:
    blockers: list[str] = []
    warnings: list[str] = []
    latest = _read_json(settings.latest_report_file)
    run_id = str(latest.get("run_id") or "").strip()
    final_count = _safe_int(latest.get("final_count"))
    artifacts = latest.get("artifacts") if isinstance(latest.get("artifacts"), dict) else {}
    filtered_csv_path = _resolve_project_path(settings, artifacts.get("filtered_csv"))
    final_codes = _read_csv_codes(filtered_csv_path) if filtered_csv_path and filtered_csv_path.exists() else []
    normalized_final_codes = [_normalize_fund_code(code) for code in final_codes if _normalize_fund_code(code)]
    sample_limit = max(0, int(sample_limit or 0))
    sample_codes = normalized_final_codes[:sample_limit] if sample_limit else []

    if final_count and normalized_final_codes and len(normalized_final_codes) != final_count:
        blockers.append("formal_filtered_csv_code_count_not_equal_final_count_for_risk_recalculation")
    if final_count and not normalized_final_codes:
        blockers.append("formal_filtered_csv_missing_for_risk_recalculation")
    if not sample_codes:
        return {
            "status": _classify_status(blockers, warnings),
            "blockers": blockers,
            "warnings": warnings,
            "run_id": run_id or None,
            "sample_limit": sample_limit,
            "sampled_code_count": 0,
            "formula_engine": "core/analyzers/risk_analyzer.py::RiskAnalyzer._calculate_period_metrics",
            "truth_note": "No formal codes were available for read-only numerical risk metric replay.",
        }

    if not settings.database_path.exists():
        return {
            "status": "needs_review",
            "blockers": ["database_missing"],
            "warnings": warnings,
            "run_id": run_id or None,
            "sample_limit": sample_limit,
            "sampled_code_count": len(sample_codes),
        }

    required_tables = ("fund_risk_metrics", "fund_returns", "fund_nav_history")
    required_risk_columns = {
        "fcode",
        "period",
        "start_date",
        "end_date",
        "annual_return",
        "volatility",
        "max_drawdown",
        "sharpe_ratio",
        "sortino_ratio",
        "raw_sharpe_ratio",
        "raw_sortino_ratio",
        "sharpe_ratio_cap_hit",
        "sortino_ratio_cap_hit",
        "calmar_ratio",
        "var_95",
        "var_99",
        "positive_ratio",
        "data_points",
        "nav_snapshot_signature",
    }
    required_return_columns = {"fcode", *RISK_RECALCULATION_RETURN_FIELDS}
    required_nav_columns = {"fcode", "nav_date", "unit_nav"}

    metric_mismatch_count = 0
    metric_mismatch_sample: list[dict[str, Any]] = []
    missing_risk_rows: list[str] = []
    missing_return_rows: list[str] = []
    missing_nav_rows: list[str] = []
    recalculated_rows: list[dict[str, Any]] = []

    with _connect_read_only(settings.database_path) as conn:
        missing_tables = [table_name for table_name in required_tables if not _table_exists(conn, table_name)]
        if missing_tables:
            return {
                "status": "needs_review",
                "blockers": [f"risk_recalculation_table_missing:{table_name}" for table_name in missing_tables],
                "warnings": warnings,
                "run_id": run_id or None,
                "sample_limit": sample_limit,
                "sampled_code_count": len(sample_codes),
            }

        risk_columns = _table_columns(conn, "fund_risk_metrics")
        return_columns = _table_columns(conn, "fund_returns")
        nav_columns = _table_columns(conn, "fund_nav_history")
        missing_risk_columns = sorted(required_risk_columns - risk_columns)
        missing_return_columns = sorted(required_return_columns - return_columns)
        missing_nav_columns = sorted(required_nav_columns - nav_columns)
        if missing_risk_columns:
            blockers.append(f"risk_recalculation_fields_missing:fund_risk_metrics:{','.join(missing_risk_columns)}")
        if missing_return_columns:
            blockers.append(f"risk_recalculation_fields_missing:fund_returns:{','.join(missing_return_columns)}")
        if missing_nav_columns:
            blockers.append(f"risk_recalculation_fields_missing:fund_nav_history:{','.join(missing_nav_columns)}")
        if blockers:
            return {
                "status": _classify_status(blockers, warnings),
                "blockers": blockers,
                "warnings": warnings,
                "run_id": run_id or None,
                "sample_limit": sample_limit,
                "sampled_code_count": len(sample_codes),
                "missing_columns": {
                    "fund_risk_metrics": missing_risk_columns,
                    "fund_returns": missing_return_columns,
                    "fund_nav_history": missing_nav_columns,
                },
            }

        values_clause = ",".join(["(?)"] * len(sample_codes))
        latest_return_updated_select = (
            "fr.last_updated_at AS last_updated_at"
            if "last_updated_at" in return_columns
            else "NULL AS last_updated_at"
        )
        rows = conn.execute(
            f"""
            WITH sample_codes(fcode) AS (
                VALUES {values_clause}
            )
            SELECT
                sample_codes.fcode AS sample_fcode,
                risk.rowid AS risk_metric_rowid,
                risk.period,
                risk.start_date,
                risk.end_date,
                risk.annual_return,
                risk.volatility,
                risk.max_drawdown,
                risk.sharpe_ratio,
                risk.sortino_ratio,
                risk.raw_sharpe_ratio,
                risk.raw_sortino_ratio,
                risk.sharpe_ratio_cap_hit,
                risk.sortino_ratio_cap_hit,
                risk.calmar_ratio,
                risk.var_95,
                risk.var_99,
                risk.positive_ratio,
                risk.data_points,
                risk.nav_snapshot_signature,
                fr.latest_nav_date,
                fr.latest_nav,
                fr.daily_growth,
                fr.return_1w,
                fr.return_1m,
                fr.return_3m,
                fr.return_6m,
                fr.return_1y,
                fr.return_2y,
                fr.return_3y,
                fr.return_5y,
                fr.return_ytd,
                fr.return_since,
                {latest_return_updated_select}{_risk_recalculation_return_provenance_select(return_columns)}
            FROM sample_codes
            LEFT JOIN fund_risk_metrics risk
              ON risk.rowid = (
                  SELECT latest_risk.rowid
                  FROM fund_risk_metrics latest_risk
                  WHERE latest_risk.fcode = sample_codes.fcode
                    AND latest_risk.period = ?
                  ORDER BY COALESCE(TRIM(latest_risk.end_date), '') DESC, latest_risk.rowid DESC
                  LIMIT 1
              )
            LEFT JOIN fund_returns fr ON fr.fcode = sample_codes.fcode
            ORDER BY sample_codes.fcode
            """,
            (*sample_codes, RISK_RECALCULATION_PERIOD),
        ).fetchall()

        nav_archive_attach, nav_archive_detach, nav_source_sql = _risk_recalculation_nav_source_for_audit(
            conn,
            settings,
        )
        try:
            runtime_config = load_runtime_config(getattr(settings, "config_path", None))
            risk_metrics_config = runtime_config.setdefault("risk_metrics", {})
            if getattr(settings, "exclude_holding_days_gt", None) is not None:
                risk_metrics_config["skip_risk_metrics_for_holding_days_gt"] = int(
                    getattr(settings, "exclude_holding_days_gt")
                )
            analyzer = RiskAnalyzer(config=runtime_config, db_manager=object())
            for row in rows:
                code = _normalize_fund_code(row["sample_fcode"])
                if not code:
                    continue
                if not row["period"]:
                    missing_risk_rows.append(code)
                    continue
                if row["latest_nav_date"] is None:
                    missing_return_rows.append(code)
                    continue
                start_date = _clean_business_text(row["start_date"])
                end_date = _clean_business_text(row["end_date"])
                if not start_date or not end_date:
                    missing_nav_rows.append(code)
                    continue
                query_start_date = _risk_recalculation_query_start_date(
                    end_date,
                    RISK_RECALCULATION_PERIOD,
                )
                nav_frame = _read_recalculation_nav_frame(
                    conn,
                    nav_source_sql,
                    fcode=code,
                    start_date=query_start_date,
                    end_date=end_date,
                )
                if nav_frame.empty:
                    missing_nav_rows.append(code)
                    continue
                returns_snapshot = _risk_recalculation_return_snapshot(row)
                nav_snapshot_signature = build_nav_snapshot_signature(returns_snapshot)
                recalculated = analyzer._calculate_period_metrics(
                    nav_frame,
                    RISK_RECALCULATION_PERIOD,
                    api_return=row["return_1y"],
                    api_return_date=returns_snapshot.get("period_return_date") if has_dated_period_snapshot(
                        returns_snapshot, verify_payload=True) else None,
                    api_snapshot_date=row["latest_nav_date"],
                    nav_snapshot_signature=nav_snapshot_signature,
                )
                if not recalculated:
                    missing_nav_rows.append(code)
                    continue

                field_mismatches: dict[str, Any] = {}
                for field, digits in RISK_RECALCULATION_NUMERIC_FIELDS.items():
                    mismatch = _numeric_metric_mismatch(row[field], recalculated.get(field), digits)
                    if mismatch:
                        field_mismatches[field] = mismatch
                for field in RISK_RECALCULATION_FLAG_FIELDS:
                    mismatch = _flag_metric_mismatch(row[field], recalculated.get(field))
                    if mismatch:
                        field_mismatches[field] = mismatch
                stored_data_points = _safe_optional_int(row["data_points"])
                expected_data_points = _safe_optional_int(recalculated.get("data_points"))
                if stored_data_points != expected_data_points:
                    field_mismatches["data_points"] = {
                        "stored": stored_data_points,
                        "expected": expected_data_points,
                    }
                expected_start_date = recalculated.get("start_date")
                expected_end_date = recalculated.get("end_date")
                expected_start_text = (
                    pd.Timestamp(expected_start_date).date().isoformat()
                    if expected_start_date is not None
                    else ""
                )
                expected_end_text = (
                    pd.Timestamp(expected_end_date).date().isoformat()
                    if expected_end_date is not None
                    else ""
                )
                if start_date != expected_start_text:
                    field_mismatches["start_date"] = {
                        "stored": start_date,
                        "expected": expected_start_text,
                    }
                if end_date != expected_end_text:
                    field_mismatches["end_date"] = {
                        "stored": end_date,
                        "expected": expected_end_text,
                    }
                stored_signature = _clean_business_text(row["nav_snapshot_signature"])
                if stored_signature != nav_snapshot_signature:
                    field_mismatches["nav_snapshot_signature"] = {
                        "stored": stored_signature,
                        "expected": nav_snapshot_signature,
                    }

                if field_mismatches:
                    metric_mismatch_count += 1
                    if len(metric_mismatch_sample) < 20:
                        metric_mismatch_sample.append(
                            {
                                "fcode": code,
                                "risk_metric_rowid": _safe_optional_int(row["risk_metric_rowid"]),
                                "period": RISK_RECALCULATION_PERIOD,
                                "fields": field_mismatches,
                            }
                        )
                recalculated_rows.append(
                    {
                        "fcode": code,
                        "risk_metric_rowid": _safe_optional_int(row["risk_metric_rowid"]),
                        "start_date": start_date,
                        "end_date": end_date,
                        "data_points": stored_data_points,
                        "nav_rows_used": int(len(nav_frame.index)),
                    }
                )
        finally:
            if nav_archive_detach is not None:
                nav_archive_detach(conn)

    if missing_risk_rows:
        blockers.append("formal_risk_recalculation_missing_risk_rows")
    if missing_return_rows:
        blockers.append("formal_risk_recalculation_missing_fund_returns_rows")
    if missing_nav_rows:
        blockers.append("formal_risk_recalculation_missing_or_invalid_nav_rows")
    if metric_mismatch_count:
        blockers.append("formal_risk_recalculation_metric_mismatch")

    return {
        "status": _classify_status(blockers, warnings),
        "blockers": blockers,
        "warnings": warnings,
        "run_id": run_id or None,
        "period": RISK_RECALCULATION_PERIOD,
        "sample_limit": sample_limit,
        "sampled_code_count": len(sample_codes),
        "recalculated_code_count": len(recalculated_rows),
        "metric_mismatch_count": metric_mismatch_count,
        "metric_mismatch_sample": metric_mismatch_sample,
        "missing_risk_rows": missing_risk_rows[:20],
        "missing_return_rows": missing_return_rows[:20],
        "missing_nav_rows": missing_nav_rows[:20],
        "recalculated_sample": recalculated_rows[:20],
        "formula_engine": "core/analyzers/risk_analyzer.py::RiskAnalyzer._calculate_period_metrics",
        "input_sources": {
            "fund_nav_history": "SQLite fund_nav_history hot table plus bound archive when available",
            "fund_returns": "SQLite fund_returns snapshot fields",
            "fund_risk_metrics": "SQLite latest period row by end_date DESC and rowid DESC",
        },
        "truth_note": (
            "Read-only replay recomputes the formal-list 1y risk metric sample from real SQLite NAV history "
            "and fund_returns snapshots using the production RiskAnalyzer formula, then compares rounded "
            "database fields. It does not call upstream APIs, write data, or change thresholds."
        ),
    }


def _risk_metric_recalculation_truth(
    settings: Settings,
    *,
    sample_limit: int = RISK_RECALCULATION_SAMPLE_LIMIT,
) -> dict[str, Any]:
    blockers: list[str] = []
    warnings: list[str] = []
    latest = _read_json(settings.latest_report_file)
    run_id = str(latest.get("run_id") or "").strip()
    final_count = _safe_int(latest.get("final_count"))
    artifacts = latest.get("artifacts") if isinstance(latest.get("artifacts"), dict) else {}
    filtered_csv_path = _resolve_project_path(settings, artifacts.get("filtered_csv"))
    final_codes = _read_csv_codes(filtered_csv_path) if filtered_csv_path and filtered_csv_path.exists() else []
    normalized_final_codes = _dedupe_preserve_order(final_codes)
    csv_rows_by_code = (
        _read_csv_rows_by_code(filtered_csv_path)
        if filtered_csv_path and filtered_csv_path.exists()
        else {}
    )
    sample_limit = max(0, int(sample_limit or 0))

    if final_count and normalized_final_codes and len(normalized_final_codes) != final_count:
        blockers.append("formal_filtered_csv_code_count_not_equal_final_count_for_risk_recalculation")
    if final_count and not normalized_final_codes:
        blockers.append("formal_filtered_csv_missing_for_risk_recalculation")

    if not settings.database_path.exists():
        return {
            "status": "needs_review",
            "blockers": ["database_missing"],
            "warnings": warnings,
            "run_id": run_id or None,
            "sample_limit": sample_limit,
            "sampled_code_count": 0,
        }

    required_tables = ("fund_risk_metrics", "fund_returns", "fund_nav_history")
    required_risk_columns = {
        "fcode",
        "period",
        "start_date",
        "end_date",
        "annual_return",
        "volatility",
        "max_drawdown",
        "sharpe_ratio",
        "sortino_ratio",
        "raw_sharpe_ratio",
        "raw_sortino_ratio",
        "sharpe_ratio_cap_hit",
        "sortino_ratio_cap_hit",
        "calmar_ratio",
        "var_95",
        "var_99",
        "positive_ratio",
        "data_points",
        "nav_snapshot_signature",
    }
    required_return_columns = {"fcode", *RISK_RECALCULATION_RETURN_FIELDS}
    required_nav_columns = {"fcode", "nav_date", "unit_nav"}

    metric_mismatch_count = 0
    metric_mismatch_sample: list[dict[str, Any]] = []
    missing_required_risk_rows: list[str] = []
    optional_missing_risk_rows: list[dict[str, Any]] = []
    missing_return_rows: list[str] = []
    missing_nav_rows: list[str] = []
    recalculated_rows: list[dict[str, Any]] = []
    sample_reason_summary: dict[str, int] = {}
    period_stats: dict[str, dict[str, Any]] = {
        period: {
            "period": period,
            "final_latest_risk_count": 0,
            "sampled_pair_count": 0,
            "recalculated_pair_count": 0,
            "missing_risk_pair_count": 0,
            "metric_mismatch_count": 0,
        }
        for period in RISK_RECALCULATION_PERIODS
    }
    sample_specs: list[dict[str, Any]] = []
    nav_archive_attach: dict[str, Any] = {}

    with _connect_read_only(settings.database_path) as conn:
        missing_tables = [table_name for table_name in required_tables if not _table_exists(conn, table_name)]
        if missing_tables:
            return {
                "status": "needs_review",
                "blockers": [f"risk_recalculation_table_missing:{table_name}" for table_name in missing_tables],
                "warnings": warnings,
                "run_id": run_id or None,
                "sample_limit": sample_limit,
                "sampled_code_count": 0,
            }

        risk_columns = _table_columns(conn, "fund_risk_metrics")
        return_columns = _table_columns(conn, "fund_returns")
        nav_columns = _table_columns(conn, "fund_nav_history")
        missing_risk_columns = sorted(required_risk_columns - risk_columns)
        missing_return_columns = sorted(required_return_columns - return_columns)
        missing_nav_columns = sorted(required_nav_columns - nav_columns)
        if missing_risk_columns:
            blockers.append(f"risk_recalculation_fields_missing:fund_risk_metrics:{','.join(missing_risk_columns)}")
        if missing_return_columns:
            blockers.append(f"risk_recalculation_fields_missing:fund_returns:{','.join(missing_return_columns)}")
        if missing_nav_columns:
            blockers.append(f"risk_recalculation_fields_missing:fund_nav_history:{','.join(missing_nav_columns)}")
        if blockers:
            return {
                "status": _classify_status(blockers, warnings),
                "blockers": blockers,
                "warnings": warnings,
                "run_id": run_id or None,
                "sample_limit": sample_limit,
                "sampled_code_count": 0,
                "missing_columns": {
                    "fund_risk_metrics": missing_risk_columns,
                    "fund_returns": missing_return_columns,
                    "fund_nav_history": missing_nav_columns,
                },
            }

        coverage = _risk_recalculation_period_coverage(conn, normalized_final_codes)
        for period, count in coverage.items():
            period_stats.setdefault(period, {"period": period})["final_latest_risk_count"] = int(count)

        sample_specs = _build_risk_recalculation_sample_specs(
            conn,
            final_codes=normalized_final_codes,
            csv_rows_by_code=csv_rows_by_code,
            sample_limit=sample_limit,
        )
        if not sample_specs:
            return {
                "status": _classify_status(blockers, warnings),
                "blockers": blockers,
                "warnings": warnings,
                "run_id": run_id or None,
                "periods": list(RISK_RECALCULATION_PERIODS),
                "required_period": RISK_RECALCULATION_REQUIRED_PERIOD,
                "sample_limit": sample_limit,
                "sampled_code_count": 0,
                "sampled_period_pair_count": 0,
                "period_summary": list(period_stats.values()),
                "formula_engine": "core/analyzers/risk_analyzer.py::RiskAnalyzer._calculate_period_metrics",
                "truth_note": "No formal codes were available for read-only numerical risk metric replay.",
            }

        for spec in sample_specs:
            period = str(spec.get("period") or "").strip()
            period_stats.setdefault(period, {"period": period})["sampled_pair_count"] = (
                _safe_int(period_stats.setdefault(period, {"period": period}).get("sampled_pair_count")) + 1
            )
            for reason in list(spec.get("sample_reasons") or []):
                reason_text = str(reason or "").strip()
                if reason_text:
                    sample_reason_summary[reason_text] = sample_reason_summary.get(reason_text, 0) + 1

        values_clause = ",".join(["(?,?,?)"] * len(sample_specs))
        params: list[Any] = []
        for spec in sample_specs:
            params.extend(
                [
                    spec["fcode"],
                    spec["period"],
                    json.dumps(list(spec.get("sample_reasons") or []), ensure_ascii=False),
                ]
            )

        latest_return_updated_select = (
            "fr.last_updated_at AS last_updated_at"
            if "last_updated_at" in return_columns
            else "NULL AS last_updated_at"
        )
        rows = conn.execute(
            f"""
            WITH sample_specs(fcode, sample_period, sample_reasons_json) AS (
                VALUES {values_clause}
            )
            SELECT
                sample_specs.fcode AS sample_fcode,
                sample_specs.sample_period AS sample_period,
                sample_specs.sample_reasons_json AS sample_reasons_json,
                risk.rowid AS risk_metric_rowid,
                risk.period,
                risk.start_date,
                risk.end_date,
                risk.annual_return,
                risk.volatility,
                risk.max_drawdown,
                risk.sharpe_ratio,
                risk.sortino_ratio,
                risk.raw_sharpe_ratio,
                risk.raw_sortino_ratio,
                risk.sharpe_ratio_cap_hit,
                risk.sortino_ratio_cap_hit,
                risk.calmar_ratio,
                risk.var_95,
                risk.var_99,
                risk.positive_ratio,
                risk.data_points,
                risk.nav_snapshot_signature,
                fr.latest_nav_date,
                fr.latest_nav,
                fr.daily_growth,
                fr.return_1w,
                fr.return_1m,
                fr.return_3m,
                fr.return_6m,
                fr.return_1y,
                fr.return_2y,
                fr.return_3y,
                fr.return_5y,
                fr.return_ytd,
                fr.return_since,
                {latest_return_updated_select}{_risk_recalculation_return_provenance_select(return_columns)}
            FROM sample_specs
            LEFT JOIN fund_risk_metrics risk
              ON risk.rowid = (
                  SELECT latest_risk.rowid
                  FROM fund_risk_metrics latest_risk
                  WHERE latest_risk.fcode = sample_specs.fcode
                    AND latest_risk.period = sample_specs.sample_period
                  ORDER BY COALESCE(TRIM(latest_risk.end_date), '') DESC, latest_risk.rowid DESC
                  LIMIT 1
              )
            LEFT JOIN fund_returns fr ON fr.fcode = sample_specs.fcode
            ORDER BY sample_specs.fcode, sample_specs.sample_period
            """,
            tuple(params),
        ).fetchall()

        nav_archive_attach, nav_archive_detach, nav_source_sql = _risk_recalculation_nav_source_for_audit(
            conn,
            settings,
        )
        try:
            runtime_config = load_runtime_config(getattr(settings, "config_path", None))
            risk_metrics_config = runtime_config.setdefault("risk_metrics", {})
            if getattr(settings, "exclude_holding_days_gt", None) is not None:
                risk_metrics_config["skip_risk_metrics_for_holding_days_gt"] = int(
                    getattr(settings, "exclude_holding_days_gt")
                )
            analyzer = RiskAnalyzer(config=runtime_config, db_manager=object())
            for row in rows:
                code = _normalize_fund_code(row["sample_fcode"])
                period = str(row["sample_period"] or "").strip()
                if not code or not period:
                    continue
                try:
                    sample_reasons = json.loads(str(row["sample_reasons_json"] or "[]"))
                except json.JSONDecodeError:
                    sample_reasons = []
                if not isinstance(sample_reasons, list):
                    sample_reasons = []

                period_stat = period_stats.setdefault(period, {"period": period})
                if not row["period"]:
                    period_stat["missing_risk_pair_count"] = _safe_int(
                        period_stat.get("missing_risk_pair_count")
                    ) + 1
                    if period == RISK_RECALCULATION_REQUIRED_PERIOD:
                        missing_required_risk_rows.append(f"{code}:{period}")
                    else:
                        optional_missing_risk_rows.append(
                            {"fcode": code, "period": period, "sample_reasons": sample_reasons}
                        )
                    continue
                if row["latest_nav_date"] is None:
                    missing_return_rows.append(f"{code}:{period}")
                    continue

                start_date = _clean_business_text(row["start_date"])
                end_date = _clean_business_text(row["end_date"])
                if not start_date or not end_date:
                    missing_nav_rows.append(f"{code}:{period}")
                    continue
                query_start_date = _risk_recalculation_query_start_date(end_date, period)
                nav_frame = _read_recalculation_nav_frame(
                    conn,
                    nav_source_sql,
                    fcode=code,
                    start_date=query_start_date,
                    end_date=end_date,
                )
                if nav_frame.empty:
                    missing_nav_rows.append(f"{code}:{period}")
                    continue

                returns_snapshot = _risk_recalculation_return_snapshot(row)
                nav_snapshot_signature = build_nav_snapshot_signature(returns_snapshot)
                api_return_field = _risk_recalculation_api_return_field(period)
                recalculated = analyzer._calculate_period_metrics(
                    nav_frame,
                    period,
                    api_return=row[api_return_field] if api_return_field else None,
                    api_return_date=returns_snapshot.get("period_return_date") if has_dated_period_snapshot(
                        returns_snapshot, verify_payload=True) else None,
                    api_snapshot_date=row["latest_nav_date"],
                    nav_snapshot_signature=nav_snapshot_signature,
                )
                if not recalculated:
                    missing_nav_rows.append(f"{code}:{period}")
                    continue

                field_mismatches, stored_data_points = _risk_recalculation_compare_row(
                    row,
                    recalculated,
                    start_date=start_date,
                    end_date=end_date,
                    nav_snapshot_signature=nav_snapshot_signature,
                )
                if field_mismatches:
                    metric_mismatch_count += 1
                    period_stat["metric_mismatch_count"] = _safe_int(
                        period_stat.get("metric_mismatch_count")
                    ) + 1
                    if len(metric_mismatch_sample) < 20:
                        metric_mismatch_sample.append(
                            {
                                "fcode": code,
                                "risk_metric_rowid": _safe_optional_int(row["risk_metric_rowid"]),
                                "period": period,
                                "sample_reasons": sample_reasons,
                                "fields": field_mismatches,
                            }
                        )
                period_stat["recalculated_pair_count"] = _safe_int(
                    period_stat.get("recalculated_pair_count")
                ) + 1
                recalculated_rows.append(
                    {
                        "fcode": code,
                        "period": period,
                        "sample_reasons": sample_reasons,
                        "risk_metric_rowid": _safe_optional_int(row["risk_metric_rowid"]),
                        "start_date": start_date,
                        "end_date": end_date,
                        "data_points": stored_data_points,
                        "nav_rows_used": int(len(nav_frame.index)),
                    }
                )
        finally:
            if nav_archive_detach is not None:
                nav_archive_detach(conn)

    if missing_required_risk_rows:
        blockers.append("formal_risk_recalculation_missing_risk_rows")
    if missing_return_rows:
        blockers.append("formal_risk_recalculation_missing_fund_returns_rows")
    if missing_nav_rows:
        blockers.append("formal_risk_recalculation_missing_or_invalid_nav_rows")
    if metric_mismatch_count:
        blockers.append("formal_risk_recalculation_metric_mismatch")

    sampled_codes = _dedupe_preserve_order([spec.get("fcode") for spec in sample_specs])
    recalculated_codes = _dedupe_preserve_order([row.get("fcode") for row in recalculated_rows])
    return {
        "status": _classify_status(blockers, warnings),
        "blockers": blockers,
        "warnings": warnings,
        "run_id": run_id or None,
        "period": RISK_RECALCULATION_REQUIRED_PERIOD,
        "periods": list(RISK_RECALCULATION_PERIODS),
        "required_period": RISK_RECALCULATION_REQUIRED_PERIOD,
        "sample_limit": sample_limit,
        "sampled_code_count": len(sampled_codes),
        "sampled_period_pair_count": len(sample_specs),
        "recalculated_code_count": len(recalculated_codes),
        "recalculated_period_pair_count": len(recalculated_rows),
        "metric_mismatch_count": metric_mismatch_count,
        "metric_mismatch_sample": metric_mismatch_sample,
        "missing_risk_rows": missing_required_risk_rows[:20],
        "missing_required_risk_rows": missing_required_risk_rows[:20],
        "optional_missing_risk_rows": optional_missing_risk_rows[:20],
        "optional_missing_risk_pair_count": len(optional_missing_risk_rows),
        "missing_return_rows": missing_return_rows[:20],
        "missing_nav_rows": missing_nav_rows[:20],
        "sample_reason_summary": sample_reason_summary,
        "period_summary": [period_stats[period] for period in RISK_RECALCULATION_PERIODS],
        "recalculated_sample": recalculated_rows[:20],
        "nav_history_archive_attach": nav_archive_attach,
        "formula_engine": "core/analyzers/risk_analyzer.py::RiskAnalyzer._calculate_period_metrics",
        "input_sources": {
            "fund_nav_history": "SQLite fund_nav_history hot table plus bound archive when available",
            "fund_returns": "SQLite fund_returns snapshot fields",
            "fund_risk_metrics": "SQLite latest period row by end_date DESC and rowid DESC",
        },
        "truth_note": (
            "Read-only replay recomputes formal-list risk metric samples across 1y/3y/5y from real SQLite NAV "
            "history and fund_returns snapshots using the production RiskAnalyzer formula, then compares rounded "
            "database fields. The required blocker period is 1y; 3y/5y missing rows preserve missing semantics "
            "because younger funds may lack mature history. It does not call upstream APIs, write data, or change "
            "thresholds."
        ),
    }


def _complete_calendar_year_window(year_count: int) -> list[int]:
    normalized_count = max(1, int(year_count or 0))
    last_complete_year = datetime.now().year - 1
    first_year = last_complete_year - normalized_count + 1
    return list(range(first_year, last_complete_year + 1))


def _resolve_rank_stale_days(settings: Settings) -> int:
    try:
        value = int(getattr(settings, "scheduler_long_term_history_rank_stale_days", 0) or 0)
    except (TypeError, ValueError):
        value = 0
    return max(1, value or RANK_HISTORY_STALE_DAYS_DEFAULT)


def _date_lag_from_today(value: Any) -> int | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text[:10]).date()
    except ValueError:
        return None
    return max(0, (datetime.now().date() - parsed).days)


def _annual_breakdown_entry_from_db_row(row: sqlite3.Row) -> dict[str, Any] | None:
    if row["year"] is None:
        return None
    estab_year = 0
    estab_text = str(row["estab_date"] or "").strip()
    if estab_text:
        try:
            estab_year = datetime.fromisoformat(estab_text[:10]).year
        except ValueError:
            estab_year = 0
    year = _safe_optional_int(row["year"])
    annual_return = _safe_float(row["annual_return"])
    percentile = _safe_float(row["percentile"])
    return {
        "year": year,
        "annual_return": _format_number_text(annual_return, digits=2) if annual_return is not None else "",
        "rank_in_category": _safe_optional_int(row["rank_in_category"]),
        "category_count": _safe_optional_int(row["category_count"]),
        "percentile": _format_number_text(percentile * 100.0, digits=2) if percentile is not None else "",
        "is_partial_year": bool(estab_year and year is not None and year <= estab_year),
    }


def _normalize_annual_breakdown_sidecar_entries(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    entries: list[dict[str, Any]] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        year = _safe_optional_int(item.get("year"))
        if year is None:
            continue
        entries.append(
            {
                "year": year,
                "annual_return": _format_number_text(item.get("annual_return"), digits=2)
                if _clean_business_text(item.get("annual_return"))
                else "",
                "rank_in_category": _safe_optional_int(item.get("rank_in_category")),
                "category_count": _safe_optional_int(item.get("category_count")),
                "percentile": _format_number_text(item.get("percentile"), digits=2)
                if _clean_business_text(item.get("percentile"))
                else "",
                "is_partial_year": bool(item.get("is_partial_year")),
            }
        )
    return sorted(entries, key=lambda item: (item["year"] or 0))


def _load_latest_long_term_history_maintenance_artifact(
    settings: Settings,
) -> tuple[Path | None, dict[str, Any] | None]:
    if not hasattr(settings, "reports_dir"):
        return None, None
    try:
        from backend.long_term_history_maintenance import (  # noqa: WPS433
            load_latest_long_term_history_maintenance_payload,
        )
    except Exception:
        return None, None
    try:
        return load_latest_long_term_history_maintenance_payload(settings)
    except AttributeError:
        return None, None


def _validate_long_term_source_entry(
    entry: Any,
    *,
    index: int,
) -> tuple[dict[str, Any], list[str], list[str]]:
    blockers: list[str] = []
    warnings: list[str] = []
    if not isinstance(entry, dict):
        return {}, [f"source_entry_invalid:{index}"], warnings

    task = str(entry.get("task") or "").strip()
    source = str(entry.get("source") or "").strip()
    expected_source_by_task = {
        "collect-annual-returns": "fundMNPeriodIncrease",
        "collect-rank-history": "fundRankDiagram",
    }
    expected_source = expected_source_by_task.get(task)
    if not task:
        blockers.append(f"source_task_missing:{index}")
    elif expected_source is None:
        blockers.append(f"source_task_unexpected:{index}:{task}")
    elif source != expected_source:
        blockers.append(f"source_endpoint_mismatch:{index}:{task}:{source}")

    target_count = _safe_int(entry.get("target_count"))
    request_count = _safe_int(entry.get("request_count"))
    response_count = _safe_int(entry.get("response_count"))
    accepted_response_count = _safe_int(entry.get("accepted_response_count"))
    failed_response_count = _safe_int(entry.get("failed_response_count"))
    has_data_response_count = _safe_int(entry.get("has_data_response_count"))
    empty_response_count = _safe_int(entry.get("empty_response_count"))
    transport_exception_count = _safe_int(entry.get("transport_exception_count"))
    returned_record_count = _safe_int(entry.get("returned_record_count"))
    saved_fund_count = _safe_int(entry.get("saved_fund_count"))
    written_record_count = _safe_int(entry.get("written_record_count"))
    save_failed_count = _safe_int(entry.get("save_failed_count"))

    if target_count > 0 and request_count <= 0:
        blockers.append(f"source_request_empty:{index}:{task or 'unknown'}")
    if target_count > 0 and request_count != target_count:
        blockers.append(f"source_request_target_count_mismatch:{index}:{task or 'unknown'}")
    if request_count != response_count + transport_exception_count:
        blockers.append(f"source_request_response_transport_mismatch:{index}:{task or 'unknown'}")
    if accepted_response_count > response_count:
        blockers.append(f"source_accepted_gt_response:{index}:{task or 'unknown'}")
    if accepted_response_count + failed_response_count != response_count + transport_exception_count:
        blockers.append(f"source_accept_failed_total_mismatch:{index}:{task or 'unknown'}")
    if response_count > 0 and has_data_response_count + empty_response_count != response_count:
        blockers.append(f"source_data_empty_total_mismatch:{index}:{task or 'unknown'}")
    if has_data_response_count > accepted_response_count:
        blockers.append(f"source_has_data_gt_accepted:{index}:{task or 'unknown'}")
    if failed_response_count > 0:
        blockers.append(f"source_failed_response_present:{index}:{task or 'unknown'}")
    if transport_exception_count > 0:
        blockers.append(f"source_transport_exception_present:{index}:{task or 'unknown'}")
    if save_failed_count > 0:
        blockers.append(f"source_save_failure_present:{index}:{task or 'unknown'}")
    if empty_response_count > 0:
        warnings.append(f"source_empty_response_present:{index}:{task or 'unknown'}")
    if saved_fund_count > has_data_response_count:
        blockers.append(f"source_saved_gt_has_data:{index}:{task or 'unknown'}")
    if saved_fund_count > accepted_response_count:
        blockers.append(f"source_saved_gt_accepted:{index}:{task or 'unknown'}")
    if written_record_count > returned_record_count:
        blockers.append(f"source_written_gt_returned:{index}:{task or 'unknown'}")
    if written_record_count > 0 and saved_fund_count <= 0:
        blockers.append(f"source_written_without_saved_fund:{index}:{task or 'unknown'}")
    if saved_fund_count > 0 and written_record_count <= 0:
        blockers.append(f"source_saved_without_written_records:{index}:{task or 'unknown'}")

    quality_status_counts = entry.get("quality_status_counts")
    status_counts = quality_status_counts if isinstance(quality_status_counts, dict) else {}
    if quality_status_counts is not None and not isinstance(quality_status_counts, dict):
        blockers.append(f"source_quality_status_counts_invalid:{index}:{task or 'unknown'}")
    if quality_status_counts is None and response_count + transport_exception_count > 0:
        blockers.append(f"source_quality_status_counts_missing:{index}:{task or 'unknown'}")
    allowed_statuses = {
        "ok",
        "empty_period_records",
        "empty_rank_records",
        "transport_exception",
    }
    bad_statuses = sorted(
        str(status)
        for status, count in status_counts.items()
        if _safe_int(count) > 0 and str(status) not in allowed_statuses
    )
    if bad_statuses:
        blockers.append(
            f"source_quality_status_not_ok:{index}:{task or 'unknown'}:{','.join(bad_statuses)}"
        )
    status_total = sum(_safe_int(value) for value in status_counts.values())
    if status_counts and status_total != response_count + transport_exception_count:
        blockers.append(f"source_quality_status_total_mismatch:{index}:{task or 'unknown'}")

    quality_payload_type_counts = entry.get("quality_payload_type_counts")
    payload_type_counts = quality_payload_type_counts if isinstance(quality_payload_type_counts, dict) else {}
    if quality_payload_type_counts is not None and not isinstance(quality_payload_type_counts, dict):
        blockers.append(f"source_quality_payload_type_counts_invalid:{index}:{task or 'unknown'}")
    if quality_payload_type_counts is None and response_count > 0:
        blockers.append(f"source_quality_payload_type_counts_missing:{index}:{task or 'unknown'}")
    allowed_payload_types = {"list"}
    bad_payload_types = sorted(
        str(payload_type)
        for payload_type, count in payload_type_counts.items()
        if _safe_int(count) > 0 and str(payload_type) not in allowed_payload_types
    )
    if bad_payload_types:
        blockers.append(
            f"source_quality_payload_type_not_ok:{index}:{task or 'unknown'}:{','.join(bad_payload_types)}"
        )
    payload_type_total = sum(_safe_int(value) for value in payload_type_counts.values())
    if payload_type_counts and response_count > 0 and payload_type_total != response_count:
        blockers.append(f"source_quality_payload_type_total_mismatch:{index}:{task or 'unknown'}")

    return {
        "task": task,
        "source": source,
        "target_count": target_count,
        "request_count": request_count,
        "response_count": response_count,
        "accepted_response_count": accepted_response_count,
        "failed_response_count": failed_response_count,
        "empty_response_count": empty_response_count,
        "transport_exception_count": transport_exception_count,
        "returned_record_count": returned_record_count,
        "saved_fund_count": saved_fund_count,
        "written_record_count": written_record_count,
        "save_failed_count": save_failed_count,
        "quality_status_counts": dict(status_counts),
        "quality_payload_type_counts": dict(payload_type_counts),
    }, blockers, warnings


def _long_term_history_api_source_evidence_truth(settings: Settings) -> dict[str, Any]:
    blockers: list[str] = []
    warnings: list[str] = []
    artifact_path, payload = _load_latest_long_term_history_maintenance_artifact(settings)
    result: dict[str, Any] = {
        "available": False,
        "status": "not_available",
        "artifact_path": str(artifact_path) if artifact_path else "",
        "blockers": blockers,
        "warnings": warnings,
        "interpretation": (
            "No long-term history maintenance artifact with API source evidence is available yet; "
            "legacy artifacts are not treated as proof of annual/rank source truth."
        ),
    }
    if artifact_path and payload is None:
        blockers.append("long_term_history_maintenance_artifact_invalid_json")
        result["status"] = _classify_status(blockers, warnings)
        return result
    if not isinstance(payload, dict) or not payload:
        return result

    maintenance_result = payload.get("maintenance_result") if isinstance(payload.get("maintenance_result"), dict) else {}
    evidence = maintenance_result.get("api_source_evidence")
    if not isinstance(evidence, dict):
        evidence = payload.get("api_source_evidence") if isinstance(payload.get("api_source_evidence"), dict) else {}
    if not evidence:
        return result

    annual_target_total = _safe_int(evidence.get("annual_target_total"))
    rank_target_total = _safe_int(evidence.get("rank_target_total"))
    annual_failed_total = _safe_int(evidence.get("annual_failed_total"))
    rank_failed_total = _safe_int(evidence.get("rank_failed_total"))
    sources = evidence.get("sources") if isinstance(evidence.get("sources"), list) else []
    if annual_target_total + rank_target_total > 0 and not sources:
        blockers.append("source_entries_missing_for_positive_target")

    source_summaries: list[dict[str, Any]] = []
    for index, entry in enumerate(sources, start=1):
        source_summary, entry_blockers, entry_warnings = _validate_long_term_source_entry(entry, index=index)
        if source_summary:
            source_summaries.append(source_summary)
        blockers.extend(entry_blockers)
        warnings.extend(entry_warnings)

    source_summaries_by_task: dict[str, list[dict[str, Any]]] = {}
    for source_summary in source_summaries:
        source_summaries_by_task.setdefault(str(source_summary.get("task") or ""), []).append(source_summary)
    expected_target_by_task = {
        "collect-annual-returns": annual_target_total,
        "collect-rank-history": rank_target_total,
    }
    for task, expected_target in expected_target_by_task.items():
        task_summaries = source_summaries_by_task.get(task) or []
        if expected_target > 0 and not task_summaries:
            blockers.append(f"source_entry_missing_for_target:{task}")
            continue
        if len(task_summaries) > 1:
            blockers.append(f"source_entry_duplicate:{task}")
        task_target_total = sum(_safe_int(item.get("target_count")) for item in task_summaries)
        if expected_target > 0 and task_target_total != expected_target:
            blockers.append(f"source_target_total_mismatch:{task}")

    total_request_count = sum(_safe_int(item.get("request_count")) for item in source_summaries)
    total_failed_response_count = sum(_safe_int(item.get("failed_response_count")) for item in source_summaries)
    total_transport_exception_count = sum(_safe_int(item.get("transport_exception_count")) for item in source_summaries)
    total_save_failed_count = sum(_safe_int(item.get("save_failed_count")) for item in source_summaries)
    expected_totals = {
        "total_request_count": total_request_count,
        "total_failed_response_count": total_failed_response_count,
        "total_transport_exception_count": total_transport_exception_count,
        "total_save_failed_count": total_save_failed_count,
    }
    for key, expected_value in expected_totals.items():
        actual_value = _safe_int(evidence.get(key))
        if actual_value != expected_value:
            blockers.append(f"{key}_mismatch")
        if key != "total_request_count" and actual_value > 0:
            blockers.append(f"{key}_present")
    if annual_failed_total > 0:
        warnings.append("annual_unresolved_target_total_present")
    if rank_failed_total > 0:
        warnings.append("rank_unresolved_target_total_present")

    result.update(
        {
            "available": True,
            "status": _classify_status(blockers, warnings),
            "schema_version": evidence.get("schema_version"),
            "task": evidence.get("task"),
            "context": evidence.get("context") if isinstance(evidence.get("context"), dict) else {},
            "annual_target_total": annual_target_total,
            "rank_target_total": rank_target_total,
            "annual_success_total": _safe_int(evidence.get("annual_success_total")),
            "rank_success_total": _safe_int(evidence.get("rank_success_total")),
            "annual_failed_total": annual_failed_total,
            "rank_failed_total": rank_failed_total,
            "total_request_count": _safe_int(evidence.get("total_request_count")),
            "total_failed_response_count": _safe_int(evidence.get("total_failed_response_count")),
            "total_transport_exception_count": _safe_int(evidence.get("total_transport_exception_count")),
            "total_save_failed_count": _safe_int(evidence.get("total_save_failed_count")),
            "source_count": len(sources),
            "source_summaries": source_summaries,
            "interpretation": (
                "Long-term annual-return and rank-history collectors must expose request/response/save "
                "evidence from real Tiantian API calls. Actual source failures are identified from "
                "failed_response_count, transport_exception_count, save_failed_count and non-ok quality "
                "statuses; unresolved target totals after a successful source read are progress warnings, "
                "not fabricated data or source failures."
            ),
        }
    )
    return result


def _validate_autostock_source_evidence_entry(
    entry: Any,
    *,
    index: int,
    required: bool,
) -> tuple[dict[str, Any], list[str], list[str]]:
    blockers: list[str] = []
    warnings: list[str] = []
    if not isinstance(entry, dict):
        return {}, [f"source_evidence_entry_invalid:{index}"], []

    source_type = str(entry.get("source_type") or "").strip()
    task = str(entry.get("task") or "").strip()
    request_count = _safe_int(entry.get("request_count"))
    response_count = _safe_int(entry.get("response_count"))
    accepted_response_count = _safe_int(entry.get("accepted_response_count"))
    failed_response_count = _safe_int(entry.get("failed_response_count"))
    transport_exception_count = _safe_int(entry.get("transport_exception_count"))
    target_count = _safe_int(entry.get("target_count"))
    api_target_count = _safe_int(entry.get("api_target_count"))
    returned_record_count = _safe_int(entry.get("returned_record_count"))
    returned_unique_count = _safe_int(entry.get("returned_unique_count"))
    missing_count = _safe_int(entry.get("missing_count"))
    failed_batch_count = _safe_int(entry.get("failed_batch_count"))
    cache_reused_count = _safe_int(entry.get("cache_reused_count"))
    source_complete = bool(entry.get("source_complete"))
    quota_exhausted = bool(entry.get("quota_exhausted"))
    response_hash = str(entry.get("response_hash") or "").strip()
    endpoint_path = str(entry.get("endpoint_path") or "").strip()
    request_window = entry.get("request_window") if isinstance(entry.get("request_window"), dict) else {}
    started_at = str(request_window.get("started_at") or "").strip()
    finished_at = str(request_window.get("finished_at") or "").strip()
    sample_codes = entry.get("sample_codes") if isinstance(entry.get("sample_codes"), list) else []
    missing_codes_preview = (
        entry.get("missing_codes_preview")
        if isinstance(entry.get("missing_codes_preview"), list)
        else []
    )
    failed_batches = entry.get("failed_batches") if isinstance(entry.get("failed_batches"), list) else None
    requested_unique_total = _safe_optional_int(entry.get("requested_unique_total"))
    requested_total = _safe_optional_int(entry.get("requested_total"))
    requested_batch_size = _safe_optional_int(entry.get("requested_batch_size"))
    effective_batch_size = _safe_optional_int(entry.get("effective_batch_size"))
    max_workers = _safe_optional_int(entry.get("max_workers"))

    if not task:
        blockers.append(f"autostock_source_evidence_task_missing:{index}")
    if source_type not in {"api", "api_batch", "cache", "budget_degraded", "unavailable"}:
        blockers.append(f"autostock_source_evidence_source_type_invalid:{index}:{source_type or 'empty'}")
    if required and target_count <= 0:
        blockers.append(f"autostock_source_evidence_target_empty:{index}")
    if source_type in {"api", "api_batch"}:
        if endpoint_path != "/fund":
            blockers.append(f"autostock_api_endpoint_path_invalid:{index}:{endpoint_path or 'empty'}")
        if not started_at or not finished_at:
            blockers.append(f"autostock_api_request_window_missing:{index}")
        if api_target_count <= 0 and target_count <= 0:
            blockers.append(f"autostock_api_target_empty:{index}")
        if api_target_count > 0 and request_count <= 0:
            blockers.append(f"autostock_api_target_without_request:{index}")
        if request_count <= 0:
            blockers.append(f"autostock_api_request_empty:{index}")
        if response_count <= 0:
            blockers.append(f"autostock_api_response_empty:{index}")
        if accepted_response_count <= 0:
            blockers.append(f"autostock_api_accepted_response_empty:{index}")
        if returned_record_count <= 0:
            blockers.append(f"autostock_api_returned_record_empty:{index}")
        if returned_unique_count <= 0:
            blockers.append(f"autostock_api_returned_unique_empty:{index}")
        if failed_response_count > 0:
            blockers.append(f"autostock_api_failed_response_present:{index}")
        if transport_exception_count > 0:
            blockers.append(f"autostock_api_transport_exception_present:{index}")
        if failed_batch_count > 0:
            blockers.append(f"autostock_api_failed_batch_present:{index}")
        if missing_count > 0:
            blockers.append(f"autostock_api_missing_response_present:{index}")
        if quota_exhausted:
            blockers.append(f"autostock_api_quota_exhausted:{index}")
        if not source_complete:
            blockers.append(f"autostock_api_source_incomplete:{index}")
        if request_count != accepted_response_count + failed_response_count:
            blockers.append(
                f"autostock_api_request_terminal_count_mismatch:{index}:"
                f"{request_count}!={accepted_response_count}+{failed_response_count}"
            )
        if response_count != accepted_response_count:
            blockers.append(
                f"autostock_api_response_acceptance_count_mismatch:{index}:"
                f"{response_count}!={accepted_response_count}"
            )
        if transport_exception_count > failed_response_count:
            blockers.append(
                f"autostock_api_transport_exception_exceeds_failed:{index}:"
                f"{transport_exception_count}>{failed_response_count}"
            )
        if returned_unique_count > returned_record_count:
            blockers.append(
                f"autostock_api_returned_unique_exceeds_records:{index}:"
                f"{returned_unique_count}>{returned_record_count}"
            )
        if api_target_count > 0 and returned_unique_count + missing_count > api_target_count:
            blockers.append(
                f"autostock_api_returned_plus_missing_exceeds_target:{index}:"
                f"{returned_unique_count}+{missing_count}>{api_target_count}"
            )
        if source_complete and api_target_count > 0 and returned_unique_count + missing_count != api_target_count:
            blockers.append(
                f"autostock_api_complete_count_not_closed:{index}:"
                f"{returned_unique_count}+{missing_count}!={api_target_count}"
            )
        if (
            failed_batches is not None
            and failed_batch_count <= 5
            and failed_batch_count != len(failed_batches)
        ):
            blockers.append(
                f"autostock_api_failed_batch_count_mismatch:{index}:"
                f"{failed_batch_count}!={len(failed_batches)}"
            )
        if failed_batches is not None and len(failed_batches) > max(failed_batch_count, 5):
            blockers.append(
                f"autostock_api_failed_batch_preview_exceeds_count:{index}:"
                f"{len(failed_batches)}>{failed_batch_count}"
            )
        if missing_count > 0 and not missing_codes_preview:
            blockers.append(f"autostock_api_missing_codes_preview_empty:{index}")
        if returned_record_count > 0 and not response_hash:
            blockers.append(f"autostock_api_response_hash_missing:{index}")
        if returned_unique_count > 0 and not sample_codes:
            blockers.append(f"autostock_api_sample_codes_missing:{index}")
        if requested_unique_total is not None and api_target_count > 0 and requested_unique_total != api_target_count:
            blockers.append(
                f"autostock_api_requested_unique_target_mismatch:{index}:"
                f"{requested_unique_total}!={api_target_count}"
            )
        if requested_total is not None and requested_unique_total is not None and requested_total < requested_unique_total:
            blockers.append(
                f"autostock_api_requested_total_lt_unique:{index}:"
                f"{requested_total}<{requested_unique_total}"
            )
        if requested_batch_size is not None and requested_batch_size <= 0:
            blockers.append(f"autostock_api_requested_batch_size_invalid:{index}:{requested_batch_size}")
        if effective_batch_size is not None and effective_batch_size <= 0:
            blockers.append(f"autostock_api_effective_batch_size_invalid:{index}:{effective_batch_size}")
        if max_workers is not None and max_workers <= 0:
            blockers.append(f"autostock_api_max_workers_invalid:{index}:{max_workers}")
    elif source_type == "cache":
        if endpoint_path not in {"cache", "/fund", "reports/latest filtered_csv"}:
            warnings.append(f"autostock_cache_endpoint_path_unexpected:{index}:{endpoint_path or 'empty'}")
        if cache_reused_count <= 0 and target_count <= 0:
            warnings.append(f"autostock_cache_reused_count_empty:{index}")
        if not response_hash:
            warnings.append(f"autostock_cache_response_hash_missing:{index}")
        if returned_unique_count > returned_record_count:
            blockers.append(
                f"autostock_cache_returned_unique_exceeds_records:{index}:"
                f"{returned_unique_count}>{returned_record_count}"
            )
    elif source_type in {"budget_degraded", "unavailable"}:
        if required:
            blockers.append(f"autostock_source_unavailable:{index}:{source_type}")
        else:
            warnings.append(f"autostock_source_unavailable:{index}:{source_type}")

    return (
        {
            "index": index,
            "task": task,
            "source": entry.get("source"),
            "source_type": source_type,
            "endpoint_path": endpoint_path,
            "source_complete": source_complete,
            "target_count": target_count,
            "api_target_count": api_target_count,
            "cache_reused_count": cache_reused_count,
            "request_count": request_count,
            "response_count": response_count,
            "accepted_response_count": accepted_response_count,
            "failed_response_count": failed_response_count,
            "transport_exception_count": transport_exception_count,
            "returned_record_count": returned_record_count,
            "returned_unique_count": returned_unique_count,
            "missing_count": missing_count,
            "failed_batch_count": failed_batch_count,
            "quota_exhausted": quota_exhausted,
            "response_hash_available": bool(response_hash),
            "sample_code_count": len(sample_codes),
            "request_window_available": bool(started_at and finished_at),
            "requested_total": requested_total,
            "requested_unique_total": requested_unique_total,
        },
        blockers,
        warnings,
    )


def _autostock_source_evidence_truth(settings: Settings) -> dict[str, Any]:
    blockers: list[str] = []
    warnings: list[str] = []
    latest = _read_json(settings.latest_report_file)
    run_id = str(latest.get("run_id") or "").strip()
    artifacts = latest.get("artifacts") if isinstance(latest.get("artifacts"), dict) else {}
    core_artifacts = artifacts.get("core") if isinstance(artifacts.get("core"), dict) else {}
    audit_path = _resolve_project_path(settings, core_artifacts.get("autostock_validation_audit_file"))
    audit_payload = _read_json(audit_path) if audit_path and audit_path.exists() else {}

    primary_evidence = {}
    if isinstance(audit_payload.get("source_evidence"), dict):
        primary_evidence = dict(audit_payload.get("source_evidence") or {})
    elif isinstance((audit_payload.get("summary") or {}).get("source_evidence"), dict):
        primary_evidence = dict((audit_payload.get("summary") or {}).get("source_evidence") or {})

    report_audit = latest.get("autostock_validation_audit") if isinstance(latest.get("autostock_validation_audit"), dict) else {}
    supplement_entries = [
        dict(item)
        for item in report_audit.get("source_evidence") or []
        if isinstance(item, dict)
    ]
    latest_supplement = report_audit.get("latest_source_evidence")
    if isinstance(latest_supplement, dict) and latest_supplement and latest_supplement not in supplement_entries:
        supplement_entries.append(dict(latest_supplement))

    result: dict[str, Any] = {
        "available": bool(primary_evidence or supplement_entries),
        "status": "not_available",
        "run_id": run_id or None,
        "autostock_validation_audit_file": str(audit_path) if audit_path else "",
        "primary_source_evidence_available": bool(primary_evidence),
        "supplement_source_evidence_count": len(supplement_entries),
        "blockers": blockers,
        "warnings": warnings,
        "interpretation": (
            "AutoStock source evidence must distinguish realtime API batches, cache reuse, quota/budget degradation "
            "and unavailable states. Old artifacts without this field are not treated as proof of current source truth."
        ),
    }
    if audit_path and not audit_path.exists():
        blockers.append("autostock_validation_audit_file_missing")
    if not primary_evidence and not supplement_entries:
        result["status"] = _classify_status(blockers, warnings)
        return result

    entry_summaries: list[dict[str, Any]] = []
    if primary_evidence:
        summary, entry_blockers, entry_warnings = _validate_autostock_source_evidence_entry(
            primary_evidence,
            index=1,
            required=True,
        )
        entry_summaries.append(summary)
        blockers.extend(f"primary:{item}" for item in entry_blockers)
        warnings.extend(f"primary:{item}" for item in entry_warnings)
    else:
        warnings.append("primary_autostock_validation_source_evidence_missing")

    for offset, entry in enumerate(supplement_entries, start=1):
        summary, entry_blockers, entry_warnings = _validate_autostock_source_evidence_entry(
            entry,
            index=offset,
            required=False,
        )
        entry_summaries.append(summary)
        blockers.extend(f"supplement:{item}" for item in entry_blockers)
        warnings.extend(f"supplement:{item}" for item in entry_warnings)

    result.update(
        {
            "status": _classify_status(blockers, warnings),
            "entry_summaries": entry_summaries,
            "api_entry_count": sum(1 for item in entry_summaries if item.get("source_type") in {"api", "api_batch"}),
            "cache_entry_count": sum(1 for item in entry_summaries if item.get("source_type") == "cache"),
            "unavailable_entry_count": sum(1 for item in entry_summaries if item.get("source_type") in {"budget_degraded", "unavailable"}),
        }
    )
    return result


def _load_latest_bond_universe_sync_artifact(
    settings: Settings,
) -> tuple[Path | None, dict[str, Any] | None]:
    project_root = Path(getattr(settings, "project_root", PROJECT_ROOT))
    reports_dir = Path(getattr(settings, "reports_dir", project_root / "reports"))
    audit_dir = reports_dir / "audit" / "bond_universe_sync"
    try:
        candidates = sorted(
            audit_dir.glob("bond_universe_sync_*.json"),
            key=lambda path: path.stat().st_mtime_ns,
            reverse=True,
        )
    except OSError:
        candidates = []
    if not candidates:
        return None, None
    artifact_path = candidates[0]
    payload = _read_json(artifact_path)
    return artifact_path, payload if payload else None


def _autostock_directory_parse_error_codes(
    samples: Sequence[Any],
    explicit_codes: Sequence[Any] | None = None,
) -> list[str]:
    codes: list[str] = []
    seen: set[str] = set()
    for raw_code in explicit_codes or []:
        code = _normalize_fund_code(raw_code)
        if code and code not in seen:
            seen.add(code)
            codes.append(code)
    for sample in samples:
        if not isinstance(sample, dict):
            continue
        code = _normalize_fund_code(sample.get("code"))
        if not code and isinstance(sample.get("row_preview"), list) and sample["row_preview"]:
            code = _normalize_fund_code(sample["row_preview"][0])
        if code and code not in seen:
            seen.add(code)
            codes.append(code)
    return codes


def _resolve_settings_path(settings: Settings, value: Any) -> Path | None:
    text = str(value or "").strip()
    if not text:
        return None
    path = Path(text)
    if path.is_absolute():
        return path
    project_root = Path(getattr(settings, "project_root", PROJECT_ROOT))
    return project_root / path


def _latest_report_file_for_settings(settings: Settings) -> Path:
    explicit = getattr(settings, "latest_report_file", None)
    if explicit:
        return Path(explicit)
    project_root = Path(getattr(settings, "project_root", PROJECT_ROOT))
    reports_dir = Path(getattr(settings, "reports_dir", project_root / "reports"))
    return reports_dir / "latest.json"


def _database_path_for_settings(settings: Settings) -> Path:
    explicit = getattr(settings, "database_path", None)
    if explicit:
        return Path(explicit)
    project_root = Path(getattr(settings, "project_root", PROJECT_ROOT))
    return project_root / "backend" / "data" / "bond_funds.db"


def _autostock_directory_parse_error_impact_scope(
    settings: Settings,
    payload: dict[str, Any],
    evidence: dict[str, Any],
    *,
    parse_error_count: int,
    parse_error_samples: Sequence[Any],
    parse_error_codes: Sequence[Any] | None = None,
    parse_error_code_truncated: bool = False,
) -> tuple[dict[str, Any], list[str], list[str]]:
    blockers: list[str] = []
    warnings: list[str] = []
    parse_error_codes = _autostock_directory_parse_error_codes(parse_error_samples, parse_error_codes)
    project_root = Path(getattr(settings, "project_root", PROJECT_ROOT))
    latest_report_file = _latest_report_file_for_settings(settings)
    latest_report = _read_json(latest_report_file) if latest_report_file.exists() else {}
    artifacts = latest_report.get("artifacts") if isinstance(latest_report.get("artifacts"), dict) else {}
    filtered_csv_path = _resolve_settings_path(settings, artifacts.get("filtered_csv")) if artifacts else None
    final_codes = _read_csv_codes(filtered_csv_path) if filtered_csv_path and filtered_csv_path.exists() else []
    final_code_set = set(final_codes)
    formal_overlap_codes = [code for code in parse_error_codes if code in final_code_set]

    if parse_error_count > 0 and not parse_error_codes:
        warnings.append("directory_parse_error_impact_codes_missing")
    if parse_error_code_truncated or (parse_error_count > len(parse_error_codes) and parse_error_codes):
        warnings.append("directory_parse_error_impact_scope_sample_limited")
    if parse_error_count > 0 and not latest_report:
        warnings.append("directory_parse_error_impact_latest_report_missing")
    elif _safe_int(latest_report.get("final_count")) > 0 and not final_codes:
        warnings.append("directory_parse_error_impact_formal_csv_unreadable")

    db_path = _database_path_for_settings(settings)
    local_rows_by_code: dict[str, dict[str, Any]] = {}
    if parse_error_codes and not db_path.exists():
        warnings.append("directory_parse_error_impact_database_missing")
    elif parse_error_codes:
        try:
            with _connect_read_only(db_path) as conn:
                if not _table_exists(conn, "funds"):
                    warnings.append("directory_parse_error_impact_funds_table_missing")
                else:
                    columns = _table_columns(conn, "funds")
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
                            if field in columns:
                                select_exprs.append(field)
                            else:
                                select_exprs.append(f"NULL AS {field}")
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
                            code = _normalize_fund_code(row["fcode"])
                            if not code:
                                continue
                            type_code = _clean_business_text(row["type_code"])
                            type_category = _clean_business_text(row["type_category"])
                            data_source = _clean_business_text(row["data_source"])
                            classification_supported = bool(
                                data_source
                                and (
                                    type_code in {"31", "32"}
                                    or type_category == "fixed_income"
                                )
                            )
                            local_rows_by_code[code] = {
                                "fcode": code,
                                "shortname": _clean_business_text(row["shortname"]),
                                "ftype": _clean_business_text(row["ftype"]),
                                "sub_type": _clean_business_text(row["sub_type"]),
                                "type_code": type_code,
                                "type_category": type_category,
                                "data_source": data_source,
                                "last_updated_at": _clean_business_text(row["last_updated_at"]),
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
    formal_overlap_unsupported_codes = [
        code
        for code in formal_overlap_codes
        if code not in local_rows_by_code or not local_rows_by_code[code].get("classification_supported")
    ]
    if formal_overlap_unsupported_codes:
        blockers.append(
            "directory_parse_error_final_overlap_without_local_classification:"
            + ",".join(formal_overlap_unsupported_codes[:20])
        )

    diffs_after_sync = payload.get("diffs_after_sync") if isinstance(payload.get("diffs_after_sync"), dict) else {}
    missing_after = _safe_int(
        evidence.get("missing_in_local_after_count")
        if "missing_in_local_after_count" in evidence
        else diffs_after_sync.get("missing_in_local_vs_autostock_total")
    )
    missing_active_after = _safe_int(
        diffs_after_sync.get("missing_in_local_active_vs_autostock_total")
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

    local_match_rows = [local_rows_by_code[code] for code in parse_error_codes if code in local_rows_by_code]
    return (
        {
            "available": parse_error_count > 0,
            "status": _classify_status(blockers, warnings),
            "publication_impact_status": publication_impact_status if parse_error_count > 0 else "not_applicable",
            "truth_policy": "preserve_missing_semantics",
            "business_value_handling": (
                "Read-only impact scope only; missing AutoStock directory type is not inferred from fund name "
                "and is not used to synthesize fund type, bond membership, or valid directory records."
            ),
            "project_root": str(project_root),
            "parse_error_count": int(parse_error_count),
            "parse_error_sample_count": len(parse_error_samples),
            "parse_error_code_count": len(parse_error_codes),
            "parse_error_codes": parse_error_codes[:30],
            "sample_limited": bool(parse_error_count > len(parse_error_codes)),
            "latest_report_file": str(latest_report_file),
            "latest_report_exists": latest_report_file.exists(),
            "latest_report_run_id": str(latest_report.get("run_id") or "") or None,
            "latest_report_final_count": _safe_int(latest_report.get("final_count")),
            "filtered_csv_path": str(filtered_csv_path) if filtered_csv_path else "",
            "filtered_csv_exists": bool(filtered_csv_path and filtered_csv_path.exists()),
            "formal_final_code_count": len(final_codes),
            "formal_final_overlap_count": len(formal_overlap_codes),
            "formal_final_overlap_codes": formal_overlap_codes[:30],
            "formal_final_overlap_unsupported_count": len(formal_overlap_unsupported_codes),
            "formal_final_overlap_unsupported_codes": formal_overlap_unsupported_codes[:30],
            "database_path": str(db_path),
            "database_exists": db_path.exists(),
            "local_funds_match_count": len(local_rows_by_code),
            "local_funds_missing_count": len(local_missing_codes),
            "local_funds_missing_codes": local_missing_codes[:30],
            "local_funds_supported_classification_count": len(local_supported_codes),
            "local_funds_supported_classification_codes": local_supported_codes[:30],
            "local_funds_unsupported_classification_count": len(local_unsupported_codes),
            "local_funds_unsupported_classification_codes": local_unsupported_codes[:30],
            "local_funds_match_samples": local_match_rows[:20],
            "sync_missing_in_local_after_count": missing_after,
            "sync_missing_active_in_local_after_count": missing_active_after,
            "sync_newly_synced_codes_total": _safe_int(evidence.get("newly_synced_codes_total")),
            "blockers": blockers,
            "warnings": warnings,
        },
        blockers,
        warnings,
    )


def _directory_cache_evidence_errors(evidence: dict[str, Any]) -> list[str]:
    """Validate cached directory provenance without claiming a new HTTP response."""
    errors: list[str] = []
    if evidence.get("cache_status") != "hit" or evidence.get("cache_integrity_verified") is not True:
        errors.append("directory_cache_integrity_not_verified")
    for field in ("request_count", "response_count", "accepted_response_count", "http_attempt_count",
                  "http_response_count", "http_retry_count", "response_body_bytes"):
        if evidence.get(field) != 0:
            errors.append(f"directory_cache_current_request_count_invalid:{field}")
    endpoint = str(evidence.get("base_url") or "").rstrip("/") + "/fund/all"
    if evidence.get("cache_endpoint") != endpoint:
        errors.append("directory_cache_endpoint_mismatch")
    if evidence.get("query_mode") != "directory_snapshot_full" or evidence.get("keyword"):
        errors.append("directory_cache_not_full_directory")
    data_hash = str(evidence.get("data_sha256") or "")
    if not re.fullmatch(r"[0-9a-f]{64}", data_hash) or data_hash != evidence.get("response_hash"):
        errors.append("directory_cache_data_hash_mismatch")
    try:
        age = float(evidence["cache_age_seconds"])
        ttl = float(evidence["cache_ttl_seconds"])
        fetched_at = float(evidence["data_fetched_at_epoch"])
        finished_at = datetime.fromisoformat(evidence["request_window"]["finished_at"]).timestamp()
        if not all(math.isfinite(value) for value in (age, ttl, fetched_at, finished_at)):
            raise ValueError("nonfinite_cache_time")
        if not 0 <= age < ttl or fetched_at <= 0 or fetched_at + age > finished_at + 1:
            raise ValueError("cache_time_outside_valid_window")
    except (KeyError, ValueError, TypeError, OverflowError):
        errors.append("directory_cache_freshness_invalid")
    origin = evidence.get("origin_request_metadata")
    if not isinstance(origin, dict):
        errors.append("directory_cache_origin_missing")
        return errors
    try:
        signature = hashlib.sha256(json.dumps(origin, ensure_ascii=False, sort_keys=True,
                                              separators=(",", ":"), allow_nan=False).encode()).hexdigest()
    except (ValueError, TypeError):
        signature = None
    if not signature or signature != evidence.get("origin_request_metadata_sha256"):
        errors.append("directory_cache_origin_hash_mismatch")
    if (origin.get("status_code") != 200 or origin.get("business_code") not in (0, 200)
            or origin.get("quota_error") or _safe_int(origin.get("http_attempt_count")) < 1
            or _safe_int(origin.get("http_response_count")) < 1):
        errors.append("directory_cache_origin_success_unproven")
    return errors


def _autostock_directory_source_evidence_truth(settings: Settings) -> dict[str, Any]:
    blockers: list[str] = []
    warnings: list[str] = []
    artifact_path, payload = _load_latest_bond_universe_sync_artifact(settings)
    result: dict[str, Any] = {
        "available": False,
        "status": "not_available",
        "artifact_path": str(artifact_path) if artifact_path else "",
        "generated_at": None,
        "blockers": blockers,
        "warnings": warnings,
        "interpretation": (
            "AutoStock /fund/all directory sync must preserve real request evidence, explicit missing business-date "
            "semantics, parse/save failure reasons, and database write counts. Legacy sync artifacts without this "
            "field are warnings, not proof of current source truth."
        ),
    }
    if artifact_path and payload is None:
        blockers.append("bond_universe_sync_artifact_invalid_json")
        result["status"] = _classify_status(blockers, warnings)
        return result
    if not isinstance(payload, dict) or not payload:
        return result

    result["generated_at"] = payload.get("generated_at")
    evidence = payload.get("autostock_directory_source_evidence")
    if not isinstance(evidence, dict) or not evidence:
        warnings.append("autostock_directory_source_evidence_missing_latest_artifact")
        result.update(
            {
                "status": _classify_status(blockers, warnings),
                "autostock_directory_meta_available": isinstance(payload.get("autostock_directory_meta"), dict),
            }
        )
        return result

    result["available"] = True
    task = str(evidence.get("task") or "").strip()
    source = str(evidence.get("source") or "").strip()
    source_type = str(evidence.get("source_type") or "").strip()
    endpoint_path = str(evidence.get("endpoint_path") or "").strip()
    request_window = evidence.get("request_window") if isinstance(evidence.get("request_window"), dict) else {}
    request_count = _safe_int(evidence.get("request_count"))
    response_count = _safe_int(evidence.get("response_count"))
    accepted_response_count = _safe_int(evidence.get("accepted_response_count"))
    failed_response_count = _safe_int(evidence.get("failed_response_count"))
    transport_exception_count = _safe_int(evidence.get("transport_exception_count"))
    returned_record_count = _safe_int(evidence.get("returned_record_count"))
    returned_unique_count = _safe_int(evidence.get("returned_unique_count"))
    missing_count = _safe_int(evidence.get("missing_count"))
    failed_batch_count = _safe_int(evidence.get("failed_batch_count"))
    raw_total = _safe_int(evidence.get("raw_total"))
    normalized_total = _safe_int(evidence.get("normalized_total"))
    unique_total = _safe_int(evidence.get("unique_total"))
    duplicate_count = _safe_int(evidence.get("duplicate_count"))
    bond_like_total = _safe_int(evidence.get("bond_like_total"))
    parse_errors = _safe_int(evidence.get("parse_errors"))
    parse_error_count = _safe_int(
        evidence.get("parse_error_count")
        if "parse_error_count" in evidence
        else evidence.get("parse_errors")
    )
    database_write_attempt_count = _safe_int(evidence.get("database_write_attempt_count"))
    database_saved_count = _safe_int(evidence.get("database_saved_count"))
    database_fund_insert_count = _safe_int(evidence.get("database_fund_insert_count"))
    database_fund_update_count = _safe_int(evidence.get("database_fund_update_count"))
    database_failed_record_count = _safe_int(evidence.get("database_failed_record_count"))
    database_skipped_count = _safe_int(evidence.get("database_skipped_count"))
    fallback_search_record_count = _safe_int(evidence.get("fallback_search_record_count"))
    direct_autostock_stub_count = _safe_int(evidence.get("direct_autostock_stub_count"))
    newly_synced_codes_total = _safe_int(evidence.get("newly_synced_codes_total"))
    response_hash = str(evidence.get("response_hash") or "").strip()
    sample_codes = evidence.get("sample_codes") if isinstance(evidence.get("sample_codes"), list) else []
    skip_reason_counts = evidence.get("skip_reason_counts") if isinstance(evidence.get("skip_reason_counts"), dict) else {}
    failure_reason_counts = (
        evidence.get("failure_reason_counts")
        if isinstance(evidence.get("failure_reason_counts"), dict)
        else {}
    )
    parse_error_reason_counts = (
        evidence.get("parse_error_reason_counts")
        if isinstance(evidence.get("parse_error_reason_counts"), dict)
        else {}
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
    explicit_parse_error_code_count = _safe_int(
        evidence.get("parse_error_code_count")
        if "parse_error_code_count" in evidence
        else len(explicit_parse_error_codes)
    )
    parse_error_code_truncated = bool(evidence.get("parse_error_code_truncated"))
    parse_error_sample_truncated = bool(evidence.get("parse_error_sample_truncated"))
    parse_error_impact_scope, impact_blockers, impact_warnings = _autostock_directory_parse_error_impact_scope(
        settings,
        payload,
        evidence,
        parse_error_count=parse_error_count,
        parse_error_samples=parse_error_samples,
        parse_error_codes=explicit_parse_error_codes,
        parse_error_code_truncated=parse_error_code_truncated,
    )
    effective_parse_error_codes = list(parse_error_impact_scope.get("parse_error_codes") or [])
    blockers.extend(impact_blockers)
    warnings.extend(impact_warnings)
    source_complete_raw = evidence.get("source_complete")
    publication_impact_status = str(
        parse_error_impact_scope.get("publication_impact_status") or ""
    ).strip()
    parse_error_publication_impact_clear = bool(
        parse_error_count > 0
        and parse_error_impact_scope.get("status") == "ok"
        and publication_impact_status == "ok_no_formal_final_overlap"
        and _safe_int(parse_error_impact_scope.get("formal_final_overlap_unsupported_count")) == 0
        and _safe_int(parse_error_impact_scope.get("sync_missing_in_local_after_count")) == 0
        and _safe_int(parse_error_impact_scope.get("sync_missing_active_in_local_after_count")) == 0
        and database_failed_record_count <= 0
        and failed_response_count <= 0
        and transport_exception_count <= 0
        and failed_batch_count <= 0
    )

    if task != "bond_universe_sync.autostock_directory":
        blockers.append(f"directory_task_unexpected:{task or 'empty'}")
    if source != "AutoStock /fund/all":
        blockers.append(f"directory_source_unexpected:{source or 'empty'}")
    if source_type not in {"api", "api_batch", "cache"}:
        blockers.append(f"directory_source_type_invalid:{source_type or 'empty'}")
    if endpoint_path != "/fund/all":
        blockers.append(f"directory_endpoint_unexpected:{endpoint_path or 'empty'}")
    if not request_window.get("started_at") or not request_window.get("finished_at"):
        blockers.append("directory_request_window_incomplete")
    if source_type == "cache":
        # Persisted evidence describes freshness at use time, not the age at a later audit.
        blockers.extend(_directory_cache_evidence_errors(evidence))
    else:
        if request_count <= 0:
            blockers.append("directory_request_count_empty")
        if response_count <= 0:
            blockers.append("directory_response_count_empty")
        if accepted_response_count <= 0:
            blockers.append("directory_accepted_response_count_empty")
    if failed_response_count > 0:
        blockers.append("directory_failed_response_present")
    if transport_exception_count > 0:
        blockers.append("directory_transport_exception_present")
    if failed_batch_count > 0:
        blockers.append("directory_failed_batch_present")
    if missing_count > 0:
        blockers.append("directory_missing_response_present")
    if returned_record_count <= 0:
        blockers.append("directory_returned_record_empty")
    if returned_unique_count <= 0:
        blockers.append("directory_returned_unique_empty")
    if not response_hash:
        blockers.append("directory_response_hash_missing")
    if returned_unique_count > 0 and not sample_codes:
        blockers.append("directory_sample_codes_missing")

    if evidence.get("business_date") not in (None, ""):
        warnings.append("directory_business_date_unexpected_value")
    if str(evidence.get("business_date_semantics") or "").strip() != "not_provided_by_autostock_directory":
        blockers.append("directory_business_date_semantics_missing")

    required_database_fields = (
        "database_write_attempt_count",
        "database_saved_count",
        "database_fund_insert_count",
        "database_fund_update_count",
        "database_failed_record_count",
        "database_skipped_count",
        "parse_error_count",
        "skip_reason_counts",
        "failure_reason_counts",
    )
    for field in required_database_fields:
        if field not in evidence:
            blockers.append(f"directory_database_field_missing:{field}")
    if database_saved_count > database_write_attempt_count:
        blockers.append("directory_database_saved_gt_write_attempt")
    if database_fund_insert_count + database_fund_update_count > database_saved_count:
        blockers.append("directory_database_mutation_gt_saved")
    if database_failed_record_count > 0:
        blockers.append("directory_database_failed_record_present")
    if database_skipped_count > 0 and not skip_reason_counts:
        warnings.append("directory_database_skipped_without_reason")
    if skip_reason_counts:
        skip_reason_total = sum(_safe_int(value) for value in skip_reason_counts.values())
        if skip_reason_total != database_skipped_count:
            warnings.append("directory_database_skip_reason_total_mismatch")
    if database_failed_record_count > 0 and not failure_reason_counts:
        blockers.append("directory_database_failed_without_reason")
    if parse_error_count > 0 and not failure_reason_counts.get("directory_row_parse_error"):
        warnings.append("directory_parse_error_without_reason")
    if parse_error_count > 0 and not parse_error_reason_counts:
        warnings.append("directory_parse_error_reason_counts_missing")
    if parse_error_count > 0 and not parse_error_samples:
        warnings.append("directory_parse_error_samples_missing")
    if parse_error_count > 0 and not explicit_parse_error_codes:
        warnings.append("directory_parse_error_codes_missing")
    if parse_error_count > 0 and explicit_parse_error_code_count != len(explicit_parse_error_codes):
        warnings.append("directory_parse_error_code_count_mismatch")
    if parse_error_count > 0 and not parse_error_code_truncated and explicit_parse_error_code_count < parse_error_count:
        warnings.append("directory_parse_error_codes_incomplete")
    if parse_error_count > 0:
        if str(evidence.get("parse_error_truth_policy") or "").strip() != "preserve_missing_semantics":
            warnings.append("directory_parse_error_truth_policy_missing")
        handling = str(evidence.get("parse_error_business_value_handling") or "")
        if "does not synthesize" not in handling and "not used to synthesize" not in handling:
            warnings.append("directory_parse_error_business_value_handling_missing")
        if parse_error_reason_counts:
            reason_total = sum(_safe_int(value) for value in parse_error_reason_counts.values())
            if reason_total != parse_error_count:
                warnings.append("directory_parse_error_reason_total_mismatch")
        for sample_index, sample in enumerate(parse_error_samples[:5], start=1):
            if not isinstance(sample, dict):
                warnings.append(f"directory_parse_error_sample_invalid:{sample_index}")
                continue
            if not sample.get("reason"):
                warnings.append(f"directory_parse_error_sample_reason_missing:{sample_index}")
            if not sample.get("row_type"):
                warnings.append(f"directory_parse_error_sample_row_type_missing:{sample_index}")
            if str(sample.get("truth_policy") or "").strip() != "preserve_missing_semantics":
                warnings.append(f"directory_parse_error_sample_truth_policy_missing:{sample_index}")

    if raw_total <= 0 or normalized_total <= 0 or unique_total <= 0 or bond_like_total <= 0:
        blockers.append("directory_normalized_counts_empty")
    if raw_total and normalized_total + parse_error_count != raw_total:
        warnings.append("directory_raw_normalized_parse_count_mismatch")
    if normalized_total and unique_total + duplicate_count != normalized_total:
        warnings.append("directory_unique_duplicate_count_mismatch")
    if unique_total and bond_like_total > unique_total:
        blockers.append("directory_bond_like_gt_unique")
    if returned_record_count and raw_total and returned_record_count != raw_total:
        warnings.append("directory_returned_record_raw_total_mismatch")
    if returned_unique_count and unique_total and returned_unique_count != unique_total:
        warnings.append("directory_returned_unique_total_mismatch")
    if parse_errors and parse_error_count and parse_errors != parse_error_count:
        warnings.append("directory_parse_error_count_mismatch")
    if parse_error_count > 0:
        warnings.append("directory_parse_errors_present")
    if source_complete_raw is None:
        blockers.append("directory_source_complete_missing")
    elif not bool(source_complete_raw):
        if parse_error_count > 0 and database_failed_record_count <= 0 and failed_response_count <= 0:
            if not parse_error_publication_impact_clear:
                warnings.append("directory_source_incomplete_due_to_parse_errors")
        else:
            blockers.append("directory_source_incomplete_without_explicit_parse_error_reason")

    result.update(
        {
            "status": _classify_status(blockers, warnings),
            "task": task,
            "source": source,
            "source_type": source_type,
            "endpoint_path": endpoint_path,
            "business_date": evidence.get("business_date"),
            "business_date_semantics": evidence.get("business_date_semantics"),
            "request_count": request_count,
            "response_count": response_count,
            "accepted_response_count": accepted_response_count,
            "failed_response_count": failed_response_count,
            "transport_exception_count": transport_exception_count,
            "returned_record_count": returned_record_count,
            "returned_unique_count": returned_unique_count,
            "raw_total": raw_total,
            "normalized_total": normalized_total,
            "unique_total": unique_total,
            "duplicate_count": duplicate_count,
            "bond_like_total": bond_like_total,
            "parse_error_count": parse_error_count,
            "source_payload_status": (
                "watch_parse_errors_preserved" if parse_error_count > 0 else "ok"
            ),
            "publication_impact_status": (
                publication_impact_status if parse_error_count > 0 else "not_applicable"
            ),
            "source_incomplete_publication_impact_clear": parse_error_publication_impact_clear,
            "parse_error_reason_counts": parse_error_reason_counts,
            "parse_error_codes": effective_parse_error_codes[:50],
            "parse_error_code_count": len(effective_parse_error_codes),
            "parse_error_explicit_codes": explicit_parse_error_codes[:50],
            "parse_error_explicit_code_count": explicit_parse_error_code_count,
            "parse_error_code_truncated": parse_error_code_truncated,
            "parse_error_samples": parse_error_samples[:10],
            "parse_error_sample_count": len(parse_error_samples),
            "parse_error_sample_truncated": parse_error_sample_truncated,
            "parse_error_impact_scope": parse_error_impact_scope,
            "database_write_attempt_count": database_write_attempt_count,
            "database_saved_count": database_saved_count,
            "database_fund_insert_count": database_fund_insert_count,
            "database_fund_update_count": database_fund_update_count,
            "database_failed_record_count": database_failed_record_count,
            "database_skipped_count": database_skipped_count,
            "fallback_search_record_count": fallback_search_record_count,
            "direct_autostock_stub_count": direct_autostock_stub_count,
            "newly_synced_codes_total": newly_synced_codes_total,
            "response_hash_available": bool(response_hash),
            "sample_codes": sample_codes[:10],
            "source_complete": bool(source_complete_raw),
        }
    )
    return result


def _annual_return_rank_consistency(settings: Settings) -> dict[str, Any]:
    blockers: list[str] = []
    warnings: list[str] = []
    latest = _read_json(settings.latest_report_file)
    run_id = str(latest.get("run_id") or "").strip()
    final_count = _safe_int(latest.get("final_count"))
    artifacts = latest.get("artifacts") if isinstance(latest.get("artifacts"), dict) else {}
    filtered_csv_path = _resolve_project_path(settings, artifacts.get("filtered_csv"))
    final_codes = _read_csv_codes(filtered_csv_path) if filtered_csv_path and filtered_csv_path.exists() else []
    annual_window_years = _complete_calendar_year_window(ANNUAL_RETURN_LONG_TERM_WINDOW_YEARS)
    rank_stale_days = _resolve_rank_stale_days(settings)
    long_term_api_source_evidence = _long_term_history_api_source_evidence_truth(settings)
    blockers.extend(
        f"long_term_history_api_source:{item}"
        for item in long_term_api_source_evidence.get("blockers", [])
    )
    warnings.extend(
        f"long_term_history_api_source:{item}"
        for item in long_term_api_source_evidence.get("warnings", [])
    )

    tracker_sidecar_path = _resolve_project_path(settings, artifacts.get("project_tracker_current_detail_json"))
    tracker_payload = _read_json(tracker_sidecar_path) if tracker_sidecar_path and tracker_sidecar_path.exists() else {}
    derived_fields = tracker_payload.get("derived_fields") if isinstance(tracker_payload.get("derived_fields"), dict) else {}
    raw_annual_breakdown_map = derived_fields.get("annual_return_breakdown_by_code")
    annual_breakdown_map_available = isinstance(raw_annual_breakdown_map, dict)
    normalized_sidecar_breakdown: dict[str, list[dict[str, Any]]] = {}
    if annual_breakdown_map_available:
        for raw_code, raw_entries in dict(raw_annual_breakdown_map or {}).items():
            code = _normalize_fund_code(raw_code)
            if code:
                normalized_sidecar_breakdown[code] = _normalize_annual_breakdown_sidecar_entries(raw_entries)

    if final_count and not annual_breakdown_map_available:
        blockers.append("tracker_annual_return_breakdown_map_missing")
    if final_count and not tracker_payload:
        blockers.append("tracker_current_detail_sidecar_missing_for_annual_breakdown")

    if not settings.database_path.exists():
        return {
            "status": "needs_review",
            "blockers": ["database_missing"],
            "warnings": warnings,
            "run_id": run_id or None,
        }

    with _connect_read_only(settings.database_path) as conn:
        for table_name in ("fund_annual_returns", "fund_rank_history", "funds"):
            if not _table_exists(conn, table_name):
                blockers.append(f"annual_rank_table_missing:{table_name}")
        if blockers and any(item.startswith("annual_rank_table_missing:") for item in blockers):
            return {
                "status": _classify_status(blockers, warnings),
                "blockers": blockers,
                "warnings": warnings,
                "run_id": run_id or None,
            }

        annual_columns = _table_columns(conn, "fund_annual_returns")
        rank_columns = _table_columns(conn, "fund_rank_history")
        funds_columns = _table_columns(conn, "funds")
        required_annual_columns = {
            "fcode",
            "year",
            "annual_return",
            "rank_in_category",
            "category_count",
            "percentile",
        }
        required_rank_columns = {
            "fcode",
            "rank_date",
            "rank_position",
            "category_total",
            "percentile",
        }
        required_funds_columns = {"fcode", "estab_date"}
        missing_annual_columns = sorted(required_annual_columns - annual_columns)
        missing_rank_columns = sorted(required_rank_columns - rank_columns)
        missing_funds_columns = sorted(required_funds_columns - funds_columns)
        if missing_annual_columns:
            blockers.append(f"fund_annual_returns_fields_missing:{','.join(missing_annual_columns)}")
        if missing_rank_columns:
            blockers.append(f"fund_rank_history_fields_missing:{','.join(missing_rank_columns)}")
        if missing_funds_columns:
            blockers.append(f"funds_annual_breakdown_fields_missing:{','.join(missing_funds_columns)}")
        if missing_annual_columns or missing_rank_columns or missing_funds_columns:
            return {
                "status": _classify_status(blockers, warnings),
                "blockers": blockers,
                "warnings": warnings,
                "run_id": run_id or None,
                "missing_annual_columns": missing_annual_columns,
                "missing_rank_columns": missing_rank_columns,
                "missing_funds_columns": missing_funds_columns,
            }

        annual_row = conn.execute(
            """
            SELECT
                COUNT(*) AS row_count,
                COUNT(DISTINCT fcode) AS code_count,
                MIN(year) AS min_year,
                MAX(year) AS max_year,
                SUM(CASE WHEN annual_return IS NULL THEN 1 ELSE 0 END) AS annual_return_null_count,
                SUM(CASE WHEN rank_in_category IS NULL THEN 1 ELSE 0 END) AS rank_null_count,
                SUM(CASE WHEN category_count IS NULL THEN 1 ELSE 0 END) AS category_count_null_count,
                SUM(CASE WHEN percentile IS NULL THEN 1 ELSE 0 END) AS percentile_null_count,
                SUM(CASE WHEN percentile IS NOT NULL AND (percentile < 0 OR percentile > 1) THEN 1 ELSE 0 END) AS percentile_out_of_range_count,
                SUM(
                    CASE
                        WHEN rank_in_category IS NOT NULL
                         AND category_count IS NOT NULL
                         AND category_count > 0
                         AND rank_in_category > category_count
                        THEN 1 ELSE 0
                    END
                ) AS rank_gt_category_count,
                SUM(
                    CASE
                        WHEN percentile IS NOT NULL
                         AND (rank_in_category IS NULL OR category_count IS NULL OR category_count <= 0)
                        THEN 1 ELSE 0
                    END
                ) AS percentile_without_rank_basis_count,
                SUM(
                    CASE
                        WHEN percentile IS NOT NULL
                         AND rank_in_category IS NOT NULL
                         AND category_count IS NOT NULL
                         AND category_count > 0
                         AND ABS(CAST(percentile AS REAL) - CAST(rank_in_category AS REAL) / CAST(category_count AS REAL)) > ?
                        THEN 1 ELSE 0
                    END
                ) AS percentile_formula_mismatch_count
            FROM fund_annual_returns
            """,
            (ANNUAL_RANK_PERCENTILE_TOLERANCE,),
        ).fetchone()
        rank_row = conn.execute(
            """
            SELECT
                COUNT(*) AS row_count,
                COUNT(DISTINCT fcode) AS code_count,
                MIN(rank_date) AS min_rank_date,
                MAX(rank_date) AS max_rank_date,
                SUM(CASE WHEN rank_position IS NULL THEN 1 ELSE 0 END) AS rank_position_null_count,
                SUM(CASE WHEN category_total IS NULL THEN 1 ELSE 0 END) AS category_total_null_count,
                SUM(CASE WHEN percentile IS NULL THEN 1 ELSE 0 END) AS percentile_null_count,
                SUM(CASE WHEN percentile IS NOT NULL AND (percentile < 0 OR percentile > 1) THEN 1 ELSE 0 END) AS percentile_out_of_range_count,
                SUM(
                    CASE
                        WHEN rank_position IS NOT NULL
                         AND category_total IS NOT NULL
                         AND category_total > 0
                         AND rank_position > category_total
                        THEN 1 ELSE 0
                    END
                ) AS rank_gt_category_count,
                SUM(
                    CASE
                        WHEN percentile IS NOT NULL
                         AND (rank_position IS NULL OR category_total IS NULL OR category_total <= 0)
                        THEN 1 ELSE 0
                    END
                ) AS percentile_without_rank_basis_count,
                SUM(
                    CASE
                        WHEN percentile IS NOT NULL
                         AND rank_position IS NOT NULL
                         AND category_total IS NOT NULL
                         AND category_total > 0
                         AND ABS(CAST(percentile AS REAL) - CAST(rank_position AS REAL) / CAST(category_total AS REAL)) > ?
                        THEN 1 ELSE 0
                    END
                ) AS percentile_formula_mismatch_count
            FROM fund_rank_history
            """,
            (ANNUAL_RANK_PERCENTILE_TOLERANCE,),
        ).fetchone()

        annual_quality_keys = {
            "annual_percentile_out_of_range": _safe_int(annual_row["percentile_out_of_range_count"]),
            "annual_rank_gt_category_count": _safe_int(annual_row["rank_gt_category_count"]),
            "annual_percentile_without_rank_basis": _safe_int(annual_row["percentile_without_rank_basis_count"]),
            "annual_percentile_formula_mismatch": _safe_int(annual_row["percentile_formula_mismatch_count"]),
        }
        rank_quality_keys = {
            "rank_percentile_out_of_range": _safe_int(rank_row["percentile_out_of_range_count"]),
            "rank_position_gt_category_total": _safe_int(rank_row["rank_gt_category_count"]),
            "rank_percentile_without_rank_basis": _safe_int(rank_row["percentile_without_rank_basis_count"]),
            "rank_percentile_formula_mismatch": _safe_int(rank_row["percentile_formula_mismatch_count"]),
        }
        for issue, count in {**annual_quality_keys, **rank_quality_keys}.items():
            if count:
                blockers.append(issue)

        latest_rank_lag_days = _date_lag_from_today(rank_row["max_rank_date"])
        if latest_rank_lag_days is not None and latest_rank_lag_days > rank_stale_days:
            warnings.append("rank_history_latest_date_stale")

        formal_summary: dict[str, Any] = {
            "final_code_count": len(final_codes),
            "final_count_from_latest": final_count,
            "annual_window_years": annual_window_years,
            "annual_long_term_ready_count": 0,
            "annual_rows_present_count": 0,
            "annual_rows_missing_count": 0,
            "annual_rows_all_return_null_count": 0,
            "rank_window_days": RANK_HISTORY_WINDOW_DAYS,
            "rank_min_observations": RANK_HISTORY_MIN_OBSERVATIONS,
            "rank_ready_count": 0,
            "rank_rows_missing_count": 0,
            "rank_stale_count": 0,
            "annual_breakdown_sidecar_available": annual_breakdown_map_available,
            "annual_breakdown_sidecar_code_count": len(normalized_sidecar_breakdown),
            "annual_breakdown_sidecar_non_final_code_count": 0,
            "annual_breakdown_sidecar_missing_db_rows_count": 0,
            "annual_breakdown_sidecar_missing_count": 0,
            "annual_breakdown_sidecar_mismatch_count": 0,
            "annual_breakdown_sidecar_fabricated_count": 0,
            "annual_breakdown_sidecar_mismatch_sample": [],
        }
        expected_annual_breakdown: dict[str, list[dict[str, Any]]] = {}
        rank_lag_sample: list[dict[str, Any]] = []
        clean_final_codes = [_normalize_fund_code(code) for code in final_codes if _normalize_fund_code(code)]

        if final_count and final_codes and len(final_codes) != final_count:
            blockers.append("formal_filtered_csv_code_count_not_equal_final_count_for_annual_rank_truth")

        if clean_final_codes:
            final_code_set = set(clean_final_codes)
            non_final_sidecar_codes = sorted(set(normalized_sidecar_breakdown) - final_code_set)
            formal_summary["annual_breakdown_sidecar_non_final_code_count"] = len(non_final_sidecar_codes)
            if non_final_sidecar_codes:
                blockers.append("tracker_annual_breakdown_sidecar_contains_non_final_codes")

            for offset in range(0, len(clean_final_codes), 400):
                chunk = clean_final_codes[offset : offset + 400]
                values_clause = ",".join(["(?)"] * len(chunk))
                annual_placeholders = ",".join("?" for _ in annual_window_years)
                annual_coverage_row = conn.execute(
                    f"""
                    WITH final_codes(fcode) AS (
                        VALUES {values_clause}
                    ),
                    annual_by_code AS (
                        SELECT
                            c.fcode,
                            COUNT(ar.year) AS row_count,
                            COUNT(DISTINCT CASE WHEN ar.annual_return IS NOT NULL THEN ar.year END) AS nonnull_year_count,
                            COUNT(
                                DISTINCT CASE
                                    WHEN ar.year IN ({annual_placeholders})
                                     AND ar.annual_return IS NOT NULL
                                    THEN ar.year
                                END
                            ) AS window_nonnull_year_count
                        FROM final_codes c
                        LEFT JOIN fund_annual_returns ar ON ar.fcode = c.fcode
                        GROUP BY c.fcode
                    )
                    SELECT
                        SUM(CASE WHEN row_count > 0 THEN 1 ELSE 0 END) AS rows_present_count,
                        SUM(CASE WHEN row_count = 0 THEN 1 ELSE 0 END) AS rows_missing_count,
                        SUM(CASE WHEN row_count > 0 AND nonnull_year_count = 0 THEN 1 ELSE 0 END) AS rows_all_return_null_count,
                        SUM(CASE WHEN window_nonnull_year_count >= ? THEN 1 ELSE 0 END) AS annual_ready_count
                    FROM annual_by_code
                    """,
                    tuple(chunk) + tuple(annual_window_years) + (ANNUAL_RETURN_LONG_TERM_WINDOW_YEARS,),
                ).fetchone()
                formal_summary["annual_rows_present_count"] += _safe_int(annual_coverage_row["rows_present_count"])
                formal_summary["annual_rows_missing_count"] += _safe_int(annual_coverage_row["rows_missing_count"])
                formal_summary["annual_rows_all_return_null_count"] += _safe_int(
                    annual_coverage_row["rows_all_return_null_count"]
                )
                formal_summary["annual_long_term_ready_count"] += _safe_int(
                    annual_coverage_row["annual_ready_count"]
                )

                annual_rows = conn.execute(
                    f"""
                    WITH final_codes(fcode) AS (
                        VALUES {values_clause}
                    )
                    SELECT
                        c.fcode,
                        f.estab_date,
                        ar.year,
                        ar.annual_return,
                        ar.rank_in_category,
                        ar.category_count,
                        ar.percentile
                    FROM final_codes c
                    LEFT JOIN funds f ON f.fcode = c.fcode
                    LEFT JOIN fund_annual_returns ar ON ar.fcode = c.fcode
                    ORDER BY c.fcode, ar.year
                    """,
                    chunk,
                ).fetchall()
                for row in annual_rows:
                    code = _normalize_fund_code(row["fcode"])
                    if not code:
                        continue
                    expected_annual_breakdown.setdefault(code, [])
                    entry = _annual_breakdown_entry_from_db_row(row)
                    if entry is not None:
                        expected_annual_breakdown[code].append(entry)

                rank_rows = conn.execute(
                    f"""
                    WITH final_codes(fcode) AS (
                        VALUES {values_clause}
                    ),
                    latest_rank AS (
                        SELECT c.fcode, MAX(r.rank_date) AS latest_rank_date
                        FROM final_codes c
                        LEFT JOIN fund_rank_history r ON r.fcode = c.fcode
                        GROUP BY c.fcode
                    )
                    SELECT
                        c.fcode,
                        l.latest_rank_date,
                        COUNT(r.percentile) AS rank_window_count
                    FROM final_codes c
                    LEFT JOIN latest_rank l ON l.fcode = c.fcode
                    LEFT JOIN fund_rank_history r
                      ON r.fcode = c.fcode
                     AND r.percentile IS NOT NULL
                     AND l.latest_rank_date IS NOT NULL
                     AND r.rank_date >= date(l.latest_rank_date, ?)
                    GROUP BY c.fcode, l.latest_rank_date
                    ORDER BY c.fcode
                    """,
                    tuple(chunk) + (f"-{RANK_HISTORY_WINDOW_DAYS} day",),
                ).fetchall()
                for row in rank_rows:
                    rank_window_count = _safe_int(row["rank_window_count"])
                    latest_rank_date = row["latest_rank_date"]
                    if latest_rank_date is None:
                        formal_summary["rank_rows_missing_count"] += 1
                    if rank_window_count >= RANK_HISTORY_MIN_OBSERVATIONS:
                        formal_summary["rank_ready_count"] += 1
                    lag_days = _date_lag_from_today(latest_rank_date)
                    if lag_days is not None and lag_days > rank_stale_days:
                        formal_summary["rank_stale_count"] += 1
                        if len(rank_lag_sample) < 10:
                            rank_lag_sample.append(
                                {
                                    "fcode": row["fcode"],
                                    "latest_rank_date": latest_rank_date,
                                    "lag_days": lag_days,
                                }
                            )

            formal_summary["rank_stale_sample"] = rank_lag_sample
            if formal_summary["annual_rows_missing_count"]:
                warnings.append("formal_list_annual_return_rows_missing")
            if formal_summary["rank_rows_missing_count"]:
                warnings.append("formal_list_rank_history_rows_missing")
            if formal_summary["rank_stale_count"]:
                warnings.append("formal_list_rank_history_stale")

            if annual_breakdown_map_available:
                mismatch_samples: list[dict[str, Any]] = []
                for code in clean_final_codes:
                    expected_entries = expected_annual_breakdown.get(code, [])
                    actual_entries = normalized_sidecar_breakdown.get(code)
                    if expected_entries and actual_entries is None:
                        formal_summary["annual_breakdown_sidecar_missing_count"] += 1
                        if len(mismatch_samples) < 10:
                            mismatch_samples.append(
                                {"fcode": code, "issue": "sidecar_missing", "expected_count": len(expected_entries)}
                            )
                        continue
                    if not expected_entries and actual_entries:
                        formal_summary["annual_breakdown_sidecar_fabricated_count"] += 1
                        if len(mismatch_samples) < 10:
                            mismatch_samples.append(
                                {"fcode": code, "issue": "sidecar_has_rows_without_db_rows", "actual": actual_entries[:3]}
                            )
                        continue
                    if actual_entries is not None and expected_entries != actual_entries:
                        formal_summary["annual_breakdown_sidecar_mismatch_count"] += 1
                        if len(mismatch_samples) < 10:
                            mismatch_samples.append(
                                {
                                    "fcode": code,
                                    "issue": "sidecar_db_value_mismatch",
                                    "expected": expected_entries[:3],
                                    "actual": actual_entries[:3],
                                }
                            )
                formal_summary["annual_breakdown_sidecar_mismatch_sample"] = mismatch_samples
                for code, actual_entries in normalized_sidecar_breakdown.items():
                    if code not in expected_annual_breakdown and actual_entries:
                        formal_summary["annual_breakdown_sidecar_missing_db_rows_count"] += 1
                if formal_summary["annual_breakdown_sidecar_missing_count"]:
                    blockers.append("tracker_annual_breakdown_sidecar_missing_formal_codes")
                if formal_summary["annual_breakdown_sidecar_mismatch_count"]:
                    blockers.append("tracker_annual_breakdown_sidecar_db_mismatch")
                if formal_summary["annual_breakdown_sidecar_fabricated_count"]:
                    blockers.append("tracker_annual_breakdown_sidecar_fabricated_rows")
                if formal_summary["annual_breakdown_sidecar_missing_db_rows_count"]:
                    blockers.append("tracker_annual_breakdown_sidecar_rows_without_db_basis")

    return {
        "status": _classify_status(blockers, warnings),
        "blockers": blockers,
        "warnings": warnings,
        "run_id": run_id or None,
        "database_summary": {
            "annual_returns": {
                "row_count": _safe_int(annual_row["row_count"]),
                "code_count": _safe_int(annual_row["code_count"]),
                "min_year": annual_row["min_year"],
                "max_year": annual_row["max_year"],
                "annual_return_null_count": _safe_int(annual_row["annual_return_null_count"]),
                "rank_null_count": _safe_int(annual_row["rank_null_count"]),
                "category_count_null_count": _safe_int(annual_row["category_count_null_count"]),
                "percentile_null_count": _safe_int(annual_row["percentile_null_count"]),
                **annual_quality_keys,
                "percentile_formula": "rank_in_category / category_count, stored as decimal ratio 0..1",
            },
            "rank_history": {
                "row_count": _safe_int(rank_row["row_count"]),
                "code_count": _safe_int(rank_row["code_count"]),
                "min_rank_date": rank_row["min_rank_date"],
                "max_rank_date": rank_row["max_rank_date"],
                "latest_rank_lag_days": latest_rank_lag_days,
                "rank_stale_days": rank_stale_days,
                "rank_position_null_count": _safe_int(rank_row["rank_position_null_count"]),
                "category_total_null_count": _safe_int(rank_row["category_total_null_count"]),
                "percentile_null_count": _safe_int(rank_row["percentile_null_count"]),
                **rank_quality_keys,
                "percentile_formula": "rank_position / category_total, stored as decimal ratio 0..1",
            },
        },
        "formal_list_summary": formal_summary,
        "long_term_history_api_source_evidence": long_term_api_source_evidence,
        "truth_note": (
            "This read-only check verifies annual return and rank tables use decimal-ratio percentile semantics, "
            "that formal-list long-term inputs are backed by SQLite rows, and that the frontend tracker annual "
            "breakdown sidecar matches fund_annual_returns instead of synthesizing values. Missing annual values "
            "remain empty strings in the sidecar and are not treated as zero."
        ),
    }


def _resolve_runtime_or_project_path(settings: Settings, value: Any) -> Path | None:
    text = str(value or "").strip()
    if not text:
        return None
    path = Path(text)
    if path.is_absolute():
        if path.exists():
            return path
        parts = path.parts
        if len(parts) >= 3 and parts[1] == "app":
            return settings.project_root.joinpath(*parts[2:])
        return path
    return settings.project_root / path


def _file_evidence(settings: Settings, value: Any) -> dict[str, Any]:
    path = _resolve_runtime_or_project_path(settings, value)
    exists = bool(path and artifact_exists(path))
    size_bytes: int | None = None
    if exists and path is not None:
        try:
            size_bytes = original_artifact_size(path)
        except (OSError, ValueError):
            size_bytes = None
    return {
        "configured_value": value,
        "path": str(path) if path else "",
        "exists": exists,
        "size_bytes": size_bytes,
    }


def _subprocess_file_evidence(settings: Settings, item: dict[str, Any], key: str) -> dict[str, Any]:
    relative_key = f"{key}_relative"
    value = item.get(relative_key) or item.get(key)
    evidence = _file_evidence(settings, value)
    evidence["source_key"] = relative_key if item.get(relative_key) else key
    return evidence


def _parse_json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if value in (None, ""):
        return {}
    try:
        payload = json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _parse_iso_datetime(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


def _read_text_tail(path: Path, *, max_bytes: int = 5_000_000) -> str:
    try:
        return read_artifact_text_tail(path, max_bytes=max_bytes)
    except (OSError, ValueError):
        return ""


def _read_complete_collection_log(path: Path) -> str:
    """A terminal event or index near the start must not disappear in a tail."""
    limit = 64 * 1024 * 1024
    try:
        raw = read_original_artifact_bytes(path, max_bytes=limit)
    except ValueError as exc:
        if str(exc) == "task_audit_exceeds_read_budget":
            raise ValueError("collection_log_exceeds_complete_read_limit") from exc
        raise
    return raw.decode("utf-8")


def _strip_ansi(value: str) -> str:
    return ANSI_ESCAPE_PATTERN.sub("", value)


def _parse_count_text(value: str) -> int:
    return _safe_int(str(value or "").replace(",", ""))


def _parse_named_count_match(match: re.Match[str]) -> dict[str, int | float]:
    payload: dict[str, int | float] = {}
    for key, value in match.groupdict().items():
        if key == "label":
            continue
        if key == "success_rate":
            parsed = _safe_float(value)
            payload[key] = parsed if parsed is not None else 0.0
        else:
            payload[key] = _parse_count_text(value)
    return payload


def _parse_api_source_evidence_entries(text: str) -> tuple[list[dict[str, Any]], list[str]]:
    """Parse structured API source evidence emitted by collectors."""
    entries: list[dict[str, Any]] = []
    parse_errors: list[str] = []
    for line_number, line in enumerate(str(text or "").splitlines(), start=1):
        if API_SOURCE_EVIDENCE_MARKER not in line:
            continue
        _, _, payload_text = line.partition(API_SOURCE_EVIDENCE_MARKER)
        payload_text = payload_text.strip()
        if not payload_text:
            parse_errors.append(f"line_{line_number}:empty_payload")
            continue
        try:
            parsed = json.loads(payload_text)
        except json.JSONDecodeError as exc:
            parse_errors.append(f"line_{line_number}:json_decode_error:{exc.msg}")
            continue
        if not isinstance(parsed, dict):
            parse_errors.append(f"line_{line_number}:payload_not_object")
            continue
        entries.append(parsed)
    return entries, parse_errors


_COLLECT_FUNDS_SNAPSHOT_FIELD_META = {
    "FSRQ": {
        "business_field": "nav_date",
        "aggregate_count_key": "total_missing_nav_date_count",
        "type_count_key": "missing_nav_date_count",
        "warning_suffix": "missing_nav_date_present",
    },
    "DWJZ": {
        "business_field": "nav_value",
        "aggregate_count_key": "total_missing_nav_value_count",
        "type_count_key": "missing_nav_value_count",
        "warning_suffix": "missing_nav_value_present",
    },
}


def _collect_funds_snapshot_missing_semantics_complete(
    evidence: dict[str, Any],
    *,
    raw_field: str,
    expected_missing_count: int,
) -> bool:
    if expected_missing_count <= 0:
        return True
    field_meta = _COLLECT_FUNDS_SNAPSHOT_FIELD_META.get(raw_field) or {}
    semantics = evidence.get("snapshot_missing_semantics")
    if not isinstance(semantics, dict):
        return False
    if str(semantics.get("truth_policy") or "").strip() != "preserve_missing_semantics":
        return False
    missing_value_handling = str(semantics.get("missing_value_handling") or "")
    if "does not synthesize" not in missing_value_handling or "0" not in missing_value_handling:
        return False
    fields = semantics.get("fields")
    if not isinstance(fields, dict):
        return False
    field_evidence = fields.get(raw_field)
    if not isinstance(field_evidence, dict):
        return False
    if str(field_evidence.get("business_field") or "").strip() != str(field_meta.get("business_field") or ""):
        return False
    if _safe_int(field_evidence.get("missing_count")) != expected_missing_count:
        return False
    reason_counts = field_evidence.get("missing_reason_counts")
    if not isinstance(reason_counts, dict) or not reason_counts:
        return False
    if sum(_safe_int(value) for value in reason_counts.values()) != expected_missing_count:
        return False
    value_type_counts = field_evidence.get("value_type_counts")
    if not isinstance(value_type_counts, dict) or not value_type_counts:
        return False
    samples = field_evidence.get("missing_samples")
    if not isinstance(samples, list) or not samples:
        return False
    first_sample = samples[0]
    if not isinstance(first_sample, dict):
        return False
    required_sample_keys = {"page", "fcode", "raw_field_present", "raw_value_type", "missing_reason"}
    return required_sample_keys.issubset(set(first_sample))


def _collect_funds_evidence_snapshot_semantics_summary(
    evidence: dict[str, Any],
) -> dict[str, Any]:
    total_missing_nav_date_count = _safe_int(evidence.get("total_missing_nav_date_count"))
    total_missing_nav_value_count = _safe_int(evidence.get("total_missing_nav_value_count"))
    aggregate_nav_date_semantics_complete = _collect_funds_snapshot_missing_semantics_complete(
        evidence,
        raw_field="FSRQ",
        expected_missing_count=total_missing_nav_date_count,
    )
    aggregate_nav_value_semantics_complete = _collect_funds_snapshot_missing_semantics_complete(
        evidence,
        raw_field="DWJZ",
        expected_missing_count=total_missing_nav_value_count,
    )
    type_summaries: list[dict[str, Any]] = []
    type_semantics_complete = True
    for type_index, type_evidence in enumerate(evidence.get("types") or [], start=1):
        if not isinstance(type_evidence, dict):
            type_semantics_complete = False
            type_summaries.append(
                {
                    "type_index": type_index,
                    "fund_type": None,
                    "valid": False,
                    "nav_date_semantics_complete": False,
                    "nav_value_semantics_complete": False,
                }
            )
            continue
        type_missing_nav_date_count = _safe_int(type_evidence.get("missing_nav_date_count"))
        type_missing_nav_value_count = _safe_int(type_evidence.get("missing_nav_value_count"))
        type_nav_date_complete = _collect_funds_snapshot_missing_semantics_complete(
            type_evidence,
            raw_field="FSRQ",
            expected_missing_count=type_missing_nav_date_count,
        )
        type_nav_value_complete = _collect_funds_snapshot_missing_semantics_complete(
            type_evidence,
            raw_field="DWJZ",
            expected_missing_count=type_missing_nav_value_count,
        )
        type_semantics_complete = bool(
            type_semantics_complete and type_nav_date_complete and type_nav_value_complete
        )
        type_summaries.append(
            {
                "type_index": type_index,
                "fund_type": type_evidence.get("fund_type"),
                "missing_nav_date_count": type_missing_nav_date_count,
                "missing_nav_value_count": type_missing_nav_value_count,
                "nav_date_semantics_complete": type_nav_date_complete,
                "nav_value_semantics_complete": type_nav_value_complete,
            }
        )
    return {
        "source": evidence.get("source"),
        "schema_version": evidence.get("schema_version"),
        "unique_record_count": _safe_int(evidence.get("unique_record_count")),
        "type_count": _safe_int(evidence.get("type_count")),
        "total_missing_nav_date_count": total_missing_nav_date_count,
        "total_missing_nav_value_count": total_missing_nav_value_count,
        "aggregate_nav_date_semantics_complete": aggregate_nav_date_semantics_complete,
        "aggregate_nav_value_semantics_complete": aggregate_nav_value_semantics_complete,
        "type_semantics_complete": type_semantics_complete,
        "semantics_complete": bool(
            aggregate_nav_date_semantics_complete
            and aggregate_nav_value_semantics_complete
            and type_semantics_complete
        ),
        "type_summaries": type_summaries,
    }


_COLLECT_FUNDS_AGGREGATE_SUM_FIELDS = (
    ("total_page_request_count", "page_request_count"),
    ("total_accepted_page_count", "accepted_page_count"),
    ("total_empty_page_count", "empty_page_count"),
    ("total_failed_page_count", "failed_page_count"),
    ("total_declared_count", "declared_total_count"),
    ("total_raw_record_count", "raw_record_count"),
    ("total_collected_record_count", "collected_record_count"),
    ("total_missing_fcode_count", "missing_fcode_count"),
    ("total_missing_shortname_count", "missing_shortname_count"),
    ("total_missing_nav_date_count", "missing_nav_date_count"),
    ("total_missing_nav_value_count", "missing_nav_value_count"),
    ("total_invalid_record_count", "invalid_record_count"),
    ("total_intra_type_duplicate_code_count", "duplicate_code_count"),
    ("total_short_page_before_last_count", "short_page_before_last_count"),
    ("total_last_page_overflow_count", "last_page_overflow_count"),
    ("total_declared_collected_shortfall", "declared_collected_shortfall"),
    ("total_declared_collected_overflow", "declared_collected_overflow"),
)


def _collect_funds_aggregate_sum_mismatches(evidence: dict[str, Any]) -> list[dict[str, Any]]:
    types = [item for item in evidence.get("types") or [] if isinstance(item, dict)]
    mismatches: list[dict[str, Any]] = []
    for aggregate_key, type_key in _COLLECT_FUNDS_AGGREGATE_SUM_FIELDS:
        if aggregate_key not in evidence:
            continue
        aggregate_value = _safe_int(evidence.get(aggregate_key))
        type_sum = sum(_safe_int(item.get(type_key)) for item in types)
        if aggregate_value != type_sum:
            mismatches.append(
                {
                    "aggregate_key": aggregate_key,
                    "type_key": type_key,
                    "aggregate_value": aggregate_value,
                    "type_sum": type_sum,
                }
            )
    return mismatches


def _validate_collect_funds_api_source_evidence(
    evidence_entries: Sequence[dict[str, Any]],
    *,
    bond_fund_count: int,
) -> tuple[list[str], list[str]]:
    blockers: list[str] = []
    warnings: list[str] = []
    collect_funds_entries: list[tuple[int, dict[str, Any]]] = []
    for index, evidence in enumerate(evidence_entries, start=1):
        task = str(evidence.get("task") or "").strip()
        if not task or task == "collect-funds":
            collect_funds_entries.append((index, evidence))
        elif task != "fundSearch-fallback":
            blockers.append(f"api_source_evidence_task_mismatch:{index}")

    if bond_fund_count > 0 and not collect_funds_entries:
        warnings.append("fundmnrank_api_source_evidence_missing")
        return blockers, warnings

    for index, evidence in collect_funds_entries:
        source = str(evidence.get("source") or "").strip()
        task = str(evidence.get("task") or "").strip()
        page_request_count = _safe_int(evidence.get("total_page_request_count"))
        accepted_page_count = _safe_int(evidence.get("total_accepted_page_count"))
        failed_page_count = _safe_int(evidence.get("total_failed_page_count"))
        type_count = _safe_int(evidence.get("type_count"))
        unique_record_count = _safe_int(evidence.get("unique_record_count"))
        total_declared_count = _safe_int(evidence.get("total_declared_count"))
        total_raw_record_count = _safe_int(evidence.get("total_raw_record_count"))
        total_collected_record_count = _safe_int(evidence.get("total_collected_record_count"))
        total_missing_fcode_count = _safe_int(evidence.get("total_missing_fcode_count"))
        total_missing_shortname_count = _safe_int(evidence.get("total_missing_shortname_count"))
        total_missing_nav_date_count = _safe_int(evidence.get("total_missing_nav_date_count"))
        total_missing_nav_value_count = _safe_int(evidence.get("total_missing_nav_value_count"))
        total_invalid_record_count = _safe_int(evidence.get("total_invalid_record_count"))
        total_short_page_before_last_count = _safe_int(evidence.get("total_short_page_before_last_count"))
        total_last_page_overflow_count = _safe_int(evidence.get("total_last_page_overflow_count"))
        total_declared_collected_shortfall = _safe_int(evidence.get("total_declared_collected_shortfall"))
        total_declared_collected_overflow = _safe_int(evidence.get("total_declared_collected_overflow"))
        total_intra_type_duplicate_code_count = _safe_int(evidence.get("total_intra_type_duplicate_code_count"))
        duplicate_record_count = _safe_int(evidence.get("duplicate_record_count"))
        types = [item for item in evidence.get("types") or [] if isinstance(item, dict)]

        if task and task != "collect-funds":
            blockers.append(f"api_source_evidence_task_mismatch:{index}")
        if source and source != "fundMNRank":
            warnings.append(f"api_source_evidence_unexpected_source:{index}:{source}")
        if type_count > 0 and len(types) != type_count:
            blockers.append(
                f"api_source_evidence_type_count_mismatch:{index}:{type_count}!={len(types)}"
            )
        for mismatch in _collect_funds_aggregate_sum_mismatches(evidence):
            blockers.append(
                f"api_source_evidence_aggregate_sum_mismatch:{index}:"
                f"{mismatch['aggregate_key']}={mismatch['aggregate_value']}!=sum({mismatch['type_key']})={mismatch['type_sum']}"
            )
        if (
            total_collected_record_count > 0
            and unique_record_count + duplicate_record_count != total_collected_record_count
        ):
            blockers.append(
                f"api_source_evidence_unique_duplicate_count_mismatch:{index}:"
                f"{unique_record_count}+{duplicate_record_count}!={total_collected_record_count}"
            )
        if duplicate_record_count < total_intra_type_duplicate_code_count:
            blockers.append(
                f"api_source_evidence_duplicate_count_lt_intra_type_duplicate:{index}:"
                f"{duplicate_record_count}<{total_intra_type_duplicate_code_count}"
            )
        if total_intra_type_duplicate_code_count > 0:
            warnings.append(f"api_source_evidence_intra_type_duplicate_coverage_unproven:{index}")
        if page_request_count <= 0:
            blockers.append(f"api_source_evidence_page_request_empty:{index}")
        if accepted_page_count <= 0:
            blockers.append(f"api_source_evidence_accepted_page_empty:{index}")
        if failed_page_count > 0:
            blockers.append(f"api_source_evidence_failed_pages_present:{index}")
        if type_count <= 0:
            blockers.append(f"api_source_evidence_type_count_empty:{index}")
        if total_invalid_record_count > 0:
            blockers.append(f"api_source_evidence_invalid_records_present:{index}")
        if total_missing_fcode_count > 0:
            blockers.append(f"api_source_evidence_missing_fcode_present:{index}")
        if total_missing_shortname_count > 0:
            blockers.append(f"api_source_evidence_missing_shortname_present:{index}")
        if total_short_page_before_last_count > 0:
            blockers.append(f"api_source_evidence_short_page_before_last_present:{index}")
        if total_last_page_overflow_count > 0:
            blockers.append(f"api_source_evidence_last_page_overflow_present:{index}")
        if total_declared_collected_shortfall > 0:
            blockers.append(f"api_source_evidence_declared_shortfall_present:{index}")
        if total_declared_collected_overflow > 0:
            blockers.append(f"api_source_evidence_declared_overflow_present:{index}")
        if (
            total_missing_nav_date_count > 0
            and not _collect_funds_snapshot_missing_semantics_complete(
                evidence,
                raw_field="FSRQ",
                expected_missing_count=total_missing_nav_date_count,
            )
        ):
            warnings.append(f"api_source_evidence_missing_nav_date_present:{index}")
        if (
            total_missing_nav_value_count > 0
            and not _collect_funds_snapshot_missing_semantics_complete(
                evidence,
                raw_field="DWJZ",
                expected_missing_count=total_missing_nav_value_count,
            )
        ):
            warnings.append(f"api_source_evidence_missing_nav_value_present:{index}")
        if bond_fund_count > 0 and unique_record_count > 0 and unique_record_count != bond_fund_count:
            blockers.append(f"api_source_evidence_unique_count_mismatch:{index}")
        if (
            total_declared_count > 0
            and total_raw_record_count > 0
            and total_declared_count != total_raw_record_count
        ):
            warnings.append(f"api_source_evidence_declared_collected_count_mismatch:{index}")
        elif (
            total_declared_count > 0
            and total_collected_record_count > 0
            and total_declared_count != total_collected_record_count
        ):
            warnings.append(f"api_source_evidence_declared_collected_count_mismatch:{index}")

        for type_index, type_evidence in enumerate(evidence.get("types") or [], start=1):
            if not isinstance(type_evidence, dict):
                warnings.append(f"api_source_evidence_type_payload_invalid:{index}:{type_index}")
                continue
            if type_evidence.get("unique_coverage_status") in {"coverage_not_proven", "unknown_declared_total"}:
                warnings.append(f"api_source_evidence_type_unique_coverage_unproven:{index}:{type_index}")
            type_failed_pages = _safe_int(type_evidence.get("failed_page_count"))
            type_page_requests = _safe_int(type_evidence.get("page_request_count"))
            type_expected_pages = _safe_int(type_evidence.get("expected_total_pages"))
            type_accepted_pages = _safe_int(type_evidence.get("accepted_page_count"))
            type_invalid_records = _safe_int(type_evidence.get("invalid_record_count"))
            type_missing_fcode = _safe_int(type_evidence.get("missing_fcode_count"))
            type_missing_shortname = _safe_int(type_evidence.get("missing_shortname_count"))
            type_missing_nav_date = _safe_int(type_evidence.get("missing_nav_date_count"))
            type_missing_nav_value = _safe_int(type_evidence.get("missing_nav_value_count"))
            type_short_page_before_last = _safe_int(type_evidence.get("short_page_before_last_count"))
            type_last_page_overflow = _safe_int(type_evidence.get("last_page_overflow_count"))
            type_declared_shortfall = _safe_int(type_evidence.get("declared_collected_shortfall"))
            type_declared_overflow = _safe_int(type_evidence.get("declared_collected_overflow"))
            first_quality = (
                type_evidence.get("first_page_quality")
                if isinstance(type_evidence.get("first_page_quality"), dict)
                else {}
            )
            first_quality_status = str(first_quality.get("status") or "").strip()
            if type_failed_pages > 0:
                blockers.append(f"api_source_evidence_type_failed_pages_present:{index}:{type_index}")
            if type_expected_pages > 0 and type_page_requests > 0 and type_page_requests < type_expected_pages:
                warnings.append(f"api_source_evidence_type_page_count_short:{index}:{type_index}")
            if type_expected_pages > 0 and type_accepted_pages <= 0:
                blockers.append(f"api_source_evidence_type_accepted_page_empty:{index}:{type_index}")
            if type_invalid_records > 0:
                blockers.append(f"api_source_evidence_type_invalid_records_present:{index}:{type_index}")
            if type_missing_fcode > 0:
                blockers.append(f"api_source_evidence_type_missing_fcode_present:{index}:{type_index}")
            if type_missing_shortname > 0:
                blockers.append(f"api_source_evidence_type_missing_shortname_present:{index}:{type_index}")
            if type_short_page_before_last > 0:
                blockers.append(f"api_source_evidence_type_short_page_before_last_present:{index}:{type_index}")
            if type_last_page_overflow > 0:
                blockers.append(f"api_source_evidence_type_last_page_overflow_present:{index}:{type_index}")
            if type_declared_shortfall > 0:
                blockers.append(f"api_source_evidence_type_declared_shortfall_present:{index}:{type_index}")
            if type_declared_overflow > 0:
                blockers.append(f"api_source_evidence_type_declared_overflow_present:{index}:{type_index}")
            if (
                type_missing_nav_date > 0
                and not _collect_funds_snapshot_missing_semantics_complete(
                    type_evidence,
                    raw_field="FSRQ",
                    expected_missing_count=type_missing_nav_date,
                )
            ):
                warnings.append(f"api_source_evidence_type_missing_nav_date_present:{index}:{type_index}")
            if (
                type_missing_nav_value > 0
                and not _collect_funds_snapshot_missing_semantics_complete(
                    type_evidence,
                    raw_field="DWJZ",
                    expected_missing_count=type_missing_nav_value,
                )
            ):
                warnings.append(f"api_source_evidence_type_missing_nav_value_present:{index}:{type_index}")
            if first_quality_status and first_quality_status not in {"ok", "empty_rank_records"}:
                blockers.append(f"api_source_evidence_first_page_quality_not_ok:{index}:{type_index}")

    return blockers, warnings


def _validate_fund_search_fallback_api_source_evidence(
    evidence_entries: Sequence[dict[str, Any]],
) -> tuple[list[str], list[str]]:
    blockers: list[str] = []
    warnings: list[str] = []
    for index, evidence in enumerate(evidence_entries, start=1):
        task = str(evidence.get("task") or "").strip()
        if task != "fundSearch-fallback":
            continue
        source = str(evidence.get("source") or "").strip()
        target_count = _safe_int(evidence.get("target_count"))
        queried_count = _safe_int(evidence.get("queried_count"))
        failed_query_count = _safe_int(evidence.get("failed_query_count"))
        failed_response_count = _safe_int(evidence.get("failed_response_count"))
        quality_unavailable_count = _safe_int(evidence.get("quality_unavailable_count"))
        matched_record_count = _safe_int(evidence.get("matched_record_count"))
        missing_record_count = _safe_int(evidence.get("missing_record_count"))

        if source and source != "fundSearch":
            warnings.append(f"fund_search_evidence_unexpected_source:{index}:{source}")
        if target_count <= 0:
            warnings.append(f"fund_search_evidence_target_empty:{index}")
        if target_count > 0 and queried_count <= 0:
            blockers.append(f"fund_search_evidence_queried_empty:{index}")
        if failed_query_count > 0:
            blockers.append(f"fund_search_evidence_failed_queries_present:{index}")
        if failed_response_count > 0:
            blockers.append(f"fund_search_evidence_failed_responses_present:{index}")
        if quality_unavailable_count > 0:
            warnings.append(f"fund_search_evidence_quality_unavailable:{index}")
        if queried_count > 0 and matched_record_count + missing_record_count + failed_query_count != queried_count:
            blockers.append(f"fund_search_evidence_count_mismatch:{index}")

        status_counts = evidence.get("quality_status_counts")
        if isinstance(status_counts, dict):
            bad_statuses = sorted(
                str(status)
                for status, count in status_counts.items()
                if _safe_int(count) > 0 and str(status) not in {"ok", "empty_search_records"}
            )
            if bad_statuses:
                blockers.append(
                    f"fund_search_evidence_bad_quality_status:{index}:{','.join(bad_statuses)}"
                )

    return blockers, warnings


def _collect_funds_task_sort_key(candidate: dict[str, Any]) -> str:
    item = candidate.get("item") if isinstance(candidate.get("item"), dict) else {}
    return str(
        item.get("finished_at")
        or item.get("started_at")
        or candidate.get("index_generated_at")
        or candidate.get("index_parent")
        or ""
    )


def _collector_evidence_contract_truth(settings: Settings) -> dict[str, Any]:
    blockers: list[str] = []
    warnings: list[str] = []
    log_root = settings.project_root / "reports" / ".audit" / "subprocess_logs"
    result: dict[str, Any] = {
        "status": "ok",
        "task": "collect-funds",
        "source": "fundMNRank",
        "log_root": str(log_root),
        "checked_index_count": 0,
        "collect_funds_task_count": 0,
        "latest_collect_funds_task": None,
        "truth_note": (
            "This section audits the latest successful collect-funds runtime evidence contract. "
            "It does not rewrite warnings that belong to an older canonical screening run; those "
            "warnings must clear through the next real canonical run."
        ),
    }
    if not log_root.exists():
        warnings.append("subprocess_log_root_missing")
        result["status"] = _classify_status(blockers, warnings)
        result["blockers"] = blockers
        result["warnings"] = warnings
        return result

    index_paths = sorted(
        log_root.glob("*/subprocess_index.json"),
        key=lambda path: path.stat().st_mtime if path.exists() else 0,
        reverse=True,
    )
    result["checked_index_count"] = len(index_paths)
    if not index_paths:
        warnings.append("subprocess_index_files_missing")
        result["status"] = _classify_status(blockers, warnings)
        result["blockers"] = blockers
        result["warnings"] = warnings
        return result

    candidates: list[dict[str, Any]] = []
    for index_path in index_paths:
        index_payload = _read_json(index_path)
        if not index_payload:
            continue
        for item in index_payload.get("items") or []:
            if not isinstance(item, dict):
                continue
            if str(item.get("task_name") or "").strip() != "collect-funds":
                continue
            index_project_path = str(index_path)
            try:
                index_project_path = str(index_path.relative_to(settings.project_root))
            except ValueError:
                pass
            candidates.append(
                {
                    "index_path": index_path,
                    "index_project_path": index_project_path,
                    "index_parent": index_path.parent.name,
                    "index_generated_at": index_payload.get("generated_at"),
                    "item": dict(item),
                }
            )
    candidates.sort(key=_collect_funds_task_sort_key, reverse=True)
    result["collect_funds_task_count"] = len(candidates)
    if not candidates:
        warnings.append("collect_funds_task_logs_missing")
        result["status"] = _classify_status(blockers, warnings)
        result["blockers"] = blockers
        result["warnings"] = warnings
        return result

    latest_successful: dict[str, Any] | None = None
    for candidate in candidates:
        task_item = candidate["item"]
        if (
            bool(task_item.get("success"))
            and task_item.get("return_code") in (0, "0")
            and not bool(task_item.get("timed_out"))
        ):
            latest_successful = candidate
            break
    if latest_successful is None:
        blockers.append("latest_successful_collect_funds_task_missing")
        result["latest_collect_funds_task"] = {
            "latest_seen": {
                "index_project_path": candidates[0].get("index_project_path"),
                "started_at": candidates[0]["item"].get("started_at"),
                "finished_at": candidates[0]["item"].get("finished_at"),
                "success": bool(candidates[0]["item"].get("success")),
                "return_code": candidates[0]["item"].get("return_code"),
                "timed_out": bool(candidates[0]["item"].get("timed_out")),
            }
        }
        result["status"] = _classify_status(blockers, warnings)
        result["blockers"] = blockers
        result["warnings"] = warnings
        return result

    task_item = latest_successful["item"]
    stdout_file = _subprocess_file_evidence(settings, task_item, "stdout_path")
    latest_summary: dict[str, Any] = {
        "index_project_path": latest_successful.get("index_project_path"),
        "index_generated_at": latest_successful.get("index_generated_at"),
        "started_at": task_item.get("started_at"),
        "finished_at": task_item.get("finished_at"),
        "duration_seconds": task_item.get("duration_seconds"),
        "success": bool(task_item.get("success")),
        "return_code": task_item.get("return_code"),
        "timed_out": bool(task_item.get("timed_out")),
        "stdout_file": stdout_file,
        "business_summary": {},
        "api_source_evidence_count": 0,
        "evidence_summaries": [],
        "stdout_validation_blockers": [],
        "stdout_validation_warnings": [],
    }
    result["latest_collect_funds_task"] = latest_summary
    if not stdout_file.get("exists"):
        blockers.append("latest_successful_collect_funds_stdout_missing")
        result["status"] = _classify_status(blockers, warnings)
        result["blockers"] = blockers
        result["warnings"] = warnings
        return result

    stdout_result, stdout_blockers, stdout_warnings = _stdout_result_semantics(
        "collect-funds",
        str(stdout_file.get("path") or ""),
    )
    latest_summary["business_summary"] = stdout_result.get("business_summary") or {}
    latest_summary["stdout_validation_blockers"] = stdout_blockers
    latest_summary["stdout_validation_warnings"] = stdout_warnings
    blockers.extend(f"latest_collect_funds_{blocker}" for blocker in stdout_blockers)
    warnings.extend(f"latest_collect_funds_{warning}" for warning in stdout_warnings)

    evidence_entries = [
        dict(evidence)
        for evidence in stdout_result.get("api_source_evidence") or []
        if isinstance(evidence, dict)
        and str(evidence.get("task") or "").strip() == "collect-funds"
        and str(evidence.get("source") or "").strip() == "fundMNRank"
    ]
    latest_summary["api_source_evidence_count"] = len(evidence_entries)
    if not evidence_entries:
        blockers.append("latest_collect_funds_fundmnrank_evidence_missing")

    evidence_summaries: list[dict[str, Any]] = []
    for index, evidence in enumerate(evidence_entries, start=1):
        summary = _collect_funds_evidence_snapshot_semantics_summary(evidence)
        summary["evidence_index"] = index
        evidence_summaries.append(summary)
        if not bool(summary.get("semantics_complete")):
            blockers.append(f"latest_collect_funds_snapshot_missing_semantics_incomplete:{index}")
    latest_summary["evidence_summaries"] = evidence_summaries
    latest_summary["snapshot_missing_semantics_contract_ok"] = bool(
        evidence_summaries and all(bool(item.get("semantics_complete")) for item in evidence_summaries)
    )

    result["status"] = _classify_status(blockers, warnings)
    result["blockers"] = blockers
    result["warnings"] = warnings
    return result


def _validate_collect_nav_api_source_evidence(
    evidence_entries: Sequence[dict[str, Any]],
    *,
    inserted_nav_record_count: int | None = None,
) -> tuple[list[str], list[str]]:
    blockers: list[str] = []
    warnings: list[str] = []
    if not evidence_entries:
        return blockers, warnings

    nav_entries = [
        dict(item)
        for item in evidence_entries
        if str(item.get("task") or "").strip() == "collect-nav"
    ]
    if not nav_entries:
        return blockers, warnings

    for index, evidence in enumerate(nav_entries, start=1):
        source = str(evidence.get("source") or "").strip()
        collection_mode = str(evidence.get("collection_mode") or "").strip()
        target_count = _safe_int(evidence.get("target_fund_count"))
        request_count = _safe_int(evidence.get("request_count"))
        response_count = _safe_int(evidence.get("response_count"))
        accepted_response_count = _safe_int(evidence.get("accepted_response_count"))
        failed_response_count = _safe_int(evidence.get("failed_response_count"))
        has_data_response_count = _safe_int(evidence.get("has_data_response_count"))
        empty_response_count = _safe_int(evidence.get("empty_response_count"))
        transport_exception_count = _safe_int(evidence.get("transport_exception_count"))
        final_failed_fund_count = _safe_int(evidence.get("final_failed_fund_count"))
        evidence_inserted_count = _safe_int(evidence.get("inserted_nav_record_count"))
        updated_fund_count = _safe_int(evidence.get("updated_fund_count"))
        returned_nav_record_count = _safe_int(evidence.get("returned_nav_record_count"))
        no_new_nav_record_response_count = _safe_int(evidence.get("no_new_nav_record_response_count"))
        reference_nav_date = str(evidence.get("reference_nav_date") or "").strip()
        latest_payload_nav_date = str(evidence.get("latest_payload_nav_date") or "").strip()
        earliest_payload_nav_date = str(evidence.get("earliest_payload_nav_date") or "").strip()

        if source and source != "fundMNHisNetList":
            warnings.append(f"collect_nav_evidence_unexpected_source:{index}:{source}")
        if collection_mode and collection_mode not in {"incremental", "full", "single", "limit"}:
            warnings.append(f"collect_nav_evidence_unknown_collection_mode:{index}:{collection_mode}")
        if target_count > 0 and request_count <= 0 and collection_mode in {"incremental", "full", "limit"}:
            blockers.append(f"collect_nav_evidence_request_empty_for_targets:{index}")
        if failed_response_count > 0:
            blockers.append(f"collect_nav_evidence_failed_responses_present:{index}")
        if _safe_int(evidence.get("empty_response_count")) > 0:
            warnings.append(f"collect_nav_evidence_empty_responses_present:{index}")
        if transport_exception_count > 0:
            blockers.append(f"collect_nav_evidence_transport_exceptions_present:{index}")
        if final_failed_fund_count > 0:
            blockers.append(f"collect_nav_evidence_final_failed_funds_present:{index}")
        if request_count > 0 and response_count + transport_exception_count != request_count:
            blockers.append(f"collect_nav_evidence_request_response_count_mismatch:{index}")
        if response_count > 0 and accepted_response_count + failed_response_count != response_count:
            blockers.append(f"collect_nav_evidence_accept_failed_count_mismatch:{index}")
        if response_count > 0 and accepted_response_count > response_count:
            blockers.append(f"collect_nav_evidence_accepted_gt_response:{index}")
        if response_count > 0 and has_data_response_count + empty_response_count != response_count:
            blockers.append(f"collect_nav_evidence_data_empty_count_mismatch:{index}")
        if has_data_response_count > accepted_response_count:
            blockers.append(f"collect_nav_evidence_has_data_gt_accepted:{index}")
        if updated_fund_count > target_count and target_count >= 0:
            blockers.append(f"collect_nav_evidence_updated_gt_target:{index}")
        if evidence_inserted_count > 0 and updated_fund_count <= 0:
            warnings.append(f"collect_nav_evidence_inserted_without_updated_fund_count:{index}")
        if (
            inserted_nav_record_count is not None
            and evidence_inserted_count != _safe_int(inserted_nav_record_count)
        ):
            blockers.append(f"collect_nav_evidence_inserted_count_mismatch:{index}")
        if evidence_inserted_count > returned_nav_record_count and returned_nav_record_count >= 0:
            blockers.append(f"collect_nav_evidence_inserted_gt_returned_records:{index}")
        if no_new_nav_record_response_count > has_data_response_count:
            blockers.append(f"collect_nav_evidence_no_advance_gt_has_data:{index}")
        if updated_fund_count + no_new_nav_record_response_count > has_data_response_count and has_data_response_count > 0:
            blockers.append(f"collect_nav_evidence_write_no_advance_gt_has_data:{index}")
        if earliest_payload_nav_date and latest_payload_nav_date and earliest_payload_nav_date > latest_payload_nav_date:
            blockers.append(f"collect_nav_evidence_payload_date_range_inverted:{index}")
        if reference_nav_date and latest_payload_nav_date and latest_payload_nav_date < reference_nav_date:
            is_incremental_no_advance_observation = (
                collection_mode == "incremental"
                and request_count > 0
                and response_count == request_count
                and accepted_response_count == response_count
                and failed_response_count == 0
                and empty_response_count == 0
                and transport_exception_count == 0
                and final_failed_fund_count == 0
                and evidence_inserted_count == 0
                and updated_fund_count == 0
                and has_data_response_count > 0
                and no_new_nav_record_response_count == has_data_response_count
            )
            if is_incremental_no_advance_observation:
                warnings.append(f"collect_nav_evidence_no_advance_latest_payload_before_reference:{index}")
            else:
                blockers.append(f"collect_nav_evidence_latest_payload_before_reference:{index}")

        status_counts = evidence.get("quality_status_counts")
        if isinstance(status_counts, dict):
            bad_statuses = sorted(
                str(status)
                for status, count in status_counts.items()
                if _safe_int(count) > 0
                and str(status) not in {"ok", "empty_nav_records", "transport_exception"}
            )
            if bad_statuses:
                blockers.append(
                    f"collect_nav_evidence_bad_quality_status:{index}:{','.join(bad_statuses)}"
                )
            status_total = sum(_safe_int(count) for count in status_counts.values())
            if response_count + transport_exception_count > 0 and status_total != response_count + transport_exception_count:
                blockers.append(f"collect_nav_evidence_quality_status_count_mismatch:{index}")
        elif status_counts is not None:
            blockers.append(f"collect_nav_evidence_quality_status_counts_invalid:{index}")

        payload_type_counts = evidence.get("quality_payload_type_counts")
        if isinstance(payload_type_counts, dict):
            payload_type_total = sum(_safe_int(count) for count in payload_type_counts.values())
            if response_count > 0 and payload_type_total != response_count:
                blockers.append(f"collect_nav_evidence_payload_type_count_mismatch:{index}")
        elif payload_type_counts is not None:
            blockers.append(f"collect_nav_evidence_payload_type_counts_invalid:{index}")

        page_index_counts = evidence.get("requested_page_index_counts")
        if isinstance(page_index_counts, dict):
            page_index_total = sum(_safe_int(count) for count in page_index_counts.values())
            if request_count > 0 and page_index_total != request_count:
                blockers.append(f"collect_nav_evidence_requested_page_index_count_mismatch:{index}")
            if collection_mode == "incremental":
                non_first_pages = [
                    str(page_index)
                    for page_index, count in page_index_counts.items()
                    if _safe_int(count) > 0 and str(page_index).strip() not in {"1", "1.0"}
                ]
                if non_first_pages:
                    blockers.append(
                        f"collect_nav_evidence_incremental_non_first_page_present:{index}:"
                        + ",".join(sorted(non_first_pages))
                    )
        elif page_index_counts is not None:
            blockers.append(f"collect_nav_evidence_requested_page_index_counts_invalid:{index}")

        min_page_size = _safe_int(evidence.get("min_requested_page_size"))
        max_page_size = _safe_int(evidence.get("max_requested_page_size"))
        if request_count > 0:
            if min_page_size <= 0 or max_page_size <= 0:
                blockers.append(f"collect_nav_evidence_requested_page_size_missing:{index}")
            elif min_page_size > max_page_size:
                blockers.append(f"collect_nav_evidence_requested_page_size_range_inverted:{index}")

    return blockers, warnings


_SINGLE_TARGET_TIANTIAN_EVIDENCE_CONTRACTS = {
    "collect-companies": {
        "sources": {
            "companyApi2": {
                "allowed_ok_statuses": {"ok"},
                "allowed_empty_statuses": {"empty_company_data"},
                "allowed_payload_types": {"dict", "NoneType"},
                "missing_evidence_warning": "company_source_evidence_missing",
                "empty_warning_suffix": "company_empty_responses_present",
            },
        },
    },
    "collect-details": {
        "sources": {
            "fundMNDetailInformation": {
                "allowed_ok_statuses": {"ok"},
                "allowed_empty_statuses": {"empty_datas", "invalid_datas_type"},
                "allowed_payload_types": {"dict", "NoneType"},
                "missing_evidence_warning": "detail_source_evidence_missing",
                "empty_warning_suffix": "detail_empty_responses_present",
                "empty_semantics": {
                    "count_key": "empty_detail_count",
                    "reason_counts_key": "empty_detail_reason_counts",
                    "scope_key": "stop_watch_fallback_scope",
                    "required_scope": "not_counted_as_detail_success",
                    "report_path_key": "empty_detail_report_path",
                },
            },
        },
    },
    "collect-managers": {
        "sources": {
            "fundMNMangerList": {
                "allowed_ok_statuses": {"ok"},
                "allowed_empty_statuses": {
                    "empty_manager_list_records",
                    "invalid_datas_type",
                    "request_failed",
                },
                "allowed_payload_types": {"list"},
                "allowed_empty_payload_types": {"APIFailureResult", "NoneType"},
                "missing_evidence_warning": "manager_list_source_evidence_missing",
                "empty_warning_suffix": "manager_list_empty_responses_present",
            },
            "fundMSNMangerInfo": {
                "allowed_ok_statuses": {"ok"},
                "allowed_empty_statuses": {"empty_manager_info", "invalid_datas_type"},
                "allowed_payload_types": {"dict", "NoneType"},
                "missing_evidence_warning": "manager_detail_source_evidence_missing",
                "empty_warning_suffix": "manager_detail_empty_responses_present",
            },
        },
    },
}


def _manager_list_request_failures_within_watchline(evidence: dict[str, Any]) -> bool:
    target_count = _safe_int(evidence.get("target_count"))
    accepted_response_count = _safe_int(evidence.get("accepted_response_count"))
    failed_response_count = _safe_int(evidence.get("failed_response_count"))
    if failed_response_count <= 0:
        return True
    if accepted_response_count <= 0:
        return False
    watchline = max(50, int(target_count * 0.01)) if target_count > 0 else 50
    return failed_response_count <= watchline


def _manager_list_empty_failures_are_explained(evidence: dict[str, Any]) -> bool:
    """Accept only bounded upstream-empty manager-list failures with exact evidence counts."""
    failed_response_count = _safe_int(evidence.get("failed_response_count"))
    if failed_response_count <= 0:
        return True
    if not _manager_list_request_failures_within_watchline(evidence):
        return False
    if _safe_int(evidence.get("transport_exception_count")) > 0:
        return False
    if _safe_int(evidence.get("save_failed_count")) > 0:
        return False
    if _safe_int(evidence.get("empty_response_count")) != failed_response_count:
        return False

    status_counts = evidence.get("quality_status_counts")
    payload_type_counts = evidence.get("quality_payload_type_counts")
    if not isinstance(status_counts, dict) or not isinstance(payload_type_counts, dict):
        return False
    empty_status_count = sum(
        _safe_int(count)
        for status, count in status_counts.items()
        if str(status) in {"empty_manager_list_records", "invalid_datas_type", "request_failed"}
    )
    empty_payload_count = sum(
        _safe_int(count)
        for payload_type, count in payload_type_counts.items()
        if str(payload_type) in {"APIFailureResult", "NoneType"}
    )
    return (
        empty_status_count == failed_response_count
        and empty_payload_count == failed_response_count
    )


def _validate_single_target_tiantian_api_source_evidence(
    evidence_entries: Sequence[dict[str, Any]],
    *,
    task_name: str,
    business_success_count: int | None = None,
) -> tuple[list[str], list[str]]:
    """Validate request/response API evidence for per-target Tiantian collectors."""
    blockers: list[str] = []
    warnings: list[str] = []
    contract = _SINGLE_TARGET_TIANTIAN_EVIDENCE_CONTRACTS.get(str(task_name or "").strip())
    if not contract:
        return blockers, warnings

    source_contracts = contract.get("sources") if isinstance(contract.get("sources"), dict) else {}
    entries_by_source: dict[str, list[tuple[int, dict[str, Any]]]] = {source: [] for source in source_contracts}
    for index, evidence in enumerate(evidence_entries, start=1):
        if not isinstance(evidence, dict):
            continue
        task = str(evidence.get("task") or "").strip()
        source = str(evidence.get("source") or "").strip()
        if task and task != task_name:
            blockers.append(f"single_target_evidence_task_mismatch:{index}:{task}")
            continue
        if source not in source_contracts:
            blockers.append(f"single_target_evidence_unexpected_source:{index}:{source or 'empty'}")
            continue
        entries_by_source.setdefault(source, []).append((index, evidence))

    has_business_success = business_success_count is None or _safe_int(business_success_count) > 0
    for source, source_contract in source_contracts.items():
        source_entries = entries_by_source.get(source) or []
        if has_business_success and not source_entries:
            warnings.append(str(source_contract.get("missing_evidence_warning") or f"{source}_evidence_missing"))
            continue
        for index, evidence in source_entries:
            target_count = _safe_int(evidence.get("target_count"))
            request_count = _safe_int(evidence.get("request_count"))
            response_count = _safe_int(evidence.get("response_count"))
            accepted_response_count = _safe_int(evidence.get("accepted_response_count"))
            failed_response_count = _safe_int(evidence.get("failed_response_count"))
            has_data_response_count = _safe_int(evidence.get("has_data_response_count"))
            empty_response_count = _safe_int(evidence.get("empty_response_count"))
            transport_exception_count = _safe_int(evidence.get("transport_exception_count"))
            returned_record_count = _safe_int(evidence.get("returned_record_count"))
            saved_fund_count = _safe_int(evidence.get("saved_fund_count"))
            written_record_count = _safe_int(evidence.get("written_record_count"))
            save_failed_count = _safe_int(evidence.get("save_failed_count"))

            if target_count > 0 and request_count <= 0:
                blockers.append(f"single_target_evidence_request_empty_for_targets:{index}:{source}")
            if request_count > 0 and response_count + transport_exception_count != request_count:
                blockers.append(f"single_target_evidence_request_response_count_mismatch:{index}:{source}")
            if request_count > 0 and accepted_response_count + failed_response_count != response_count + transport_exception_count:
                blockers.append(f"single_target_evidence_accept_failed_count_mismatch:{index}:{source}")
            if response_count > 0 and has_data_response_count + empty_response_count != response_count:
                blockers.append(f"single_target_evidence_data_empty_count_mismatch:{index}:{source}")
            if accepted_response_count > response_count:
                blockers.append(f"single_target_evidence_accepted_gt_response:{index}:{source}")
            if has_data_response_count > accepted_response_count:
                blockers.append(f"single_target_evidence_has_data_gt_accepted:{index}:{source}")
            if transport_exception_count > 0:
                blockers.append(f"single_target_evidence_transport_exceptions_present:{index}:{source}")
            if failed_response_count > empty_response_count:
                blockers.append(f"single_target_evidence_failed_nonempty_responses_present:{index}:{source}")
            if (
                task_name == "collect-managers"
                and source == "fundMNMangerList"
                and failed_response_count > 0
            ):
                if _manager_list_empty_failures_are_explained(evidence):
                    warnings.append(
                        f"manager_list_upstream_request_failed_watch:{index}:{source}"
                    )
                elif _manager_list_request_failures_within_watchline(evidence):
                    blockers.append(
                        f"single_target_evidence_manager_list_empty_semantics_invalid:{index}:{source}"
                    )
                else:
                    blockers.append(
                        f"single_target_evidence_manager_list_request_failed_exceeds_watchline:{index}:{source}"
                    )
            if save_failed_count > 0:
                blockers.append(f"single_target_evidence_save_failures_present:{index}:{source}")
            if saved_fund_count > has_data_response_count and has_data_response_count >= 0:
                blockers.append(f"single_target_evidence_saved_gt_has_data:{index}:{source}")
            if written_record_count > returned_record_count and returned_record_count >= 0:
                blockers.append(f"single_target_evidence_written_gt_returned:{index}:{source}")
            if written_record_count > 0 and saved_fund_count <= 0:
                blockers.append(f"single_target_evidence_written_without_saved:{index}:{source}")
            if task_name == "collect-companies":
                try:
                    success, failed, skipped = (int(evidence[key]) for key in ("success_count", "failed_count", "skipped_count"))
                    if min(success, failed, skipped) < 0 or success + failed + skipped != target_count:
                        raise ValueError("outcomes do not balance")
                    if success != written_record_count or success != saved_fund_count:
                        blockers.append(f"company_evidence_success_written_mismatch:{index}")
                    if business_success_count is not None and success != business_success_count:
                        blockers.append(f"company_evidence_business_success_mismatch:{index}")
                except (TypeError, ValueError, KeyError):
                    blockers.append(f"company_evidence_outcome_counts_invalid:{index}")
                if _safe_int(evidence.get("cooldown_count")) > 0:
                    warnings.append(f"company_retry_cooldown_pending:{index}")
                if _safe_int(evidence.get("remaining_eligible_count")) > 0:
                    warnings.append(f"company_refresh_backlog_pending:{index}")
            if empty_response_count > 0:
                warnings.append(
                    f"{source_contract.get('empty_warning_suffix') or 'single_target_empty_responses_present'}:"
                    f"{index}:{source}"
                )

            status_counts = evidence.get("quality_status_counts")
            allowed_statuses = set(source_contract.get("allowed_ok_statuses") or set()) | set(
                source_contract.get("allowed_empty_statuses") or set()
            ) | {"transport_exception"}
            if isinstance(status_counts, dict):
                status_total = sum(_safe_int(count) for count in status_counts.values())
                if response_count + transport_exception_count > 0 and status_total != response_count + transport_exception_count:
                    blockers.append(f"single_target_evidence_quality_status_count_mismatch:{index}:{source}")
                bad_statuses = sorted(
                    str(status)
                    for status, count in status_counts.items()
                    if _safe_int(count) > 0 and str(status) not in allowed_statuses
                )
                if bad_statuses:
                    blockers.append(
                        f"single_target_evidence_bad_quality_status:{index}:{source}:{','.join(bad_statuses)}"
                    )
            elif status_counts is not None:
                blockers.append(f"single_target_evidence_quality_status_counts_invalid:{index}:{source}")

            payload_type_counts = evidence.get("quality_payload_type_counts")
            allowed_payload_types = set(source_contract.get("allowed_payload_types") or set())
            if isinstance(payload_type_counts, dict):
                payload_type_total = sum(_safe_int(count) for count in payload_type_counts.values())
                if response_count > 0 and payload_type_total != response_count:
                    blockers.append(f"single_target_evidence_payload_type_count_mismatch:{index}:{source}")
                allowed_empty_payload_types = set(source_contract.get("allowed_empty_payload_types") or set())
                bad_payload_types = sorted(
                    str(payload_type)
                    for payload_type, count in payload_type_counts.items()
                    if _safe_int(count) > 0
                    and allowed_payload_types
                    and str(payload_type) not in allowed_payload_types | allowed_empty_payload_types
                )
                if bad_payload_types:
                    blockers.append(
                        f"single_target_evidence_bad_payload_type:{index}:{source}:{','.join(bad_payload_types)}"
                    )
            elif payload_type_counts is not None:
                blockers.append(f"single_target_evidence_payload_type_counts_invalid:{index}:{source}")

            empty_semantics = source_contract.get("empty_semantics")
            if isinstance(empty_semantics, dict) and empty_response_count > 0:
                semantics_count = _safe_int(evidence.get(str(empty_semantics.get("count_key") or "")))
                reason_counts = evidence.get(str(empty_semantics.get("reason_counts_key") or ""))
                scope_key = str(empty_semantics.get("scope_key") or "")
                required_scope = str(empty_semantics.get("required_scope") or "")
                report_path_key = str(empty_semantics.get("report_path_key") or "")
                if semantics_count != empty_response_count:
                    blockers.append(f"single_target_evidence_empty_semantics_count_mismatch:{index}:{source}")
                if not isinstance(reason_counts, dict) or not reason_counts:
                    blockers.append(f"single_target_evidence_empty_semantics_reason_missing:{index}:{source}")
                elif sum(_safe_int(count) for count in reason_counts.values()) != empty_response_count:
                    blockers.append(f"single_target_evidence_empty_semantics_reason_count_mismatch:{index}:{source}")
                if scope_key and required_scope and str(evidence.get(scope_key) or "").strip() != required_scope:
                    blockers.append(f"single_target_evidence_empty_semantics_scope_mismatch:{index}:{source}")
                if report_path_key and not str(evidence.get(report_path_key) or "").strip():
                    warnings.append(f"single_target_evidence_empty_semantics_report_missing:{index}:{source}")

    return blockers, warnings


def _single_target_completion_failure_matches_empty_semantics(
    task_name: str,
    *,
    failed_count: int,
    evidence_entries: Sequence[dict[str, Any]],
) -> bool:
    if failed_count <= 0:
        return True
    contract = _SINGLE_TARGET_TIANTIAN_EVIDENCE_CONTRACTS.get(str(task_name or "").strip())
    if not contract:
        return False
    source_contracts = contract.get("sources") if isinstance(contract.get("sources"), dict) else {}
    explained_empty_count = 0
    for evidence in evidence_entries:
        if not isinstance(evidence, dict):
            continue
        source = str(evidence.get("source") or "").strip()
        source_contract = source_contracts.get(source)
        if not isinstance(source_contract, dict):
            continue
        if _safe_int(evidence.get("transport_exception_count")) > 0:
            return False
        if _safe_int(evidence.get("save_failed_count")) > 0:
            return False
        failed_response_count = _safe_int(evidence.get("failed_response_count"))
        empty_response_count = _safe_int(evidence.get("empty_response_count"))
        if failed_response_count > empty_response_count:
            return False
        if (
            task_name == "collect-managers"
            and source == "fundMNMangerList"
            and failed_response_count > 0
            and not _manager_list_empty_failures_are_explained(evidence)
        ):
            return False
        status_counts = evidence.get("quality_status_counts")
        allowed_statuses = set(source_contract.get("allowed_ok_statuses") or set()) | set(
            source_contract.get("allowed_empty_statuses") or set()
        )
        if isinstance(status_counts, dict):
            for status, count in status_counts.items():
                if _safe_int(count) > 0 and str(status) not in allowed_statuses:
                    return False
        payload_type_counts = evidence.get("quality_payload_type_counts")
        allowed_payload_types = set(source_contract.get("allowed_payload_types") or set())
        allowed_empty_payload_types = set(source_contract.get("allowed_empty_payload_types") or set())
        if isinstance(payload_type_counts, dict):
            for payload_type, count in payload_type_counts.items():
                if (
                    _safe_int(count) > 0
                    and allowed_payload_types
                    and str(payload_type) not in allowed_payload_types | allowed_empty_payload_types
                ):
                    return False
        empty_semantics = source_contract.get("empty_semantics")
        if isinstance(empty_semantics, dict) and empty_response_count > 0:
            semantics_count = _safe_int(evidence.get(str(empty_semantics.get("count_key") or "")))
            reason_counts = evidence.get(str(empty_semantics.get("reason_counts_key") or ""))
            scope_key = str(empty_semantics.get("scope_key") or "")
            required_scope = str(empty_semantics.get("required_scope") or "")
            if semantics_count != empty_response_count:
                return False
            if not isinstance(reason_counts, dict) or sum(_safe_int(value) for value in reason_counts.values()) != empty_response_count:
                return False
            if scope_key and required_scope and str(evidence.get(scope_key) or "").strip() != required_scope:
                return False
        explained_empty_count += empty_response_count
    return explained_empty_count >= failed_count


def _single_target_stderr_blockers_match_empty_semantics(
    task_name: str,
    *,
    stdout_semantics: dict[str, Any],
    stderr_summary: dict[str, Any],
) -> bool:
    evidence_entries = [
        dict(item)
        for item in stdout_semantics.get("api_source_evidence") or []
        if isinstance(item, dict)
    ]
    if not evidence_entries:
        return False
    total_empty_count = sum(_safe_int(item.get("empty_response_count")) for item in evidence_entries)
    blocker_samples = [
        str(item or "").strip()
        for item in stderr_summary.get("blocker_samples") or []
        if str(item or "").strip()
    ]
    if not blocker_samples or total_empty_count < len(blocker_samples):
        return False
    if not _single_target_completion_failure_matches_empty_semantics(
        task_name,
        failed_count=len(blocker_samples),
        evidence_entries=evidence_entries,
    ):
        return False

    sample_text = "\n".join(blocker_samples)
    if task_name == "collect-details":
        return "fundMNDetailInformation" in sample_text and "Datas 类型异常: NoneType" in sample_text
    if task_name == "collect-managers":
        return ("fundMNMangerList" in sample_text or "fundMSNMangerInfo" in sample_text) and (
            "Datas 类型异常: NoneType" in sample_text
            or "返回空" in sample_text
            or "请求失败" in sample_text
            or "Read timed out" in sample_text
        )
    return False


def _stdout_result_semantics(
    task_name: str, stdout_path: str, *, settings: Settings | None = None,
) -> tuple[dict[str, Any], list[str], list[str]]:
    blockers: list[str] = []
    warnings: list[str] = []
    path = Path(stdout_path) if stdout_path else None
    text = _strip_ansi(_read_text_tail(path)) if path else ""
    if path and task_name == "collect-period-returns":
        try:
            text = _strip_ansi(_read_complete_collection_log(path))
        except (OSError, ValueError) as exc:
            text = ""
            blockers.append(f"collect_period_returns_complete_log_unavailable:{exc}")
    result: dict[str, Any] = {
        "available": bool(text),
        "task_name": task_name,
        "completion_stats": [],
        "collector_stats": [],
        "business_summary": {},
        "api_source_evidence": [],
        "api_source_evidence_parse_errors": [],
    }
    if not text:
        blockers.append(f"stdout_result_missing:{task_name}")
        return result, blockers, warnings

    completion_stats = [
        _parse_named_count_match(match)
        for match in TASK_COMPLETION_STAT_PATTERN.finditer(text)
    ]
    collector_stats = [
        {
            "label": match.group("label"),
            **_parse_named_count_match(match),
        }
        for match in COLLECTOR_STAT_PATTERN.finditer(text)
    ]
    result["completion_stats"] = completion_stats
    result["collector_stats"] = collector_stats
    api_source_evidence, api_source_parse_errors = _parse_api_source_evidence_entries(text)
    result["api_source_evidence"] = api_source_evidence
    result["api_source_evidence_parse_errors"] = api_source_parse_errors
    if api_source_parse_errors:
        blockers.append(f"stdout_api_source_evidence_parse_error:{task_name}")

    # Structured terminals carry strict source crosschecks below; zero-target
    # runs need not fabricate a human progress block.
    structured_manager_terminal_present = task_name == "collect-managers" and "manager_collection_summary=" in text
    structured_detail_terminal_present = task_name == "collect-details" and "detail_collection_summary=" in text
    if not completion_stats and task_name not in {"collect-companies", "collect-period-returns"} and not structured_manager_terminal_present and not structured_detail_terminal_present:
        blockers.append(f"stdout_completion_stats_missing:{task_name}")
    for index, stat in enumerate(completion_stats, start=1):
        failed = _safe_int(stat.get("failed"))
        total = _safe_int(stat.get("total"))
        success = _safe_int(stat.get("success"))
        processed = _safe_int(stat.get("processed"))
        if failed:
            if _single_target_completion_failure_matches_empty_semantics(
                task_name,
                failed_count=failed,
                evidence_entries=api_source_evidence,
            ):
                warnings.append(f"stdout_completion_failed_matches_upstream_empty_semantics:{task_name}:{index}")
            else:
                blockers.append(f"stdout_completion_failed_nonzero:{task_name}:{index}")
        if total and processed and processed > total:
            blockers.append(f"stdout_completion_processed_gt_total:{task_name}:{index}")
        if total and success > total:
            blockers.append(f"stdout_completion_success_gt_total:{task_name}:{index}")
    for stat in collector_stats:
        failed = _safe_int(stat.get("failed"))
        total = _safe_int(stat.get("total"))
        success = _safe_int(stat.get("success"))
        if failed:
            if _single_target_completion_failure_matches_empty_semantics(
                task_name,
                failed_count=failed,
                evidence_entries=api_source_evidence,
            ):
                warnings.append(
                    f"stdout_collector_failed_matches_upstream_empty_semantics:{task_name}:{stat.get('label')}"
                )
            else:
                blockers.append(f"stdout_collector_failed_nonzero:{task_name}:{stat.get('label')}")
        if total and success > total:
            blockers.append(f"stdout_collector_success_gt_total:{task_name}:{stat.get('label')}")

    if task_name == "collect-funds":
        match = COLLECT_FUNDS_SUMMARY_PATTERN.search(text)
        if match is None:
            blockers.append("collect_funds_business_summary_missing")
        else:
            count = _parse_count_text(match.group("count"))
            result["business_summary"] = {"bond_fund_count": count}
            if count <= 0:
                blockers.append("collect_funds_business_count_empty")
            evidence_blockers, evidence_warnings = _validate_collect_funds_api_source_evidence(
                api_source_evidence,
                bond_fund_count=count,
            )
            blockers.extend(f"collect_funds_{item}" for item in evidence_blockers)
            warnings.extend(f"collect_funds_{item}" for item in evidence_warnings)
            search_blockers, search_warnings = _validate_fund_search_fallback_api_source_evidence(
                api_source_evidence
            )
            blockers.extend(f"collect_funds_{item}" for item in search_blockers)
            warnings.extend(f"collect_funds_{item}" for item in search_warnings)
        if not any(stat.get("label") == "FundCollector" for stat in collector_stats):
            warnings.append("collect_funds_collector_stats_missing")
    elif task_name == "collect-nav":
        match = COLLECT_NAV_SUMMARY_PATTERN.search(text)
        if match is None:
            blockers.append("collect_nav_business_summary_missing")
        else:
            inserted_count = _parse_count_text(match.group("count"))
            result["business_summary"] = {"inserted_nav_record_count": inserted_count}
            evidence_blockers, evidence_warnings = _validate_collect_nav_api_source_evidence(
                api_source_evidence,
                inserted_nav_record_count=inserted_count,
            )
            blockers.extend(f"collect_nav_{item}" for item in evidence_blockers)
            warnings.extend(f"collect_nav_{item}" for item in evidence_warnings)
        if not any(stat.get("label") == "NavCollector" for stat in collector_stats):
            warnings.append("collect_nav_collector_stats_missing")
    elif task_name == "collect-period-returns":
        summaries = [item for item in api_source_evidence if item.get("task") == task_name]
        if len(summaries) != 1:
            blockers.append("collect_period_returns_summary_missing_or_duplicated")
        else:
            summary = summaries[0]
            result["business_summary"] = summary
            try:
                keys = ("target_count", "saved_fund_count", "failed_count", "request_count",
                        "changed_count", "pending_count", "cooldown_count", "unselected_count")
                if any(type(summary[key]) is not int or summary[key] < 0 for key in keys):
                    raise ValueError("count_missing_or_invalid")
                outcomes = summary["outcomes"]
                success_codes = {item["fcode"] for item in outcomes if item["status"] == "ok"}
                if (len(outcomes) != summary["target_count"]
                        or len({item["fcode"] for item in outcomes}) != len(outcomes)
                        or len(success_codes) != summary["saved_fund_count"]
                        or summary["saved_fund_count"] + summary["failed_count"] != summary["target_count"]
                        or summary["request_count"] > summary["target_count"]
                        or summary["changed_count"] != len(set(summary["changed_fcodes"]))
                        or not set(summary["changed_fcodes"]).issubset(success_codes)
                        or summary["pending_count"] != summary["cooldown_count"] + summary["unselected_count"] + summary["failed_count"]):
                    raise ValueError("counts_do_not_balance")
                expected = "degraded" if summary["failed_count"] or summary["cooldown_count"] else "batch_complete" if summary["target_count"] else "skipped"
                if summary["status"] != expected:
                    raise ValueError("terminal_status_mismatch")
                period_collection_blocking_count(summary)
                response_path = (
                    _resolve_runtime_or_project_path(settings, summary["response_artifact"])
                    if settings is not None else Path(summary["response_artifact"])
                )
                if response_path is None:
                    raise ValueError("response_artifact_path_missing")
                if not response_path.is_file() or hashlib.sha256(response_path.read_bytes()).hexdigest() != summary["response_artifact_sha256"]:
                    raise ValueError("response_artifact_missing_or_hash_mismatch")
                response_payload = json.loads(response_path.read_text(encoding="utf-8"))
                source_summary = {key: value for key, value in summary.items()
                                  if key not in {"response_artifact", "response_artifact_sha256"}}
                if (response_payload["summary"] != source_summary
                        or len(response_payload["observations"]) != summary["request_count"]):
                    raise ValueError("response_artifact_contract_mismatch")
            except (KeyError, TypeError, ValueError, OSError) as exc:
                blockers.append(f"collect_period_returns_contract_invalid:{exc}")
            if summary.get("status") == "degraded":
                warnings.append("collect_period_returns_terminal_degraded")
            if summary.get("pending_count"):
                warnings.append("collect_period_returns_backlog_pending")
    elif task_name == "collect-details":
        summary, summary_errors = parse_detail_collection_summary(text.splitlines())
        match = COLLECT_DETAILS_SUMMARY_PATTERN.search(text)
        if summary_errors:
            blockers.extend(summary_errors)
        elif summary is not None:
            success_count = summary["write_success_count"]
            result["business_summary"] = {**summary, "success_fund_count": success_count}
            if len(api_source_evidence) != 1:
                blockers.append("detail_collection_single_source_record_required")
            else:
                blockers.extend(detail_collection_source_errors(summary, api_source_evidence[0]))
            evidence_blockers, evidence_warnings = _validate_single_target_tiantian_api_source_evidence(
                api_source_evidence, task_name=task_name, business_success_count=success_count,
            )
            blockers.extend(f"collect_details_{item}" for item in evidence_blockers)
            warnings.extend(f"collect_details_{item}" for item in evidence_warnings)
            expected_counts = {"total": summary["target_count"], "success": success_count,
                               "failed": summary["failed_count"], "skipped": summary["skipped_count"]}
            for stat in completion_stats:
                if any(stat.get(key) != value for key, value in expected_counts.items()):
                    blockers.append("detail_collection_completion_committed_counts_mismatch")
            if summary["status"] in {"degraded", "completed_with_unavailable"}:
                warnings.append(f"collect_details_terminal_{summary['status']}:{summary['reason']}")
        elif match is None:
            blockers.append("collect_details_business_summary_missing")
        else:
            success_count = _parse_count_text(match.group("count"))
            result["business_summary"] = {"success_fund_count": success_count}
            evidence_blockers, evidence_warnings = _validate_single_target_tiantian_api_source_evidence(
                api_source_evidence,
                task_name=task_name,
                business_success_count=success_count,
            )
            blockers.extend(f"collect_details_{item}" for item in evidence_blockers)
            warnings.extend(f"collect_details_{item}" for item in evidence_warnings)
    elif task_name == "collect-managers":
        summary, summary_errors = parse_manager_collection_summary(text.splitlines())
        match = COLLECT_MANAGERS_SUMMARY_PATTERN.search(text)
        if summary_errors:
            blockers.extend(summary_errors)
        elif summary is not None:
            success_count = summary["manager_details"]["updated_manager_count"]
            result["business_summary"] = {**summary, "success_manager_count": success_count}
            blockers.extend(manager_collection_source_errors(summary, api_source_evidence))
            evidence_blockers, evidence_warnings = _validate_single_target_tiantian_api_source_evidence(
                api_source_evidence, task_name=task_name, business_success_count=success_count,
            )
            blockers.extend(f"collect_managers_{item}" for item in evidence_blockers)
            warnings.extend(f"collect_managers_{item}" for item in evidence_warnings)
            if summary["status"] == "degraded":
                warnings.append(f"collect_managers_terminal_degraded:{summary['reason']}")
        elif match is None:
            blockers.append("collect_managers_business_summary_missing")
        else:
            success_count = _parse_count_text(match.group("count"))
            result["business_summary"] = {"success_manager_count": success_count}
            evidence_blockers, evidence_warnings = _validate_single_target_tiantian_api_source_evidence(
                api_source_evidence,
                task_name=task_name,
                business_success_count=success_count,
            )
            blockers.extend(f"collect_managers_{item}" for item in evidence_blockers)
            warnings.extend(f"collect_managers_{item}" for item in evidence_warnings)
    elif task_name == "collect-companies":
        summaries = []
        for line in text.splitlines():
            if line.startswith("company_collection_summary="):
                try:
                    summaries.append(json.loads(line.split("=", 1)[1]))
                except (ValueError, TypeError):
                    pass
        if len(summaries) != 1 or not isinstance(summaries[0], dict):
            blockers.append("collect_companies_business_summary_missing_or_invalid")
        else:
            summary = summaries[0]
            result["business_summary"] = summary
            evidence_blockers, evidence_warnings = _validate_single_target_tiantian_api_source_evidence(
                api_source_evidence, task_name=task_name,
                business_success_count=_safe_int(summary.get("success_count")),
            )
            blockers.extend(f"collect_companies_{item}" for item in evidence_blockers)
            warnings.extend(f"collect_companies_{item}" for item in evidence_warnings)
            if len(api_source_evidence) != 1:
                blockers.append("collect_companies_source_evidence_required")
            else:
                source = api_source_evidence[0]
                for key in ("target_count", "success_count", "failed_count", "skipped_count",
                            "inserted_count", "updated_count", "status", "skip_reason", "cooldown_count"):
                    if key not in summary or summary[key] != source.get(key):
                        blockers.append(f"collect_companies_summary_source_mismatch:{key}")
            failed = _safe_int(summary.get("failed_count"))
            cooldown = _safe_int(summary.get("cooldown_count"))
            expected_status = "degraded" if failed or cooldown else "success" if _safe_int(summary.get("target_count")) else "skipped"
            if summary.get("status") != expected_status:
                blockers.append("collect_companies_status_outcome_mismatch")
    elif task_name == "calculate-risk":
        match = CALCULATE_RISK_SUMMARY_PATTERN.search(text)
        full_matches = list(CALCULATE_RISK_FULL_SUMMARY_PATTERN.finditer(text))
        completion = result.get("completion_stats") or []
        full_stats = completion[0] if len(completion) == 1 else {}
        if match is not None and not full_matches:
            target_count = _parse_count_text(match.group("target"))
            generated_period_count = _parse_count_text(match.group("periods"))
        elif (match is None and len(full_matches) == 1 and full_stats
                and full_stats.get("total") == full_stats.get("processed")
                and full_stats.get("processed") == sum(_safe_int(full_stats.get(key)) for key in ("success", "skipped", "failed"))):
            target_count = _safe_int(full_stats.get("total"))
            generated_period_count = _parse_count_text(full_matches[0].group("periods"))
        else:
            target_count = generated_period_count = None
        if target_count is None:
            blockers.append("calculate_risk_business_summary_missing")
        else:
            result["business_summary"] = {
                "target_fund_count": target_count,
                "generated_period_count": generated_period_count,
            }
            if target_count <= 0:
                blockers.append("calculate_risk_target_count_empty")
            if generated_period_count <= 0:
                blockers.append("calculate_risk_generated_period_count_empty")

    return result, blockers, warnings


def _stderr_signal_summary(
    task_name: str,
    stderr_path: str,
    *,
    success: bool | None = None,
    return_code: Any = None,
    timed_out: bool | None = None,
    timeout_reason: str | None = None,
) -> tuple[dict[str, Any], list[str], list[str]]:
    blockers: list[str] = []
    warnings: list[str] = []
    result = classify_stderr_signals(
        task_name=task_name,
        stderr_path=stderr_path,
        success=success,
        return_code=return_code,
        timed_out=timed_out,
        timeout_reason=timeout_reason,
    )
    classification = str(result.get("classification") or "").strip()
    if classification == "blocker_signals":
        blockers.append(f"stderr_blocker_signal_present:{task_name}")
    elif classification == "warning_signals":
        warnings.append(f"stderr_warning_signal_present:{task_name}")

    return result, blockers, warnings


def _validate_runtime_stderr_signal_summary(
    *,
    issue_prefix: str,
    task_name: str,
    runtime_summary: dict[str, Any],
    recomputed_summary: dict[str, Any],
    allow_recomputed_legacy_summary: bool = False,
) -> tuple[list[str], list[str]]:
    blockers: list[str] = []
    warnings: list[str] = []
    if not isinstance(runtime_summary, dict) or not runtime_summary:
        if allow_recomputed_legacy_summary and recomputed_summary:
            return blockers, warnings
        warnings.append(f"{issue_prefix}_stderr_signal_summary_missing:{task_name}")
        return blockers, warnings

    if str(runtime_summary.get("truth_policy") or "").strip() != (
        "classify_log_signals_only_no_business_value_synthesis"
    ):
        blockers.append(f"{issue_prefix}_stderr_signal_truth_policy_mismatch:{task_name}")

    comparable_keys = (
        "classification",
        "non_empty_line_count",
        "warning_line_count",
        "blocker_line_count",
    )
    mismatched_keys = [
        key
        for key in comparable_keys
        if runtime_summary.get(key) != recomputed_summary.get(key)
    ]
    if mismatched_keys:
        blockers.append(
            f"{issue_prefix}_stderr_signal_summary_mismatch:{task_name}:{','.join(mismatched_keys)}"
        )
    return blockers, warnings


def _subprocess_index_path_from_log(path: Path) -> str:
    try:
        text = _read_complete_collection_log(path)
    except (OSError, ValueError):
        return ""
    matches = list(AUTOMATION_SUBPROCESS_INDEX_PATTERN.finditer(text))
    if not matches:
        return ""
    return matches[-1].group("path")


def _verified_annual_scenario_gap(
    settings: Settings, *, run_id: str, item: dict[str, Any], stdout_file: dict[str, Any],
) -> dict[str, Any] | None:
    """Explain a known research gap without relabeling the child as successful."""
    if (item.get("task_name") != "annual-return-gate-shadow"
            or item.get("return_code") != 2 or item.get("success") is not False
            or item.get("timed_out") or item.get("timeout_reason")
            or not stdout_file.get("exists")):
        return None
    terminal = load_incomplete_annual_shadow_terminal(
        settings, run_id=run_id,
        stdout_tail=_read_text_tail(Path(stdout_file["path"])).splitlines(), return_code=2,
    )
    if not terminal or terminal["status"] != "scenario_inputs_incomplete":
        return None
    payload = _read_json(Path(terminal["artifact"]))
    baseline = payload.get("baseline_verification") or {}
    if not isinstance(baseline, dict):
        return None
    checks = baseline.get("checks") or []
    if (baseline.get("baseline_verified") is not True or baseline.get("run_id") != run_id
            or not isinstance(checks, list) or len(checks) != 9
            or not all(isinstance(check, dict) and check.get("aligned") is True for check in checks)
            or payload.get("shadow_final_count") is not None
            or payload.get("production_thresholds_changed") is not False
            or payload.get("later_database_reads") is not False
            or payload.get("runtime_upstream_requests") != 0):
        return None
    source_files = baseline.get("source_files") or {}
    if (not isinstance(source_files, dict) or not source_files.get("report_json")
            or not baseline.get("snapshot_path")):
        return None
    bindings = [*source_files.items(), ("snapshot", {
        "path": baseline["snapshot_path"], "sha256": baseline.get("snapshot_sha256"),
    })]
    try:
        for name, binding in bindings:
            if not isinstance(binding, dict):
                return None
            path = resolve_project_artifact_path(binding.get("path"), project_root=settings.project_root)
            raw = path.read_bytes()
            if not binding.get("sha256") or hashlib.sha256(raw).hexdigest() != binding["sha256"]:
                return None
            if name == "report_json":
                report_path, report_raw = path, raw
        report = json.loads(report_raw)
        if not isinstance(report, dict) or report.get("run_id") != run_id:
            return None
        report_control = report.get("publication_control") or {}
        if not isinstance(report_control, dict):
            return None
        source_control = payload.get("source_publication_control")
        if source_control is not None and (not isinstance(source_control, dict)
                or source_control.get("revision") != report_control.get("revision")):
            return None
        if payload.get("source_report") is not None and resolve_project_artifact_path(
                payload["source_report"], project_root=settings.project_root) != report_path:
            return None
        source = payload.get("source_report_binding")
        if source is not None:
            if not isinstance(source, dict):
                return None
            source = {**source, "report_path": project_relative_artifact_path(
                source.get("report_path"), project_root=settings.project_root)}
            expected = {"run_id": run_id, "revision": report_control.get("revision"),
                        "report_path": project_relative_artifact_path(report_path, project_root=settings.project_root),
                        "report_sha256": hashlib.sha256(report_raw).hexdigest()}
            if source != expected:
                return None
    except (OSError, ValueError, TypeError):
        return None
    return {**terminal, "business_status": "degraded", "baseline_verified": True,
            "baseline_check_count": len(checks), "child_success": False,
            "truth_policy": "bound_research_gap_preserves_nonzero_exit_and_uncomputed_counts"}


def _validate_subprocess_index_payload(
    settings: Settings,
    *,
    index_file: dict[str, Any],
    index_payload: dict[str, Any],
    expected_run_id: str,
    required_tasks: Sequence[str],
    optional_tasks: Sequence[str],
    issue_prefix: str,
    allow_missing_run_id: bool = False,
    include_stdout_result_semantics: bool = False,
    stdout_semantics_task_allowlist: Sequence[str] | None = None,
    allow_recomputed_legacy_stderr_summary: bool = False,
) -> tuple[dict[str, Any], list[str], list[str]]:
    blockers: list[str] = []
    warnings: list[str] = []
    index_run_id = str(index_payload.get("run_id") or "").strip()
    if expected_run_id and index_run_id and index_run_id != expected_run_id:
        blockers.append(f"{issue_prefix}_run_id_mismatch")
    if expected_run_id and not index_run_id and not allow_missing_run_id:
        blockers.append(f"{issue_prefix}_run_id_missing")
    items = [dict(item) for item in index_payload.get("items") or [] if isinstance(item, dict)]
    task_names = [str(item.get("task_name") or "").strip() for item in items if str(item.get("task_name") or "").strip()]
    task_name_set = set(task_names)
    required_tasks = list(required_tasks)
    if index_payload.get("period_return_collection_planned") is True and "collect-period-returns" not in required_tasks:
        required_tasks.append("collect-period-returns")
    missing_required = [task for task in required_tasks if task not in task_name_set]
    missing_optional = [task for task in optional_tasks if task not in task_name_set]
    if missing_required:
        blockers.append(f"{issue_prefix}_required_tasks_missing:{','.join(missing_required)}")
    if missing_optional:
        warnings.append(f"{issue_prefix}_optional_tasks_missing:{','.join(missing_optional)}")

    subprocess_count = _safe_int(index_payload.get("subprocess_count"))
    success_count = _safe_int(index_payload.get("success_count"))
    failed_count = _safe_int(index_payload.get("failed_count"))
    failed_task_names = [
        str(item).strip()
        for item in index_payload.get("failed_task_names") or []
        if str(item).strip()
    ]
    if subprocess_count and subprocess_count != len(items):
        blockers.append(f"{issue_prefix}_count_mismatch")
    actual_success_count = sum(1 for item in items if bool(item.get("success")))
    if success_count and success_count != actual_success_count:
        blockers.append(f"{issue_prefix}_success_count_mismatch")

    task_summaries: list[dict[str, Any]] = []
    explained_failures: list[str] = []
    stdout_allowlist = {str(item).strip() for item in stdout_semantics_task_allowlist or [] if str(item).strip()}
    for item in items:
        task_name = str(item.get("task_name") or "").strip()
        command = item.get("command") if isinstance(item.get("command"), list) else []
        success = bool(item.get("success"))
        return_code = item.get("return_code")
        timed_out = bool(item.get("timed_out"))
        meta_file = _subprocess_file_evidence(settings, item, "meta_path")
        stdout_file = _subprocess_file_evidence(settings, item, "stdout_path")
        stderr_file = _subprocess_file_evidence(settings, item, "stderr_path")
        stdout_semantics: dict[str, Any] = {}
        stderr_summary: dict[str, Any] = {}
        runtime_stderr_summary = (
            dict(item.get("stderr_signal_summary"))
            if isinstance(item.get("stderr_signal_summary"), dict)
            else {}
        )
        scenario_gap = _verified_annual_scenario_gap(
            settings, run_id=expected_run_id, item=item, stdout_file=stdout_file,
        )
        if scenario_gap:
            explained_failures.append(task_name)
            warnings.append(f"{issue_prefix}_annual_shadow_scenario_inputs_incomplete")

        if not task_name:
            blockers.append(f"{issue_prefix}_task_name_missing")
        if not success and not scenario_gap:
            blockers.append(f"{issue_prefix}_task_not_success:{task_name}")
        if return_code not in (0, "0") and not scenario_gap:
            blockers.append(f"{issue_prefix}_task_return_code_nonzero:{task_name}")
        if timed_out:
            blockers.append(f"{issue_prefix}_task_timed_out:{task_name}")
        if not command:
            blockers.append(f"{issue_prefix}_task_command_missing:{task_name}")
        for file_key, evidence in (
            ("meta_path", meta_file),
            ("stdout_path", stdout_file),
            ("stderr_path", stderr_file),
        ):
            if not evidence.get("exists"):
                blockers.append(f"{issue_prefix}_log_file_missing:{task_name}:{file_key}")
        parse_stdout_semantics = bool(stdout_file.get("exists") and (
            task_name == "collect-period-returns"
            or (include_stdout_result_semantics and (not stdout_allowlist or task_name in stdout_allowlist))
        ))
        if parse_stdout_semantics:
            stdout_semantics, stdout_blockers, stdout_warnings = _stdout_result_semantics(
                task_name,
                str(stdout_file.get("path") or ""),
                settings=settings,
            )
            blockers.extend(f"{issue_prefix}_{item}" for item in stdout_blockers)
            warnings.extend(f"{issue_prefix}_{item}" for item in stdout_warnings)
        if stderr_file.get("exists"):
            stderr_summary, stderr_blockers, stderr_warnings = _stderr_signal_summary(
                task_name,
                str(stderr_file.get("path") or ""),
                success=success,
                return_code=return_code,
                timed_out=timed_out,
                timeout_reason=str(item.get("timeout_reason") or "") or None,
            )
            if (
                stderr_blockers
                and parse_stdout_semantics
                and _single_target_stderr_blockers_match_empty_semantics(
                    task_name,
                    stdout_semantics=stdout_semantics,
                    stderr_summary=stderr_summary,
                )
            ):
                warnings.append(f"{issue_prefix}_stderr_blocker_signals_match_upstream_empty_semantics:{task_name}")
                stderr_blockers = []
            if (scenario_gap and stderr_blockers
                    and stderr_summary.get("blocker_line_count") == 1
                    and not stderr_summary.get("blocker_samples")
                    and (stderr_summary.get("execution_status") or {}).get("execution_blocker") is True):
                # The classifier counts the nonzero exit itself. Actual error
                # lines, missing files and inconsistent stored summaries still block.
                stderr_blockers = []
            blockers.extend(f"{issue_prefix}_{item}" for item in stderr_blockers)
            warnings.extend(f"{issue_prefix}_{item}" for item in stderr_warnings)
            summary_blockers, summary_warnings = _validate_runtime_stderr_signal_summary(
                issue_prefix=issue_prefix,
                task_name=task_name,
                runtime_summary=runtime_stderr_summary,
                recomputed_summary=stderr_summary,
                allow_recomputed_legacy_summary=allow_recomputed_legacy_stderr_summary,
            )
            blockers.extend(summary_blockers)
            warnings.extend(summary_warnings)

        stderr_summary_source = "runtime_index"
        if not runtime_stderr_summary and stderr_summary:
            stderr_summary_source = (
                "recomputed_from_stderr_log_legacy_index"
                if allow_recomputed_legacy_stderr_summary
                else "recomputed_from_stderr_log_runtime_missing"
            )

        task_summaries.append(
            {
                "task_name": task_name,
                "success": success,
                "return_code": return_code,
                "timed_out": timed_out,
                "timeout_reason": item.get("timeout_reason"),
                "duration_seconds": item.get("duration_seconds"),
                "started_at": item.get("started_at"),
                "finished_at": item.get("finished_at"),
                "command": command,
                "files": {
                    "meta": meta_file,
                    "stdout": stdout_file,
                    "stderr": stderr_file,
                },
                "stdout_result_semantics": stdout_semantics,
                "stderr_signal_summary": stderr_summary,
                "runtime_stderr_signal_summary": runtime_stderr_summary,
                "annual_shadow_terminal": scenario_gap,
                "stderr_signal_summary_source": stderr_summary_source,
            }
        )

    actual_failed_names = [str(item.get("task_name") or "") for item in items if not item.get("success")]
    only_explained_failures = bool(
        explained_failures and len(explained_failures) == len(actual_failed_names)
        and failed_count == len(actual_failed_names)
        and sorted(failed_task_names) == sorted(actual_failed_names)
    )
    if (failed_count or failed_task_names) and not only_explained_failures:
        blockers.append(f"{issue_prefix}_failed_tasks_present")
    return (
        {
            "index_file": index_file,
            "generated_at": index_payload.get("generated_at"),
            "index_run_id": index_run_id or None,
            "task_count": len(items),
            "subprocess_count": subprocess_count,
            "success_count": success_count,
            "failed_count": failed_count,
            "failed_task_names": failed_task_names,
            "explained_research_gap_task_names": explained_failures,
            "required_tasks": list(required_tasks),
            "optional_tasks": list(optional_tasks),
            "missing_required_tasks": missing_required,
            "missing_optional_tasks": missing_optional,
            "task_names": task_names,
            "tasks": task_summaries,
        },
        blockers,
        warnings,
    )


def _automation_scheduler_state_truth(settings: Settings) -> tuple[dict[str, Any], list[str], list[str]]:
    blockers: list[str] = []
    warnings: list[str] = []
    state_file = getattr(
        settings,
        "scheduler_state_file",
        settings.project_root / "backend" / "data" / "scheduler_state.json",
    )
    state_path = Path(state_file)
    if not state_path.is_absolute():
        state_path = settings.project_root / state_path
    state = _read_json(state_path) if state_path.exists() else {}

    scheduler_running = bool(state.get("scheduler_running"))
    pending_manual_requests = state.get("pending_manual_run_requests")
    if pending_manual_requests is None:
        pending_manual_requests = []
    pending_manual_count = len(pending_manual_requests) if isinstance(pending_manual_requests, list) else 1
    current_task = state.get("current_task")
    last_heartbeat_at = str(state.get("last_heartbeat_at") or "").strip()
    heartbeat_age_seconds: int | None = None
    heartbeat_dt = _parse_iso_datetime(last_heartbeat_at)
    if heartbeat_dt is not None:
        now = datetime.now(tz=heartbeat_dt.tzinfo) if heartbeat_dt.tzinfo else datetime.now()
        heartbeat_age_seconds = max(0, int((now - heartbeat_dt).total_seconds()))

    if not state_path.exists():
        blockers.append("scheduler_state_file_missing")
    if state_path.exists() and not scheduler_running:
        blockers.append("scheduler_not_running")
    if pending_manual_count:
        blockers.append("pending_manual_run_requests_present")
    if heartbeat_dt is None:
        warnings.append("scheduler_last_heartbeat_missing_or_invalid")
    elif heartbeat_age_seconds is not None and heartbeat_age_seconds > 1800:
        warnings.append("scheduler_last_heartbeat_stale")

    return (
        {
            "scheduler_state_file": str(state_path),
            "exists": state_path.exists(),
            "scheduler_running": scheduler_running,
            "last_heartbeat_at": last_heartbeat_at or None,
            "heartbeat_age_seconds": heartbeat_age_seconds,
            "current_task": current_task if current_task else {},
            "pending_manual_run_request_count": int(pending_manual_count),
        },
        blockers,
        warnings,
    )


def _collection_index_zero_increment_changed_nav_risk_scan(index_payload: dict[str, Any]) -> bool:
    tasks = {
        str(item.get("task_name") or "").strip(): dict(item)
        for item in index_payload.get("tasks") or []
        if isinstance(item, dict) and str(item.get("task_name") or "").strip()
    }
    collect_nav = tasks.get("collect-nav") or {}
    calculate_risk = tasks.get("calculate-risk") or {}
    if not collect_nav or not calculate_risk:
        return False
    collect_stdout = (
        collect_nav.get("stdout_result_semantics")
        if isinstance(collect_nav.get("stdout_result_semantics"), dict)
        else {}
    )
    collect_business = (
        collect_stdout.get("business_summary")
        if isinstance(collect_stdout.get("business_summary"), dict)
        else {}
    )
    if "inserted_nav_record_count" not in collect_business:
        return False
    if _safe_int(collect_business.get("inserted_nav_record_count")) != 0:
        return False
    command = calculate_risk.get("command") if isinstance(calculate_risk.get("command"), list) else []
    command_parts = [str(item) for item in command]
    return bool(
        calculate_risk.get("success")
        and calculate_risk.get("return_code") in (0, "0")
        and not bool(calculate_risk.get("timed_out"))
        and "--changed-nav-only" in command_parts
        and "--force" in command_parts
    )


def _date_prefix_from_task_row(row: dict[str, Any]) -> str:
    for key in ("started_at", "completed_at"):
        text = str(row.get(key) or "").strip()
        if len(text) >= 10:
            return text[:10]
    return ""


def _refresh_required_subprocess_tasks(task_name: str) -> tuple[str, ...]:
    # Both jobs persist collection_profile="daily". The scheduler job owns
    # the actual collection scope: intraday does not re-fetch the fund directory.
    if task_name == AUTOMATION_INTRADAY_REFRESH_TASK_NAME:
        return AUTOMATION_REQUIRED_INTRADAY_SUBPROCESS_TASKS
    return AUTOMATION_REQUIRED_REFRESH_SUBPROCESS_TASKS


def _refresh_collection_index_from_task_log(
    settings: Settings,
    *,
    task_log_path_raw: Any,
    run_id: str,
    required_tasks: Sequence[str] = AUTOMATION_REQUIRED_REFRESH_SUBPROCESS_TASKS,
    task_audit: dict[str, Any] | None = None,
    source_task: dict[str, Any] | None = None,
) -> tuple[str, dict[str, Any], dict[str, Any], list[str], list[str]]:
    blockers: list[str] = []
    warnings: list[str] = []
    index_raw = ""
    index_file: dict[str, Any] = {
        "configured_value": "",
        "path": "",
        "exists": False,
        "size_bytes": None,
    }
    subprocess_index: dict[str, Any] = {
        "index_file": index_file,
        "task_count": 0,
        "required_tasks": list(required_tasks),
        "optional_tasks": [],
        "missing_required_tasks": list(required_tasks),
        "missing_optional_tasks": [],
        "tasks": [],
    }

    task_log = _file_evidence(settings, task_log_path_raw)
    task_log_path = Path(task_log["path"]) if task_log.get("exists") else None
    if task_log_path is not None:
        index_raw = _subprocess_index_path_from_log(task_log_path)
    if not index_raw:
        blockers.append("scheduled_refresh_collection_subprocess_index_missing")
        return index_raw, index_file, subprocess_index, blockers, warnings

    index_file = _file_evidence(settings, index_raw)
    if not index_file.get("exists"):
        blockers.append("scheduled_refresh_collection_subprocess_index_file_missing")
        subprocess_index["index_file"] = index_file
        return index_raw, index_file, subprocess_index, blockers, warnings

    index_payload = _read_json(Path(index_file["path"]))
    expected_index_run_id = run_id
    parent_binding: dict[str, Any] = {}
    if index_payload.get("run_id") and index_payload.get("run_id") != run_id:
        from backend.collection_calculation_capture import validate_collection_capture_reference
        audit = task_audit or {}
        workflow = audit.get("workflow_summary") or {}
        task = source_task or audit.get("task") or {}
        references = (workflow.get("options") or {}).get("collection_calculation_captures") or []
        matches = [ref for ref in references if isinstance(ref, dict)
                   and ref.get("capture_run_id") == index_payload.get("run_id")]
        try:
            if workflow.get("run_id") != run_id or len(matches) != 1:
                raise ValueError("parent_capture_workflow_binding_missing")
            reference = validate_collection_capture_reference(settings, matches[0])
            parent_task = reference["task"]
            task_id = task.get("task_history_id", task.get("id"))
            if (parent_task.get("task_history_id") != task_id or not task_id
                    or parent_task.get("task_name") != task.get("task_name")
                    or parent_task.get("started_at") != task.get("started_at")):
                raise ValueError("parent_capture_task_binding_mismatch")
            expected_path = (settings.project_root / reference["execution_path"]).parent / "logs/subprocess/subprocess_index.json"
            if Path(index_file["path"]).resolve() != expected_path.resolve():
                raise ValueError("parent_capture_subprocess_path_mismatch")
            expected_index_run_id = reference["capture_run_id"]
            parent_binding = {"verified": True, "capture_run_id": expected_index_run_id,
                              "workflow_run_id": run_id, "task_history_id": task_id,
                              "execution_sha256": reference["execution_sha256"],
                              "index_sha256": reference["index_sha256"],
                              "terminal_sha256": reference["terminal_sha256"]}
        except (OSError, ValueError, KeyError, TypeError, RuntimeError) as exc:
            blockers.append(f"scheduled_refresh_collection_capture_binding_invalid:{exc}")
    subprocess_index, index_blockers, index_warnings = _validate_subprocess_index_payload(
        settings,
        index_file=index_file,
        index_payload=index_payload,
        expected_run_id=expected_index_run_id,
        required_tasks=required_tasks,
        optional_tasks=(),
        issue_prefix="scheduled_refresh_collection_subprocess",
        allow_missing_run_id=True,
        include_stdout_result_semantics=True,
    )
    subprocess_index["parent_capture_binding"] = parent_binding
    if _collection_index_zero_increment_changed_nav_risk_scan(subprocess_index):
        zero_increment_allowlist = {
            "scheduled_refresh_collection_subprocess_stdout_completion_stats_missing:calculate-risk",
            "scheduled_refresh_collection_subprocess_calculate_risk_business_summary_missing",
        }
        index_blockers = [
            item for item in index_blockers if item not in zero_increment_allowlist
        ]
        subprocess_index["zero_increment_changed_nav_risk_scan"] = True
    blockers.extend(index_blockers)
    warnings.extend(index_warnings)
    return index_raw, index_file, subprocess_index, blockers, warnings


def _find_prior_same_day_refresh_collection_evidence(
    settings: Settings,
    *,
    rows: Sequence[sqlite3.Row],
    matched_row: dict[str, Any],
    run_id: str,
    required_tasks: Sequence[str] = AUTOMATION_REQUIRED_REFRESH_SUBPROCESS_TASKS,
    task_audit: dict[str, Any] | None = None,
) -> tuple[str, dict[str, Any], dict[str, Any], list[str], list[str]]:
    blockers: list[str] = []
    warnings: list[str] = []
    empty_index_file: dict[str, Any] = {
        "configured_value": "",
        "path": "",
        "exists": False,
        "size_bytes": None,
    }
    empty_index: dict[str, Any] = {
        "index_file": empty_index_file,
        "task_count": 0,
        "required_tasks": list(required_tasks),
        "optional_tasks": [],
        "missing_required_tasks": list(required_tasks),
        "missing_optional_tasks": [],
        "tasks": [],
    }
    matched_id = _safe_int(matched_row.get("id"))
    matched_date = _date_prefix_from_task_row(matched_row)
    checked_count = 0
    latest_candidate_blockers: list[str] = []

    for row in rows:
        row_id = _safe_int(row["id"])
        if matched_id and row_id >= matched_id:
            continue
        row_dict = {key: row[key] for key in row.keys() if key != "extra_data"}
        if matched_date and _date_prefix_from_task_row(row_dict) != matched_date:
            continue
        source_finished = _parse_iso_datetime(row_dict.get("completed_at"))
        matched_started = _parse_iso_datetime(matched_row.get("started_at"))
        try:
            if source_finished is None or matched_started is None or source_finished > matched_started:
                continue
        except TypeError:
            continue
        extra = _parse_json_object(row["extra_data"])
        task_log_path_raw = (
            extra.get("task_log_path")
            or (extra.get("task_run_audit") or {}).get("task_log_project_path")
            or (extra.get("task_run_audit") or {}).get("task_log_path_relative")
        )
        checked_count += 1
        index_raw, index_file, subprocess_index, index_blockers, index_warnings = (
            _refresh_collection_index_from_task_log(
                settings,
                task_log_path_raw=task_log_path_raw,
                run_id=run_id,
                required_tasks=required_tasks,
                task_audit=task_audit,
                source_task=row_dict,
            )
        )
        if latest_candidate_blockers == [] and index_blockers:
            latest_candidate_blockers = list(index_blockers)
        missing_required = subprocess_index.get("missing_required_tasks") or []
        if index_file.get("exists") and not index_blockers and not missing_required:
            subprocess_index = {
                **subprocess_index,
                "evidence_lineage": {
                    "source": "prior_same_day_collection_evidence_for_collection_skipped_rescreen",
                    "source_task_history_id": row_id,
                    "source_task_status": row_dict.get("status"),
                    "source_task_started_at": row_dict.get("started_at"),
                    "source_task_completed_at": row_dict.get("completed_at"),
                    "matched_task_history_id": matched_row.get("id"),
                    "matched_task_started_at": matched_row.get("started_at"),
                    "same_day_checked_prior_task_count": checked_count,
                },
            }
            warnings.extend(index_warnings)
            return index_raw, index_file, subprocess_index, blockers, warnings

    blockers.append("scheduled_refresh_prior_same_day_collection_evidence_missing")
    if latest_candidate_blockers:
        blockers.extend(
            f"scheduled_refresh_prior_same_day_collection_evidence_invalid:{item}"
            for item in latest_candidate_blockers
        )
    empty_index["evidence_lineage"] = {
        "source": "prior_same_day_collection_evidence_for_collection_skipped_rescreen",
        "matched_task_history_id": matched_row.get("id"),
        "matched_task_started_at": matched_row.get("started_at"),
        "same_day_checked_prior_task_count": checked_count,
    }
    return "", empty_index_file, empty_index, blockers, warnings


def _refresh_collection_payload_binding(
    settings: Settings, row: dict[str, Any], extra: dict[str, Any], task_audit: dict[str, Any],
) -> tuple[dict[str, Any], list[str]]:
    """Check the full recorded result even when the recorded status is success."""
    errors: list[str] = []
    task = task_audit.get("task") or {}
    result = task_audit.get("task_result") or {}
    proof = {"verified": False, "sql_full_payload_verified": False,
             "sql_inline_period_summary_present": "period_return_collection" in extra}
    if (not isinstance(task, dict) or not isinstance(result, dict)
            or task.get("task_history_id") != row.get("id")
            or task.get("task_name") != row.get("task_name")):
        return proof, ["task_audit_identity_mismatch"]
    for label, record in (("task_audit", task), ("task_result", result), ("sql_extra", extra)):
        if record.get("status") != row.get("status"):
            errors.append(f"{label}_recorded_status_mismatch")
    for key in ("screening_run_id", "screening_final_count", "screening_triggered"):
        if key not in result or result[key] != extra.get(key):
            errors.append(f"task_result_{key}_mismatch")
    for key in ("degraded", "degraded_stage", "warning_reason"):
        recorded, compacted = result.get(key), extra.get(key)
        # The history compactor canonicalizes absent reason/stage labels from
        # "" to null. Keep their original representations in the evidence; this
        # equivalence does not apply to booleans or business measurements.
        if key != "degraded":
            recorded = None if recorded == "" else recorded
            compacted = None if compacted == "" else compacted
        if recorded != compacted:
            errors.append(f"task_result_{key}_mismatch")
    binding = extra.get("task_history_full_payload")
    if extra.get("task_history_extra_data_compacted") is True:
        if (extra.get("task_history_extra_data_schema_version") != 1
                or not isinstance(binding, dict) or binding.get("available") is not True
                or binding.get("source") != "scheduler_task_run_audit"):
            errors.append("sql_full_payload_binding_invalid")
        else:
            bound = _file_evidence(settings, binding.get("sidecar_project_path")
                                   or binding.get("sidecar_path_relative") or binding.get("sidecar_path"))
            actual = _file_evidence(settings, extra.get("task_run_audit_path"))
            full_bytes = json.dumps(result, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
            if (not bound.get("exists") or not actual.get("exists") or bound["path"] != actual["path"]
                    or type(binding.get("full_payload_bytes")) is not int
                    or binding["full_payload_bytes"] != len(full_bytes)
                    or binding.get("full_payload_sha256") != hashlib.sha256(full_bytes).hexdigest()):
                errors.append("sql_full_payload_identity_mismatch")
            else:
                proof["sql_full_payload_verified"] = True
    elif binding is not None or "task_history_extra_data_schema_version" in extra:
        errors.append("sql_full_payload_compact_flag_missing")
    proof["verified"] = not errors
    return proof, errors


def _refresh_collection_phase_reconciliation(
    settings: Settings, *, row: dict[str, Any], extra: dict[str, Any], task_audit: dict[str, Any],
    collection_index: dict[str, Any], collection_blockers: Sequence[str], task_log: dict[str, Any],
    state_payload: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], dict[str, Any], list[str], list[str]]:
    """Reconcile complete, task-bound phases; never repair the historical record."""
    prefix = "scheduled_refresh_collection"
    source_errors: list[str] = list(collection_blockers)
    warnings: list[str] = []
    lineage = collection_index.get("evidence_lineage") or {}
    prior = lineage.get("source") == "prior_same_day_collection_evidence_for_collection_skipped_rescreen"
    contexts = [("prior_task" if prior else "parent", collection_index)]
    workflow = task_audit.get("workflow_summary") or {}
    result = task_audit.get("task_result") or {}
    if not isinstance(workflow, dict):
        workflow = {}
    if not isinstance(result, dict):
        result = {}
    main_run_id = str(extra.get("screening_run_id") or "")
    run_ids = [main_run_id] if main_run_id else []
    for field in ("initial_screening", "maintenance_rescreen", "report_refresh"):
        if extra.get(f"{field}_triggered") is True:
            value = str(extra.get(f"{field}_run_id") or "")
            if not value:
                source_errors.append(f"{prefix}_workflow_run_id_missing:{field}")
            elif value not in run_ids:
                run_ids.append(value)
    reports_dir = Path(getattr(settings, "reports_dir", settings.project_root / "reports"))
    runs_dir = Path(getattr(settings, "runs_dir", reports_dir / "runs"))
    has_period = any(item.get("task_name") == "collect-period-returns"
                     for item in collection_index.get("tasks") or [])
    workflow_checks = []
    option_sources: list[dict[str, Any]] = []
    for run_id in run_ids:
        if not re.fullmatch(r"[A-Za-z0-9_-]+", run_id):
            source_errors.append(f"{prefix}_workflow_run_id_invalid")
            continue
        options = workflow.get("options") if workflow.get("run_id") == run_id else None
        if not isinstance(options, dict):
            # Only this run's immutable report may establish its options. Never
            # borrow current configuration or the current publication's report.
            report_path = runs_dir / run_id / f"bond_analysis_report_{run_id}.json"
            report = _read_json(report_path)
            option_sources.append(_file_evidence(settings, str(report_path)))
            options = (report.get("publication_control") or {}).get("workflow_options") if report.get("run_id") == run_id else None
        options = options if isinstance(options, dict) else {}
        no_core = options.get("reuse_existing_outputs") is True or (
            options.get("run_collection") is False and options.get("run_screening") is False)
        expected_period = not no_core and (options.get("run_collection") is True or options.get("run_screening") is True)
        path = runs_dir / run_id / "logs" / "subprocess" / "subprocess_index.json"
        file = _file_evidence(settings, str(path))
        workflow_checks.append({"run_id": run_id, "index_file": file, "options": options,
                                "collection_expected": expected_period, "core_explicitly_skipped": no_core})
        if not file["exists"]:
            if expected_period or (has_period and not no_core):
                source_errors.append(f"{prefix}_workflow_index_missing:{run_id}")
            continue
        payload = _read_json(path)
        items = payload.get("items") or []
        if not isinstance(items, list) or not all(isinstance(item, dict) for item in items):
            source_errors.append(f"{prefix}_workflow_index_items_invalid:{run_id}")
            continue
        actual_failed = [item.get("task_name") for item in items if item.get("success") is not True]
        if (type(payload.get("subprocess_count")) is not int or payload["subprocess_count"] != len(items)
                or type(payload.get("success_count")) is not int
                or payload["success_count"] != len(items) - len(actual_failed)
                or type(payload.get("failed_count")) is not int or payload["failed_count"] != len(actual_failed)
                or sorted(payload.get("failed_task_names") or []) != sorted(actual_failed)):
            source_errors.append(f"{prefix}_workflow_index_counts_mismatch:{run_id}")
        selected = [item for item in items if str(item.get("task_name") or "").startswith("collect-")
                    or item.get("task_name") == "calculate-risk"]
        if no_core and selected:
            source_errors.append(f"{prefix}_workflow_options_conflict:{run_id}")
        # Later research audits rebuild the index without the plan flag. The
        # task's frozen workflow options still prove that collection was due.
        planned = payload.get("period_return_collection_planned")
        if expected_period and planned is False:
            source_errors.append(f"{prefix}_workflow_plan_conflict:{run_id}")
        selected_failed = [item.get("task_name") for item in selected if item.get("success") is not True]
        filtered = {**payload, "items": selected, "subprocess_count": len(selected),
                    "success_count": len(selected) - len(selected_failed), "failed_count": len(selected_failed),
                    "failed_task_names": selected_failed}
        checked, errors, notes = _validate_subprocess_index_payload(
            settings, index_file=file, index_payload=filtered, expected_run_id=run_id,
            required_tasks=("collect-period-returns",) if expected_period else (), optional_tasks=(),
            issue_prefix=f"{prefix}_workflow_subprocess", include_stdout_result_semantics=True)
        # A signature scan with no changed inputs legitimately omits progress
        # counts; it still has to have exited successfully with the exact flags.
        if _collection_index_zero_increment_changed_nav_risk_scan(checked):
            errors = [error for error in errors if error not in {
                f"{prefix}_workflow_subprocess_stdout_completion_stats_missing:calculate-risk",
                f"{prefix}_workflow_subprocess_calculate_risk_business_summary_missing"}]
        source_errors.extend(errors)
        warnings.extend(notes)
        contexts.append((f"workflow:{run_id}", checked))
    phases = []
    current_tasks = []
    source_files: dict[str, str] = {}
    for evidence in option_sources:
        if evidence.get("exists"):
            source = Path(evidence["path"])
            source_files[str(source)] = hashlib.sha256(source.read_bytes()).hexdigest()
    seen_stdout: set[str] = set()
    for scope, index in contexts:
        # Bind every file read to establish the collection phases, including
        # supporting NAV/risk logs and an index with no period-return task.
        evidence_files = [index.get("index_file") or {}]
        for item in index.get("tasks") or []:
            evidence_files.extend((item.get("files") or {}).values())
        for evidence in evidence_files:
            if evidence.get("exists"):
                source = Path(evidence["path"])
                source_files[str(source)] = hashlib.sha256(source.read_bytes()).hexdigest()
        for item in index.get("tasks") or []:
            if item.get("task_name") != "collect-period-returns":
                continue
            if item.get("timed_out") or item.get("timeout_reason"):
                source_errors.append(f"{prefix}_phase_timeout:{scope}")
            summary = (item.get("stdout_result_semantics") or {}).get("business_summary") or {}
            files = item.get("files") or {}
            out = (files.get("stdout") or {}).get("path")
            if not out or out in seen_stdout:
                source_errors.append(f"{prefix}_phase_source_missing_or_reused:{scope}")
            seen_stdout.add(str(out))
            meta = _read_json(Path(files["meta"]["path"])) if (files.get("meta") or {}).get("exists") else {}
            identity_keys = ("task_name", "command", "started_at", "finished_at", "success", "return_code", "timed_out", "timeout_reason")
            if any(key not in meta or meta.get(key) != item.get(key) for key in identity_keys):
                source_errors.append(f"{prefix}_phase_meta_mismatch:{scope}")
            for key in ("meta", "stdout", "stderr"):
                actual = files.get(key) or {}
                bound = _file_evidence(settings, meta.get(f"{key}_path"))
                if not bound.get("exists") or bound["path"] != actual.get("path"):
                    source_errors.append(f"{prefix}_phase_meta_path_mismatch:{scope}:{key}")
            started = _parse_iso_datetime(item.get("started_at"))
            finished = _parse_iso_datetime(item.get("finished_at"))
            owner = lineage if scope == "prior_task" else row
            owner_start = _parse_iso_datetime(owner.get("source_task_started_at") if scope == "prior_task" else owner.get("started_at"))
            owner_end = _parse_iso_datetime(owner.get("source_task_completed_at") if scope == "prior_task" else owner.get("completed_at"))
            try:
                if (None in (started, finished, owner_start, owner_end) or started > finished
                        or started < owner_start - timedelta(seconds=1) or finished > owner_end + timedelta(seconds=1)):
                    raise ValueError("phase_outside_owning_task")
                for key in ("started_at", "finished_at"):
                    stamp = _parse_iso_datetime(summary.get(key))
                    if key in summary and stamp is None:
                        raise ValueError("summary_timestamp_invalid")
                    if stamp is not None and not started <= stamp <= finished + timedelta(seconds=1):
                        raise ValueError("summary_outside_phase")
            except (TypeError, ValueError) as exc:
                source_errors.append(f"{prefix}_phase_time_invalid:{scope}:{exc}")
            evidence_files = [index.get("index_file") or {}, *files.values(),
                              _file_evidence(settings, summary.get("response_artifact"))]
            for evidence in evidence_files:
                if evidence.get("exists"):
                    source = Path(evidence["path"])
                    source_files[str(source)] = hashlib.sha256(source.read_bytes()).hexdigest()
            phases.append({"scope": scope, "source_task_history_id": lineage.get("source_task_history_id") if scope == "prior_task" else row.get("id"),
                "started_at": item.get("started_at"), "finished_at": item.get("finished_at"),
                "success": item.get("success"), "return_code": item.get("return_code"), "timed_out": item.get("timed_out"),
                "stdout_path": out, **{key: summary.get(key) for key in (
                    "status", "target_count", "saved_fund_count", "failed_count", "cooldown_count", "pending_count",
                    "blocking_count", "quarantined_date_lag_fcodes", "response_artifact", "response_artifact_sha256")}})
            if scope != "prior_task":
                current_tasks.append(item)
    try:
        current_tasks.sort(key=lambda item: _parse_iso_datetime(item.get("started_at")))
        for earlier, later in zip(current_tasks, current_tasks[1:]):
            if _parse_iso_datetime(earlier.get("finished_at")) > _parse_iso_datetime(later.get("started_at")):
                source_errors.append(f"{prefix}_phase_times_overlap")
        phases.sort(key=lambda item: _parse_iso_datetime(item.get("started_at")))
    except TypeError:
        source_errors.append(f"{prefix}_phase_order_unproven")
    payload = {"applicable": bool(phases or any(check["collection_expected"] for check in workflow_checks)),
        "source_evidence_verified": not source_errors, "source_phase_count": len(phases),
        "current_task_phase_count": len(current_tasks), "phases": phases, "workflow_checks": workflow_checks,
        "source_files": source_files, "prior_collection_lineage": lineage or None, "derived": None,
        "recorded": {"task_history_id": row.get("id"), "status": row.get("status")},
        "parent_views": [], "differences": [], "source_errors": list(dict.fromkeys(source_errors)),
        "truth_policy": "read_only_bound_phase_reconciliation_preserves_original_records_and_unknowns"}
    if not payload["applicable"]:
        payload["source_evidence_verified"] = None
        return payload, collection_index, [], list(dict.fromkeys(warnings))
    errors = list(source_errors)
    proof, identity_errors = _refresh_collection_payload_binding(settings, row, extra, task_audit)
    payload["parent_payload_binding"] = proof
    errors.extend(f"{prefix}_{error}" for error in identity_errors)
    latest = (current_tasks[-1].get("stdout_result_semantics") or {}).get("business_summary") if current_tasks else None
    if not source_errors and latest:
        classification = classify_collection_log_severity(["api_source_evidence=" + json.dumps(latest, ensure_ascii=False)])
        payload["derived"] = {"status": "degraded" if classification["degraded"] else "success",
            "degraded": classification["degraded"], "degraded_stage": classification["degraded_stage"] or None,
            "warning_reason": classification["reason"] or None, "pending_count": latest.get("pending_count"),
            "blocking_count": latest.get("blocking_count"),
            "source_response_sha256": latest.get("response_artifact_sha256")}
    # Read the full parent log independently of its compact SQL/UI tail.
    try:
        parent_text = _read_complete_collection_log(Path(task_log["path"]))
        period_lines = [line for line in parent_text.splitlines() if "api_source_evidence=" in line
                        and '"collect-period-returns"' in line
                        and not any(char in line.partition("api_source_evidence=")[0] for char in ('"', '{'))]
        entries, parse_errors = _parse_api_source_evidence_entries("\n".join(period_lines))
        entries = [entry for entry in entries if entry.get("task") == "collect-period-returns"]
        # Initial core stdout is read on a worker thread. The main task log
        # binds that full stdout through structured_logs, rather than repeating
        # its entire terminal event. Nested CLI events do reach the parent
        # thread, and must be present unless an exact child log binding exists.
        structured = []
        for match in re.finditer(r"structured_logs stdout=(\S+) stderr=(\S+) meta=(\S+)", parent_text):
            structured.append({key: _file_evidence(settings, raw).get("path")
                               for key, raw in zip(("stdout", "stderr", "meta"), match.groups())})
        bound_phases = []
        remaining_entries = list(entries)
        for item in current_tasks:
            expected = (item.get("stdout_result_semantics") or {}).get("business_summary")
            files = {key: value.get("path") for key, value in (item.get("files") or {}).items()}
            if remaining_entries and remaining_entries[0] == expected:
                remaining_entries.pop(0)
                mode = "complete_forwarded_terminal"
            elif files in structured:
                mode = "exact_structured_child_log_paths"
            else:
                mode = None
            bound_phases.append({"stdout_path": files.get("stdout"), "binding": mode})
        payload["parent_log_phase_count"] = len(entries)
        payload["parent_log_phase_bindings"] = bound_phases
        payload["parent_log_bound_phase_count"] = sum(item["binding"] is not None for item in bound_phases)
        payload["parent_log_complete_sha256"] = hashlib.sha256(
            read_original_artifact_bytes(Path(task_log["path"]))
        ).hexdigest()
        source_files[task_log["path"]] = payload["parent_log_complete_sha256"]
        if parse_errors or remaining_entries or any(item["binding"] is None for item in bound_phases):
            errors.append(f"{prefix}_parent_log_phase_mismatch")
    except (OSError, ValueError, KeyError) as exc:
        errors.append(f"{prefix}_parent_log_unavailable:{exc}")
        payload["parent_log_phase_count"] = None
    views = [("sql", {**extra, "status": row.get("status")}), ("task_result", result)]
    scheduler = task_audit.get("scheduler") or {}
    if isinstance(scheduler, dict):
        for key in ("task_run_summary", "task_closure"):
            value = scheduler.get(key)
            if isinstance(value, dict) and value:
                views.append((f"task_audit.scheduler.{key}", value))
    if state_payload is None:
        state_path = _scheduler_state_file_path(settings)
        state = _read_json(state_path) if state_path.exists() else {}
    else:
        # Historical explanations use the task's own sealed scheduler views;
        # later mutable scheduler state cannot replace that historical input.
        state = state_payload
    task_key = "intraday_refresh" if row.get("task_name") == AUTOMATION_INTRADAY_REFRESH_TASK_NAME else "daily_refresh"
    state_views = [(f"state.last_runs.{row.get('task_name')}", (state.get("last_runs") or {}).get(row.get("task_name"))),
                   (f"state.task_closures.{task_key}", (state.get("task_closures") or {}).get(task_key))]
    for name, value in state_views:
        same = isinstance(value, dict) and value.get("task_name") == row.get("task_name") and (
            _parse_iso_datetime(value.get("started_at")) == _parse_iso_datetime(row.get("started_at")))
        if same:
            if value.get("task_history_id") not in (None, row.get("id")) or value.get("screening_run_id") != main_run_id:
                errors.append(f"{prefix}_state_identity_mismatch:{name}")
            views.append((name, value))
    for name, value in views:
        period = value.get("period_return_collection")
        fields = []
        if value.get("status") != row.get("status"):
            fields.append("recorded_status")
        if name in {"sql", "task_result"}:
            required = name == "task_result" or not proof["sql_full_payload_verified"] or "period_return_collection" in value
            if required and period != latest:
                fields.append("period_return_collection")
        elif "period_return_collection" in value and period != latest:
            fields.append("period_return_collection")
        if name.startswith("state.") and value.get("task_history_extra_data_compacted") is True:
            state_proof, state_errors = _refresh_collection_payload_binding(settings, row, value, task_audit)
            if not state_proof["verified"]:
                fields.extend(state_errors)
        derived = payload["derived"]
        if derived and derived["degraded"]:
            if value.get("status") in AUTOMATION_SUCCESS_STATUSES:
                fields.append("status")
            if value.get("degraded") is not True:
                fields.append("degraded")
            if value.get("degraded_stage") in (None, "", "collect-period-returns"):
                for key in ("degraded_stage", "warning_reason"):
                    if value.get(key) != derived[key]:
                        fields.append(key)
        payload["parent_views"].append({"source": name, "status": value.get("status"),
            "degraded": value.get("degraded"), "degraded_stage": value.get("degraded_stage"),
            "warning_reason": value.get("warning_reason"),
            "pending_count": period.get("pending_count") if isinstance(period, dict) else None,
            "period_summary_inline": isinstance(period, dict),
            "record_sha256": hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True).encode()).hexdigest()})
        if fields:
            payload["differences"].append({"source": name, "fields": fields})
            errors.append(f"{prefix}_parent_mismatch:{name}:{','.join(fields)}")
    payload["parent_matches_derived"] = (not errors) if payload["derived"] is not None else None
    audit_file = _file_evidence(settings, extra.get("task_run_audit_path"))
    if audit_file.get("exists"):
        source_files[audit_file["path"]] = hashlib.sha256(Path(audit_file["path"]).read_bytes()).hexdigest()
    payload["blockers"] = list(dict.fromkeys(errors))
    return payload, {"tasks": current_tasks[-1:]}, list(dict.fromkeys(errors)), list(dict.fromkeys(warnings))


def _verified_refresh_collection_degradation(
    *,
    settings: Settings,
    row: dict[str, Any],
    extra: dict[str, Any],
    task_audit: dict[str, Any],
    collection_index: dict[str, Any],
    collection_blockers: Sequence[str],
) -> dict[str, Any] | None:
    """Explain a real degraded terminal only with matching, verified source evidence."""
    if collection_blockers or row.get("status") != "degraded":
        return None
    audit_task = task_audit.get("task") or {}
    audit_result = task_audit.get("task_result") or {}
    if (not isinstance(audit_task, dict) or not isinstance(audit_result, dict)
            or audit_task.get("status") != "degraded"
            or audit_task.get("task_history_id") != row.get("id")
            or audit_task.get("task_name") != row.get("task_name")):
        return None
    sql_full_payload_verified = False
    if extra.get("task_history_extra_data_compacted") is True:
        binding = extra.get("task_history_full_payload")
        if (extra.get("task_history_extra_data_schema_version") != 1
                or not isinstance(binding, dict) or binding.get("available") is not True
                or binding.get("source") != "scheduler_task_run_audit"):
            return None
        bound_file = _file_evidence(settings, binding.get("sidecar_project_path")
                                    or binding.get("sidecar_path_relative") or binding.get("sidecar_path"))
        task_file = _file_evidence(settings, extra.get("task_run_audit_path"))
        if (not bound_file.get("exists") or not task_file.get("exists")
                or bound_file["path"] != task_file["path"]):
            return None
        # The existing history compactor hashes this exact serialization of
        # task_result, not the containing audit file or a sorted projection.
        full_bytes = json.dumps(audit_result, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        if (type(binding.get("full_payload_bytes")) is not int
                or binding["full_payload_bytes"] != len(full_bytes)
                or binding.get("full_payload_sha256") != hashlib.sha256(full_bytes).hexdigest()):
            return None
        sql_full_payload_verified = True
    period_tasks = [item for item in collection_index.get("tasks") or []
                    if item.get("task_name") == "collect-period-returns"]
    if len(period_tasks) != 1:
        return None
    task = period_tasks[0]
    summary = (task.get("stdout_result_semantics") or {}).get("business_summary")
    if (not isinstance(summary, dict) or summary.get("status") != "degraded"
            or task.get("success") is not True or task.get("return_code") not in (0, "0")
            or task.get("timed_out")):
        return None
    try:
        if period_collection_blocking_count(summary) != 0:
            return None
    except (KeyError, TypeError, ValueError):
        return None
    classification = classify_collection_log_severity([
        "api_source_evidence=" + json.dumps(summary, ensure_ascii=False),
    ])
    if (classification.get("degraded") is not True
            or classification.get("degraded_stage") != "collect-period-returns"):
        return None
    for record in (extra, audit_result):
        if (record.get("status") != "degraded" or record.get("degraded") is not True
                or record.get("degraded_stage") != classification["degraded_stage"]
                or record.get("warning_reason") != classification["reason"]):
            return None
    if audit_result.get("period_return_collection") != summary:
        return None
    if "period_return_collection" in extra:
        if extra["period_return_collection"] != summary:
            return None
    elif not sql_full_payload_verified:
        return None
    return {
        "verified": True,
        "status": "degraded",
        "degraded_stage": classification["degraded_stage"],
        "warning_reason": classification["reason"],
        "failed_count": summary["failed_count"],
        "cooldown_count": summary["cooldown_count"],
        "pending_count": summary["pending_count"],
        "blocking_count": summary.get("blocking_count"),
        "source_task_history_id": row["id"],
        "sql_full_payload_verified": sql_full_payload_verified,
        "sql_inline_period_summary_present": "period_return_collection" in extra,
        "truth_policy": "validated_source_and_sql_and_task_audit_agree_degraded_remains_degraded",
    }


def _find_scheduled_refresh_task_truth(
    settings: Settings,
    *,
    run_id: str,
    final_count: int,
) -> tuple[dict[str, Any], list[str], list[str]]:
    blockers: list[str] = []
    warnings: list[str] = []
    result: dict[str, Any] = {
        "task_name": AUTOMATION_SCHEDULED_REFRESH_TASK_NAME,
        "matched": False,
        "run_id": run_id or None,
        "recent_task_rows_checked": 0,
    }
    if not run_id:
        blockers.append("latest_report_run_id_missing")
        return result, blockers, warnings
    if not settings.database_path.exists():
        blockers.append("database_missing")
        return result, blockers, warnings

    matched_row: dict[str, Any] | None = None
    matched_extra: dict[str, Any] = {}
    rows: Sequence[sqlite3.Row] = []
    with _connect_read_only(settings.database_path) as conn:
        if not _table_exists(conn, "task_history"):
            blockers.append("task_history_table_missing")
            return result, blockers, warnings
        columns = _table_columns(conn, "task_history")
        required_columns = {
            "id",
            "task_name",
            "status",
            "started_at",
            "completed_at",
            "duration",
            "records_failed",
            "error_message",
            "extra_data",
        }
        missing_columns = sorted(required_columns - columns)
        if missing_columns:
            blockers.append(f"task_history_columns_missing:{','.join(missing_columns)}")
            return result, blockers, warnings
        rows = conn.execute(
            f"""
            SELECT
                id,
                task_name,
                status,
                started_at,
                completed_at,
                duration,
                records_processed,
                records_failed,
                error_message,
                extra_data
            FROM task_history
            WHERE task_name IN (?, ?) AND {SCHEDULER_EXECUTION_ROW_SQL}
            ORDER BY id DESC
            LIMIT 200
            """,
            (AUTOMATION_SCHEDULED_REFRESH_TASK_NAME, AUTOMATION_INTRADAY_REFRESH_TASK_NAME),
        ).fetchall()
        result["recent_task_rows_checked"] = len(rows)
        for row in rows:
            extra = _parse_json_object(row["extra_data"])
            candidate_run_id = str(
                extra.get("screening_run_id")
                or (extra.get("workflow_summary") or {}).get("run_id")
                or ""
            ).strip()
            if candidate_run_id != run_id:
                continue
            matched_row = {key: row[key] for key in row.keys() if key != "extra_data"}
            matched_extra = extra
            break

    if rows:
        latest_row = rows[0]
        latest_extra = _parse_json_object(latest_row["extra_data"])
        latest_run_id = str(
            latest_extra.get("screening_run_id")
            or (latest_extra.get("workflow_summary") or {}).get("run_id")
            or ""
        ).strip()
        result["latest_scheduler_run"] = {
            "row": {
                key: latest_row[key]
                for key in latest_row.keys()
                if key != "extra_data"
            },
            "run_id": latest_run_id or None,
            "screening_final_count": _safe_int(
                latest_extra.get("screening_final_count")
            ),
            "screening_triggered": bool(latest_extra.get("screening_triggered")),
            "collection_profile": latest_extra.get("collection_profile"),
        }

    # A completed degraded run can still have a valid formal publication. Keep
    # its identity; full source/parent/degradation verification below still
    # decides whether it is usable. Do not silently fall back to an older success.
    for row in rows:
        extra = _parse_json_object(row["extra_data"])
        historical_run_id = str(extra.get("screening_run_id")
                                or (extra.get("workflow_summary") or {}).get("run_id") or "").strip()
        if (str(row["status"] or "").lower() in AUTOMATION_SUCCESS_STATUSES | AUTOMATION_DEGRADED_STATUSES
                and row["completed_at"] and extra.get("screening_triggered") is True and historical_run_id):
            result["latest_completed_screening_run"] = {
                "row": {key: row[key] for key in row.keys() if key != "extra_data"},
                "run_id": historical_run_id,
                "screening_final_count": _safe_optional_int(extra.get("screening_final_count")),
                "screening_triggered": True,
                "collection_profile": extra.get("collection_profile"),
            }
            break

    # Retain the historical success-only field for existing consumers.
    for row in rows:
        extra = _parse_json_object(row["extra_data"])
        historical_run_id = str(extra.get("screening_run_id")
                                or (extra.get("workflow_summary") or {}).get("run_id") or "").strip()
        if (str(row["status"] or "").lower() in AUTOMATION_SUCCESS_STATUSES
                and extra.get("screening_triggered") is True and historical_run_id):
            result["latest_successful_screening_run"] = {
                "row": {key: row[key] for key in row.keys() if key != "extra_data"},
                "run_id": historical_run_id,
                "screening_final_count": _safe_optional_int(extra.get("screening_final_count")),
                "screening_triggered": True,
                "collection_profile": extra.get("collection_profile"),
            }
            break

    if not matched_row:
        blockers.append("scheduled_refresh_task_history_missing_for_run")
        return result, blockers, warnings

    task_name = str(matched_row["task_name"])
    required_tasks = _refresh_required_subprocess_tasks(task_name)
    status_text = str(matched_row.get("status") or matched_extra.get("status") or "").strip().lower()
    records_failed = _safe_int(matched_row.get("records_failed"))
    error_message = str(matched_row.get("error_message") or "").strip()
    screening_triggered = bool(matched_extra.get("screening_triggered"))
    screening_final_count = _safe_int(matched_extra.get("screening_final_count"))
    task_log_path_raw = (
        matched_extra.get("task_log_path")
        or (matched_extra.get("task_run_audit") or {}).get("task_log_project_path")
        or (matched_extra.get("task_run_audit") or {}).get("task_log_path_relative")
    )
    task_audit_path_raw = (
        matched_extra.get("task_run_audit_path")
        or (matched_extra.get("task_run_audit") or {}).get("artifact_project_path")
        or (matched_extra.get("task_run_audit") or {}).get("artifact_path_relative")
    )
    task_log = _file_evidence(settings, task_log_path_raw)
    task_run_audit = _file_evidence(settings, task_audit_path_raw)
    task_log_path = Path(task_log["path"]) if task_log.get("exists") else None
    task_run_audit_payload = _read_json(Path(task_run_audit["path"])) if task_run_audit.get("exists") else {}
    audit_task = task_run_audit_payload.get("task") if isinstance(task_run_audit_payload.get("task"), dict) else {}
    audit_workflow = (
        task_run_audit_payload.get("workflow_summary")
        if isinstance(task_run_audit_payload.get("workflow_summary"), dict)
        else {}
    )

    if status_text not in AUTOMATION_SUCCESS_STATUSES:
        blockers.append("scheduled_refresh_task_not_success")
    if records_failed:
        blockers.append("scheduled_refresh_records_failed_nonzero")
    if error_message:
        blockers.append("scheduled_refresh_error_message_present")
    if not screening_triggered:
        blockers.append("scheduled_refresh_did_not_trigger_screening")
    if final_count and screening_final_count != final_count:
        blockers.append("scheduled_refresh_final_count_mismatch")
    if not task_log.get("exists"):
        blockers.append("scheduled_refresh_task_log_missing")
    if not task_run_audit.get("exists"):
        blockers.append("scheduled_refresh_task_run_audit_missing")
    if task_run_audit.get("exists") and not audit_task:
        blockers.append("scheduled_refresh_task_run_audit_task_missing")
    if task_run_audit.get("exists") and not audit_workflow:
        blockers.append("scheduled_refresh_task_run_audit_workflow_missing")
    if audit_task and str(audit_task.get("status") or "").strip().lower() not in AUTOMATION_SUCCESS_STATUSES:
        blockers.append("scheduled_refresh_task_run_audit_status_not_success")
    if audit_workflow and str(audit_workflow.get("run_id") or "").strip() != run_id:
        blockers.append("scheduled_refresh_task_run_audit_run_id_mismatch")
    if audit_workflow and str(audit_workflow.get("status") or "").strip().lower() not in AUTOMATION_SUCCESS_STATUSES:
        blockers.append("scheduled_refresh_workflow_summary_not_success")
    if task_name == AUTOMATION_INTRADAY_REFRESH_TASK_NAME:
        if (audit_task.get("task_history_id") != matched_row["id"]
                or audit_task.get("task_name") != task_name):
            blockers.append("scheduled_refresh_task_run_audit_identity_mismatch")
        if (audit_task.get("status") != status_text
                or matched_extra.get("status") != status_text):
            blockers.append("scheduled_refresh_task_run_audit_terminal_mismatch")
        audit_result = task_run_audit_payload.get("task_result") or {}
        if (not isinstance(audit_result, dict)
                or audit_result.get("status") != status_text
                or audit_result.get("screening_run_id") != run_id
                or audit_result.get("screening_final_count") != screening_final_count
                or audit_result.get("screening_triggered") is not True):
            blockers.append("scheduled_refresh_task_run_audit_result_mismatch")

    refresh_index_raw = ""
    refresh_index_file: dict[str, Any] = {
        "configured_value": "",
        "path": "",
        "exists": False,
        "size_bytes": None,
    }
    refresh_subprocess_index: dict[str, Any] = {
        "index_file": refresh_index_file,
        "task_count": 0,
        "required_tasks": list(required_tasks),
        "optional_tasks": [],
        "missing_required_tasks": list(required_tasks),
        "missing_optional_tasks": [],
        "tasks": [],
    }
    (
        refresh_index_raw,
        refresh_index_file,
        refresh_subprocess_index,
        refresh_index_blockers,
        refresh_index_warnings,
    ) = _refresh_collection_index_from_task_log(
        settings,
        task_log_path_raw=task_log_path_raw,
        run_id=run_id,
        required_tasks=required_tasks,
        task_audit=task_run_audit_payload,
    )
    allow_prior_same_day_collection_evidence = bool(
        matched_extra.get("collection_skipped")
        and str(matched_extra.get("screening_reason") or "").strip() == "maintenance_changed_rescreen"
        and any(
            blocker in {
                "scheduled_refresh_collection_subprocess_index_missing",
                "scheduled_refresh_collection_subprocess_index_file_missing",
            }
            for blocker in refresh_index_blockers
        )
    )
    if allow_prior_same_day_collection_evidence:
        (
            prior_index_raw,
            prior_index_file,
            prior_subprocess_index,
            prior_index_blockers,
            prior_index_warnings,
        ) = _find_prior_same_day_refresh_collection_evidence(
            settings,
            rows=rows,
            matched_row=matched_row,
            run_id=run_id,
            required_tasks=required_tasks,
            task_audit=task_run_audit_payload,
        )
        if not prior_index_blockers:
            refresh_index_raw = prior_index_raw
            refresh_index_file = prior_index_file
            refresh_subprocess_index = prior_subprocess_index
            refresh_index_blockers = []
            refresh_index_warnings.extend(prior_index_warnings)
        else:
            refresh_index_blockers = prior_index_blockers
            refresh_index_warnings.extend(prior_index_warnings)
    blockers.extend(refresh_index_blockers)
    warnings.extend(refresh_index_warnings)
    reconciliation, terminal_index, phase_blockers, phase_warnings = _refresh_collection_phase_reconciliation(
        settings, row=matched_row, extra=matched_extra, task_audit=task_run_audit_payload,
        collection_index=refresh_subprocess_index, collection_blockers=refresh_index_blockers, task_log=task_log,
    )
    blockers.extend(phase_blockers)
    warnings.extend(phase_warnings)
    degradation = _verified_refresh_collection_degradation(
        settings=settings, row=matched_row, extra=matched_extra, task_audit=task_run_audit_payload,
        collection_index=terminal_index, collection_blockers=[*refresh_index_blockers, *phase_blockers],
    )
    if degradation:
        status_blockers = {
            "scheduled_refresh_task_not_success",
            "scheduled_refresh_task_run_audit_status_not_success",
        }
        blockers = [item for item in blockers if item not in status_blockers]
        warnings.append(f"scheduled_refresh_collection_degraded:{degradation['warning_reason']}")

    result.update(
        {
            "task_name": task_name,
            "matched": True,
            "row": matched_row,
            "collection_degradation": degradation,
            "collection_reconciliation": reconciliation,
            "extra_summary": {
                "screening_run_id": matched_extra.get("screening_run_id"),
                "screening_final_count": screening_final_count,
                "screening_triggered": screening_triggered,
                "screening_reason": matched_extra.get("screening_reason"),
                "collection_profile": matched_extra.get("collection_profile"),
                "status": matched_extra.get("status"),
                "started_at": matched_extra.get("started_at"),
                "finished_at": matched_extra.get("finished_at"),
                "degraded": bool(matched_extra.get("degraded")),
                "degraded_stage": matched_extra.get("degraded_stage"),
                "warning_reason": matched_extra.get("warning_reason"),
            },
            "task_log": task_log,
            "task_run_audit": {
                **task_run_audit,
                "audit_task_status": audit_task.get("status"),
                "audit_task_name": audit_task.get("task_name"),
                "workflow_status": audit_workflow.get("status"),
                "workflow_run_id": audit_workflow.get("run_id"),
            },
            "collection_subprocess_index": {
                **refresh_subprocess_index,
                "configured_value_from_task_log": refresh_index_raw,
                "source": (
                    (refresh_subprocess_index.get("evidence_lineage") or {}).get("source")
                    if isinstance(refresh_subprocess_index.get("evidence_lineage"), dict)
                    else None
                ),
            },
        }
    )
    return result, blockers, warnings


def _scheduler_state_file_path(settings: Settings) -> Path:
    state_file = getattr(
        settings,
        "scheduler_state_file",
        settings.project_root / "backend" / "data" / "scheduler_state.json",
    )
    state_path = Path(state_file)
    if not state_path.is_absolute():
        state_path = settings.project_root / state_path
    return state_path


def _read_weekly_maintenance_state_snapshot(settings: Settings) -> dict[str, Any]:
    state_path = _scheduler_state_file_path(settings)
    state = _read_json(state_path) if state_path.exists() else {}
    current_task = state.get("current_task") if isinstance(state.get("current_task"), dict) else {}
    task_closures = state.get("task_closures") if isinstance(state.get("task_closures"), dict) else {}
    weekly = (
        task_closures.get("weekly_maintenance")
        if isinstance(task_closures.get("weekly_maintenance"), dict)
        else {}
    )
    return {
        "scheduler_state_file": str(state_path),
        "exists": state_path.exists(),
        "scheduler_running": bool(state.get("scheduler_running")),
        "current_task": current_task,
        "current_task_name": current_task.get("task_name"),
        "current_task_started_at": current_task.get("started_at"),
        "pending_manual_run_request_count": (
            len(state.get("pending_manual_run_requests") or [])
            if isinstance(state.get("pending_manual_run_requests"), list)
            else 0
        ),
        "startup_catch_up_due_jobs": [
            str(item)
            for item in (state.get("startup_catch_up_due_jobs") or [])
            if str(item).strip()
        ],
        "weekly_maintenance_closure_present": bool(weekly),
        "status": weekly.get("status"),
        "run_semantic_status": weekly.get("run_semantic_status"),
        "warning_reason": weekly.get("warning_reason"),
        "degraded_stage": weekly.get("degraded_stage"),
        "degraded": bool(weekly.get("degraded")),
        "manual_intervention_hint": bool(weekly.get("manual_intervention_hint")),
        "attention_needed": bool(weekly.get("attention_needed")),
        "screening_triggered": bool(weekly.get("screening_triggered")),
        "screening_run_id": weekly.get("screening_run_id"),
        "maintenance_rescreen_run_id": weekly.get("maintenance_rescreen_run_id"),
        "report_refresh_run_id": weekly.get("report_refresh_run_id"),
        "task_run_audit_json_project_path": weekly.get("task_run_audit_json_project_path"),
        "task_log_file_project_path": weekly.get("task_log_file_project_path"),
    }


def _is_weekly_maintenance_currently_recovering(
    *,
    status_text: str,
    matched_row: dict[str, Any],
    state_snapshot: dict[str, Any],
) -> bool:
    if status_text != "running":
        return False
    current_task = state_snapshot.get("current_task") if isinstance(state_snapshot.get("current_task"), dict) else {}
    current_task_name = str(state_snapshot.get("current_task_name") or current_task.get("task_name") or "").strip()
    if current_task_name != AUTOMATION_WEEKLY_MAINTENANCE_TASK_NAME:
        return False
    if not bool(state_snapshot.get("scheduler_running")):
        return False
    if _safe_int(state_snapshot.get("pending_manual_run_request_count")):
        return False
    row_started = str(matched_row.get("started_at") or "").strip()
    current_started = str(state_snapshot.get("current_task_started_at") or current_task.get("started_at") or "").strip()
    if row_started and current_started:
        row_dt = _parse_iso_datetime(row_started.replace(" ", "T"))
        current_dt = _parse_iso_datetime(current_started)
        if row_dt is not None and current_dt is not None:
            return abs((row_dt - current_dt).total_seconds()) <= 5
    return bool(current_started)


def _weekly_maintenance_live_write_window(settings: Settings) -> dict[str, Any]:
    state_snapshot = _read_weekly_maintenance_state_snapshot(settings)
    result: dict[str, Any] = {
        "active": False,
        "scheduler_state_snapshot": state_snapshot,
        "matched_row": None,
        "reason": None,
    }
    if not settings.database_path.exists():
        result["reason"] = "database_missing"
        return result

    with _connect_read_only(settings.database_path) as conn:
        if not _table_exists(conn, "task_history"):
            result["reason"] = "task_history_table_missing"
            return result
        columns = _table_columns(conn, "task_history")
        required_columns = {"id", "task_name", "status", "started_at", "completed_at"}
        missing_columns = sorted(required_columns - columns)
        if missing_columns:
            result["reason"] = f"task_history_columns_missing:{','.join(missing_columns)}"
            return result
        # Legacy/minimal history can identify its active owner without JSON metadata.
        # Only exclude busy attempts when their explicit reason can be read.
        execution_filter = SCHEDULER_EXECUTION_ROW_SQL if "extra_data" in columns else "1 = 1"
        row = conn.execute(
            f"""
            SELECT id, task_name, status, started_at, completed_at
            FROM task_history
            WHERE task_name = ? AND {execution_filter}
            ORDER BY id DESC
            LIMIT 1
            """,
            (AUTOMATION_WEEKLY_MAINTENANCE_TASK_NAME,),
        ).fetchone()

    if row is None:
        result["reason"] = "weekly_maintenance_task_history_missing"
        return result

    matched_row = {key: row[key] for key in row.keys()}
    status_text = str(matched_row.get("status") or "").strip().lower()
    active = _is_weekly_maintenance_currently_recovering(
        status_text=status_text,
        matched_row=matched_row,
        state_snapshot=state_snapshot,
    )
    result.update(
        {
            "active": active,
            "matched_row": matched_row,
            "reason": "weekly_maintenance_running_live_write_window" if active else "weekly_maintenance_not_currently_running",
        }
    )
    return result


def _find_weekly_maintenance_warning_truth(
    settings: Settings,
) -> tuple[dict[str, Any], list[str], list[str]]:
    blockers: list[str] = []
    warnings: list[str] = []
    state_snapshot = _read_weekly_maintenance_state_snapshot(settings)
    result: dict[str, Any] = {
        "task_name": AUTOMATION_WEEKLY_MAINTENANCE_TASK_NAME,
        "matched": False,
        "recent_task_rows_checked": 0,
        "scheduler_state_snapshot": state_snapshot,
    }
    if not settings.database_path.exists():
        blockers.append("database_missing")
        return result, blockers, warnings

    matched_row: dict[str, Any] | None = None
    matched_extra: dict[str, Any] = {}
    with _connect_read_only(settings.database_path) as conn:
        if not _table_exists(conn, "task_history"):
            blockers.append("task_history_table_missing")
            return result, blockers, warnings
        columns = _table_columns(conn, "task_history")
        required_columns = {
            "id",
            "task_name",
            "status",
            "started_at",
            "completed_at",
            "duration",
            "records_failed",
            "error_message",
            "extra_data",
        }
        missing_columns = sorted(required_columns - columns)
        if missing_columns:
            blockers.append(f"task_history_columns_missing:{','.join(missing_columns)}")
            return result, blockers, warnings
        rows = conn.execute(
            f"""
            SELECT
                id,
                task_name,
                status,
                started_at,
                completed_at,
                duration,
                records_processed,
                records_failed,
                error_message,
                extra_data
            FROM task_history
            WHERE task_name = ? AND {SCHEDULER_EXECUTION_ROW_SQL}
            ORDER BY id DESC
            LIMIT 20
            """,
            (AUTOMATION_WEEKLY_MAINTENANCE_TASK_NAME,),
        ).fetchall()
        result["recent_task_rows_checked"] = len(rows)
        if rows:
            row = rows[0]
            matched_row = {key: row[key] for key in row.keys() if key != "extra_data"}
            matched_extra = _parse_json_object(row["extra_data"])

    if not matched_row:
        blockers.append("weekly_maintenance_task_history_missing")
        return result, blockers, warnings

    status_text = str(matched_row.get("status") or matched_extra.get("status") or "").strip().lower()
    currently_recovering = _is_weekly_maintenance_currently_recovering(
        status_text=status_text,
        matched_row=matched_row,
        state_snapshot=state_snapshot,
    )
    records_failed = _safe_int(matched_row.get("records_failed") or matched_extra.get("records_failed"))
    error_message = str(matched_row.get("error_message") or matched_extra.get("error_message") or "").strip()
    degraded = bool(matched_extra.get("degraded")) or status_text in AUTOMATION_DEGRADED_STATUSES
    warning_reason = str(matched_extra.get("warning_reason") or "").strip()
    degraded_stage = str(matched_extra.get("degraded_stage") or "").strip()
    screening_triggered = bool(
        matched_extra.get("screening_triggered")
        or matched_extra.get("maintenance_rescreen_triggered")
    )
    maintenance_rescreen_run_id = str(
        matched_extra.get("maintenance_rescreen_run_id")
        or matched_extra.get("screening_run_id")
        or ""
    ).strip()
    report_refresh_run_id = str(matched_extra.get("report_refresh_run_id") or "").strip()
    task_log_path_raw = (
        matched_extra.get("task_log_path")
        or matched_extra.get("task_log_file_project_path")
        or (matched_extra.get("task_run_audit") or {}).get("task_log_project_path")
        or (None if currently_recovering else state_snapshot.get("task_log_file_project_path"))
    )
    task_audit_path_raw = (
        matched_extra.get("task_run_audit_path")
        or matched_extra.get("task_run_audit_json")
        or matched_extra.get("task_run_audit_json_project_path")
        or (matched_extra.get("task_run_audit") or {}).get("artifact_project_path")
        or (None if currently_recovering else state_snapshot.get("task_run_audit_json_project_path"))
    )
    task_log = _file_evidence(settings, task_log_path_raw)
    task_run_audit = _file_evidence(settings, task_audit_path_raw)
    task_run_audit_payload = _read_json(Path(task_run_audit["path"])) if task_run_audit.get("exists") else {}
    audit_task = task_run_audit_payload.get("task") if isinstance(task_run_audit_payload.get("task"), dict) else {}
    audit_workflow = (
        task_run_audit_payload.get("workflow_summary")
        if isinstance(task_run_audit_payload.get("workflow_summary"), dict)
        else {}
    )
    audit_task_result = (
        task_run_audit_payload.get("task_result")
        if isinstance(task_run_audit_payload.get("task_result"), dict)
        else {}
    )
    audit_task_status = str(audit_task.get("status") or "").strip().lower()
    audit_task_name = str(audit_task.get("task_name") or "").strip()
    audit_result_status = str(audit_task_result.get("status") or "").strip().lower()
    audit_warning_reason = str(audit_task_result.get("warning_reason") or "").strip()
    audit_degraded_stage = str(audit_task_result.get("degraded_stage") or "").strip()
    audit_workflow_status = str(audit_workflow.get("status") or "").strip().lower()
    audit_workflow_run_id = str(audit_workflow.get("run_id") or "").strip()
    expected_workflow_run_id = maintenance_rescreen_run_id or report_refresh_run_id
    state_status = str(state_snapshot.get("status") or "").strip().lower()
    state_warning_reason = str(state_snapshot.get("warning_reason") or "").strip()
    state_degraded_stage = str(state_snapshot.get("degraded_stage") or "").strip()
    from backend.weekly_warning_correction import load_verified_correction

    historical_correction = load_verified_correction(settings, int(matched_row["id"]))
    from backend.collection_count_correction import load_verified_correction as load_count_correction

    historical_counts = load_count_correction(settings, int(matched_row["id"]))
    if historical_counts.get("status") == "invalid":
        blockers.append("weekly_maintenance_historical_count_correction_invalid")
    if historical_counts.get("verified"):
        warnings.append("weekly_maintenance_historical_stage_counts_corrected_originals_preserved")
        if any(stage.get("outcome_labels_match") is False for stage in historical_counts.get("stages", [])):
            warnings.append("weekly_maintenance_detail_outcome_labels_differ")
    corrected_reason = str((historical_correction.get("correction") or {}).get("warning_reason") or "") if historical_correction.get("verified") else ""
    if historical_correction.get("status") == "invalid":
        blockers.append("weekly_maintenance_historical_correction_invalid")
    if corrected_reason:
        warnings.append("weekly_maintenance_historical_reason_corrected_collection_still_unresolved")
    maintenance_index_raw = ""
    maintenance_index_file: dict[str, Any] = {
        "configured_value": "",
        "path": "",
        "exists": False,
        "size_bytes": None,
    }
    required_collection_tasks = ["collect-details", "collect-managers"]
    if matched_extra.get("company_collection_enabled") is True:
        required_collection_tasks.append("collect-companies")
    if matched_extra.get("period_return_collection_enabled") is True:
        required_collection_tasks.append("collect-period-returns")
    maintenance_subprocess_index: dict[str, Any] = {
        "index_file": maintenance_index_file,
        "task_count": 0,
        "required_tasks": list(required_collection_tasks),
        "optional_tasks": ["collect-funds"],
        "missing_required_tasks": list(required_collection_tasks),
        "missing_optional_tasks": [],
        "tasks": [],
    }

    if task_log.get("exists"):
        maintenance_index_raw = _subprocess_index_path_from_log(Path(str(task_log.get("path") or "")))
        if maintenance_index_raw:
            maintenance_index_file = _file_evidence(settings, maintenance_index_raw)
            if maintenance_index_file.get("exists"):
                maintenance_index_payload = _read_json(Path(maintenance_index_file["path"]))
                maintenance_subprocess_index, index_blockers, index_warnings = _validate_subprocess_index_payload(
                    settings,
                    index_file=maintenance_index_file,
                    index_payload=maintenance_index_payload,
                    expected_run_id="",
                    required_tasks=required_collection_tasks,
                    optional_tasks=("collect-funds",),
                    issue_prefix="weekly_maintenance_subprocess",
                    allow_missing_run_id=True,
                    include_stdout_result_semantics=True,
                    stdout_semantics_task_allowlist=("collect-funds", "collect-details", "collect-managers", "collect-companies"),
                    allow_recomputed_legacy_stderr_summary=True,
                )
                blockers.extend(index_blockers)
                warnings.extend(index_warnings)
            elif not currently_recovering:
                blockers.append("weekly_maintenance_subprocess_index_file_missing")
        elif not currently_recovering:
            blockers.append("weekly_maintenance_subprocess_index_missing")

    if currently_recovering:
        warnings.append("weekly_maintenance_recovery_in_progress")
    elif status_text not in AUTOMATION_SUCCESS_STATUSES | AUTOMATION_DEGRADED_STATUSES:
        blockers.append("weekly_maintenance_status_unexpected")
    if status_text == "failed":
        blockers.append("weekly_maintenance_task_failed")
    if records_failed:
        blockers.append("weekly_maintenance_records_failed_nonzero")
    if error_message:
        blockers.append("weekly_maintenance_error_message_present")
    if currently_recovering:
        pass
    elif not task_log.get("exists"):
        blockers.append("weekly_maintenance_task_log_missing")
    if currently_recovering:
        pass
    elif not task_run_audit.get("exists"):
        blockers.append("weekly_maintenance_task_run_audit_missing")
    if task_run_audit.get("exists") and not audit_task:
        blockers.append("weekly_maintenance_task_run_audit_task_missing")
    if task_run_audit.get("exists") and not audit_task_result:
        blockers.append("weekly_maintenance_task_run_audit_result_missing")
    if audit_task_name and audit_task_name != AUTOMATION_WEEKLY_MAINTENANCE_TASK_NAME:
        blockers.append("weekly_maintenance_task_run_audit_task_name_mismatch")
    if audit_task_status and status_text and audit_task_status != status_text:
        blockers.append("weekly_maintenance_task_run_audit_status_mismatch")
    if degraded:
        if not warning_reason and not corrected_reason:
            blockers.append("weekly_maintenance_degraded_warning_reason_missing")
        if not degraded_stage:
            blockers.append("weekly_maintenance_degraded_stage_missing")
        if audit_task_result and not audit_warning_reason and not corrected_reason:
            blockers.append("weekly_maintenance_task_run_audit_warning_reason_missing")
        if audit_task_result and not audit_degraded_stage:
            blockers.append("weekly_maintenance_task_run_audit_degraded_stage_missing")
        if warning_reason and audit_warning_reason and audit_warning_reason != warning_reason:
            blockers.append("weekly_maintenance_task_run_audit_warning_reason_mismatch")
        if degraded_stage and audit_degraded_stage and audit_degraded_stage != degraded_stage:
            blockers.append("weekly_maintenance_task_run_audit_degraded_stage_mismatch")
    if screening_triggered:
        if not expected_workflow_run_id:
            blockers.append("weekly_maintenance_rescreen_run_id_missing")
        if not audit_workflow:
            blockers.append("weekly_maintenance_task_run_audit_workflow_missing")
        elif audit_workflow_status not in AUTOMATION_SUCCESS_STATUSES:
            blockers.append("weekly_maintenance_workflow_summary_not_success")
        if audit_workflow_run_id and expected_workflow_run_id and audit_workflow_run_id != expected_workflow_run_id:
            blockers.append("weekly_maintenance_workflow_run_id_mismatch")
    if state_snapshot.get("weekly_maintenance_closure_present"):
        if state_status and status_text and state_status != status_text:
            warnings.append(
                "weekly_maintenance_scheduler_state_status_mismatch_recovering"
                if currently_recovering
                else "weekly_maintenance_scheduler_state_status_mismatch"
            )
        if degraded and not state_warning_reason and not corrected_reason:
            blockers.append("weekly_maintenance_scheduler_state_warning_reason_missing")
        if degraded and not state_degraded_stage:
            blockers.append("weekly_maintenance_scheduler_state_degraded_stage_missing")
        if warning_reason and state_warning_reason and state_warning_reason != warning_reason:
            blockers.append("weekly_maintenance_scheduler_state_warning_reason_mismatch")
        if degraded_stage and state_degraded_stage and state_degraded_stage != degraded_stage:
            blockers.append("weekly_maintenance_scheduler_state_degraded_stage_mismatch")
        if bool(state_snapshot.get("attention_needed")):
            blockers.append("weekly_maintenance_current_attention_needed")

    result.update(
        {
            "matched": True,
            "row": matched_row,
            "extra_summary": {
                "status": matched_extra.get("status"),
                "degraded": degraded,
                "warning_reason": warning_reason or None,
                "degraded_stage": degraded_stage or None,
                "manual_intervention_hint": bool(matched_extra.get("manual_intervention_hint")),
                "screening_triggered": screening_triggered,
                "screening_reason": matched_extra.get("screening_reason"),
                "maintenance_rescreen_run_id": maintenance_rescreen_run_id or None,
                "report_refresh_run_id": report_refresh_run_id or None,
                "records_failed": records_failed,
            },
            "task_log": task_log,
            "task_run_audit": {
                **task_run_audit,
                "audit_task_status": audit_task.get("status"),
                "audit_task_name": audit_task.get("task_name"),
                "audit_result_status": audit_task_result.get("status"),
                "audit_result_warning_reason": audit_task_result.get("warning_reason"),
                "audit_result_degraded_stage": audit_task_result.get("degraded_stage"),
                "workflow_status": audit_workflow.get("status"),
                "workflow_run_id": audit_workflow.get("run_id"),
            },
            "maintenance_subprocess_index": {
                **maintenance_subprocess_index,
                "index_file": maintenance_index_file,
            },
            "interpretation": {
                "recovery_in_progress": currently_recovering,
                "historical_warning_correction": historical_correction,
                "historical_collection_counts": historical_counts,
                "degraded_warning_has_structured_cause": bool(
                    degraded and (warning_reason or corrected_reason) and degraded_stage
                    and (audit_warning_reason or corrected_reason) and audit_degraded_stage
                ),
                "screening_refresh_evidence_present": bool(
                    not screening_triggered
                    or (expected_workflow_run_id and audit_workflow_run_id == expected_workflow_run_id)
                ),
                "current_manual_intervention_required_by_state": bool(state_snapshot.get("attention_needed")),
                "manual_intervention_hint_is_historical_only": bool(
                    matched_extra.get("manual_intervention_hint")
                    and not state_snapshot.get("attention_needed")
                ),
            },
        }
    )
    return result, blockers, warnings


def _aggregate_task_count_stats(stats: Sequence[dict[str, Any]]) -> dict[str, Any]:
    return {
        "block_count": len(stats),
        "total": sum(_safe_int(item.get("total")) for item in stats),
        "processed": sum(_safe_int(item.get("processed")) for item in stats),
        "success": sum(_safe_int(item.get("success")) for item in stats),
        "skipped": sum(_safe_int(item.get("skipped")) for item in stats),
        "failed": sum(_safe_int(item.get("failed")) for item in stats),
    }


def _collection_task_command_summary(task: dict[str, Any]) -> dict[str, Any]:
    command = task.get("command") if isinstance(task.get("command"), list) else []
    command_parts = [str(item) for item in command]
    return {
        "command": command_parts,
        "uses_project_cli": "main.py" in command_parts,
        "dry_run": any(str(item).strip() in {"--dry-run", "--preview"} for item in command_parts),
        "incremental": "--incremental" in command_parts,
        "changed_nav_only": "--changed-nav-only" in command_parts,
    }


def _weekly_maintenance_live_write_window_from_automation_payload(
    automation_payload: dict[str, Any],
) -> dict[str, Any]:
    weekly_truth = (
        automation_payload.get("weekly_maintenance_warning_truth")
        if isinstance(automation_payload.get("weekly_maintenance_warning_truth"), dict)
        else {}
    )
    interpretation = (
        weekly_truth.get("interpretation")
        if isinstance(weekly_truth.get("interpretation"), dict)
        else {}
    )
    state_snapshot = (
        weekly_truth.get("scheduler_state_snapshot")
        if isinstance(weekly_truth.get("scheduler_state_snapshot"), dict)
        else {}
    )
    current_task = (
        state_snapshot.get("current_task")
        if isinstance(state_snapshot.get("current_task"), dict)
        else {}
    )
    current_task_name = str(
        state_snapshot.get("current_task_name") or current_task.get("task_name") or ""
    ).strip()
    active = bool(
        interpretation.get("recovery_in_progress")
        and state_snapshot.get("scheduler_running")
        and current_task_name == AUTOMATION_WEEKLY_MAINTENANCE_TASK_NAME
        and not _safe_int(state_snapshot.get("pending_manual_run_request_count"))
    )
    return {
        "active": active,
        "reason": "weekly_maintenance_running_live_write_window" if active else "weekly_maintenance_not_currently_running",
        "scheduler_state_snapshot": state_snapshot,
        "row": weekly_truth.get("row") if isinstance(weekly_truth.get("row"), dict) else None,
    }


def _formal_snapshot_alignment_from_sections(
    *sections: dict[str, Any],
) -> dict[str, Any]:
    for section in sections:
        alignment = (
            section.get("formal_artifact_snapshot_alignment")
            if isinstance(section, dict)
            else {}
        )
        if isinstance(alignment, dict) and alignment.get("available"):
            return alignment
    return {}


def _formal_artifacts_clean_for_live_write(alignment: dict[str, Any], *, require_risk: bool) -> bool:
    if not isinstance(alignment, dict):
        return False
    key = "clean_for_risk_during_live_write" if require_risk else "clean_for_nav_during_live_write"
    return bool(alignment.get("available") and alignment.get(key))


def _upstream_collection_truth(
    *,
    automation_payload: dict[str, Any],
    freshness_payload: dict[str, Any],
    formal_snapshot_alignment: dict[str, Any] | None = None,
) -> dict[str, Any]:
    blockers: list[str] = []
    warnings: list[str] = []
    weekly_live_publication_window = bool(
        automation_payload.get("weekly_maintenance_live_publication_window")
    )
    scheduled_refresh = (
        automation_payload.get("scheduled_refresh_task")
        if isinstance(automation_payload.get("scheduled_refresh_task"), dict)
        else {}
    )
    scheduler_task_name = str((scheduled_refresh.get("row") or {}).get("task_name")
                              or scheduled_refresh.get("task_name") or "")
    required_tasks = _refresh_required_subprocess_tasks(scheduler_task_name)
    current_run_collection_evidence_valid = bool(
        scheduled_refresh.get("current_run_collection_evidence_valid")
    )
    publication_collection_index = (
        scheduled_refresh.get("publication_collection_subprocess_index")
        if isinstance(
            scheduled_refresh.get("publication_collection_subprocess_index"),
            dict,
        )
        else {}
    )
    if current_run_collection_evidence_valid:
        required_tasks = (("calculate-risk",) if publication_collection_index.get("collection_expected") is False
                          else AUTOMATION_REQUIRED_REFRESH_SUBPROCESS_TASKS)
    scheduler_collection_index = (
        scheduled_refresh.get("collection_subprocess_index")
        if isinstance(scheduled_refresh.get("collection_subprocess_index"), dict)
        else {}
    )
    collection_index = (
        publication_collection_index
        if current_run_collection_evidence_valid and publication_collection_index
        else scheduler_collection_index
    )
    task_items = [
        dict(item)
        for item in collection_index.get("tasks") or []
        if isinstance(item, dict)
    ]
    task_by_name = {
        str(item.get("task_name") or "").strip(): item
        for item in task_items
        if str(item.get("task_name") or "").strip()
    }
    missing_required = [
        task_name
        for task_name in required_tasks
        if task_name not in task_by_name
    ]
    if not bool(scheduled_refresh.get("matched")) and not current_run_collection_evidence_valid:
        blockers.append("scheduled_refresh_task_not_matched")
    degradation = scheduled_refresh.get("collection_degradation") or {}
    if degradation.get("verified") is True:
        warnings.append(f"scheduled_refresh_collection_degraded:{degradation['warning_reason']}")
    reconciliation = scheduled_refresh.get("collection_reconciliation") or {}
    if reconciliation.get("applicable"):
        blockers.extend(reconciliation.get("blockers") or [])
    if missing_required:
        blockers.append(f"upstream_collection_required_tasks_missing:{','.join(missing_required)}")
    for task_name in collection_index.get("missing_required_tasks") or []:
        normalized_task_name = str(task_name or "").strip()
        if normalized_task_name and normalized_task_name not in missing_required:
            blockers.append(f"upstream_collection_index_required_task_missing:{normalized_task_name}")

    if weekly_live_publication_window and scheduled_refresh.get("pending_weekly_maintenance_completion"):
        blockers = []
        warnings.append("upstream_collection_evidence_pending_weekly_maintenance_completion")

    database_freshness = (
        freshness_payload.get("database_freshness")
        if isinstance(freshness_payload.get("database_freshness"), dict)
        else {}
    )
    effective_snapshot = (
        freshness_payload.get("effective_nav_snapshot")
        if isinstance(freshness_payload.get("effective_nav_snapshot"), dict)
        else {}
    )
    risk_lag = (
        freshness_payload.get("risk_lag")
        if isinstance(freshness_payload.get("risk_lag"), dict)
        else {}
    )
    latest_nav_business_date = str(database_freshness.get("latest_nav_business_date") or "").strip()
    candidate_date = str(effective_snapshot.get("candidate_date") or "").strip()
    risk_effective_date = str(effective_snapshot.get("risk_1y_effective_end_date") or "").strip()
    dates_aligned = bool(candidate_date and latest_nav_business_date and risk_effective_date) and (
        candidate_date == latest_nav_business_date == risk_effective_date
    )
    coverage_sufficient = bool(risk_lag.get("coverage_sufficient"))
    fresh_snapshot_aligned = bool(dates_aligned and coverage_sufficient)
    published_snapshot_dates_aligned = bool(
        candidate_date
        and risk_effective_date
        and candidate_date == risk_effective_date
    )
    weekly_write_window = _weekly_maintenance_live_write_window_from_automation_payload(automation_payload)
    formal_artifacts_clean = _formal_artifacts_clean_for_live_write(
        formal_snapshot_alignment or {},
        require_risk=True,
    )
    current_db_dates_incomplete_during_weekly_write = bool(
        latest_nav_business_date
        and candidate_date
        and risk_effective_date
        and not dates_aligned
    )
    daily_evidence_stale_due_to_weekly_write = bool(
        weekly_write_window.get("active")
        and formal_artifacts_clean
        and current_db_dates_incomplete_during_weekly_write
        and latest_nav_business_date
        and candidate_date
    )

    if weekly_live_publication_window and scheduled_refresh.get("pending_weekly_maintenance_completion"):
        return {
            "status": _classify_status([], warnings),
            "blockers": [],
            "warnings": warnings,
            "collection_profile": None,
            "screening_run_id": automation_payload.get("run_id"),
            "screening_final_count": automation_payload.get("final_count"),
            "task_count": 0,
            "required_tasks": list(required_tasks),
            "missing_required_tasks": missing_required,
            "tasks": {},
            "database_freshness_crosscheck": {
                "latest_nav_business_date": latest_nav_business_date or None,
                "candidate_date": candidate_date or None,
                "risk_1y_effective_end_date": risk_effective_date or None,
                "dates_aligned": dates_aligned,
                "risk_coverage_sufficient": coverage_sufficient,
                "fresh_snapshot_aligned": fresh_snapshot_aligned,
                "published_snapshot_dates_aligned": published_snapshot_dates_aligned,
                "weekly_maintenance_live_write_window": weekly_write_window,
                "formal_artifact_snapshot_alignment": formal_snapshot_alignment or {},
            },
            "truth_note": (
                "The current formal run was published by an active weekly maintenance workflow. "
                "Its final task-history evidence is pending until that task closes, so the audit "
                "reports a transient warning instead of fabricating missing subprocess results."
            ),
        }

    task_summaries: dict[str, Any] = {}
    collect_nav_inserted_count: int | None = None
    for task_name in required_tasks:
        task = dict(task_by_name.get(task_name) or {})
        stdout = (
            task.get("stdout_result_semantics")
            if isinstance(task.get("stdout_result_semantics"), dict)
            else {}
        )
        stderr = (
            task.get("stderr_signal_summary")
            if isinstance(task.get("stderr_signal_summary"), dict)
            else {}
        )
        command_summary = _collection_task_command_summary(task)
        completion_stats = [
            dict(item)
            for item in stdout.get("completion_stats") or []
            if isinstance(item, dict)
        ]
        collector_stats = [
            dict(item)
            for item in stdout.get("collector_stats") or []
            if isinstance(item, dict)
        ]
        business_summary = (
            dict(stdout.get("business_summary"))
            if isinstance(stdout.get("business_summary"), dict)
            else {}
        )
        api_source_evidence = [
            dict(item)
            for item in stdout.get("api_source_evidence") or []
            if isinstance(item, dict)
        ]
        api_source_parse_errors = [
            str(item)
            for item in stdout.get("api_source_evidence_parse_errors") or []
            if str(item)
        ]
        completion_totals = _aggregate_task_count_stats(completion_stats)
        collector_totals = _aggregate_task_count_stats(collector_stats)
        task_blockers: list[str] = []
        task_warnings: list[str] = []

        if not task:
            task_blockers.append("task_missing")
        if not bool(task.get("success")):
            task_blockers.append("task_not_success")
        if task.get("return_code") not in (0, "0"):
            task_blockers.append("return_code_nonzero")
        if bool(task.get("timed_out")):
            task_blockers.append("task_timed_out")
        if not command_summary["uses_project_cli"]:
            task_warnings.append("project_cli_command_not_detected")
        if command_summary["dry_run"]:
            task_blockers.append("dry_run_command_not_valid_for_business_truth")
        zero_increment_changed_nav_risk_scan = bool(
            task_name == "calculate-risk"
            and command_summary.get("changed_nav_only")
            and collect_nav_inserted_count == 0
            and (fresh_snapshot_aligned or daily_evidence_stale_due_to_weekly_write)
            and bool(task.get("success"))
            and task.get("return_code") in (0, "0")
        )
        if not stdout.get("available"):
            task_blockers.append("stdout_semantics_missing")
        if not completion_stats and not zero_increment_changed_nav_risk_scan:
            task_blockers.append("completion_stats_missing")
        if completion_totals["failed"]:
            task_blockers.append("completion_failed_nonzero")
        if collector_stats and collector_totals["failed"]:
            task_blockers.append("collector_failed_nonzero")
        if api_source_parse_errors:
            task_blockers.append("api_source_evidence_parse_error")
        stderr_classification = str(stderr.get("classification") or "").strip()
        if stderr_classification == "blocker_signals":
            task_blockers.append("stderr_blocker_signals")
        elif stderr_classification == "warning_signals":
            task_warnings.append("stderr_warning_signals")

        if task_name == "collect-funds":
            bond_fund_count = _safe_int(business_summary.get("bond_fund_count"))
            has_fund_collector = any(str(item.get("label") or "") == "FundCollector" for item in collector_stats)
            if bond_fund_count <= 0:
                task_blockers.append("bond_fund_count_missing_or_empty")
            if not has_fund_collector:
                task_blockers.append("fund_collector_stats_missing")
            evidence_blockers, evidence_warnings = _validate_collect_funds_api_source_evidence(
                api_source_evidence,
                bond_fund_count=bond_fund_count,
            )
            task_blockers.extend(evidence_blockers)
            task_warnings.extend(evidence_warnings)
            search_blockers, search_warnings = _validate_fund_search_fallback_api_source_evidence(
                api_source_evidence
            )
            task_blockers.extend(search_blockers)
            task_warnings.extend(search_warnings)
        elif task_name == "collect-nav":
            if "inserted_nav_record_count" not in business_summary:
                task_blockers.append("inserted_nav_record_count_missing")
            else:
                collect_nav_inserted_count = _safe_int(business_summary.get("inserted_nav_record_count"))
                if collect_nav_inserted_count < 0:
                    task_blockers.append("inserted_nav_record_count_negative")
            has_nav_collector = any(str(item.get("label") or "") == "NavCollector" for item in collector_stats)
            if not has_nav_collector:
                task_blockers.append("nav_collector_stats_missing")
            nav_evidence_blockers, nav_evidence_warnings = _validate_collect_nav_api_source_evidence(
                api_source_evidence,
                inserted_nav_record_count=(
                    collect_nav_inserted_count
                    if collect_nav_inserted_count is not None
                    else None
                ),
            )
            task_blockers.extend(nav_evidence_blockers)
            task_warnings.extend(nav_evidence_warnings)
        elif task_name == "calculate-risk":
            target_count = _safe_int(business_summary.get("target_fund_count"))
            generated_period_count = _safe_int(business_summary.get("generated_period_count"))
            if target_count <= 0 and not zero_increment_changed_nav_risk_scan:
                task_blockers.append("risk_target_fund_count_missing_or_empty")
            if generated_period_count <= 0 and not zero_increment_changed_nav_risk_scan:
                task_blockers.append("risk_generated_period_count_missing_or_empty")

        blockers.extend(f"{task_name}:{item}" for item in task_blockers)
        warnings.extend(f"{task_name}:{item}" for item in task_warnings)
        task_summaries[task_name] = {
            "status": _classify_status(task_blockers, task_warnings),
            "blockers": task_blockers,
            "warnings": task_warnings,
            "success": bool(task.get("success")),
            "return_code": task.get("return_code"),
            "timed_out": bool(task.get("timed_out")),
            "duration_seconds": task.get("duration_seconds"),
            "started_at": task.get("started_at"),
            "finished_at": task.get("finished_at"),
            "command_summary": command_summary,
            "completion_totals": completion_totals,
            "collector_totals": collector_totals,
            "business_summary": business_summary,
            "api_source_evidence_available": bool(api_source_evidence),
            "api_source_evidence": api_source_evidence,
            "api_source_evidence_parse_errors": api_source_parse_errors,
            "stderr_classification": stderr_classification or None,
            "zero_increment_changed_nav_risk_scan": zero_increment_changed_nav_risk_scan,
        }

    if not latest_nav_business_date:
        blockers.append("database_latest_nav_business_date_missing")
    if not candidate_date:
        blockers.append("effective_candidate_date_missing")
    if not risk_effective_date:
        blockers.append("risk_effective_date_missing")
    if not coverage_sufficient:
        if daily_evidence_stale_due_to_weekly_write:
            warnings.append("risk_coverage_not_sufficient_for_collection_truth_during_weekly_maintenance")
        else:
            blockers.append("risk_coverage_not_sufficient_for_collection_truth")
    if collect_nav_inserted_count == 0 and not fresh_snapshot_aligned:
        if daily_evidence_stale_due_to_weekly_write:
            warnings.append("collect_nav_zero_insert_daily_evidence_stale_during_weekly_maintenance")
        else:
            blockers.append("collect_nav_zero_insert_without_fresh_snapshot_alignment")

    return {
        "status": _classify_status(blockers, warnings),
        "blockers": blockers,
        "warnings": warnings,
        "collection_profile": (
            None
            if current_run_collection_evidence_valid
            else (scheduled_refresh.get("extra_summary") or {}).get("collection_profile")
        ),
        "screening_run_id": (
            automation_payload.get("run_id")
            if current_run_collection_evidence_valid
            else (scheduled_refresh.get("extra_summary") or {}).get("screening_run_id")
        ),
        "screening_final_count": (
            automation_payload.get("final_count")
            if current_run_collection_evidence_valid
            else (scheduled_refresh.get("extra_summary") or {}).get("screening_final_count")
        ),
        "evidence_scope": scheduled_refresh.get("evidence_scope"),
        "scheduler_task_name": scheduler_task_name or None,
        "collection_degradation": degradation or None,
        "collection_reconciliation": {
            key: reconciliation.get(key) for key in ("applicable", "source_evidence_verified", "current_task_phase_count",
                "recorded", "derived", "parent_matches_derived", "differences")
        } if reconciliation else None,
        "current_publication_run_scheduler_matched": scheduled_refresh.get(
            "current_publication_run_scheduler_matched",
            bool(scheduled_refresh.get("matched")),
        ),
        "task_count": len(task_items),
        "required_tasks": list(required_tasks),
        "missing_required_tasks": missing_required,
        "tasks": task_summaries,
        "database_freshness_crosscheck": {
            "latest_nav_business_date": latest_nav_business_date or None,
            "candidate_date": candidate_date or None,
            "risk_1y_effective_end_date": risk_effective_date or None,
            "dates_aligned": dates_aligned,
            "risk_coverage_sufficient": coverage_sufficient,
            "fresh_snapshot_aligned": fresh_snapshot_aligned,
            "published_snapshot_dates_aligned": published_snapshot_dates_aligned,
            "collect_nav_inserted_record_count": collect_nav_inserted_count,
            "weekly_maintenance_live_write_window": weekly_write_window,
            "daily_evidence_stale_due_to_weekly_write": daily_evidence_stale_due_to_weekly_write,
            "formal_artifact_snapshot_alignment": formal_snapshot_alignment or {},
            "zero_inserted_nav_interpretation": (
                "collector_reported_zero_increment_and_snapshot_still_fresh"
                if collect_nav_inserted_count == 0 and fresh_snapshot_aligned
                else "collector_reported_zero_increment_before_weekly_maintenance_live_write"
                if collect_nav_inserted_count == 0 and daily_evidence_stale_due_to_weekly_write
                else "collector_reported_positive_increment"
                if collect_nav_inserted_count and collect_nav_inserted_count > 0
                else "not_available"
            ),
        },
        "truth_note": (
            "This section validates the matched scheduler job's collection scope: daily includes the fund directory, "
            "while intraday refreshes NAV and risk. Planned period-return collection is validated independently "
            "from its complete response artifact and keeps any degraded terminal state. "
            "Counts come from real collector stdout semantics and freshness snapshots; a zero NAV increment "
            "is accepted only when the effective NAV and risk dates remain aligned."
        ),
    }


def _automation_subprocess_index_truth(
    settings: Settings,
    latest_report: dict[str, Any],
) -> tuple[dict[str, Any], list[str], list[str]]:
    blockers: list[str] = []
    warnings: list[str] = []
    run_id = str(latest_report.get("run_id") or "").strip()
    artifacts = latest_report.get("artifacts") if isinstance(latest_report.get("artifacts"), dict) else {}
    core_artifacts = artifacts.get("core") if isinstance(artifacts.get("core"), dict) else {}
    raw_index_path = core_artifacts.get("subprocess_log_index_file") or artifacts.get("subprocess_log_index_file")
    index_file = _file_evidence(settings, raw_index_path)
    result: dict[str, Any] = {
        "run_id": run_id or None,
        "index_file": index_file,
        "task_count": 0,
        "required_tasks": list(AUTOMATION_REQUIRED_SUBPROCESS_TASKS),
        "optional_tasks": list(AUTOMATION_OPTIONAL_SUBPROCESS_TASKS),
        "missing_required_tasks": list(AUTOMATION_REQUIRED_SUBPROCESS_TASKS),
        "missing_optional_tasks": list(AUTOMATION_OPTIONAL_SUBPROCESS_TASKS),
        "tasks": [],
    }
    if not run_id:
        blockers.append("latest_report_run_id_missing")
    if not index_file.get("exists"):
        blockers.append("subprocess_log_index_missing")
        return result, blockers, warnings

    index_payload = _read_json(Path(index_file["path"]))
    result, validation_blockers, validation_warnings = _validate_subprocess_index_payload(
        settings,
        index_file=index_file,
        index_payload=index_payload,
        expected_run_id=run_id,
        required_tasks=AUTOMATION_REQUIRED_SUBPROCESS_TASKS,
        optional_tasks=AUTOMATION_OPTIONAL_SUBPROCESS_TASKS,
        issue_prefix="subprocess",
    )
    blockers.extend(validation_blockers)
    warnings.extend(validation_warnings)
    return result, blockers, warnings


def _canonical_run_refresh_collection_truth(
    settings: Settings,
    *,
    latest_report: dict[str, Any],
    run_id: str,
) -> tuple[dict[str, Any], list[str], list[str]]:
    """Validate refresh evidence embedded in the current canonical workflow run.

    This is deliberately independent from scheduler task history: an operator-run
    acceptance workflow can legitimately become the formal publication while the
    scheduler's latest successful run remains a different, earlier run.
    """
    blockers: list[str] = []
    warnings: list[str] = []
    control = latest_report.get("publication_control") or {}
    options = control.get("workflow_options") if isinstance(control, dict) else None
    # Require explicit booleans in the canonical report. Missing/legacy options
    # never excuse missing collection logs, nor do reused outputs or skipped risk.
    full_risk_without_collection = isinstance(options, dict) and all(
        options.get(key) is value for key, value in {
            "run_collection": False, "run_screening": True, "reuse_existing_outputs": False,
            "force_calculate_risk": True, "skip_risk_calculation": False,
            "risk_refresh_changed_nav_only": False,
        }.items()
    )
    required_tasks = ("calculate-risk",) if full_risk_without_collection else AUTOMATION_REQUIRED_REFRESH_SUBPROCESS_TASKS
    artifacts = (
        latest_report.get("artifacts")
        if isinstance(latest_report.get("artifacts"), dict)
        else {}
    )
    core_artifacts = (
        artifacts.get("core")
        if isinstance(artifacts.get("core"), dict)
        else {}
    )
    raw_index_path = (
        core_artifacts.get("subprocess_log_index_file")
        or artifacts.get("subprocess_log_index_file")
    )
    index_file = _file_evidence(settings, raw_index_path)
    empty_result: dict[str, Any] = {
        "index_file": index_file,
        "index_run_id": run_id or None,
        "task_count": 0,
        "required_tasks": list(required_tasks),
        "optional_tasks": [],
        "missing_required_tasks": list(required_tasks),
        "tasks": [],
        "evidence_lineage": {
            "source": "current_canonical_run_subprocess_index",
            "run_id": run_id or None,
        },
    }
    if not run_id:
        blockers.append("current_publication_run_id_missing")
        return empty_result, blockers, warnings
    if not index_file.get("exists"):
        blockers.append("current_publication_collection_subprocess_index_missing")
        return empty_result, blockers, warnings

    index_payload = _read_json(Path(index_file["path"]))
    result, validation_blockers, validation_warnings = _validate_subprocess_index_payload(
        settings,
        index_file=index_file,
        index_payload=index_payload,
        expected_run_id=run_id,
        required_tasks=required_tasks,
        optional_tasks=(),
        issue_prefix="current_publication_collection_subprocess",
        include_stdout_result_semantics=True,
        stdout_semantics_task_allowlist=AUTOMATION_REQUIRED_REFRESH_SUBPROCESS_TASKS,
    )
    if _collection_index_zero_increment_changed_nav_risk_scan(result):
        zero_increment_allowlist = {
            "current_publication_collection_subprocess_stdout_completion_stats_missing:calculate-risk",
            "current_publication_collection_subprocess_calculate_risk_business_summary_missing",
        }
        validation_blockers = [
            item for item in validation_blockers if item not in zero_increment_allowlist
        ]
        result["zero_increment_changed_nav_risk_scan"] = True
    result["evidence_lineage"] = {
        "source": "current_canonical_run_subprocess_index",
        "run_id": run_id,
    }
    result["collection_expected"] = not full_risk_without_collection
    if full_risk_without_collection:
        risk_tasks = [item for item in result.get("tasks", []) if item.get("task_name") == "calculate-risk"]
        risk_task = risk_tasks[0] if len(risk_tasks) == 1 else {}
        command = risk_task.get("command") or []
        option_names = {str(part).split("=", 1)[0] for part in command[3:]
                        if str(part).startswith("-")}
        # Only the workflow's full-refresh CLI is supported here. Checking
        # option names also rejects --fcode=value and abbreviated selectors.
        if (len(command) < 4 or command[1:3] != ["main.py", "calculate-risk"]
                or not re.fullmatch(r"python(?:\d+(?:\.\d+)*)?", Path(str(command[0])).name)
                or "--force" not in command[3:]
                or not option_names <= {"--force", "--yes", "--workers"}):
            validation_blockers.append("current_publication_full_risk_command_mismatch")
        meta_path = ((risk_task.get("files") or {}).get("meta") or {}).get("path")
        meta = _read_json(Path(meta_path)) if meta_path else {}
        if meta.get("command") != command or meta.get("task_name") != "calculate-risk":
            validation_blockers.append("current_publication_full_risk_meta_command_mismatch")
        result["workflow_options"] = dict(options)
        warnings.append("current_publication_explicitly_skipped_collection_full_risk_recalculated")
    blockers.extend(validation_blockers)
    warnings.extend(validation_warnings)
    return result, blockers, warnings


def _automation_script_run_truth(settings: Settings) -> dict[str, Any]:
    blockers: list[str] = []
    warnings: list[str] = []
    latest = _read_json(settings.latest_report_file)
    run_id = str(latest.get("run_id") or "").strip()
    final_count = _safe_int(latest.get("final_count"))

    subprocess_index, subprocess_blockers, subprocess_warnings = _automation_subprocess_index_truth(
        settings,
        latest,
    )
    scheduler_state, scheduler_blockers, scheduler_warnings = _automation_scheduler_state_truth(settings)
    scheduled_refresh, refresh_blockers, refresh_warnings = _find_scheduled_refresh_task_truth(
        settings,
        run_id=run_id,
        final_count=final_count,
    )
    weekly_maintenance_warning, weekly_blockers, weekly_warnings = _find_weekly_maintenance_warning_truth(
        settings,
    )

    weekly_interpretation = (
        weekly_maintenance_warning.get("interpretation")
        if isinstance(weekly_maintenance_warning.get("interpretation"), dict)
        else {}
    )
    weekly_state = (
        weekly_maintenance_warning.get("scheduler_state_snapshot")
        if isinstance(weekly_maintenance_warning.get("scheduler_state_snapshot"), dict)
        else {}
    )
    weekly_current_task = (
        weekly_state.get("current_task")
        if isinstance(weekly_state.get("current_task"), dict)
        else {}
    )
    weekly_live_publication_window = bool(
        weekly_interpretation.get("recovery_in_progress")
        and weekly_state.get("scheduler_running")
        and str(
            weekly_state.get("current_task_name")
            or weekly_current_task.get("task_name")
            or ""
        ).strip()
        == AUTOMATION_WEEKLY_MAINTENANCE_TASK_NAME
        and not _safe_int(weekly_state.get("pending_manual_run_request_count"))
        and run_id
        and run_id == str(_read_json(settings.publication_registry_file).get("current_published_run_id") or "").strip()
    )
    if weekly_live_publication_window and "scheduled_refresh_task_history_missing_for_run" in refresh_blockers:
        refresh_blockers = [
            item
            for item in refresh_blockers
            if item != "scheduled_refresh_task_history_missing_for_run"
        ]
        refresh_warnings.append("scheduled_refresh_task_history_pending_weekly_maintenance_completion")
        scheduled_refresh["pending_weekly_maintenance_completion"] = True
        scheduled_refresh["matched"] = True
        scheduled_refresh["evidence_scope"] = "current_weekly_maintenance_live_publication_window"
    elif refresh_blockers == ["scheduled_refresh_task_history_missing_for_run"]:
        latest_scheduler_run = (
            scheduled_refresh.get("latest_completed_screening_run")
            or scheduled_refresh.get("latest_successful_screening_run")
            or scheduled_refresh.get("latest_scheduler_run")
            if isinstance(scheduled_refresh.get("latest_completed_screening_run")
                          or scheduled_refresh.get("latest_successful_screening_run")
                          or scheduled_refresh.get("latest_scheduler_run"), dict)
            else {}
        )
        latest_scheduler_run_id = str(
            latest_scheduler_run.get("run_id") or ""
        ).strip()
        if latest_scheduler_run_id and latest_scheduler_run_id != run_id:
            (
                publication_collection_index,
                publication_collection_blockers,
                publication_collection_warnings,
            ) = _canonical_run_refresh_collection_truth(
                settings,
                latest_report=latest,
                run_id=run_id,
            )
            (
                latest_scheduler_refresh,
                latest_scheduler_blockers,
                latest_scheduler_warnings,
            ) = _find_scheduled_refresh_task_truth(
                settings,
                run_id=latest_scheduler_run_id,
                final_count=_safe_int(
                    latest_scheduler_run.get("screening_final_count")
                ),
            )
            if not publication_collection_blockers and not latest_scheduler_blockers:
                scheduled_refresh = {
                    **latest_scheduler_refresh,
                    "current_publication_run_id": run_id,
                    "current_publication_run_scheduler_matched": False,
                    "current_run_collection_evidence_valid": True,
                    "publication_collection_subprocess_index": publication_collection_index,
                    "scheduler_lineage": {
                        "current_publication_run_id": run_id,
                        "latest_scheduler_run_id": latest_scheduler_run_id,
                        "aligned": False,
                        "interpretation": (
                            "current formal publication is an operator or external acceptance run; "
                            "latest scheduler run remains independently validated"
                        ),
                    },
                    "evidence_scope": (
                        "current_canonical_run_collection_plus_latest_completed_scheduler_run"
                    ),
                }
                refresh_blockers = []
                refresh_warnings = [
                    *publication_collection_warnings,
                    *latest_scheduler_warnings,
                    "current_publication_run_not_scheduler_owned",
                    "scheduler_latest_run_id_differs_from_current_publication",
                ]
            else:
                refresh_blockers.extend(
                    f"current_publication_refresh:{item}"
                    for item in publication_collection_blockers
                )
                refresh_blockers.extend(
                    f"latest_scheduler_refresh:{item}"
                    for item in latest_scheduler_blockers
                )
                refresh_warnings.extend(publication_collection_warnings)
                refresh_warnings.extend(latest_scheduler_warnings)

    blockers.extend(subprocess_blockers)
    blockers.extend(scheduler_blockers)
    blockers.extend(refresh_blockers)
    blockers.extend(weekly_blockers)
    warnings.extend(subprocess_warnings)
    warnings.extend(scheduler_warnings)
    warnings.extend(refresh_warnings)
    warnings.extend(weekly_warnings)

    return {
        "status": _classify_status(blockers, warnings),
        "blockers": blockers,
        "warnings": warnings,
        "run_id": run_id or None,
        "final_count": final_count,
        "subprocess_index": subprocess_index,
        "scheduler_state": scheduler_state,
        "scheduled_refresh_task": scheduled_refresh,
        "weekly_maintenance_warning_truth": weekly_maintenance_warning,
        "weekly_maintenance_live_publication_window": weekly_live_publication_window,
        "truth_note": (
            "Automation evidence is read from the canonical run subprocess index, scheduler state and SQLite task_history. "
            "It verifies real script execution records, structured log files, job-specific daily/intraday collection/risk subprocesses, "
            "and weekly warning/degraded cause chains. When an operator acceptance run supersedes the scheduler's formal "
            "publication, current-run collection evidence and the latest scheduler task are validated independently. "
            "Stderr content is not treated as failure when return_code/success/timed_out indicate a successful logger-backed process."
        ),
    }


def _publication_truth(settings: Settings) -> dict[str, Any]:
    latest = _read_json(settings.latest_report_file)
    registry = _read_json(settings.publication_registry_file)
    latest_run_id = str(latest.get("run_id") or "").strip()
    latest_canonical_run_id = str(registry.get("latest_canonical_run_id") or "").strip()
    current_published_run_id = str(registry.get("current_published_run_id") or "").strip()
    integrity = dict(registry.get("publication_integrity") or latest.get("publication_integrity") or {})
    publication_status_summary = (
        build_publication_status_summary(registry) if isinstance(registry, dict) and registry else {}
    )
    blockers: list[str] = []
    warnings: list[str] = []

    if not latest:
        blockers.append("latest_report_missing_or_invalid")
    if not registry:
        blockers.append("publication_registry_missing_or_invalid")
    if not latest_run_id:
        blockers.append("latest_report_missing_run_id")
    if latest_run_id and latest_canonical_run_id and latest_run_id != latest_canonical_run_id:
        blockers.append("latest_report_not_latest_canonical")
    if latest_run_id and current_published_run_id and latest_run_id != current_published_run_id:
        blockers.append("latest_report_not_current_published")
    if integrity and not bool(integrity.get("aligned")):
        blockers.append("publication_integrity_not_aligned")
    if not integrity:
        warnings.append("publication_integrity_summary_missing")

    counts = {
        "final_count": _safe_int(latest.get("final_count")),
        "stable_bond_final_count": _safe_int(latest.get("stable_bond_final_count")),
        "enhanced_bond_final_count": _safe_int(latest.get("enhanced_bond_final_count")),
        "dynamic_final_count": _safe_int(latest.get("dynamic_final_count")),
        "project_tracker_current_active_count": _safe_int(latest.get("project_tracker_current_active_count")),
    }
    runtime_diagnostic_runs: dict[str, dict[str, Any]] = {}
    for group_key in ("canonical_runs", "superseded_runs"):
        records = registry.get(group_key) if isinstance(registry, dict) else []
        if not isinstance(records, list):
            continue
        for record in records:
            if not isinstance(record, dict):
                continue
            run_id = str(record.get("run_id") or "").strip()
            if not run_id:
                continue
            runtime_diagnostic_runs[run_id] = {
                "group": group_key,
                "generated_at": str(record.get("generated_at") or "").strip() or None,
                "final_count": _safe_optional_int(record.get("final_count")),
                "dynamic_final_count": _safe_optional_int(record.get("dynamic_final_count")),
                "stable_bond_final_count": _safe_optional_int(record.get("stable_bond_final_count")),
                "enhanced_bond_final_count": _safe_optional_int(record.get("enhanced_bond_final_count")),
                "publishable": record.get("publishable"),
                "published": record.get("published"),
                "publish_hold": record.get("publish_hold"),
            }
    if counts["final_count"] and (
        counts["stable_bond_final_count"] + counts["enhanced_bond_final_count"] != counts["final_count"]
    ):
        blockers.append("formal_group_counts_do_not_sum_to_final_count")
    if counts["project_tracker_current_active_count"] and counts["project_tracker_current_active_count"] != counts["final_count"]:
        blockers.append("tracker_current_count_not_equal_final_count")
    if counts["dynamic_final_count"] and counts["dynamic_final_count"] < counts["final_count"]:
        warnings.append("dynamic_reference_count_below_formal_final_count")

    report_binding: dict[str, Any] = {}
    control = latest.get("publication_control") or {}
    if control.get("commit_protocol"):
        try:
            head = load_publication_head(settings)
            entry = (head.get("committed_runs") or {}).get(latest_run_id) or {}
            path = (settings.project_root / latest["artifacts"]["report_json"]).resolve()
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
            if (head.get("current_published_run_id") != latest_run_id
                    or entry.get("revision") != control.get("revision")
                    or digest != entry.get("report_sha256")):
                raise ValueError("current_report_binding_mismatch")
            report_binding = {"run_id": latest_run_id, "revision": entry["revision"],
                              "report_path": str(path.relative_to(settings.project_root.resolve())),
                              "report_sha256": digest}
        except (OSError, ValueError, KeyError, TypeError, PublicationIntegrityError) as exc:
            blockers.append(f"current_report_binding_invalid:{type(exc).__name__}:{exc}")

    return {
        "status": _classify_status(blockers, warnings),
        "blockers": blockers,
        "warnings": warnings,
        "latest_report_file": str(settings.latest_report_file),
        "publication_registry_file": str(settings.publication_registry_file),
        "latest_report_run_id": latest_run_id or None,
        "latest_canonical_run_id": latest_canonical_run_id or None,
        "current_published_run_id": current_published_run_id or None,
        "latest_report_matches_latest_canonical": bool(latest_run_id and latest_run_id == latest_canonical_run_id),
        "latest_report_matches_current_published": bool(latest_run_id and latest_run_id == current_published_run_id),
        "publication_integrity": {
            "aligned": bool(integrity.get("aligned")) if integrity else None,
            "blocker_count": _safe_int(integrity.get("blocker_count")) if integrity else None,
            "check_count": _safe_int(integrity.get("check_count")) if integrity else None,
        },
        "publication_status": publication_status_summary,
        "runtime_diagnostic_runs": runtime_diagnostic_runs,
        "report_binding": report_binding,
        "counts": counts,
        "truth_note": "Formal publication truth is derived from reports/latest.json and reports/publication_registry.json, not from frontend-rendered tables.",
    }


def _freshness_truth(
    settings: Settings,
    *,
    formal_snapshot_alignment: dict[str, Any] | None = None,
) -> dict[str, Any]:
    blockers: list[str] = []
    warnings: list[str] = []
    try:
        freshness = build_database_freshness_summary(settings)
    except Exception as exc:  # noqa: BLE001 - audit must expose evidence instead of hiding it
        return {
            "status": "needs_review",
            "blockers": [f"database_freshness_build_failed:{exc}"],
            "warnings": [],
        }
    try:
        effective_snapshot = load_effective_nav_snapshot(
            settings.database_path,
            coverage_ratio=settings.effective_nav_coverage_ratio,
            weekday_only=settings.effective_nav_weekday_only,
        ).to_dict()
    except Exception as exc:  # noqa: BLE001
        effective_snapshot = {}
        blockers.append(f"effective_nav_snapshot_failed:{exc}")
    risk_ratio = effective_snapshot.get("risk_1y_effective_coverage_ratio_vs_candidate")
    risk_coverage_semantics = build_risk_coverage_threshold_semantics(
        {"coverage_ratio_vs_candidate": risk_ratio}
    )

    overview = dict(freshness.get("overview_fields") or {})
    coverage = dict(freshness.get("coverage") or {})
    candidate_date = str(effective_snapshot.get("candidate_date") or "").strip()
    latest_nav_business_date = str(overview.get("latest_nav_business_date") or "").strip()
    risk_effective_date = str(effective_snapshot.get("risk_1y_effective_end_date") or "").strip()
    if not candidate_date:
        blockers.append("effective_nav_candidate_date_missing")
    if not latest_nav_business_date:
        blockers.append("latest_nav_business_date_missing")
    if candidate_date and latest_nav_business_date and candidate_date > latest_nav_business_date:
        blockers.append("candidate_date_after_database_latest_nav_date")
    dates_aligned = bool(candidate_date and risk_effective_date and candidate_date == risk_effective_date)
    coverage_sufficient = bool(risk_coverage_semantics.get("coverage_sufficient"))
    weekly_write_window = _weekly_maintenance_live_write_window(settings)
    formal_artifacts_clean = _formal_artifacts_clean_for_live_write(
        formal_snapshot_alignment or {},
        require_risk=True,
    )
    stale_current_db_due_to_weekly_write = bool(
        weekly_write_window.get("active")
        and formal_artifacts_clean
        and latest_nav_business_date
        and candidate_date
        and latest_nav_business_date != candidate_date
        and dates_aligned
    )
    if not coverage_sufficient:
        if stale_current_db_due_to_weekly_write:
            warnings.append("risk_1y_effective_coverage_insufficient_during_weekly_maintenance")
        else:
            blockers.append("risk_1y_effective_coverage_insufficient")
    if not dates_aligned:
        warnings.append("risk_1y_effective_date_not_equal_candidate_date")
    if freshness.get("source_db_changed_during_build"):
        warnings.append("database_changed_during_freshness_build")

    return {
        "status": _classify_status(blockers, warnings),
        "blockers": blockers,
        "warnings": warnings,
        "database_freshness": {
            "status": freshness.get("status"),
            "latest_nav_business_date": latest_nav_business_date or None,
            "risk_1y_business_date": overview.get("risk_1y_business_date"),
            "latest_nav_covered_fund_count": _safe_int(coverage.get("latest_nav_covered_fund_count")),
            "latest_nav_coverage_ratio_vs_funds": coverage.get("latest_nav_coverage_ratio_vs_funds"),
            "risk_1y_latest_fund_count": _safe_int(coverage.get("risk_1y_latest_fund_count")),
            "risk_1y_latest_coverage_ratio_vs_funds": coverage.get("risk_1y_latest_coverage_ratio_vs_funds"),
            "nav_history_archive_bound": bool(overview.get("nav_history_archive_bound")),
            "nav_history_archive_cutoff_date": overview.get("nav_history_archive_cutoff_date"),
        },
        "effective_nav_snapshot": {
            "ready": bool(effective_snapshot.get("ready")),
            "readiness_reason": effective_snapshot.get("readiness_reason"),
            "candidate_date": candidate_date or None,
            "candidate_fund_count": _safe_int(effective_snapshot.get("candidate_fund_count")),
            "coverage_ratio_vs_total": effective_snapshot.get("coverage_ratio_vs_total"),
            "risk_1y_effective_end_date": risk_effective_date or None,
            "risk_1y_effective_fund_count": _safe_int(effective_snapshot.get("risk_1y_effective_fund_count")),
            "risk_1y_effective_coverage_ratio_vs_candidate": effective_snapshot.get(
                "risk_1y_effective_coverage_ratio_vs_candidate"
            ),
        },
        "risk_lag": {
            "aligned": bool(effective_snapshot.get("ready")) and dates_aligned and coverage_sufficient,
            "dates_aligned": dates_aligned,
            "coverage_sufficient": coverage_sufficient,
            "coverage_gate_status": risk_coverage_semantics.get("coverage_gate_status"),
            "lag_days_candidate_vs_effective": _lag_days(candidate_date, risk_effective_date),
            "effective_risk_uncovered_candidate_total": max(
                0,
                _safe_int(effective_snapshot.get("candidate_fund_count"))
                - _safe_int(effective_snapshot.get("risk_1y_effective_fund_count")),
            ),
        },
        "weekly_maintenance_live_write_window": weekly_write_window,
        "formal_artifact_snapshot_alignment": formal_snapshot_alignment or {},
        "stale_current_db_due_to_weekly_write": stale_current_db_due_to_weekly_write,
        "truth_note": "Freshness evidence is computed from SQLite tables and effective NAV/risk snapshots; it does not call upstream APIs or fabricate dates.",
    }


def _risk_metric_truth(settings: Settings) -> dict[str, Any]:
    blockers: list[str] = []
    warnings: list[str] = []
    contract = risk_metric_formula_contract()
    contract_units = contract.get("unit_contract") if isinstance(contract.get("unit_contract"), dict) else {}
    contract_metrics = (
        (contract.get("database_engine") or {}).get("metrics")
        if isinstance(contract.get("database_engine"), dict)
        else {}
    )
    contract_database_engine = (
        contract.get("database_engine") if isinstance(contract.get("database_engine"), dict) else {}
    )
    contract_metrics = contract_metrics if isinstance(contract_metrics, dict) else {}
    input_guard_contracts = contract_database_engine.get("input_window_guard_contracts")
    input_guard_contracts = input_guard_contracts if isinstance(input_guard_contracts, list) else []
    input_guard_by_code = {
        str(item.get("guard_code") or "").strip(): item
        for item in input_guard_contracts
        if isinstance(item, dict)
    }
    calculation_parameter_contracts = contract_database_engine.get("calculation_parameter_contracts")
    calculation_parameter_contracts = (
        calculation_parameter_contracts
        if isinstance(calculation_parameter_contracts, list)
        else []
    )
    calculation_parameter_by_code = {
        str(item.get("parameter_code") or "").strip(): item
        for item in calculation_parameter_contracts
        if isinstance(item, dict)
    }
    risk_latest_reader_contracts = [
        {
            "path": "backend/risk_coverage_gap_audit.py",
            "required_patterns": (
                "ORDER BY COALESCE(TRIM(latest.end_date), '') DESC, latest.rowid DESC",
                "ORDER BY latest.rowid DESC",
            ),
        },
        {
            "path": "backend/app.py",
            "required_patterns": (
                "_latest_risk_rowid_predicate(\"rm\", \"rm_latest\")",
                "AND {latest_risk_rowid_predicate}",
            ),
        },
        {
            "path": "core/utils/bond_mainlist_audit.py",
            "required_patterns": (
                "ORDER BY rm_latest.end_date DESC, rm_latest.rowid DESC",
            ),
        },
        {
            "path": "scripts/audit_final_selection_contraction_shadow.py",
            "required_patterns": (
                "ORDER BY COALESCE(TRIM(risk_row.end_date), '') DESC, risk_row.rowid DESC",
            ),
        },
        {
            "path": "backend/report_builder.py",
            "required_patterns": (
                "_latest_risk_rowid_predicate(\"risk\", \"latest\")",
                "AND {latest_risk_rowid_predicate}",
            ),
        },
        {
            "path": "backend/database_freshness_summary.py",
            "required_patterns": (
                "_latest_risk_rowid_predicate(\"risk\", \"latest\")",
                "AND {latest_rowid_predicate}",
                "COUNT(DISTINCT risk.fcode)",
            ),
        },
        {
            "path": "backend/risk_sync_audit.py",
            "required_patterns": (
                "_latest_risk_rowid_predicate(\"risk\", \"latest\")",
                "AND {latest_rowid_predicate}",
            ),
        },
        {
            "path": "backend/future_outlier_monitor.py",
            "required_patterns": (
                "ORDER BY COALESCE(TRIM(latest.end_date), '') DESC, latest.rowid DESC",
            ),
        },
        {
            "path": "scripts/audit_database_hotspot_profile.py",
            "required_patterns": (
                "ORDER BY rm_latest.end_date DESC, rm_latest.rowid DESC",
                "ORDER BY COALESCE(TRIM(latest.end_date), '') DESC, latest.rowid DESC",
            ),
        },
        {
            "path": "scripts/audit_data_truthfulness.py",
            "required_patterns": (
                "_latest_risk_rowid_predicate(\"risk\", \"latest\")",
                "AND {latest_risk_rowid_predicate}",
                "missing_ready_risk_snapshot_count",
            ),
        },
        {
            "path": "core/cli/aux_commands.py",
            "required_patterns": (
                "ORDER BY COALESCE(TRIM(latest_risk.end_date), '') DESC, latest_risk.rowid DESC",
            ),
        },
    ]
    risk_readiness_reader_contracts = [
        {
            "path": "backend/effective_nav.py",
            "reader_scope": "effective_nav_snapshot_risk_coverage_and_retrigger_readiness",
            "required_patterns": (
                "RISK_READY_COLUMNS = (",
                "annual_return",
                "sharpe_ratio",
                "max_drawdown",
                "volatility",
                "ORDER BY COALESCE(TRIM({latest_alias}.end_date), '') DESC, {latest_alias}.rowid DESC",
                "AND {risk_ready_predicate}",
                "AND {latest_rowid_predicate}",
            ),
        },
        {
            "path": "backend/scheduler.py",
            "reader_scope": "empty_detail_payload_screening_context_risk_readiness",
            "required_patterns": (
                "_risk_metric_ready_predicate(conn, \"rm\")",
                "_latest_risk_rowid_predicate(\"rm\", \"rm_latest\")",
                "AND {risk_ready_predicate}",
                "AND {latest_rowid_predicate}",
            ),
        },
        {
            "path": "backend/app.py",
            "reader_scope": "favorite_score_database_snapshot_risk_display",
            "required_patterns": (
                "_risk_metric_ready_predicate(conn, \"rm\")",
                "_latest_risk_rowid_predicate(\"rm\", \"rm_latest\")",
                "CASE WHEN {risk_ready_predicate} THEN 1 ELSE 0 END",
                "AND {latest_risk_rowid_predicate}",
                "prepare_risk_display_row(source_row)",
            ),
        },
        {
            "path": "backend/long_term_history_maintenance.py",
            "reader_scope": "long_term_history_screenable_universe_risk_readiness",
            "required_patterns": (
                "_risk_metric_ready_predicate(conn, \"risk\")",
                "_latest_risk_rowid_predicate(\"risk\", \"latest\")",
                "AND {risk_ready_predicate}",
                "AND {latest_rowid_predicate}",
            ),
        },
        {
            "path": "core/collectors/detail_collector.py",
            "reader_scope": "detail_empty_cooldown_context_risk_readiness",
            "required_patterns": (
                "RISK_READY_COLUMNS = (",
                "_risk_metric_ready_predicate(conn, 'risk')",
                "_latest_risk_rowid_predicate('risk', 'latest')",
                "AND {risk_ready_predicate}",
                "AND {latest_rowid_predicate}",
            ),
        },
        {
            "path": "backend/risk_lag_audit.py",
            "reader_scope": "risk_lag_audit_effective_risk_coverage_and_missing_semantics",
            "required_patterns": (
                "_risk_metric_ready_predicate(conn, \"risk\")",
                "_latest_risk_rowid_predicate(\"risk\", \"latest\")",
                "AND {risk_ready_predicate}",
                "AND {latest_rowid_predicate}",
                "risk_1y_effective_end_date = str(snapshot.risk_1y_effective_end_date or \"\").strip()",
                "risk_1y_effective_fund_count = int(snapshot.risk_1y_effective_fund_count or 0)",
            ),
        },
        {
            "path": "backend/report_builder.py",
            "reader_scope": "report_builder_risk_snapshot_display_values",
            "required_patterns": (
                "_risk_metric_ready_predicate(conn, \"risk\")",
                "_latest_risk_rowid_predicate(\"risk\", \"latest\")",
                "AND {risk_ready_predicate}",
                "AND {latest_risk_rowid_predicate}",
            ),
        },
        {
            "path": "backend/database_freshness_summary.py",
            "reader_scope": "database_freshness_summary_ready_risk_coverage",
            "required_patterns": (
                "DATABASE_FRESHNESS_SUMMARY_SCHEMA_VERSION = 2",
                "_risk_metric_ready_predicate(conn, \"risk\")",
                "_latest_risk_rowid_predicate(\"risk\", \"latest\")",
                "AND {risk_ready_predicate}",
                "AND {latest_rowid_predicate}",
                "risk_1y_raw_business_date",
            ),
        },
        {
            "path": "backend/risk_sync_audit.py",
            "reader_scope": "risk_sync_audit_recalculation_gap_readiness",
            "required_patterns": (
                "_risk_metric_ready_predicate(conn, \"risk\")",
                "_latest_risk_rowid_predicate(\"risk\", \"latest\")",
                "AND {risk_ready_predicate}",
                "AND {latest_rowid_predicate}",
            ),
        },
        {
            "path": "backend/risk_coverage_gap_audit.py",
            "reader_scope": "risk_coverage_gap_audit_ready_risk_gap_explanation",
            "required_patterns": (
                "_risk_metric_ready_predicate(conn, \"r1\")",
                "_latest_risk_rowid_predicate(\"r1\", \"latest\")",
                "_risk_metric_ready_predicate(conn, \"r2\")",
                "_latest_risk_rowid_predicate(\"r2\", \"latest\")",
                "AND {latest_risk_ready_predicate}",
                "AND {latest_risk_rowid_predicate}",
                "AND {effective_risk_ready_predicate}",
                "AND {effective_risk_rowid_predicate}",
                "raw_effective_risk_end_date",
            ),
        },
    ]
    risk_latest_reader_checks: list[dict[str, Any]] = []
    risk_readiness_reader_checks: list[dict[str, Any]] = []
    project_root = getattr(settings, "project_root", None) or Path(__file__).resolve().parent.parent
    for contract_item in risk_latest_reader_contracts:
        relative_path = str(contract_item.get("path") or "").strip()
        source_path = Path(project_root) / relative_path
        try:
            source_text = source_path.read_text(encoding="utf-8")
        except OSError:
            source_text = ""
        normalized_source_text = " ".join(source_text.split())
        required_patterns = [
            str(pattern)
            for pattern in contract_item.get("required_patterns") or ()
            if str(pattern).strip()
        ]
        missing_patterns = [
            pattern
            for pattern in required_patterns
            if " ".join(pattern.split()) not in normalized_source_text
        ]
        if missing_patterns:
            blockers.append(f"risk_latest_reader_rowid_tiebreak_missing:{relative_path}")
        risk_latest_reader_checks.append(
            {
                "path": relative_path,
                "exists": bool(source_text),
                "required_patterns": required_patterns,
                "missing_patterns": missing_patterns,
            }
        )
    for contract_item in risk_readiness_reader_contracts:
        relative_path = str(contract_item.get("path") or "").strip()
        source_path = Path(project_root) / relative_path
        try:
            source_text = source_path.read_text(encoding="utf-8")
        except OSError:
            source_text = ""
        normalized_source_text = " ".join(source_text.split())
        required_patterns = [
            str(pattern)
            for pattern in contract_item.get("required_patterns") or ()
            if str(pattern).strip()
        ]
        missing_patterns = [
            pattern
            for pattern in required_patterns
            if " ".join(pattern.split()) not in normalized_source_text
        ]
        if missing_patterns:
            blockers.append(f"risk_readiness_reader_latest_row_contract_missing:{relative_path}")
        risk_readiness_reader_checks.append(
            {
                "path": relative_path,
                "reader_scope": str(contract_item.get("reader_scope") or "").strip(),
                "exists": bool(source_text),
                "required_patterns": required_patterns,
                "missing_patterns": missing_patterns,
                "truth_scope": (
                    "Readiness/count/date coverage paths do not publish business risk values, "
                    "but they must only count the latest rowid risk record with required risk fields present."
                ),
            }
        )
    frozen_audit_path = Path(project_root) / "scripts/audit_low_vol_risk_metric_distortion.py"
    required_calls = {"bind_frozen_report_sources", "verify_frozen_baseline", "_frozen_monthly_inputs", "write_compressed_audit_csv"}
    forbidden_calls = {"connect", "read_sql_query", "_build_candidate_universe", "_run_publish_pipeline", "_load_nav_history_frame"}
    try:
        frozen_tree = ast.parse(frozen_audit_path.read_text(encoding="utf-8"))
        audit_calls = {node.func.id if isinstance(node.func, ast.Name) else node.func.attr
                       for node in ast.walk(frozen_tree) if isinstance(node, ast.Call)
                       and isinstance(node.func, (ast.Name, ast.Attribute))}
    except (OSError, SyntaxError):
        audit_calls = set()
    frozen_monthly_contract = {"path": str(frozen_audit_path.relative_to(project_root)),
        "missing_required_calls": sorted(required_calls - audit_calls),
        "forbidden_current_data_calls": sorted(forbidden_calls & audit_calls),
        "scope": "static_reader_contract; runtime input completeness is verified by the frozen audit itself"}
    if frozen_monthly_contract["missing_required_calls"] or frozen_monthly_contract["forbidden_current_data_calls"]:
        blockers.append("low_vol_monthly_frozen_reader_contract_invalid")
    var_contract = {
        key: contract_metrics.get(key) if isinstance(contract_metrics.get(key), dict) else {}
        for key in ("var_95", "var_99")
    }
    decimal_ratio_fields = set(contract_units.get("decimal_ratio_percent_fields") or [])
    percentage_point_fields = set(contract_units.get("percentage_point_fields") or [])
    database_path = settings.database_path
    if not database_path.exists():
        return {
            "status": "needs_review",
            "blockers": ["database_missing"],
            "warnings": [],
        }
    period_summary: list[dict[str, Any]] = []
    invalid_counts: dict[str, int] = {}
    extreme_value_counts: dict[str, int] = {}
    extreme_value_samples: dict[str, list[dict[str, Any]]] = {}
    extreme_value_sample_limit = 20
    with _connect_read_only(database_path) as conn:
        if not _table_exists(conn, "fund_risk_metrics"):
            return {
                "status": "needs_review",
                "blockers": ["fund_risk_metrics_missing"],
                "warnings": [],
            }
        rows = conn.execute(
            """
            SELECT
                period,
                COUNT(*) AS row_count,
                MIN(volatility) AS min_volatility,
                MAX(volatility) AS max_volatility,
                MIN(max_drawdown) AS min_max_drawdown,
                MAX(max_drawdown) AS max_max_drawdown,
                MIN(var_95) AS min_var_95,
                MAX(var_95) AS max_var_95,
                MIN(var_99) AS min_var_99,
                MAX(var_99) AS max_var_99,
                MIN(annual_return) AS min_annual_return,
                MAX(annual_return) AS max_annual_return,
                MIN(positive_ratio) AS min_positive_ratio,
                MAX(positive_ratio) AS max_positive_ratio
            FROM fund_risk_metrics
            GROUP BY period
            ORDER BY period
            """
        ).fetchall()
        for row in rows:
            period_summary.append(
                {
                    "period": str(row["period"] or ""),
                    "row_count": _safe_int(row["row_count"]),
                    "volatility_range": [row["min_volatility"], row["max_volatility"]],
                    "max_drawdown_range": [row["min_max_drawdown"], row["max_max_drawdown"]],
                    "var_95_range": [row["min_var_95"], row["max_var_95"]],
                    "var_99_range": [row["min_var_99"], row["max_var_99"]],
                    "annual_return_range": [row["min_annual_return"], row["max_annual_return"]],
                    "positive_ratio_range": [row["min_positive_ratio"], row["max_positive_ratio"]],
                }
            )
        checks = {
            "negative_volatility": "volatility < 0",
            "non_numeric_volatility": "volatility IS NOT NULL AND typeof(volatility) NOT IN ('integer', 'real')",
            "non_finite_volatility": f"ABS(volatility) > {sys.float_info.max!r}",
            "negative_max_drawdown": "max_drawdown < 0",
            "max_drawdown_above_one": "max_drawdown > 1",
            "var_95_out_of_decimal_return_bounds": "var_95 < -1 OR var_95 > 1",
            "var_99_out_of_decimal_return_bounds": "var_99 < -1 OR var_99 > 1",
            "positive_ratio_percent_point_out_of_range": "positive_ratio < 0 OR positive_ratio > 100",
        }
        for key, where_sql in checks.items():
            row = conn.execute(f"SELECT COUNT(*) FROM fund_risk_metrics WHERE {where_sql}").fetchone()
            invalid_counts[key] = _safe_int(row[0] if row else 0)
        # Annualized sample standard deviation has no universal 100% cap.
        # Finite extremes retain a diagnostic warning; invalid values still
        # block above. This audit does not alter stored metrics or screening.
        extreme_where = "typeof(volatility) IN ('integer', 'real') AND volatility > 1 AND volatility <= ?"
        row = conn.execute(
            f"SELECT COUNT(*) FROM fund_risk_metrics WHERE {extreme_where}",
            (sys.float_info.max,),
        ).fetchone()
        extreme_value_counts["volatility_above_one"] = _safe_int(row[0] if row else 0)
        extreme_value_samples["volatility_above_one"] = [
            dict(row)
            for row in conn.execute(
                f"""SELECT rowid AS risk_rowid, fcode, period, volatility
                    FROM fund_risk_metrics WHERE {extreme_where}
                    ORDER BY volatility DESC, fcode, period, rowid LIMIT ?""",
                (sys.float_info.max, extreme_value_sample_limit),
            ).fetchall()
        ]

    for key, count in invalid_counts.items():
        if count:
            blockers.append(key)
    if extreme_value_counts["volatility_above_one"]:
        warnings.append("volatility_above_one_requires_source_review")
    unit_fields = sorted(field for field in DECIMAL_RATIO_PERCENT_FIELDS if field in {"volatility", "max_drawdown", "var_95", "var_99"})
    if not {"volatility", "max_drawdown", "var_95", "var_99"}.issubset(set(unit_fields)):
        warnings.append("metric_unit_registry_missing_core_decimal_ratio_fields")
    if not {"volatility", "max_drawdown", "var_95", "var_99"}.issubset(decimal_ratio_fields):
        blockers.append("risk_formula_contract_decimal_ratio_fields_missing")
    if not {"annual_return", "positive_ratio"}.issubset(percentage_point_fields):
        blockers.append("risk_formula_contract_percentage_point_fields_missing")
    for var_field, var_meta in var_contract.items():
        if var_meta.get("sign_convention") != "lower_tail_return_percentile":
            blockers.append(f"risk_formula_contract_var_sign_convention_invalid:{var_field}")
        if var_meta.get("positive_values_allowed") is not True:
            blockers.append(f"risk_formula_contract_var_positive_values_not_allowed:{var_field}")
        if var_meta.get("raw_unit") != "decimal_ratio":
            blockers.append(f"risk_formula_contract_var_unit_invalid:{var_field}")
    required_input_guard_codes = {
        "duplicate_nav_dates",
        "missing_selected_nav_values",
        "short_history_window",
        "insufficient_period_coverage",
        "stale_nav_against_snapshot",
    }
    for guard_code in sorted(required_input_guard_codes):
        guard_contract = input_guard_by_code.get(guard_code)
        if not guard_contract:
            blockers.append(f"risk_formula_contract_input_guard_missing:{guard_code}")
            continue
        if guard_contract.get("truth_policy") != "preserve_missing_semantics":
            blockers.append(f"risk_formula_contract_input_guard_truth_policy_invalid:{guard_code}")
        missing_semantics = str(guard_contract.get("missing_metric_semantics") or "").lower()
        if "none" not in missing_semantics and "do not" not in missing_semantics:
            blockers.append(f"risk_formula_contract_input_guard_missing_semantics_weak:{guard_code}")
        if not str(guard_contract.get("regression_test") or "").strip():
            blockers.append(f"risk_formula_contract_input_guard_regression_missing:{guard_code}")

    required_parameter_codes = {
        "risk_free_rate",
        "sortino_minimum_acceptable_return",
        "annualization_frequency",
        "return_series_selection",
        "risk_adjusted_ratio_caps",
        "layer6_calmar_rules",
        "var_percentile_rules",
        "missing_metric_semantics",
    }
    for parameter_code in sorted(required_parameter_codes):
        parameter_contract = calculation_parameter_by_code.get(parameter_code)
        if not parameter_contract:
            blockers.append(f"risk_formula_contract_parameter_missing:{parameter_code}")
            continue
        if not str(parameter_contract.get("truth_policy") or "").strip():
            blockers.append(f"risk_formula_contract_parameter_truth_policy_missing:{parameter_code}")
        if not str(parameter_contract.get("regression_test") or "").strip():
            blockers.append(f"risk_formula_contract_parameter_regression_missing:{parameter_code}")

    risk_free_contract = calculation_parameter_by_code.get("risk_free_rate") or {}
    if risk_free_contract and risk_free_contract.get("raw_unit") != "decimal_ratio":
        blockers.append("risk_formula_contract_parameter_unit_invalid:risk_free_rate")
    if risk_free_contract and "sharpe_ratio" not in set(risk_free_contract.get("applies_to") or []):
        blockers.append("risk_formula_contract_parameter_binding_missing:risk_free_rate")

    sortino_contract = calculation_parameter_by_code.get("sortino_minimum_acceptable_return") or {}
    if sortino_contract and sortino_contract.get("raw_unit") != "decimal_ratio":
        blockers.append("risk_formula_contract_parameter_unit_invalid:sortino_minimum_acceptable_return")
    if sortino_contract and "periods_per_year" not in str(sortino_contract.get("daily_mar_formula") or ""):
        blockers.append("risk_formula_contract_parameter_formula_weak:sortino_minimum_acceptable_return")

    annualization_contract = calculation_parameter_by_code.get("annualization_frequency") or {}
    if annualization_contract and annualization_contract.get("configured_default_trading_days_per_year") != 252:
        blockers.append("risk_formula_contract_parameter_trading_days_invalid:annualization_frequency")
    if annualization_contract and "365.25" not in str(annualization_contract.get("actual_years_formula") or ""):
        blockers.append("risk_formula_contract_parameter_actual_years_formula_weak:annualization_frequency")

    return_series_contract = calculation_parameter_by_code.get("return_series_selection") or {}
    return_series_priority = " ".join(str(item) for item in return_series_contract.get("source_priority") or [])
    if return_series_contract and "daily_growth" not in return_series_priority:
        blockers.append("risk_formula_contract_parameter_daily_growth_priority_missing:return_series_selection")
    if return_series_contract and "pct_change" not in return_series_priority:
        blockers.append("risk_formula_contract_parameter_nav_return_fallback_missing:return_series_selection")
    if return_series_contract and return_series_contract.get("converted_return_unit") != "decimal_ratio":
        blockers.append("risk_formula_contract_parameter_converted_unit_invalid:return_series_selection")

    ratio_cap_contract = calculation_parameter_by_code.get("risk_adjusted_ratio_caps") or {}
    if ratio_cap_contract and ratio_cap_contract.get("preserve_raw_fields") is not True:
        blockers.append("risk_formula_contract_parameter_raw_ratio_preservation_missing:risk_adjusted_ratio_caps")
    if ratio_cap_contract and ratio_cap_contract.get("sharpe_display_cap") != 5.0:
        blockers.append("risk_formula_contract_parameter_sharpe_cap_invalid:risk_adjusted_ratio_caps")
    if ratio_cap_contract and ratio_cap_contract.get("sortino_display_cap_abs") != 10.0:
        blockers.append("risk_formula_contract_parameter_sortino_cap_invalid:risk_adjusted_ratio_caps")

    var_rule_contract = calculation_parameter_by_code.get("var_percentile_rules") or {}
    if var_rule_contract and var_rule_contract.get("var_95_percentile") != 5:
        blockers.append("risk_formula_contract_parameter_var95_percentile_invalid:var_percentile_rules")
    if var_rule_contract and var_rule_contract.get("var_99_percentile") != 1:
        blockers.append("risk_formula_contract_parameter_var99_percentile_invalid:var_percentile_rules")
    if var_rule_contract and var_rule_contract.get("positive_values_allowed") is not True:
        blockers.append("risk_formula_contract_parameter_positive_var_not_allowed:var_percentile_rules")

    missing_contract = calculation_parameter_by_code.get("missing_metric_semantics") or {}
    if missing_contract and missing_contract.get("disallow_zero_fill") is not True:
        blockers.append("risk_formula_contract_parameter_zero_fill_not_forbidden:missing_metric_semantics")
    if missing_contract and "do not write" not in str(missing_contract.get("missing_value_policy") or "").lower():
        blockers.append("risk_formula_contract_parameter_missing_policy_weak:missing_metric_semantics")

    return {
        "status": _classify_status(blockers, warnings),
        "blockers": blockers,
        "warnings": warnings,
        "period_summary": period_summary,
        "invalid_counts": invalid_counts,
        "extreme_value_counts": extreme_value_counts,
        "extreme_value_samples": extreme_value_samples,
        "extreme_value_sample_limit": extreme_value_sample_limit,
        "extreme_value_note": (
            "Finite annualized volatility above 1 is mathematically possible and requires source review. "
            "Counts cover all stored risk rows; samples are bounded. Values and production gates are unchanged."
        ),
        "metric_unit_registry": {
            "decimal_ratio_percent_fields": unit_fields,
            "metadata": {
                key: metric_unit_metadata().get(key)
                for key in unit_fields
            },
            "percentage_point_fields": {
                "annual_return": {
                    "raw_unit": "percent_point",
                    "validity_rule": "range is strategy-dependent; not constrained to 0..100",
                },
                "positive_ratio": {
                    "raw_unit": "percent_point",
                    "validity_rule": "0 <= positive_ratio <= 100",
                    "source": "positive return days / observed return days * 100",
                },
            },
        },
        "risk_metric_formula_contract": contract,
        "risk_metric_input_guard_contracts": {
            "required_guard_codes": sorted(required_input_guard_codes),
            "observed_guard_codes": sorted(input_guard_by_code),
        },
        "risk_metric_calculation_parameter_contracts": {
            "required_parameter_codes": sorted(required_parameter_codes),
            "observed_parameter_codes": sorted(calculation_parameter_by_code),
        },
        "risk_latest_reader_contracts": risk_latest_reader_checks,
        "frozen_monthly_audit_contract": frozen_monthly_contract,
        "risk_readiness_reader_contracts": risk_readiness_reader_checks,
        "truth_note": (
            "Risk ranges are read from fund_risk_metrics. annual_return and positive_ratio remain "
            "percentage-point data; volatility/max_drawdown/VaR use decimal ratios. VaR95/99 are lower-tail "
            "return percentiles, so positive values are valid for all-positive return windows and are not "
            "treated as fabricated losses."
        ),
    }


def _frontend_truth(settings: Settings) -> dict[str, Any]:
    frontend_dir = settings.project_root / "frontend"
    pages = {
        "/": frontend_dir / "index.html",
        "/tracker": frontend_dir / "tracker.html",
        "/recommendations": frontend_dir / "recommendations.html",
        "/favorites": frontend_dir / "favorites.html",
        "/reports": frontend_dir / "reports.html",
        "/task-summary": frontend_dir / "task-summary.html",
        "/operations": frontend_dir / "operations.html",
        "/api-settings": frontend_dir / "settings.html",
        "/settings": frontend_dir / "settings.html",
        "/excel-viewer": frontend_dir / "excel_viewer.html",
    }
    blockers: list[str] = []
    warnings: list[str] = []
    page_checks: list[dict[str, Any]] = []
    scheduler_helper_routes = {"/", "/operations", "/api-settings", "/settings"}
    for route, path in pages.items():
        try:
            body = path.read_text(encoding="utf-8")
        except OSError:
            blockers.append(f"frontend_page_missing:{route}")
            page_checks.append({"route": route, "path": str(path), "exists": False})
            continue
        has_version_widget = "version-widget.js" in body
        has_scheduler_shared = "scheduler-shared.js" in body
        has_operations_watch = "operations-dashboard-watch.js" in body
        if not has_version_widget:
            blockers.append(f"version_widget_missing:{route}")
        if route not in scheduler_helper_routes and has_scheduler_shared:
            warnings.append(f"scheduler_helper_loaded_on_non_scheduler_page:{route}")
        if route != "/operations" and has_operations_watch:
            warnings.append(f"operations_watch_loaded_outside_operations:{route}")
        page_checks.append(
            {
                "route": route,
                "path": str(path),
                "exists": True,
                "has_version_widget": has_version_widget,
                "loads_scheduler_shared": has_scheduler_shared,
                "loads_operations_dashboard_watch": has_operations_watch,
            }
        )

    frontend_js_files = sorted(frontend_dir.glob("*.js"))
    risky_literal_matches: list[dict[str, Any]] = []
    literal_pattern = re.compile(r"\b(?:mock|fake|dummy|fabricat|假数据|示例数据)\b", re.IGNORECASE)
    fake_zero_percent_matches: list[dict[str, Any]] = []
    fake_zero_percent_pattern = re.compile(
        r"formatSignedPercentText\(.*(?:\|\|\s*['\"]0['\"]|\?\?\s*['\"]0['\"])",
    )
    fake_zero_ratio_matches: list[dict[str, Any]] = []
    fake_zero_ratio_patterns = (
        re.compile(r"formatPercent\(.*(?:\|\|\s*0|\?\?\s*0)"),
        re.compile(r"formatPercent\(.*\*\s*100"),
        re.compile(r"formatNumber\(.*(?:rate_pct|gap_pct).*?(?:\|\|\s*0|\?\?\s*0)"),
    )
    scheduler_boolean_runtime_matches: list[dict[str, Any]] = []
    scheduler_boolean_runtime_pattern = re.compile(
        r"formatScheduler(?:Runtime|Config)\([^)]*Boolean\([^)]*(?:scheduler_running|enabled)"
    )
    publish_count_truthy_fallback_matches: list[dict[str, Any]] = []
    publish_count_truthy_fallback_pattern = re.compile(
        r"(?:official_publish_count|dynamic_reference_(?:export_)?count|"
        r"screenable_universe_count|stable_bond_final_count|enhanced_bond_final_count)"
        r"\s*\|\|"
    )
    for path in frontend_js_files:
        rel_path = str(path.relative_to(settings.project_root))
        try:
            for line_no, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
                if literal_pattern.search(line):
                    risky_literal_matches.append(
                        {
                            "path": rel_path,
                            "line": line_no,
                            "text": line.strip()[:240],
                        }
                    )
                if fake_zero_percent_pattern.search(line):
                    fake_zero_percent_matches.append(
                        {
                            "path": rel_path,
                            "line": line_no,
                            "text": line.strip()[:240],
                        }
                    )
                if rel_path != "frontend/shared.js" and any(pattern.search(line) for pattern in fake_zero_ratio_patterns):
                    fake_zero_ratio_matches.append(
                        {
                            "path": rel_path,
                            "line": line_no,
                            "text": line.strip()[:240],
                        }
                    )
                if scheduler_boolean_runtime_pattern.search(line):
                    scheduler_boolean_runtime_matches.append(
                        {
                            "path": rel_path,
                            "line": line_no,
                            "text": line.strip()[:240],
                        }
                    )
                if publish_count_truthy_fallback_pattern.search(line):
                    publish_count_truthy_fallback_matches.append(
                        {
                            "path": rel_path,
                            "line": line_no,
                            "text": line.strip()[:240],
                        }
                    )
        except OSError:
            continue
    if risky_literal_matches:
        warnings.append("frontend_risky_literal_matches_present")
    if fake_zero_percent_matches:
        blockers.append("frontend_missing_percent_rendered_as_zero")
    if fake_zero_ratio_matches:
        blockers.append("frontend_missing_ratio_rendered_as_zero")
    if scheduler_boolean_runtime_matches:
        blockers.append("frontend_scheduler_status_missing_semantics_lost")
    if publish_count_truthy_fallback_matches:
        blockers.append("frontend_publish_count_truthy_fallback")

    return {
        "status": "blocker" if blockers else _classify_status(blockers, warnings),
        "blockers": blockers,
        "warnings": warnings,
        "page_checks": page_checks,
        "risky_literal_matches": risky_literal_matches[:50],
        "fake_zero_percent_matches": fake_zero_percent_matches[:50],
        "fake_zero_ratio_matches": fake_zero_ratio_matches[:50],
        "scheduler_boolean_runtime_matches": scheduler_boolean_runtime_matches[:50],
        "publish_count_truthy_fallback_matches": publish_count_truthy_fallback_matches[:50],
        "truth_note": "Frontend audit checks script wiring and suspicious literals; business values must still be verified through API payloads and canonical reports.",
    }


def _compare_tracker_occurrence_rows(
    tracker_rows: list[dict[str, Any]],
    displayed_rows: list[dict[str, Any]],
    total_periods: Any,
) -> dict[str, Any]:
    """Compare observed values independently of the runtime display normalizer."""
    def number(value: Any) -> float | None:
        if isinstance(value, bool):
            return None
        value = _safe_float(value)
        return value if value is not None and math.isfinite(value) else None

    total = number(total_periods)
    if total is None or total <= 0 or not total.is_integer():
        return {"status": "unavailable", "aligned": None, "reason": "unique_observation_count_missing_or_invalid",
                "checked_rows": 0, "difference_count": None}
    sources = {_normalize_fund_code(row.get("基金代码")): row for row in tracker_rows}
    differences = []
    seen = set()
    for row in displayed_rows:
        code = _normalize_fund_code(row.get("基金代码"))
        source = sources.get(code)
        reasons = []
        if not code or code in seen:
            reasons.append("empty_or_duplicate_code")
        seen.add(code)
        if source is None:
            reasons.append("same_revision_tracker_row_missing")
        for field in ("出现次数", "出现频率(%)"):
            expected, actual = number((source or {}).get(field)), number(row.get(field))
            if expected != actual:
                reasons.append(field + "_differs_from_tracker")
        count, frequency = number(row.get("出现次数")), number(row.get("出现频率(%)"))
        if count is None or frequency is None:
            reasons.append("observation_values_unavailable")
        elif not count.is_integer() or not 0 <= count <= total:
            reasons.append("count_outside_unique_observations")
        elif not 0 <= frequency <= 100 or abs(frequency - round(count / total * 100, 2)) > 0.005:
            reasons.append("frequency_denominator_mismatch")
        reason_text = str(row.get("入选原因") or "")
        for match in re.finditer(r"累计入围\s+(\d+)\s*/\s*(\d+)\s*期", reason_text):
            if int(match[1]) != count or int(match[2]) != total:
                reasons.append("reason_observation_count_mismatch")
        for match in re.finditer(r"累计出现频率\s+(\d+(?:\.\d+)?)\s*%", reason_text):
            if frequency is None or float(match[1]) != float(f"{frequency:.0f}"):
                reasons.append("reason_observation_frequency_mismatch")
        if reasons:
            differences.append({"code": code, "reasons": reasons})
    return {"status": "mismatch" if differences else "matched", "aligned": not differences,
            "total_periods": int(total), "checked_rows": len(displayed_rows),
            "difference_count": len(differences), "differences": differences[:30]}


def _compare_recommendation_reason_briefs(
    groups: list[dict[str, Any]], briefs: dict[str, Any],
) -> dict[str, Any]:
    """Check summary counts against the reason text in the corresponding rows."""
    checked = 0
    differences = []
    for group in groups:
        counts: dict[str, int] = {}
        for row in group.get("funds") or []:
            parts = [part.strip() for part in str(row.get("入选原因") or "").replace(";", "；").split("；") if part.strip()]
            for part in parts[:4]:
                counts[part] = counts.get(part, 0) + 1
        if not counts:
            continue
        checked += 1
        name = str(group.get("name") or "").strip()
        brief = briefs.get(name) or {}
        if "reason_counts" in brief:
            aligned = brief["reason_counts"] == counts
        elif "top_reason_tags" in brief:
            expected = [{"label": label, "count": count}
                        for label, count in sorted(counts.items(), key=lambda item: -item[1])[:4]]
            aligned = brief["top_reason_tags"] == expected
        else:
            aligned = False
        if not aligned:
            differences.append({"group": name, "reason": "reason_brief_differs_from_group_rows"})
    return {"status": "mismatch" if differences else "matched" if checked else "unavailable",
            "aligned": not differences if checked else None, "checked_groups": checked,
            "differences": differences}


def _frontend_field_lineage_truth(
    settings: Settings,
    *,
    include_live_api: bool,
    live_base_url: str,
    timeout_seconds: float,
) -> dict[str, Any]:
    blockers: list[str] = []
    warnings: list[str] = []
    latest = _read_json(settings.latest_report_file)
    run_id = str(latest.get("run_id") or "").strip()
    artifacts = latest.get("artifacts") if isinstance(latest.get("artifacts"), dict) else {}
    filtered_csv_path = _resolve_project_path(settings, artifacts.get("filtered_csv"))
    tracker_detail_path = _resolve_project_path(settings, artifacts.get("project_tracker_current_detail_json"))
    recommendations_artifact_declared = bool(
        _clean_business_text(artifacts.get("recommendations_detail_json"))
        or _clean_business_text(artifacts.get("recommendations_detail"))
    )
    recommendations_detail_path_source = "latest_artifacts"
    recommendations_detail_path = _resolve_project_path(
        settings,
        artifacts.get("recommendations_detail_json") or artifacts.get("recommendations_detail"),
    )
    if recommendations_detail_path is None and run_id:
        runs_dir = getattr(settings, "runs_dir", None)
        if runs_dir is not None:
            recommendations_detail_path = Path(runs_dir) / run_id / "recommendations_detail.json"
            recommendations_detail_path_source = "legacy_run_id_fallback"
    if not recommendations_artifact_declared:
        blockers.append("recommendations_detail_artifact_not_declared_in_latest_report")
    protected_live_artifacts = {
        "favorite_query_history": getattr(settings, "favorites_history_file", None),
        "tracker_current_detail_sidecar": tracker_detail_path,
        "recommendations_detail_sidecar": recommendations_detail_path,
    }
    live_probe_protected_artifacts: dict[str, Any] = {
        name: {
            "path": str(path) if path else "",
            "before": None,
            "after": None,
            "unchanged": None,
        }
        for name, path in protected_live_artifacts.items()
    }

    csv_fields: list[str] = []
    sample_codes: list[str] = []
    filtered_row_count = 0
    if not filtered_csv_path or not filtered_csv_path.exists():
        blockers.append("filtered_csv_missing")
    else:
        csv_fields, sample_codes, filtered_row_count = _read_csv_header_and_sample_codes(filtered_csv_path)
        missing_csv_fields = [field for field in FILTERED_CSV_REQUIRED_FIELDS if field not in csv_fields]
        if missing_csv_fields:
            blockers.append(f"filtered_csv_required_fields_missing:{','.join(missing_csv_fields)}")
        if _safe_int(latest.get("final_count")) and filtered_row_count != _safe_int(latest.get("final_count")):
            blockers.append("filtered_csv_row_count_not_equal_final_count")
        if not sample_codes:
            blockers.append("filtered_csv_sample_codes_missing")

    tracker_payload: dict[str, Any] = {}
    tracker_rows: list[dict[str, Any]] = []
    derived_fields: dict[str, Any] = {}
    tracker_cache_basis_summary: dict[str, Any] = {
        "present": False,
        "required_keys": list(TRACKER_DERIVED_CACHE_BASIS_REQUIRED_KEYS),
        "missing_required_keys": list(TRACKER_DERIVED_CACHE_BASIS_REQUIRED_KEYS),
    }
    if not tracker_detail_path or not tracker_detail_path.exists():
        blockers.append("project_tracker_current_detail_json_missing")
    else:
        tracker_payload = _read_json(tracker_detail_path)
        if str(tracker_payload.get("artifact_type") or "").strip() != "project_tracker_current_detail_sidecar":
            blockers.append("project_tracker_current_detail_json_unexpected_type")
        if run_id and str(tracker_payload.get("run_id") or "").strip() != run_id:
            blockers.append("project_tracker_current_detail_json_run_id_mismatch")
        tracker_rows = [dict(item) for item in tracker_payload.get("rows") or [] if isinstance(item, dict)]
        if _safe_int(latest.get("final_count")) and len(tracker_rows) != _safe_int(latest.get("final_count")):
            blockers.append("project_tracker_current_detail_row_count_not_equal_final_count")
        tracker_field_presence = _summarize_field_presence(tracker_rows, TRACKER_SIDECAR_REQUIRED_FIELDS)
        missing_tracker_fields = [
            field
            for field, payload in tracker_field_presence.get("fields", {}).items()
            if not payload.get("present")
        ]
        if missing_tracker_fields:
            blockers.append(f"tracker_sidecar_required_fields_missing:{','.join(missing_tracker_fields)}")
        derived_fields = tracker_payload.get("derived_fields") if isinstance(tracker_payload.get("derived_fields"), dict) else {}
        if derived_fields.get("schema_version") != 2:
            blockers.append("tracker_derived_platform_return_schema_mismatch")
        for map_key in TRACKER_DERIVED_REQUIRED_MAPS:
            source_map = derived_fields.get(map_key)
            if not isinstance(source_map, dict):
                blockers.append(f"tracker_derived_map_missing:{map_key}")
            elif not source_map:
                warnings.append(f"tracker_derived_map_empty:{map_key}")
        tracker_cache_basis_summary, cache_basis_blockers = _summarize_frontend_cache_basis_contract(
            derived_fields.get("cache_basis"),
            artifact_key="tracker_sidecar",
            required_keys=TRACKER_DERIVED_CACHE_BASIS_REQUIRED_KEYS,
            expected_run_id=run_id,
        )
        blockers.extend(cache_basis_blockers)

    recommendations_payload: dict[str, Any] = {}
    recommendations_response: dict[str, Any] = {}
    recommendations_rows: list[dict[str, Any]] = []
    recommendations_funds: list[dict[str, Any]] = []
    recommendations_field_presence: dict[str, Any] = {"fields": {}}
    recommendations_required_basis_keys = (
        RECOMMENDATIONS_FROZEN_BASIS_REQUIRED_KEYS
        if (latest.get("publication_control") or {}).get("commit_protocol")
        else RECOMMENDATIONS_CACHE_BASIS_REQUIRED_KEYS
    )
    recommendations_cache_basis_summary: dict[str, Any] = {
        "present": False,
        "required_keys": list(recommendations_required_basis_keys),
        "missing_required_keys": list(recommendations_required_basis_keys),
    }
    recommendations_response_cache_basis_summary: dict[str, Any] = {
        "present": False,
        "required_keys": list(recommendations_required_basis_keys),
        "missing_required_keys": list(recommendations_required_basis_keys),
    }
    recommendations_response_source_basis_summary: dict[str, Any] = {
        "present": False,
        "required_keys": list(recommendations_required_basis_keys),
        "missing_required_keys": list(recommendations_required_basis_keys),
    }
    recommendations_response_source_contract_summary: dict[str, Any] = {
        "present": False,
        "expected": dict(RECOMMENDATIONS_DETAIL_SOURCE_CONTRACT_EXPECTED),
        "observed": {},
    }
    recommendations_response_payload_scope_source_contract: dict[str, Any] = {
        "present": False,
    }
    if not recommendations_detail_path or not recommendations_detail_path.exists():
        blockers.append("recommendations_detail_sidecar_missing")
    else:
        recommendations_payload = _read_json(recommendations_detail_path)
        if str(recommendations_payload.get("artifact_type") or "").strip() != "recommendations_detail_sidecar":
            blockers.append("recommendations_detail_sidecar_unexpected_type")
        if run_id and str(recommendations_payload.get("run_id") or "").strip() != run_id:
            blockers.append("recommendations_detail_sidecar_run_id_mismatch")
        recommendations_response = (
            recommendations_payload.get("response")
            if isinstance(recommendations_payload.get("response"), dict)
            else {}
        )
        if not recommendations_response:
            blockers.append("recommendations_detail_sidecar_response_missing")
        elif run_id and str(recommendations_response.get("run_id") or "").strip() != run_id:
            blockers.append("recommendations_detail_sidecar_response_run_id_mismatch")
        recommendations_rows = [
            dict(item)
            for item in recommendations_response.get("rows") or []
            if isinstance(item, dict)
        ]
        recommendations_funds = [
            dict(item)
            for item in (recommendations_response.get("portfolio_suggestions") or {}).get("funds") or []
            if isinstance(item, dict)
        ]
        if not recommendations_rows:
            blockers.append("recommendations_detail_sidecar_rows_missing")
        if recommendations_funds and len(recommendations_rows) != len(recommendations_funds):
            blockers.append("recommendations_detail_sidecar_rows_funds_count_mismatch")
        recommendations_field_presence = _summarize_field_presence(
            recommendations_rows,
            RECOMMENDATIONS_SIDECAR_REQUIRED_FIELDS,
        )
        missing_recommendation_fields = [
            field
            for field, payload in recommendations_field_presence.get("fields", {}).items()
            if not payload.get("present")
        ]
        if missing_recommendation_fields:
            blockers.append(
                "recommendations_detail_sidecar_required_fields_missing:"
                + ",".join(missing_recommendation_fields)
            )
        recommendations_cache_basis_summary, cache_basis_blockers = _summarize_recommendations_cache_basis_contract(
            recommendations_payload.get("cache_basis"),
            artifact_key="recommendations_detail_sidecar",
            settings=settings, report=latest,
        )
        blockers.extend(cache_basis_blockers)
        response_cache_basis = (
            recommendations_response.get("cache_basis")
            if isinstance(recommendations_response.get("cache_basis"), dict)
            else {}
        )
        response_source_basis = (
            recommendations_response.get("source_basis")
            if isinstance(recommendations_response.get("source_basis"), dict)
            else {}
        )
        response_source_contract = (
            recommendations_response.get("source_contract")
            if isinstance(recommendations_response.get("source_contract"), dict)
            else {}
        )
        recommendations_response_cache_basis_summary, response_cache_blockers = (
            _summarize_recommendations_cache_basis_contract(
                response_cache_basis,
                artifact_key="recommendations_detail_response",
                settings=settings, report=latest,
            )
        )
        blockers.extend(response_cache_blockers)
        recommendations_response_source_basis_summary, response_source_blockers = (
            _summarize_recommendations_cache_basis_contract(
                response_source_basis,
                artifact_key="recommendations_detail_response_source_basis",
                settings=settings, report=latest,
            )
        )
        blockers.extend(response_source_blockers)
        if response_cache_basis and recommendations_payload.get("cache_basis") != response_cache_basis:
            blockers.append("recommendations_detail_response_cache_basis_mismatch")
        if response_source_basis and response_cache_basis and response_source_basis != response_cache_basis:
            blockers.append("recommendations_detail_response_source_basis_cache_basis_mismatch")
        recommendations_response_source_contract_summary, response_contract_blockers = (
            _summarize_recommendations_detail_source_contract(
                response_source_contract,
                artifact_key="recommendations_detail_response",
                expected_contract=RECOMMENDATIONS_DETAIL_SIDECAR_SOURCE_CONTRACT_EXPECTED,
                allowed_values={
                    "truth_policy": {RECOMMENDATIONS_DETAIL_LEGACY_TRUTH_POLICY},
                },
            )
        )
        blockers.extend(response_contract_blockers)
        response_payload_scope = (
            recommendations_response.get("payload_scope")
            if isinstance(recommendations_response.get("payload_scope"), dict)
            else {}
        )
        payload_scope_source_contract = (
            response_payload_scope.get("source_contract")
            if isinstance(response_payload_scope.get("source_contract"), dict)
            else {}
        )
        recommendations_response_payload_scope_source_contract = {
            "present": bool(payload_scope_source_contract),
            "source": payload_scope_source_contract.get("source") if payload_scope_source_contract else None,
            "cache_basis_present": payload_scope_source_contract.get("cache_basis_present")
            if payload_scope_source_contract
            else None,
            "source_basis_matches_cache_basis": payload_scope_source_contract.get(
                "source_basis_matches_cache_basis"
            )
            if payload_scope_source_contract
            else None,
            "truth_policy": payload_scope_source_contract.get("truth_policy")
            if payload_scope_source_contract
            else None,
            "missing_value_policy": payload_scope_source_contract.get("missing_value_policy")
            if payload_scope_source_contract
            else None,
        }
        if not payload_scope_source_contract:
            blockers.append("recommendations_detail_response_payload_scope_source_contract_missing")
        elif str(payload_scope_source_contract.get("source") or "").strip() != "run_local_recommendations_detail":
            blockers.append("recommendations_detail_response_payload_scope_source_contract_source_mismatch")
        elif payload_scope_source_contract.get("cache_basis_present") is not True:
            blockers.append("recommendations_detail_response_payload_scope_source_contract_cache_basis_missing")
        elif payload_scope_source_contract.get("source_basis_matches_cache_basis") is not True:
            blockers.append("recommendations_detail_response_payload_scope_source_contract_basis_mismatch")
        elif str(payload_scope_source_contract.get("truth_policy") or "").strip() not in {
            "real_api_database_runtime_artifact_and_publication_registry_only",
            RECOMMENDATIONS_DETAIL_LEGACY_TRUTH_POLICY,
        }:
            blockers.append("recommendations_detail_response_payload_scope_source_contract_truth_policy_mismatch")
        elif str(payload_scope_source_contract.get("missing_value_policy") or "").strip() != "preserve_missing_semantics":
            blockers.append("recommendations_detail_response_payload_scope_source_contract_missing_value_policy_mismatch")

    occurrence_total = ((latest.get("project_publish_summary") or {}).get("project_tracker_summary") or {}).get("total_periods")
    stored_occurrence_check = _compare_tracker_occurrence_rows(tracker_rows, recommendations_rows, occurrence_total)
    if stored_occurrence_check["aligned"] is False:
        # Immutable old artifacts remain historical evidence. Live serving is
        # checked separately and must not reproduce their invalid counts.
        if recommendations_response.get("tracker_occurrence_basis"):
            blockers.append("recommendations_sidecar_tracker_occurrence_mismatch")
        else:
            warnings.append("legacy_recommendations_sidecar_tracker_occurrence_mismatch")

    frontend_references: dict[str, Any] = {}
    frontend_dir = settings.project_root / "frontend"
    try:
        shared_frontend_source = (frontend_dir / "shared.js").read_text(encoding="utf-8")
    except OSError:
        shared_frontend_source = ""
        warnings.append("frontend_source_missing:shared.js")
    for filename, fields in FRONTEND_FIELD_PAGE_REFERENCES.items():
        path = frontend_dir / filename
        try:
            page_source = path.read_text(encoding="utf-8")
        except OSError:
            warnings.append(f"frontend_source_missing:{filename}")
            frontend_references[filename] = {"exists": False, "field_references": {}}
            continue
        source = page_source + "\n" + shared_frontend_source
        field_references = {field: field in source for field in fields}
        missing_references = [field for field, present in field_references.items() if not present]
        if missing_references:
            warnings.append(f"frontend_field_references_missing:{filename}:{','.join(missing_references)}")
        frontend_references[filename] = {
            "exists": True,
            "reference_sources": [filename, "shared.js"],
            "field_references": field_references,
        }

    field_lineage: dict[str, Any] = {}
    for field in FRONTEND_FIELD_LINEAGE_FIELDS:
        if field == "平台成立以来收益年化(%)":
            source_type = "run_local_tracker_current_derived_fields"
            source_detail = "derived_fields.annualized_return_by_code"
            available = (derived_fields.get("schema_version") == 2
                         and isinstance(derived_fields.get("annualized_return_by_code"), dict))
            available_count = len(derived_fields["annualized_return_by_code"]) if available else 0
        elif field in {"成立以来年度收益明细"}:
            source_type = "run_local_tracker_current_derived_fields"
            source_detail = "derived_fields.annual_return_breakdown_by_code"
            available = isinstance(derived_fields.get("annual_return_breakdown_by_code"), dict)
            available_count = len(derived_fields.get("annual_return_breakdown_by_code") or {})
        elif field == "数据来源":
            source_type = "run_local_tracker_current_derived_fields"
            source_detail = "derived_fields.data_source_by_code"
            available = isinstance(derived_fields.get("data_source_by_code"), dict)
            available_count = len(derived_fields.get("data_source_by_code") or {})
        elif field in {"实时估值", "实时估值时间", "实时估算净值"}:
            source_type = "live_optional_provider_tracker_valuations_endpoint"
            source_detail = "/api/tracker/valuations?audit_read_only=1"
            available = True
            available_count = 0
        elif field in csv_fields:
            source_type = "canonical_filtered_csv"
            source_detail = "artifacts.filtered_csv"
            available = True
            available_count = filtered_row_count
        elif tracker_rows and field in {key for row in tracker_rows for key in row.keys()}:
            source_type = "project_tracker_current_detail_sidecar"
            source_detail = "artifacts.project_tracker_current_detail_json.rows"
            available = True
            available_count = sum(1 for row in tracker_rows if _clean_business_text(row.get(field)))
        else:
            source_type = "missing"
            source_detail = ""
            available = False
            available_count = 0
        field_lineage[field] = {
            "source_type": source_type,
            "source_detail": source_detail,
            "available": bool(available),
            "available_count": int(available_count),
            "missing_semantics": "preserve_empty_or_null; frontend must not synthesize zero/pass values",
        }

    missing_lineage_fields = [
        field for field, payload in field_lineage.items() if not payload.get("available")
    ]
    if missing_lineage_fields:
        blockers.append(f"frontend_field_lineage_missing:{','.join(missing_lineage_fields)}")

    live_field_endpoints: dict[str, Any] = {}
    if include_live_api:
        for name, path in protected_live_artifacts.items():
            live_probe_protected_artifacts[name]["before"] = _read_file_integrity_token(path)
        for endpoint_spec in LIVE_FIELD_ENDPOINTS:
            result = _sample_live_field_endpoint(
                base_url=live_base_url,
                timeout_seconds=timeout_seconds,
                endpoint_spec=endpoint_spec,
                sample_codes=sample_codes[:5],
            )
            endpoint_key = str(endpoint_spec.get("key") or endpoint_spec.get("endpoint"))
            live_field_endpoints[endpoint_key] = result
            if result.get("status_code") != 200:
                blockers.append(f"frontend_field_live_endpoint_non_200:{endpoint_key}")
                continue
            if result.get("run_id") and run_id and str(result.get("run_id")) != run_id:
                blockers.append(f"frontend_field_live_run_id_mismatch:{endpoint_key}")
            if endpoint_key == "recommendations_detail":
                observed = result.get("tracker_occurrence_payload") or {}
                occurrence_checks = {}
                for lane in ("rows", "funds", "portfolio_funds", "group_funds"):
                    occurrence_checks[lane] = _compare_tracker_occurrence_rows(
                        tracker_rows, observed.get(lane) or [], occurrence_total,
                    )
                    if occurrence_checks[lane]["aligned"] is False:
                        blockers.append(f"frontend_field_live_recommendations_{lane}_tracker_occurrence_mismatch")
                result["tracker_occurrence_checks"] = occurrence_checks
                if observed.get("total_periods") != occurrence_total:
                    blockers.append("frontend_field_live_recommendations_observation_total_mismatch")
                if any(len(observed.get(lane) or []) != len(recommendations_rows)
                       for lane in ("rows", "funds", "portfolio_funds", "group_funds")):
                    blockers.append("frontend_field_live_recommendations_occurrence_lane_count_mismatch")
            expected_status = str(endpoint_spec.get("expected_status") or "").strip()
            if expected_status and str(result.get("status") or "").strip() != expected_status:
                blockers.append(f"frontend_field_live_status_mismatch:{endpoint_key}")
            expected_diagnostics = (
                endpoint_spec.get("expected_diagnostics")
                if isinstance(endpoint_spec.get("expected_diagnostics"), dict)
                else {}
            )
            if expected_diagnostics:
                diagnostics = result.get("diagnostics") if isinstance(result.get("diagnostics"), dict) else {}
                if not diagnostics:
                    blockers.append(f"frontend_field_live_diagnostics_missing:{endpoint_key}")
                if "audit_read_only" in expected_diagnostics and diagnostics.get("audit_read_only") != expected_diagnostics.get("audit_read_only"):
                    blockers.append(f"frontend_field_live_audit_read_only_mismatch:{endpoint_key}")
                write_status_field = str(expected_diagnostics.get("write_status_field") or "").strip()
                if write_status_field:
                    write_status = str(diagnostics.get(write_status_field) or "").strip()
                    allowed_write_statuses = {
                        str(item)
                        for item in expected_diagnostics.get("allowed_write_statuses") or ()
                        if str(item).strip()
                    }
                    forbidden_write_statuses = {
                        str(item)
                        for item in expected_diagnostics.get("forbidden_write_statuses") or ()
                        if str(item).strip()
                    }
                    if not write_status:
                        blockers.append(f"frontend_field_live_write_status_missing:{endpoint_key}")
                    if allowed_write_statuses and write_status not in allowed_write_statuses:
                        blockers.append(f"frontend_field_live_write_status_unexpected:{endpoint_key}:{write_status}")
                    if forbidden_write_statuses and write_status in forbidden_write_statuses:
                        blockers.append(f"frontend_field_live_write_status_forbidden:{endpoint_key}:{write_status}")
            expected_payload_scope = str(endpoint_spec.get("expected_payload_scope") or "").strip()
            if expected_payload_scope and str(result.get("payload_scope") or "").strip() != expected_payload_scope:
                blockers.append(
                    f"frontend_field_live_payload_scope_mismatch:{endpoint_key}:"
                    f"{result.get('payload_scope') or '-'}"
                )
            if endpoint_key == "tracker_detail":
                source_basis = result.get("source_basis") if isinstance(result.get("source_basis"), dict) else {}
                cache_basis = result.get("cache_basis") if isinstance(result.get("cache_basis"), dict) else {}
                if not source_basis:
                    blockers.append("frontend_field_live_tracker_current_source_basis_missing")
                if source_basis and cache_basis and source_basis != cache_basis:
                    blockers.append("frontend_field_live_tracker_current_cache_source_basis_mismatch")
                tracker_current_basis_summary, tracker_current_basis_blockers = (
                    _summarize_frontend_cache_basis_contract(
                        source_basis or cache_basis,
                        artifact_key="frontend_field_live_tracker_current_source_basis",
                        required_keys=TRACKER_CURRENT_SOURCE_BASIS_REQUIRED_KEYS,
                        expected_run_id=run_id,
                    )
                )
                live_field_endpoints[endpoint_key]["source_basis_summary"] = tracker_current_basis_summary
                blockers.extend(tracker_current_basis_blockers)
                source_contract = (
                    result.get("source_contract")
                    if isinstance(result.get("source_contract"), dict)
                    else {}
                )
                tracker_current_contract_summary, tracker_current_contract_blockers = (
                    _summarize_live_endpoint_source_contract(
                        source_contract,
                        artifact_key="frontend_field_live_tracker_current",
                        expected=TRACKER_CURRENT_SOURCE_CONTRACT_EXPECTED,
                        source_basis=source_basis,
                    )
                )
                live_field_endpoints[endpoint_key]["source_contract_summary"] = tracker_current_contract_summary
                blockers.extend(tracker_current_contract_blockers)
                if source_basis:
                    if _clean_live_text(source_basis.get("source")) != "tracker_current_published_report_tracker_artifact":
                        blockers.append("frontend_field_live_tracker_current_source_basis_source_mismatch")
                    if str(source_basis.get("payload_mode") or "").strip() != "detail":
                        blockers.append("frontend_field_live_tracker_current_source_basis_payload_mode_mismatch")
                    if source_basis.get("audit_read_only") is not True:
                        blockers.append("frontend_field_live_tracker_current_source_basis_audit_read_only_mismatch")
                    if str(source_basis.get("missing_value_policy") or "").strip() != "preserve_missing_semantics":
                        blockers.append("frontend_field_live_tracker_current_missing_value_policy_mismatch")
                    if str(source_basis.get("business_value_synthesis") or "").strip() != "forbidden":
                        blockers.append("frontend_field_live_tracker_current_business_value_synthesis_mismatch")
                    if _clean_business_text(source_basis.get("latest_report_file")) in {"", "missing"}:
                        blockers.append("frontend_field_live_tracker_current_source_basis_latest_report_missing")
                    database_token = _clean_business_text(source_basis.get("database_file"))
                    if not database_token or database_token == "missing" or database_token.startswith("missing::"):
                        blockers.append("frontend_field_live_tracker_current_source_basis_database_missing")
                    detail_diagnostics = (
                        source_basis.get("detail_diagnostics")
                        if isinstance(source_basis.get("detail_diagnostics"), dict)
                        else {}
                    )
                    if detail_diagnostics.get("audit_read_only") is not True:
                        blockers.append("frontend_field_live_tracker_current_detail_diagnostics_audit_read_only_mismatch")
                    if str(detail_diagnostics.get("derived_sidecar_write_status") or "").strip() == "written":
                        blockers.append("frontend_field_live_tracker_current_detail_diagnostics_written")
                    if _safe_int(source_basis.get("row_count")) <= 0:
                        blockers.append("frontend_field_live_tracker_current_source_basis_rows_missing")
            if endpoint_key == "recommendations_detail":
                source_basis = result.get("source_basis") if isinstance(result.get("source_basis"), dict) else {}
                cache_basis = result.get("cache_basis") if isinstance(result.get("cache_basis"), dict) else {}
                if not source_basis:
                    blockers.append("frontend_field_live_recommendations_detail_source_basis_missing")
                live_recommendations_basis_summary, live_recommendations_basis_blockers = (
                    _summarize_recommendations_cache_basis_contract(
                        source_basis or cache_basis,
                        artifact_key="frontend_field_live_recommendations_detail_source_basis",
                        settings=settings, report=latest,
                    )
                )
                live_field_endpoints[endpoint_key]["source_basis_summary"] = live_recommendations_basis_summary
                blockers.extend(live_recommendations_basis_blockers)
                if source_basis and cache_basis and source_basis != cache_basis:
                    blockers.append("frontend_field_live_recommendations_detail_cache_source_basis_mismatch")
                publication_source = (
                    source_basis.get("publication_status_source")
                    if isinstance(source_basis.get("publication_status_source"), dict)
                    else {}
                )
                live_field_endpoints[endpoint_key]["publication_status_source"] = {
                    "present": bool(publication_source),
                    "source": publication_source.get("source") if publication_source else None,
                    "truth_policy": publication_source.get("truth_policy") if publication_source else None,
                    "publication_registry_file": publication_source.get("publication_registry_file")
                    if publication_source
                    else None,
                    "current_published_run_id": publication_source.get("current_published_run_id")
                    if publication_source
                    else None,
                    "publication_status": publication_source.get("publication_status")
                    if publication_source
                    else None,
                }
                if not publication_source:
                    blockers.append("frontend_field_live_recommendations_detail_publication_status_source_missing")
                elif str(publication_source.get("source") or "").strip() != "publication_registry_runtime_overlay":
                    blockers.append("frontend_field_live_recommendations_detail_publication_status_source_mismatch")
                elif str(publication_source.get("truth_policy") or "").strip() != "real_publication_registry_artifact_only":
                    blockers.append("frontend_field_live_recommendations_detail_publication_status_truth_policy_mismatch")
                elif not str(publication_source.get("publication_registry_file") or "").strip():
                    blockers.append("frontend_field_live_recommendations_detail_publication_registry_token_missing")
                elif str(publication_source.get("current_published_run_id") or "").strip() != run_id:
                    blockers.append("frontend_field_live_recommendations_detail_publication_run_id_mismatch")
                source_contract = (
                    result.get("source_contract")
                    if isinstance(result.get("source_contract"), dict)
                    else {}
                )
                live_recommendations_contract_summary, live_recommendations_contract_blockers = (
                    _summarize_recommendations_detail_source_contract(
                        source_contract,
                        artifact_key="frontend_field_live_recommendations_detail",
                    )
                )
                live_field_endpoints[endpoint_key]["source_contract_summary"] = live_recommendations_contract_summary
                blockers.extend(live_recommendations_contract_blockers)
                payload_scope_source_contract = (
                    result.get("payload_scope_source_contract")
                    if isinstance(result.get("payload_scope_source_contract"), dict)
                    else {}
                )
                live_field_endpoints[endpoint_key]["payload_scope_source_contract"] = {
                    "present": bool(payload_scope_source_contract),
                    "source": payload_scope_source_contract.get("source")
                    if payload_scope_source_contract
                    else None,
                }
                if not payload_scope_source_contract:
                    blockers.append("frontend_field_live_recommendations_detail_payload_scope_source_contract_missing")
                elif str(payload_scope_source_contract.get("source") or "").strip() != "run_local_recommendations_detail":
                    blockers.append("frontend_field_live_recommendations_detail_payload_scope_source_contract_source_mismatch")
                elif payload_scope_source_contract.get("cache_basis_present") is not True:
                    blockers.append("frontend_field_live_recommendations_detail_payload_scope_source_contract_cache_basis_missing")
                elif payload_scope_source_contract.get("source_basis_matches_cache_basis") is not True:
                    blockers.append("frontend_field_live_recommendations_detail_payload_scope_source_contract_basis_mismatch")
                elif str(payload_scope_source_contract.get("truth_policy") or "").strip() != "real_api_database_runtime_artifact_and_publication_registry_only":
                    blockers.append("frontend_field_live_recommendations_detail_payload_scope_source_contract_truth_policy_mismatch")
                elif str(payload_scope_source_contract.get("missing_value_policy") or "").strip() != "preserve_missing_semantics":
                    blockers.append("frontend_field_live_recommendations_detail_payload_scope_source_contract_missing_value_policy_mismatch")
                elif str(payload_scope_source_contract.get("publication_status_source") or "").strip() != "publication_registry_runtime_overlay":
                    blockers.append("frontend_field_live_recommendations_detail_payload_scope_publication_source_mismatch")
            if endpoint_key == "recommendations_summary":
                source_basis = result.get("source_basis") if isinstance(result.get("source_basis"), dict) else {}
                cache_basis = result.get("cache_basis") if isinstance(result.get("cache_basis"), dict) else {}
                if not source_basis:
                    blockers.append("frontend_field_live_recommendations_summary_source_basis_missing")
                summary_basis_summary, summary_basis_blockers = _summarize_frontend_cache_basis_contract(
                    source_basis or cache_basis,
                    artifact_key="frontend_field_live_recommendations_summary_source_basis",
                    required_keys=RECOMMENDATIONS_SUMMARY_SOURCE_BASIS_REQUIRED_KEYS,
                    expected_run_id=run_id,
                )
                live_field_endpoints[endpoint_key]["source_basis_summary"] = summary_basis_summary
                blockers.extend(summary_basis_blockers)
                if source_basis and cache_basis and source_basis != cache_basis:
                    blockers.append("frontend_field_live_recommendations_summary_cache_source_basis_mismatch")
                source_contract = (
                    result.get("source_contract")
                    if isinstance(result.get("source_contract"), dict)
                    else {}
                )
                summary_contract_summary, summary_contract_blockers = (
                    _summarize_live_endpoint_source_contract(
                        source_contract,
                        artifact_key="frontend_field_live_recommendations_summary",
                        expected=RECOMMENDATIONS_SUMMARY_SOURCE_CONTRACT_EXPECTED,
                        source_basis=source_basis,
                    )
                )
                live_field_endpoints[endpoint_key]["source_contract_summary"] = summary_contract_summary
                blockers.extend(summary_contract_blockers)
                payload_scope_source_contract = (
                    result.get("payload_scope_source_contract")
                    if isinstance(result.get("payload_scope_source_contract"), dict)
                    else {}
                )
                live_field_endpoints[endpoint_key]["payload_scope_source_contract"] = {
                    "present": bool(payload_scope_source_contract),
                    "source": payload_scope_source_contract.get("source")
                    if payload_scope_source_contract
                    else None,
                }
                if not payload_scope_source_contract:
                    blockers.append("frontend_field_live_recommendations_summary_payload_scope_source_contract_missing")
                elif str(payload_scope_source_contract.get("source") or "").strip() != "recommendations_summary_current_published_report_artifact":
                    blockers.append("frontend_field_live_recommendations_summary_payload_scope_source_contract_source_mismatch")
                elif payload_scope_source_contract.get("cache_basis_present") is not True:
                    blockers.append("frontend_field_live_recommendations_summary_payload_scope_source_contract_cache_basis_missing")
                elif payload_scope_source_contract.get("source_basis_matches_cache_basis") is not True:
                    blockers.append("frontend_field_live_recommendations_summary_payload_scope_source_contract_basis_mismatch")
                elif str(payload_scope_source_contract.get("truth_policy") or "").strip() != "real_publication_registry_and_report_artifact_summary_only":
                    blockers.append("frontend_field_live_recommendations_summary_payload_scope_source_contract_truth_policy_mismatch")
                elif str(payload_scope_source_contract.get("missing_value_policy") or "").strip() != "preserve_missing_semantics":
                    blockers.append("frontend_field_live_recommendations_summary_payload_scope_source_contract_missing_value_policy_mismatch")
                if str(result.get("payload_scope_mode") or "").strip() != "summary":
                    blockers.append("frontend_field_live_recommendations_summary_payload_mode_mismatch")
                if bool(result.get("top_level_rows_present")):
                    blockers.append("frontend_field_live_recommendations_summary_rows_present")
                if bool(result.get("top_level_funds_present")):
                    blockers.append("frontend_field_live_recommendations_summary_funds_present")
                if bool(result.get("portfolio_group_funds_present")):
                    blockers.append("frontend_field_live_recommendations_summary_group_funds_present")
                if source_basis:
                    if _clean_live_text(source_basis.get("source")) != "recommendations_summary_current_published_report_artifact":
                        blockers.append("frontend_field_live_recommendations_summary_source_basis_source_mismatch")
                    if str(source_basis.get("payload_mode") or "").strip() != "summary":
                        blockers.append("frontend_field_live_recommendations_summary_source_basis_payload_mode_mismatch")
                    if source_basis.get("summary_rows_omitted") is not True:
                        blockers.append("frontend_field_live_recommendations_summary_source_basis_rows_omitted_mismatch")
                    if source_basis.get("portfolio_group_funds_omitted") is not True:
                        blockers.append("frontend_field_live_recommendations_summary_source_basis_group_funds_omitted_mismatch")
                    if str(source_basis.get("missing_value_policy") or "").strip() != "preserve_missing_semantics":
                        blockers.append("frontend_field_live_recommendations_summary_missing_value_policy_mismatch")
                    if str(source_basis.get("business_value_synthesis") or "").strip() != "forbidden":
                        blockers.append("frontend_field_live_recommendations_summary_business_value_synthesis_mismatch")
                    if _clean_business_text(source_basis.get("latest_report_file")) in {"", "missing"}:
                        blockers.append("frontend_field_live_recommendations_summary_source_basis_latest_report_missing")
                    if _clean_business_text(source_basis.get("publication_registry_file")) in {"", "missing"}:
                        blockers.append("frontend_field_live_recommendations_summary_source_basis_publication_registry_missing")
            if "expected_detail_included" in endpoint_spec:
                expected_detail_included = bool(endpoint_spec.get("expected_detail_included"))
                if bool(result.get("detail_included")) != expected_detail_included:
                    blockers.append(f"frontend_field_live_detail_mode_mismatch:{endpoint_key}")
            expected_omitted_detail_fields = {
                str(field)
                for field in endpoint_spec.get("expected_omitted_detail_fields") or ()
                if str(field).strip()
            }
            if expected_omitted_detail_fields:
                omitted_detail_fields = {
                    str(field)
                    for field in result.get("omitted_detail_fields") or ()
                    if str(field).strip()
                }
                missing_omitted_fields = sorted(expected_omitted_detail_fields - omitted_detail_fields)
                if missing_omitted_fields:
                    blockers.append(
                        f"frontend_field_live_omitted_detail_fields_missing:{endpoint_key}:"
                        + ",".join(missing_omitted_fields)
                    )
            if endpoint_key == "favorites_score":
                request_payload = result.get("request_payload") if isinstance(result.get("request_payload"), dict) else {}
                if request_payload.get("save_history") is not False:
                    blockers.append("frontend_field_live_favorites_score_has_history_side_effect")
                if _safe_int(result.get("row_count")) <= 0:
                    blockers.append("frontend_field_live_favorites_score_rows_missing")
                funds_count = result.get("funds_count")
                if funds_count is not None and _safe_int(funds_count) != _safe_int(result.get("row_count")):
                    blockers.append("frontend_field_live_favorites_score_rows_funds_count_mismatch")
                source_basis = result.get("source_basis") if isinstance(result.get("source_basis"), dict) else {}
                cache_basis = result.get("cache_basis") if isinstance(result.get("cache_basis"), dict) else {}
                if not source_basis:
                    blockers.append("frontend_field_live_favorites_score_source_basis_missing")
                favorites_basis_summary, favorites_basis_blockers = _summarize_frontend_cache_basis_contract(
                    source_basis or cache_basis,
                    artifact_key="frontend_field_live_favorites_score_source_basis",
                    required_keys=FAVORITES_SCORE_SOURCE_BASIS_REQUIRED_KEYS,
                    expected_run_id=run_id,
                )
                live_field_endpoints[endpoint_key]["source_basis_summary"] = favorites_basis_summary
                blockers.extend(favorites_basis_blockers)
                if source_basis and cache_basis and source_basis != cache_basis:
                    blockers.append("frontend_field_live_favorites_score_cache_source_basis_mismatch")
                source_contract = (
                    result.get("source_contract")
                    if isinstance(result.get("source_contract"), dict)
                    else {}
                )
                favorites_contract_summary, favorites_contract_blockers = (
                    _summarize_live_endpoint_source_contract(
                        source_contract,
                        artifact_key="frontend_field_live_favorites_score",
                        expected=FAVORITES_SCORE_SOURCE_CONTRACT_EXPECTED,
                        source_basis=source_basis,
                    )
                )
                live_field_endpoints[endpoint_key]["source_contract_summary"] = favorites_contract_summary
                blockers.extend(favorites_contract_blockers)
                if source_basis:
                    if _clean_live_text(source_basis.get("source")) != "favorite_fund_score_runtime_query":
                        blockers.append("frontend_field_live_favorites_score_source_basis_source_mismatch")
                    if str(source_basis.get("payload_mode") or "").strip() != "compact":
                        blockers.append("frontend_field_live_favorites_score_source_basis_payload_mode_mismatch")
                    if source_basis.get("save_history") is not False:
                        blockers.append("frontend_field_live_favorites_score_source_basis_save_history_mismatch")
                    requested_codes = [
                        _normalize_fund_code(item)
                        for item in list(source_basis.get("requested_codes") or [])
                        if _normalize_fund_code(item)
                    ]
                    expected_codes = [
                        _normalize_fund_code(item)
                        for item in list(request_payload.get("codes") or [])
                        if _normalize_fund_code(item)
                    ]
                    if expected_codes and requested_codes != expected_codes:
                        blockers.append("frontend_field_live_favorites_score_source_basis_requested_codes_mismatch")
                    if _clean_business_text(source_basis.get("latest_report_file")) in {"", "missing"}:
                        blockers.append("frontend_field_live_favorites_score_source_basis_latest_report_missing")
                    database_token = _clean_business_text(source_basis.get("database_file"))
                    if not database_token or database_token == "missing" or database_token.startswith("missing::"):
                        blockers.append("frontend_field_live_favorites_score_source_basis_database_missing")
            if endpoint_key == "tracker_valuations":
                source_basis = result.get("source_basis") if isinstance(result.get("source_basis"), dict) else {}
                cache_basis = result.get("cache_basis") if isinstance(result.get("cache_basis"), dict) else {}
                if not source_basis:
                    blockers.append("frontend_field_live_tracker_valuations_source_basis_missing")
                if source_basis and cache_basis and source_basis != cache_basis:
                    blockers.append("frontend_field_live_tracker_valuations_cache_source_basis_mismatch")
                source_contract = (
                    result.get("source_contract")
                    if isinstance(result.get("source_contract"), dict)
                    else {}
                )
                tracker_contract_summary, tracker_contract_blockers = (
                    _summarize_live_endpoint_source_contract(
                        source_contract,
                        artifact_key="frontend_field_live_tracker_valuations",
                        expected=TRACKER_VALUATIONS_SOURCE_CONTRACT_EXPECTED,
                        source_basis=source_basis,
                    )
                )
                live_field_endpoints[endpoint_key]["source_contract_summary"] = tracker_contract_summary
                blockers.extend(tracker_contract_blockers)
                missing_required_keys = [
                    key
                    for key in TRACKER_VALUATIONS_SOURCE_BASIS_REQUIRED_KEYS
                    if key not in source_basis
                ]
                live_field_endpoints[endpoint_key]["source_basis_summary"] = {
                    "present": bool(source_basis),
                    "schema_version": source_basis.get("schema_version") if source_basis else None,
                    "required_keys": list(TRACKER_VALUATIONS_SOURCE_BASIS_REQUIRED_KEYS),
                    "missing_required_keys": missing_required_keys,
                    "source": source_basis.get("source") if source_basis else None,
                    "cache_scope": source_basis.get("cache_scope") if source_basis else None,
                }
                if missing_required_keys:
                    blockers.append(
                        "frontend_field_live_tracker_valuations_source_basis_required_keys_missing:"
                        + ",".join(missing_required_keys)
                    )
                expected_codes = [
                    _normalize_fund_code(item)
                    for item in result.get("sample_codes") or []
                    if _normalize_fund_code(item)
                ]
                requested_codes = [
                    _normalize_fund_code(item)
                    for item in list(source_basis.get("requested_codes") or [])
                    if _normalize_fund_code(item)
                ]
                if expected_codes and requested_codes != expected_codes:
                    blockers.append("frontend_field_live_tracker_valuations_source_basis_requested_codes_mismatch")
                if str(source_basis.get("missing_value_policy") or "").strip() != "preserve_missing_semantics":
                    blockers.append("frontend_field_live_tracker_valuations_missing_value_policy_mismatch")
                if str(source_basis.get("cache_scope") or "").strip() != "process_memory_only":
                    blockers.append("frontend_field_live_tracker_valuations_cache_scope_mismatch")
                if str(source_basis.get("source") or "").strip() != "tiantian_fund_valuation_last_or_short_lived_memory_cache":
                    blockers.append("frontend_field_live_tracker_valuations_source_mismatch")
                expected_detail_fields = {
                    str(field)
                    for field in endpoint_spec.get("expected_detail_fields") or ()
                    if str(field).strip()
                }
                basis_detail_fields = {
                    str(field)
                    for field in source_basis.get("display_fields") or ()
                    if str(field).strip()
                }
                response_detail_fields = {
                    str(field)
                    for field in result.get("detail_fields") or ()
                    if str(field).strip()
                }
                if sorted(expected_detail_fields - basis_detail_fields):
                    blockers.append(
                        "frontend_field_live_tracker_valuations_source_basis_detail_fields_missing:"
                        + ",".join(sorted(expected_detail_fields - basis_detail_fields))
                    )
                if sorted(expected_detail_fields - response_detail_fields):
                    blockers.append(
                        "frontend_field_live_tracker_valuations_response_detail_fields_missing:"
                        + ",".join(sorted(expected_detail_fields - response_detail_fields))
                    )
                if _safe_int(result.get("row_count")) <= 0:
                    blockers.append("frontend_field_live_tracker_valuations_rows_missing")
                if result.get("upstream_requested_count") is None:
                    blockers.append("frontend_field_live_tracker_valuations_upstream_requested_count_missing")
                if _safe_int(result.get("upstream_requested_count")) != 0:
                    blockers.append("frontend_field_live_tracker_valuations_audit_called_upstream")
            if endpoint_key == "favorite_metric_details":
                request_payload = result.get("request_payload") if isinstance(result.get("request_payload"), dict) else {}
                source_basis = result.get("source_basis") if isinstance(result.get("source_basis"), dict) else {}
                cache_basis = result.get("cache_basis") if isinstance(result.get("cache_basis"), dict) else {}
                if not source_basis:
                    blockers.append("frontend_field_live_favorite_metric_details_source_basis_missing")
                metric_basis_summary, metric_basis_blockers = _summarize_frontend_cache_basis_contract(
                    source_basis or cache_basis,
                    artifact_key="frontend_field_live_favorite_metric_details_source_basis",
                    required_keys=FAVORITE_METRIC_DETAILS_SOURCE_BASIS_REQUIRED_KEYS,
                    expected_run_id="",
                )
                live_field_endpoints[endpoint_key]["source_basis_summary"] = metric_basis_summary
                blockers.extend(metric_basis_blockers)
                if source_basis and cache_basis and source_basis != cache_basis:
                    blockers.append("frontend_field_live_favorite_metric_details_cache_source_basis_mismatch")
                source_contract = (
                    result.get("source_contract")
                    if isinstance(result.get("source_contract"), dict)
                    else {}
                )
                metric_contract_summary, metric_contract_blockers = (
                    _summarize_live_endpoint_source_contract(
                        source_contract,
                        artifact_key="frontend_field_live_favorite_metric_details",
                        expected=FAVORITE_METRIC_DETAILS_SOURCE_CONTRACT_EXPECTED,
                        source_basis=source_basis,
                    )
                )
                live_field_endpoints[endpoint_key]["source_contract_summary"] = metric_contract_summary
                blockers.extend(metric_contract_blockers)
                if source_basis:
                    if _clean_live_text(source_basis.get("source")) != "favorite_metric_details_runtime_query":
                        blockers.append("frontend_field_live_favorite_metric_details_source_basis_source_mismatch")
                    requested_codes = [
                        _normalize_fund_code(item)
                        for item in list(source_basis.get("requested_codes") or [])
                        if _normalize_fund_code(item)
                    ]
                    expected_codes = [
                        _normalize_fund_code(item)
                        for item in list(request_payload.get("codes") or [])
                        if _normalize_fund_code(item)
                    ]
                    if expected_codes and requested_codes != expected_codes:
                        blockers.append("frontend_field_live_favorite_metric_details_source_basis_requested_codes_mismatch")
                    database_token = _clean_business_text(source_basis.get("database_file"))
                    if not database_token or database_token == "missing" or database_token.startswith("missing::"):
                        blockers.append("frontend_field_live_favorite_metric_details_source_basis_database_missing")
                    if str(source_basis.get("missing_value_policy") or "").strip() != "preserve_missing_semantics":
                        blockers.append("frontend_field_live_favorite_metric_details_missing_value_policy_mismatch")
                    expected_detail_fields = {
                        str(field)
                        for field in endpoint_spec.get("expected_detail_fields") or ()
                        if str(field).strip()
                    }
                    basis_detail_fields = {
                        str(field)
                        for field in source_basis.get("detail_fields") or ()
                        if str(field).strip()
                    }
                    response_detail_fields = {
                        str(field)
                        for field in result.get("detail_fields") or ()
                        if str(field).strip()
                    }
                    missing_basis_fields = sorted(expected_detail_fields - basis_detail_fields)
                    missing_response_fields = sorted(expected_detail_fields - response_detail_fields)
                    if missing_basis_fields:
                        blockers.append(
                            "frontend_field_live_favorite_metric_details_source_basis_detail_fields_missing:"
                            + ",".join(missing_basis_fields)
                        )
                    if missing_response_fields:
                        blockers.append(
                            "frontend_field_live_favorite_metric_details_response_detail_fields_missing:"
                            + ",".join(missing_response_fields)
                        )
            field_presence = result.get("field_presence") or {}
            missing_payload_fields = [
                field
                for field, payload in (field_presence.get("fields") or {}).items()
                if not payload.get("present")
            ]
            if missing_payload_fields:
                blockers.append(
                    f"frontend_field_live_payload_fields_missing:{endpoint_key}:{','.join(missing_payload_fields)}"
                )
        detail_observations = (live_field_endpoints.get("recommendations_detail") or {}).get("tracker_occurrence_payload") or {}
        for key in ("recommendations_detail", "recommendations_summary"):
            endpoint_result = live_field_endpoints.get(key) or {}
            observed = endpoint_result.get("tracker_occurrence_payload") or {}
            brief_check = _compare_recommendation_reason_briefs(
                detail_observations.get("groups") or [], observed.get("group_briefs") or {},
            )
            endpoint_result["reason_brief_check"] = brief_check
            if brief_check["aligned"] is False:
                blockers.append(f"frontend_field_live_{key}_reason_brief_mismatch")
        for name, path in protected_live_artifacts.items():
            after_token = _read_file_integrity_token(path)
            artifact_payload = live_probe_protected_artifacts[name]
            before_token = artifact_payload.get("before") if isinstance(artifact_payload.get("before"), dict) else {}
            artifact_payload["after"] = after_token
            artifact_payload["unchanged"] = before_token.get("token") == after_token.get("token")
            if artifact_payload["unchanged"] is not True:
                blockers.append(f"frontend_field_live_protected_artifact_changed:{name}")

    return {
        "status": _classify_status(blockers, warnings),
        "blockers": blockers,
        "warnings": warnings,
        "run_id": run_id or None,
        "filtered_csv": {
            "path": str(filtered_csv_path) if filtered_csv_path else "",
            "exists": bool(filtered_csv_path and filtered_csv_path.exists()),
            "row_count": filtered_row_count,
            "sample_codes": sample_codes,
            "required_fields": list(FILTERED_CSV_REQUIRED_FIELDS),
            "missing_required_fields": [field for field in FILTERED_CSV_REQUIRED_FIELDS if field not in csv_fields],
        },
        "tracker_current_detail_sidecar": {
            "path": str(tracker_detail_path) if tracker_detail_path else "",
            "exists": bool(tracker_detail_path and tracker_detail_path.exists()),
            "artifact_type": tracker_payload.get("artifact_type"),
            "row_count": len(tracker_rows),
            "derived_maps": {
                key: {
                    "available": isinstance(derived_fields.get(key), dict),
                    "code_count": len(derived_fields.get(key) or {}) if isinstance(derived_fields.get(key), dict) else 0,
                }
                for key in TRACKER_DERIVED_REQUIRED_MAPS
            },
            "cache_basis": tracker_cache_basis_summary,
        },
        "recommendations_detail_sidecar": {
            "path": str(recommendations_detail_path) if recommendations_detail_path else "",
            "exists": bool(recommendations_detail_path and recommendations_detail_path.exists()),
            "artifact_declared_in_latest_report": recommendations_artifact_declared,
            "path_source": recommendations_detail_path_source,
            "artifact_type": recommendations_payload.get("artifact_type"),
            "schema_version": recommendations_payload.get("schema_version"),
            "run_id": recommendations_payload.get("run_id"),
            "response_run_id": recommendations_response.get("run_id"),
            "row_count": len(recommendations_rows),
            "portfolio_fund_count": len(recommendations_funds),
            "tracker_occurrence_check": stored_occurrence_check,
            "required_fields": list(RECOMMENDATIONS_SIDECAR_REQUIRED_FIELDS),
            "field_presence": recommendations_field_presence,
            "cache_basis": recommendations_cache_basis_summary,
            "response_cache_basis": recommendations_response_cache_basis_summary,
            "response_source_basis": recommendations_response_source_basis_summary,
            "response_source_contract": recommendations_response_source_contract_summary,
            "response_payload_scope_source_contract": recommendations_response_payload_scope_source_contract,
        },
        "field_lineage": field_lineage,
        "frontend_references": frontend_references,
        "live_field_endpoints": live_field_endpoints,
        "live_probe_protected_artifacts": live_probe_protected_artifacts,
        "truth_note": (
            "Frontend business fields are traced to canonical run artifacts or run-local derived fields. "
            "Compact endpoints may intentionally omit heavy derived fields; detail endpoints must expose them from real artifacts/calculations."
        ),
    }


def _iter_source_files(settings: Settings) -> list[Path]:
    files: list[Path] = []
    for dirname in SOURCE_SCAN_DIRS:
        root = settings.project_root / dirname
        if not root.exists():
            continue
        for path in root.rglob("*"):
            if not path.is_file() or path.suffix not in SOURCE_SCAN_SUFFIXES:
                continue
            rel_parts = path.relative_to(settings.project_root).parts
            if any(part in SOURCE_SCAN_EXCLUDED_DIRS for part in rel_parts):
                continue
            files.append(path)
    return sorted(files)


def _source_scan_context_category(stripped: str, rel_path: str) -> str:
    lowered = stripped.lower()
    rel_lowered = rel_path.lower()

    if "mocked_" in lowered and ("loader" in lowered or "_load_" in lowered):
        return "allowed_test_loader_naming"
    if "unittest.mock" in lowered:
        return "allowed_test_double_reference"
    if "magicmock" in lowered:
        return "allowed_test_double_reference"
    if "isinstance(" in lowered and "mock" in lowered:
        return "allowed_test_double_reference"

    if any(hint in stripped for hint in SOURCE_SCAN_ALLOWED_HINTS):
        return "allowed_policy_or_test_hint"

    if "holding_period_generic_placeholder_status" in lowered or "通用占位持有期" in stripped:
        return "allowed_domain_holding_placeholder"
    if (
        "generic_holding_placeholder" in lowered
        or "generic_holding_period_placeholder" in lowered
        or "generic_placeholder_holding" in lowered
        or "official_placeholder" in lowered
    ):
        return "allowed_domain_holding_placeholder"
    if "placeholders" in lowered and "?" in stripped:
        return "allowed_sql_placeholder"
    if "placeholders" in lowered and (" in ({" in lowered or "values {" in lowered or "period in ({" in lowered):
        return "allowed_sql_placeholder"
    if "operator_placeholder" in lowered or "operator_placeholders" in lowered:
        return "allowed_scheduler_operator_placeholder"
    if rel_lowered.startswith("frontend/") and "placeholder" in lowered:
        return "allowed_ui_placeholder"
    if rel_lowered.startswith("core/collectors/") and "placeholder" in lowered:
        return "allowed_upstream_missing_placeholder_parser"
    if "placeholder" in lowered and (
        "text_values" in lowered
        or "placeholder_values" in lowered
        or "benchmarks" in lowered
        or "text_placeholder" in lowered
    ):
        return "allowed_text_placeholder_parser"
    if "模拟人工操作" in stripped:
        return "allowed_operator_simulation_text"
    if "shadow" in lowered or "影子" in stripped or "不影响实际筛选" in stripped:
        return "allowed_shadow_audit"
    if "does not fabricate" in lowered or "not fabricate" in lowered or "no fabricated" in lowered:
        return "allowed_truth_policy_statement"
    if "fake_data_policy" in lowered and "forbidden" in lowered:
        return "allowed_truth_policy_statement"
    if "not converted to fake" in lowered or "not convert" in lowered and "fake" in lowered:
        return "allowed_truth_policy_statement"
    if "不伪造" in stripped or "避免伪造" in stripped or "不会写入伪造" in stripped or "未写入任何回退或伪造" in stripped:
        return "allowed_truth_policy_statement"
    if "不在列表链路直接伪造" in stripped or "伪造 fallback" in stripped:
        return "allowed_truth_policy_statement"
    if rel_lowered in {"scripts/audit_data_truthfulness.py", "core/version.py"}:
        return "allowed_truth_audit_implementation"
    if rel_lowered.endswith("bond_mainlist_audit.py") and "模拟" in stripped:
        return "allowed_audit_simulation"
    if re.search(r"\b(?:sample|samples|sampled|sampling|sample_limit)\b", lowered):
        return "allowed_statistical_sample"
    return "suspicious_production_keyword"


def _is_source_scan_allowed_context(stripped: str, rel_path: str) -> bool:
    return _source_scan_context_category(stripped, rel_path).startswith("allowed_")


def _is_runtime_test_double_reference(stripped: str, rel_path: str) -> bool:
    rel_lowered = rel_path.lower()
    if (
        rel_lowered.startswith("tests/")
        or rel_lowered.startswith("core/docs/")
        or rel_lowered == "scripts/audit_data_truthfulness.py"
    ):
        return False
    lowered = stripped.lower()
    return (
        "unittest.mock" in lowered
        or "isinstance(" in lowered and "mock" in lowered
        or "magicmock" in lowered
    )


def _source_scan_non_business_lines(source: str) -> dict[int, str]:
    """Review non-executing text and sentinel sets used only in if-tests."""
    reviewed = {i: "allowed_non_executable_comment" for i, line in enumerate(source.splitlines(), 1)
                if line.lstrip().startswith("#")}
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return reviewed
    parents = {child: node for node in ast.walk(tree) for child in ast.iter_child_nodes(node)}
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)) and node.body:
            first = node.body[0]
            if isinstance(first, ast.Expr) and isinstance(first.value, ast.Constant) and isinstance(first.value.value, str):
                reviewed.update({line: "allowed_non_executable_docstring"
                                 for line in range(first.lineno, first.end_lineno + 1)})
    assignments = [node for node in ast.walk(tree) if isinstance(node, ast.Assign)
                   and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name)
                   and node.targets[0].id == "placeholders"]
    if len(assignments) != 1:
        return reviewed
    assignment = assignments[0]
    if not (isinstance(assignment.value, ast.Set) and assignment.value.elts
            and all(isinstance(value, ast.Constant) and isinstance(value.value, str)
                    and value.value in {"", "-", "--", "None", "none", "NaN", "nan", "null", "N/A"}
                    for value in assignment.value.elts)):
        return reviewed
    uses = [node for node in ast.walk(tree) if isinstance(node, ast.Name) and node.id == "placeholders"
            and isinstance(node.ctx, ast.Load)]
    for use in uses:
        comparison = parents.get(use)
        if not (isinstance(comparison, ast.Compare) and len(comparison.ops) == 1
                and isinstance(comparison.ops[0], (ast.In, ast.NotIn)) and use in comparison.comparators):
            return reviewed
        ancestor = comparison
        while isinstance(parents.get(ancestor), (ast.BoolOp, ast.UnaryOp)):
            ancestor = parents[ancestor]
        parent = parents.get(ancestor)
        if not isinstance(parent, ast.If) or parent.test is not ancestor:
            return reviewed
    if uses:
        reviewed[assignment.lineno] = "allowed_missing_sentinel_guard"
        reviewed.update({use.lineno: "allowed_missing_sentinel_guard" for use in uses})
    return reviewed


def _source_scan(settings: Settings, *, max_matches: int = 200) -> dict[str, Any]:
    keyword_pattern = re.compile("|".join(re.escape(item) for item in SOURCE_SCAN_KEYWORDS), re.IGNORECASE)
    source_files = _iter_source_files(settings)
    matches: list[dict[str, Any]] = []
    suspicious_matches: list[dict[str, Any]] = []
    runtime_test_double_matches: list[dict[str, Any]] = []
    category_counts: dict[str, int] = {}
    total_match_count = 0
    suspicious_count = 0
    reviewed_allowed_count = 0
    runtime_test_double_count = 0
    for path in source_files:
        rel_path = str(path.relative_to(settings.project_root))
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
        except (OSError, UnicodeDecodeError):
            continue
        reviewed_lines = _source_scan_non_business_lines("\n".join(lines)) if path.suffix == ".py" else {}
        for line_no, line in enumerate(lines, start=1):
            if not keyword_pattern.search(line):
                continue
            total_match_count += 1
            stripped = line.strip()
            category = reviewed_lines.get(line_no) or _source_scan_context_category(stripped, rel_path)
            allowed = category.startswith("allowed_")
            category_counts[category] = category_counts.get(category, 0) + 1
            record = {
                "path": rel_path,
                "line": line_no,
                "allowed_context": allowed,
                "context_category": category,
                "text": stripped[:260],
            }
            if _is_runtime_test_double_reference(stripped, rel_path):
                runtime_test_double_count += 1
                if len(runtime_test_double_matches) < max_matches:
                    runtime_test_double_matches.append(record)
            if allowed:
                reviewed_allowed_count += 1
            else:
                suspicious_count += 1
                if len(suspicious_matches) < max_matches:
                    suspicious_matches.append(record)
            if len(matches) < max_matches:
                matches.append(record)
    warnings: list[str] = []
    blockers: list[str] = []
    if runtime_test_double_count:
        blockers.append("runtime_source_references_test_double_api")
    if suspicious_count:
        blockers.append("source_truthfulness_suspicious_production_keywords")
    return {
        "status": _classify_status(blockers, warnings),
        "blockers": blockers,
        "warnings": warnings,
        "scan_scope": {
            "dirs": list(SOURCE_SCAN_DIRS),
            "suffixes": sorted(SOURCE_SCAN_SUFFIXES),
            "excluded_dir_names": list(SOURCE_SCAN_EXCLUDED_DIRS_SORTED),
            "runtime_data_dirs_excluded": all(
                name in SOURCE_SCAN_EXCLUDED_DIRS
                for name in ("data", "reports", "logs", "__pycache__", ".pytest_cache")
            ),
        },
        "scanned_file_count": len(source_files),
        "match_count_seen": total_match_count,
        "match_count_sampled": len(matches),
        "context_category_counts": dict(sorted(category_counts.items())),
        "allowed_context_match_count_seen": reviewed_allowed_count,
        "suspicious_match_count_seen": suspicious_count,
        "suspicious_sample_count": len(suspicious_matches),
        "runtime_test_double_match_count_seen": runtime_test_double_count,
        "runtime_test_double_sample_count": len(runtime_test_double_matches),
        "matches": matches,
        "suspicious_matches": suspicious_matches,
        "runtime_test_double_matches": runtime_test_double_matches,
        "truth_note": (
            "Keyword scanning is heuristic evidence only; shadow simulations and tests may legitimately mention these words "
            "but must remain outside publication truth. Runtime source must not import or branch on test double APIs."
        ),
    }


def _fetch_live_json(base_url: str, endpoint: str, timeout_seconds: float) -> tuple[int | None, dict[str, Any], str | None, float]:
    url = base_url.rstrip("/") + endpoint
    started = time.perf_counter()
    try:
        with urllib.request.urlopen(url, timeout=timeout_seconds) as response:
            status_code = int(response.status)
            body = response.read()
    except urllib.error.HTTPError as exc:
        elapsed = round(time.perf_counter() - started, 6)
        try:
            payload = json.loads(exc.read())
        except Exception:
            payload = {}
        return int(exc.code), payload if isinstance(payload, dict) else {}, str(exc), elapsed
    except Exception as exc:  # noqa: BLE001
        elapsed = round(time.perf_counter() - started, 6)
        return None, {}, str(exc), elapsed
    elapsed = round(time.perf_counter() - started, 6)
    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        payload = {}
    return status_code, payload if isinstance(payload, dict) else {}, None, elapsed


def _fetch_live_json_post(
    base_url: str,
    endpoint: str,
    payload: dict[str, Any],
    timeout_seconds: float,
) -> tuple[int | None, dict[str, Any], str | None, float]:
    url = base_url.rstrip("/") + endpoint
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    started = time.perf_counter()
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            status_code = int(response.status)
            raw_body = response.read()
    except urllib.error.HTTPError as exc:
        elapsed = round(time.perf_counter() - started, 6)
        try:
            error_payload = json.loads(exc.read())
        except Exception:
            error_payload = {}
        return int(exc.code), error_payload if isinstance(error_payload, dict) else {}, str(exc), elapsed
    except Exception as exc:  # noqa: BLE001
        elapsed = round(time.perf_counter() - started, 6)
        return None, {}, str(exc), elapsed
    elapsed = round(time.perf_counter() - started, 6)
    try:
        response_payload = json.loads(raw_body)
    except json.JSONDecodeError:
        response_payload = {}
    return status_code, response_payload if isinstance(response_payload, dict) else {}, None, elapsed


def _fetch_live_head(
    base_url: str,
    endpoint: str,
    timeout_seconds: float,
) -> tuple[int | None, dict[str, str], str | None, float]:
    url = base_url.rstrip("/") + endpoint
    request = urllib.request.Request(url, method="HEAD")
    started = time.perf_counter()
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            status_code = int(response.status)
            headers = {str(key): str(value) for key, value in response.headers.items()}
    except urllib.error.HTTPError as exc:
        elapsed = round(time.perf_counter() - started, 6)
        return int(exc.code), {str(key): str(value) for key, value in exc.headers.items()}, str(exc), elapsed
    except Exception as exc:  # noqa: BLE001
        elapsed = round(time.perf_counter() - started, 6)
        return None, {}, str(exc), elapsed
    elapsed = round(time.perf_counter() - started, 6)
    return status_code, headers, None, elapsed


def _sample_live_field_endpoint(
    *,
    base_url: str,
    timeout_seconds: float,
    endpoint_spec: dict[str, Any],
    sample_codes: list[str],
) -> dict[str, Any]:
    method = str(endpoint_spec.get("method") or "GET").upper()
    endpoint = str(endpoint_spec.get("endpoint") or "")
    request_payload: dict[str, Any] = {}
    if method == "POST":
        request_payload = {
            key: value
            for key, value in (endpoint_spec.get("post_payload") or {}).items()
            if isinstance(key, str)
        }
        request_payload["codes"] = list(sample_codes)
        status_code, payload, error, elapsed = _fetch_live_json_post(
            base_url,
            endpoint,
            request_payload,
            timeout_seconds,
        )
    else:
        if endpoint_spec.get("use_sample_codes_query") and sample_codes:
            separator = "&" if "?" in endpoint else "?"
            endpoint = (
                f"{endpoint}{separator}"
                + urllib.parse.urlencode({"codes": ",".join(sample_codes)})
            )
        status_code, payload, error, elapsed = _fetch_live_json(
            base_url,
            endpoint,
            timeout_seconds,
        )
    rows = _rows_from_path(payload, endpoint_spec.get("row_path") or ("rows",))
    required_fields = tuple(endpoint_spec.get("required_fields") or ())
    omitted_detail_fields = [
        str(field)
        for field in payload.get("omitted_detail_fields") or []
        if str(field).strip()
    ]
    diagnostics: dict[str, Any] = {}
    expected_diagnostics = endpoint_spec.get("expected_diagnostics")
    if isinstance(expected_diagnostics, dict):
        current: Any = payload
        for part in expected_diagnostics.get("path") or ():
            if not isinstance(current, dict):
                current = {}
                break
            current = current.get(part)
        diagnostics = dict(current) if isinstance(current, dict) else {}
    payload_scope = payload.get("payload_scope") or payload.get("payload_mode")
    payload_scope_source_contract = {}
    if isinstance(payload_scope, dict) and isinstance(payload_scope.get("source_contract"), dict):
        payload_scope_source_contract = dict(payload_scope.get("source_contract") or {})
    portfolio_suggestions = (
        payload.get("portfolio_suggestions")
        if isinstance(payload.get("portfolio_suggestions"), dict)
        else {}
    )
    portfolio_groups = (
        portfolio_suggestions.get("groups")
        if isinstance(portfolio_suggestions.get("groups"), list)
        else []
    )
    occurrence_payload = {}
    if endpoint_spec.get("key") in {"recommendations_detail", "recommendations_summary"}:
        def project_rows(values: Any) -> list[dict[str, Any]]:
            return [{field: row.get(field) for field in ("基金代码", "出现次数", "出现频率(%)", "入选原因")}
                    for row in values if isinstance(row, dict)] if isinstance(values, list) else []
        occurrence_payload = {
            "total_periods": (payload.get("tracker_summary") or {}).get("total_periods"),
            "rows": project_rows(payload.get("rows")),
            "funds": project_rows(payload.get("funds")),
            "portfolio_funds": project_rows(portfolio_suggestions.get("funds")),
            "group_funds": [row for group in portfolio_groups if isinstance(group, dict)
                            for row in project_rows(group.get("funds"))],
            "groups": [{"name": group.get("name"), "funds": project_rows(group.get("funds"))}
                       for group in portfolio_groups if isinstance(group, dict)],
            "group_briefs": payload.get("portfolio_group_brief") or {},
        }
    return {
        "key": endpoint_spec.get("key"),
        "method": method,
        "endpoint": endpoint,
        "status_code": status_code,
        "elapsed_seconds": elapsed,
        "error": error,
        "request_payload": request_payload if method == "POST" else {},
        "sample_codes": (
            list(sample_codes)
            if method == "POST" or endpoint_spec.get("use_sample_codes_query")
            else []
        ),
        "run_id": payload.get("run_id"),
        "status": payload.get("status"),
        "payload_scope": payload_scope,
        "payload_scope_mode": payload_scope.get("mode") if isinstance(payload_scope, dict) else payload_scope,
        "detail_included": payload.get("detail_included"),
        "omitted_detail_fields": omitted_detail_fields,
        "top_level_rows_present": isinstance(payload.get("rows"), list),
        "top_level_funds_present": isinstance(payload.get("funds"), list),
        "portfolio_group_funds_present": any(
            isinstance(group, dict) and isinstance(group.get("funds"), list)
            for group in portfolio_groups
        ),
        "portfolio_suggestions_group_count": portfolio_suggestions.get("group_count"),
        "portfolio_suggestions_fund_count": portfolio_suggestions.get("fund_count"),
        "row_count": len(rows),
        "funds_count": len(payload.get("funds") or []) if isinstance(payload.get("funds"), list) else None,
        "details_count": len(payload.get("details") or {}) if isinstance(payload.get("details"), dict) else None,
        "upstream_requested_count": payload.get("upstream_requested_count"),
        "detail_fields": [
            str(field)
            for field in payload.get("detail_fields") or []
            if str(field).strip()
        ],
        "diagnostics": diagnostics,
        "source_basis": payload.get("source_basis") if isinstance(payload.get("source_basis"), dict) else {},
        "cache_basis": payload.get("cache_basis") if isinstance(payload.get("cache_basis"), dict) else {},
        "source_contract": payload.get("source_contract") if isinstance(payload.get("source_contract"), dict) else {},
        "payload_scope_source_contract": payload_scope_source_contract,
        "tracker_occurrence_payload": occurrence_payload,
        "field_presence": _summarize_field_presence(rows, required_fields),
    }


def _clean_live_text(value: Any) -> str:
    return str(value or "").strip()


def _publication_status_allows_candidate_run(
    summary: dict[str, Any] | None,
    *,
    expected_run_id: str,
    candidate_run_id: str,
) -> bool:
    if not isinstance(summary, dict):
        return False
    expected_text = _clean_live_text(expected_run_id)
    candidate_text = _clean_live_text(candidate_run_id)
    if not expected_text or not candidate_text or candidate_text == expected_text:
        return False

    latest_successful_run_id = _clean_live_text(summary.get("latest_successful_run_id"))
    if latest_successful_run_id != candidate_text:
        return False
    if summary.get("latest_successful_is_currently_published") is not False:
        return False
    if _clean_live_text(summary.get("current_published_run_id")) not in {"", expected_text}:
        return False
    if _clean_live_text(summary.get("latest_canonical_run_id")) not in {"", expected_text}:
        return False
    publication_status = _clean_live_text(summary.get("status"))
    publish_blocked = summary.get("publish_blocked") is True
    return publication_status == "publication_blocked" and publish_blocked


def _live_expected_candidate_run_ids(
    expected_publication: dict[str, Any],
    *,
    payload: dict[str, Any] | None = None,
) -> set[str]:
    expected_run_id = _clean_live_text(expected_publication.get("latest_report_run_id"))
    candidates: set[str] = set()
    status_summaries: list[dict[str, Any]] = []
    publication_status = expected_publication.get("publication_status")
    if isinstance(publication_status, dict):
        status_summaries.append(publication_status)
    if isinstance(payload, dict):
        top_summary = payload.get("publication_status_summary")
        if isinstance(top_summary, dict):
            status_summaries.append(top_summary)
        nested_summary = _nested_publication_status_summary(payload)
        if nested_summary:
            status_summaries.append(nested_summary)
        publication_summary = payload.get("publication_summary")
        if isinstance(publication_summary, dict):
            nested_status = publication_summary.get("status_summary")
            if isinstance(nested_status, dict):
                status_summaries.append(nested_status)

    for summary in status_summaries:
        candidate_run_id = _clean_live_text(summary.get("latest_successful_run_id"))
        if _publication_status_allows_candidate_run(
            summary,
            expected_run_id=expected_run_id,
            candidate_run_id=candidate_run_id,
        ):
            candidates.add(candidate_run_id)
    return candidates


def _live_lightweight_runtime_run_ids(
    payload: dict[str, Any],
    *,
    expected_source: str,
) -> set[str]:
    if expected_source != "workflow_status_summary_runtime_manager" or not isinstance(payload, dict):
        return set()

    source_basis = payload.get("source_basis") if isinstance(payload.get("source_basis"), dict) else {}
    source_contract = payload.get("source_contract") if isinstance(payload.get("source_contract"), dict) else {}
    payload_run_id = _clean_live_text(payload.get("run_id"))
    basis_run_id = _clean_live_text(source_basis.get("run_id"))
    if not payload_run_id or payload_run_id != basis_run_id:
        return set()
    if _clean_live_text(source_basis.get("source")) != expected_source:
        return set()
    if _clean_live_text(source_contract.get("source")) != expected_source:
        return set()
    if _clean_live_text(source_contract.get("official_publication_truth")) != "false_workflow_runtime_diagnostics_only":
        return set()
    if source_contract.get("frontend_rendered_truth_allowed") is not False:
        return set()
    return {payload_run_id}


def _is_allowed_live_candidate_run(
    expected_publication: dict[str, Any],
    candidate_run_id: Any,
    *,
    payload: dict[str, Any] | None = None,
) -> bool:
    candidate_text = _clean_live_text(candidate_run_id)
    return bool(candidate_text and candidate_text in _live_expected_candidate_run_ids(
        expected_publication,
        payload=payload,
    ))


def _expected_runtime_run_record(
    expected_publication: dict[str, Any],
    candidate_run_id: Any,
) -> dict[str, Any]:
    candidate_text = _clean_live_text(candidate_run_id)
    runtime_runs = expected_publication.get("runtime_diagnostic_runs")
    runtime_runs = runtime_runs if isinstance(runtime_runs, dict) else {}
    record = runtime_runs.get(candidate_text)
    return record if isinstance(record, dict) else {}


def _is_known_runtime_diagnostic_run(
    expected_publication: dict[str, Any],
    candidate_run_id: Any,
) -> bool:
    return bool(_expected_runtime_run_record(expected_publication, candidate_run_id))


def _expected_runtime_run_final_count(
    expected_publication: dict[str, Any],
    candidate_run_id: Any,
) -> int | None:
    return _safe_optional_int(_expected_runtime_run_record(expected_publication, candidate_run_id).get("final_count"))


def _nested_publication_status_summary(payload: dict[str, Any]) -> dict[str, Any]:
    publication_summary = payload.get("publication_summary")
    if not isinstance(publication_summary, dict):
        return {}
    status_summary = publication_summary.get("status_summary")
    return dict(status_summary) if isinstance(status_summary, dict) else {}


def _validate_live_publication_status(
    endpoint_key: str,
    payload: dict[str, Any],
    *,
    expected_publication: dict[str, Any],
    require_top_level: bool,
) -> tuple[dict[str, Any], list[str]]:
    blockers: list[str] = []
    top_summary = payload.get("publication_status_summary")
    top_summary = dict(top_summary) if isinstance(top_summary, dict) else {}
    nested_summary = _nested_publication_status_summary(payload)
    summary = top_summary or nested_summary
    publication_summary = payload.get("publication_summary")
    publication_summary = publication_summary if isinstance(publication_summary, dict) else {}

    if require_top_level and not top_summary:
        blockers.append(f"{endpoint_key}_publication_status_summary_missing")
    if not summary:
        blockers.append(f"{endpoint_key}_publication_status_missing")
        return {"available": False}, blockers

    required_non_empty_fields = (
        "run_id",
        "status",
        "status_label",
        "recommended_action",
        "replay_precision_status",
        "auxiliary_scope",
        "latest_canonical_run_id",
        "current_published_run_id",
        "latest_successful_run_id",
    )
    for field in required_non_empty_fields:
        if not _clean_live_text(summary.get(field)):
            blockers.append(f"{endpoint_key}_publication_status_{field}_missing")

    expected_latest_run_id = _clean_live_text(expected_publication.get("latest_report_run_id"))
    expected_latest_canonical_run_id = _clean_live_text(expected_publication.get("latest_canonical_run_id"))
    expected_current_published_run_id = _clean_live_text(expected_publication.get("current_published_run_id"))
    expected_final_count = _safe_optional_int((expected_publication.get("counts") or {}).get("final_count"))
    status_run_id = _clean_live_text(summary.get("run_id"))
    latest_canonical_run_id = _clean_live_text(summary.get("latest_canonical_run_id"))
    current_published_run_id = _clean_live_text(summary.get("current_published_run_id"))
    latest_successful_run_id = _clean_live_text(summary.get("latest_successful_run_id"))
    publication_status = _clean_live_text(summary.get("status"))
    publish_blocked = summary.get("publish_blocked") is True
    latest_successful_is_published = summary.get("latest_successful_is_currently_published")
    latest_successful_not_current_by_gate = bool(
        latest_successful_is_published is False
        and latest_successful_run_id
        and expected_latest_run_id
        and latest_successful_run_id != expected_latest_run_id
        and publication_status == "publication_blocked"
        and publish_blocked
    )

    if (
        expected_latest_run_id
        and status_run_id
        and status_run_id != expected_latest_run_id
        and not (
            latest_successful_not_current_by_gate
            and status_run_id == latest_successful_run_id
        )
    ):
        blockers.append(f"{endpoint_key}_publication_status_run_id_mismatch")
    if expected_latest_canonical_run_id and latest_canonical_run_id and latest_canonical_run_id != expected_latest_canonical_run_id:
        blockers.append(f"{endpoint_key}_publication_status_latest_canonical_run_id_mismatch")
    if expected_current_published_run_id and current_published_run_id and current_published_run_id != expected_current_published_run_id:
        blockers.append(f"{endpoint_key}_publication_status_current_published_run_id_mismatch")
    if (
        expected_latest_run_id
        and latest_successful_run_id
        and latest_successful_run_id != expected_latest_run_id
        and not latest_successful_not_current_by_gate
    ):
        blockers.append(f"{endpoint_key}_publication_status_latest_successful_run_id_mismatch")

    if latest_successful_is_published is True:
        if publication_status not in {"success_published", "success_published_with_backlog"}:
            blockers.append(f"{endpoint_key}_publication_status_success_state_mismatch")
        if summary.get("publish_hold") is True:
            blockers.append(f"{endpoint_key}_publication_status_success_publish_hold_true")
        if publish_blocked:
            blockers.append(f"{endpoint_key}_publication_status_success_publish_blocked_true")
    if publication_status in {"success_published", "success_published_with_backlog"} and (summary.get("publish_hold") is True or publish_blocked):
        blockers.append(f"{endpoint_key}_publication_status_success_with_hold_or_block")
    if publication_status == "publication_blocked" and not publish_blocked:
        blockers.append(f"{endpoint_key}_publication_status_blocked_flag_missing")
    if publication_status == "publication_hold" or summary.get("publish_hold") is True:
        blockers.append(f"{endpoint_key}_publication_status_hold_state_forbidden")

    if expected_final_count is not None and publication_summary:
        live_final_count = _safe_optional_int(publication_summary.get("final_count"))
        if live_final_count is None:
            blockers.append(f"{endpoint_key}_publication_summary_final_count_missing")
        elif live_final_count != expected_final_count:
            blockers.append(f"{endpoint_key}_publication_summary_final_count_mismatch")

    if top_summary and nested_summary:
        for field in (
            "run_id",
            "status",
            "latest_canonical_run_id",
            "current_published_run_id",
            "latest_successful_run_id",
            "publish_hold",
            "publish_blocked",
        ):
            if top_summary.get(field) != nested_summary.get(field):
                blockers.append(f"{endpoint_key}_publication_status_nested_{field}_mismatch")

    return {
        "available": True,
        "run_id": status_run_id or None,
        "status": publication_status or None,
        "status_label": _clean_live_text(summary.get("status_label")) or None,
        "latest_canonical_run_id": latest_canonical_run_id or None,
        "current_published_run_id": current_published_run_id or None,
        "latest_successful_run_id": latest_successful_run_id or None,
        "latest_successful_is_currently_published": latest_successful_is_published,
        "publish_hold": summary.get("publish_hold"),
        "publish_blocked": summary.get("publish_blocked"),
        "recommended_action": _clean_live_text(summary.get("recommended_action")) or None,
        "replay_precision_status": _clean_live_text(summary.get("replay_precision_status")) or None,
        "auxiliary_scope": _clean_live_text(summary.get("auxiliary_scope")) or None,
        "publication_summary_final_count": _safe_optional_int(publication_summary.get("final_count")),
    }, blockers


def _daily_screening_absence_explained(payload: dict[str, Any]) -> bool:
    """Accept NULL for a bound terminal skip or failure, never invent completion."""
    automation = payload.get("automation_autonomy") or {}
    if not isinstance(automation, dict):
        return False
    lineage = automation.get("automation_lineage") or {}
    basis = payload.get("source_basis") or payload.get("cache_basis") or {}
    if not isinstance(lineage, dict) or not isinstance(basis, dict):
        return False
    if any(container.get("daily_screening_run_id") for container in (payload, automation, lineage, basis)):
        return False
    candidates = [container["daily_screening_evidence"] for container in (lineage, basis)
                  if isinstance(container.get("daily_screening_evidence"), dict)]
    if isinstance(payload.get("daily_run"), dict):
        candidates.append(daily_screening_evidence(payload["daily_run"]))
    return bool(candidates and all(
        evidence == candidates[0] and (
            is_busy_skip_without_screening(evidence) or is_failed_without_completed_screening(evidence))
        for evidence in candidates
    ))


def _validate_live_scheduler_status(
    payload: dict[str, Any],
    *,
    expected_publication: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], list[str]]:
    blockers: list[str] = []
    expected = expected_publication or {}
    expected_latest_canonical_run_id = _clean_live_text(expected.get("latest_canonical_run_id"))
    expected_current_published_run_id = _clean_live_text(expected.get("current_published_run_id"))
    expected_latest_run_id = _clean_live_text(expected.get("latest_report_run_id"))
    automation = payload.get("automation_autonomy")
    automation = dict(automation) if isinstance(automation, dict) else {}
    automation_lineage = automation.get("automation_lineage")
    automation_lineage = dict(automation_lineage) if isinstance(automation_lineage, dict) else {}
    daily_run = payload.get("daily_run")
    daily_run = dict(daily_run) if isinstance(daily_run, dict) else {}
    current_task = payload.get("current_task")
    current_task = dict(current_task) if isinstance(current_task, dict) else {}
    weekly_run = payload.get("weekly_run")
    weekly_run = dict(weekly_run) if isinstance(weekly_run, dict) else {}
    weekly_degradation = weekly_run.get("degradation_summary")
    weekly_degradation = dict(weekly_degradation) if isinstance(weekly_degradation, dict) else {}
    warning_issue_codes = automation.get("warning_issue_codes")
    warning_issue_codes = warning_issue_codes if isinstance(warning_issue_codes, list) else []
    warning_issue_details = automation.get("warning_issue_details")
    warning_issue_details = warning_issue_details if isinstance(warning_issue_details, dict) else {}
    resolved_warning_issue_codes = automation.get("resolved_warning_issue_codes")
    resolved_warning_issue_codes = (
        resolved_warning_issue_codes if isinstance(resolved_warning_issue_codes, list) else []
    )
    resolved_warning_issue_details = automation.get("resolved_warning_issue_details")
    resolved_warning_issue_details = (
        resolved_warning_issue_details if isinstance(resolved_warning_issue_details, dict) else {}
    )

    if not automation:
        blockers.append("scheduler_automation_autonomy_missing")
        return {"available": False}, blockers

    for field in ("status", "auxiliary_scope"):
        if not _clean_live_text(automation.get(field)):
            blockers.append(f"scheduler_automation_{field}_missing")
    for field in ("manual_intervention_required", "scheduler_running"):
        if field not in automation:
            blockers.append(f"scheduler_automation_{field}_missing")

    if automation.get("scheduler_running") is not True:
        blockers.append("scheduler_automation_scheduler_not_running")
    if automation.get("manual_intervention_required") is True:
        blockers.append("scheduler_automation_manual_intervention_required")
    if not automation_lineage:
        blockers.append("scheduler_automation_lineage_missing")
    elif automation_lineage.get("truth_policy") != "real_publication_registry_and_scheduler_state_only":
        blockers.append("scheduler_automation_lineage_truth_policy_mismatch")

    automation_latest_canonical_run_id = _clean_live_text(automation.get("latest_canonical_run_id"))
    automation_current_published_run_id = _clean_live_text(automation.get("current_published_run_id"))
    automation_daily_screening_run_id = _clean_live_text(automation.get("daily_screening_run_id"))
    lineage_latest_canonical_run_id = _clean_live_text(automation_lineage.get("latest_canonical_run_id"))
    lineage_current_published_run_id = _clean_live_text(automation_lineage.get("current_published_run_id"))
    lineage_daily_screening_run_id = _clean_live_text(automation_lineage.get("daily_screening_run_id"))
    daily_run_screening_run_id = _clean_live_text(daily_run.get("screening_run_id"))

    if not automation_latest_canonical_run_id:
        blockers.append("scheduler_automation_latest_canonical_run_id_missing")
    if not automation_current_published_run_id:
        blockers.append("scheduler_automation_current_published_run_id_missing")
    if not automation_daily_screening_run_id and not _daily_screening_absence_explained(payload):
        blockers.append("scheduler_automation_daily_screening_run_id_missing")
    if automation_latest_canonical_run_id and lineage_latest_canonical_run_id and automation_latest_canonical_run_id != lineage_latest_canonical_run_id:
        blockers.append("scheduler_automation_lineage_latest_canonical_run_id_mismatch")
    if automation_current_published_run_id and lineage_current_published_run_id and automation_current_published_run_id != lineage_current_published_run_id:
        blockers.append("scheduler_automation_lineage_current_published_run_id_mismatch")
    if automation_daily_screening_run_id and lineage_daily_screening_run_id and automation_daily_screening_run_id != lineage_daily_screening_run_id:
        blockers.append("scheduler_automation_lineage_daily_screening_run_id_mismatch")
    if expected_latest_canonical_run_id and automation_latest_canonical_run_id and automation_latest_canonical_run_id != expected_latest_canonical_run_id:
        blockers.append("scheduler_automation_latest_canonical_run_id_mismatch")
    if expected_current_published_run_id and automation_current_published_run_id and automation_current_published_run_id != expected_current_published_run_id:
        blockers.append("scheduler_automation_current_published_run_id_mismatch")
    if (
        expected_latest_run_id
        and automation_daily_screening_run_id
        and automation_daily_screening_run_id != expected_latest_run_id
        and not _is_allowed_live_candidate_run(
            expected,
            automation_daily_screening_run_id,
            payload=payload,
        )
        and not _is_known_runtime_diagnostic_run(expected, automation_daily_screening_run_id)
    ):
        blockers.append("scheduler_automation_daily_screening_run_id_mismatch")
    if daily_run_screening_run_id and automation_daily_screening_run_id and daily_run_screening_run_id != automation_daily_screening_run_id:
        blockers.append("scheduler_automation_daily_run_screening_run_id_mismatch")

    header_status = _clean_live_text(payload.get("automation_status")) or _clean_live_text(
        payload.get("automation_mode")
    )
    automation_status = _clean_live_text(automation.get("status"))
    if automation_status and not header_status:
        blockers.append("scheduler_header_automation_status_missing")
    if automation_status and header_status and header_status != automation_status:
        blockers.append("scheduler_header_automation_status_mismatch")
    if "manual_intervention_required" not in payload:
        blockers.append("scheduler_header_manual_intervention_required_missing")
    elif payload.get("manual_intervention_required") != automation.get("manual_intervention_required"):
        blockers.append("scheduler_header_manual_intervention_required_mismatch")
    if "scheduler_running" not in payload:
        blockers.append("scheduler_header_scheduler_running_missing")
    elif payload.get("scheduler_running") != automation.get("scheduler_running"):
        blockers.append("scheduler_header_scheduler_running_mismatch")

    if current_task:
        for field in ("task_name", "started_at"):
            if not _clean_live_text(current_task.get(field)):
                blockers.append(f"scheduler_current_task_{field}_missing")
        automation_task_name = _clean_live_text(automation.get("current_task_name"))
        current_task_name = _clean_live_text(current_task.get("task_name"))
        if automation_task_name and current_task_name and automation_task_name != current_task_name:
            blockers.append("scheduler_current_task_name_mismatch")
        header_task_name = _clean_live_text(payload.get("current_task_name"))
        header_task_started_at = _clean_live_text(payload.get("current_task_started_at"))
        if current_task_name and not header_task_name:
            blockers.append("scheduler_header_current_task_name_missing")
        if current_task_name and header_task_name and header_task_name != current_task_name:
            blockers.append("scheduler_header_current_task_name_mismatch")
        if _clean_live_text(current_task.get("started_at")) and not header_task_started_at:
            blockers.append("scheduler_header_current_task_started_at_missing")
        if (
            _clean_live_text(current_task.get("started_at"))
            and header_task_started_at
            and header_task_started_at != _clean_live_text(current_task.get("started_at"))
        ):
            blockers.append("scheduler_header_current_task_started_at_mismatch")
        if automation_task_name and header_task_name and header_task_name != automation_task_name:
            blockers.append("scheduler_header_automation_current_task_name_mismatch")

    same_task_weekly_recovery = bool(
        automation.get("automatic_recovering") is True
        and automation.get("manual_intervention_required") is False
        and _clean_live_text(automation.get("current_task_name")) == "scheduled_weekly_bond_maintenance"
        and _clean_live_text(current_task.get("task_name")) == "scheduled_weekly_bond_maintenance"
    )
    if same_task_weekly_recovery:
        if weekly_run and weekly_run.get("current_recovery_in_progress") is not True:
            blockers.append("scheduler_weekly_current_recovery_flag_missing")
        if weekly_degradation and weekly_degradation.get("current_recovery_in_progress") is not True:
            blockers.append("scheduler_weekly_degradation_current_recovery_flag_missing")
        if weekly_degradation.get("manual_intervention_required") is True:
            blockers.append("scheduler_weekly_degradation_manual_intervention_conflicts_with_recovery")
        if weekly_degradation.get("blocking_current_publication") is True:
            blockers.append("scheduler_weekly_degradation_publication_block_conflicts_with_recovery")

    weekly_warning_present = "weekly_maintenance_degraded" in warning_issue_codes
    weekly_resolved_warning_present = "weekly_maintenance_degraded" in resolved_warning_issue_codes
    if weekly_warning_present or weekly_resolved_warning_present:
        detail = warning_issue_details.get("weekly_maintenance_degraded")
        if not isinstance(detail, dict) or not detail:
            detail = resolved_warning_issue_details.get("weekly_maintenance_degraded")
        detail = detail if isinstance(detail, dict) else {}
        latest_evidence = detail.get("latest_resolution_evidence")
        latest_evidence = latest_evidence if isinstance(latest_evidence, dict) else {}
        if not detail:
            blockers.append("scheduler_weekly_warning_resolution_detail_missing")
        if detail.get("root_cause_status") == UNRESOLVED_STAGE_STATUS:
            blockers.extend(
                f"scheduler_weekly_warning_{error}" for error in unresolved_stage_detail_errors(detail)
            )
            if weekly_resolved_warning_present:
                blockers.append("scheduler_weekly_warning_unresolved_stage_in_resolved_signals")
        else:
            root_cause_resolved = bool(detail.get("root_cause_resolved"))
            root_cause_status = _clean_live_text(detail.get("root_cause_status"))
            if not root_cause_resolved:
                if root_cause_status not in {
                    "resolved_awaiting_next_weekly_maintenance",
                    "unresolved_or_resolution_evidence_missing",
                }:
                    blockers.append("scheduler_weekly_warning_root_cause_status_mismatch")
                if root_cause_status == "unresolved_or_resolution_evidence_missing":
                    if not _clean_live_text(latest_evidence.get("artifact_path")):
                        blockers.append("scheduler_weekly_warning_resolution_artifact_missing")
                    if latest_evidence.get("blocker_count") not in (0, "0"):
                        blockers.append("scheduler_weekly_warning_resolution_blocker_count_nonzero")
                    if latest_evidence.get("stale_hot_missing_archive_key_row_count") not in (0, "0"):
                        blockers.append("scheduler_weekly_warning_stale_hot_missing_archive_key_nonzero")
                else:
                    blockers.append("scheduler_weekly_warning_root_cause_not_resolved")
            elif root_cause_status != "resolved_awaiting_next_weekly_maintenance":
                blockers.append("scheduler_weekly_warning_root_cause_status_mismatch")
            if _clean_live_text(detail.get("truth_policy")) != "read_only_runtime_artifact_evidence_no_state_mutation":
                blockers.append("scheduler_weekly_warning_truth_policy_mismatch")
            if not _clean_live_text(latest_evidence.get("artifact_path")):
                blockers.append("scheduler_weekly_warning_resolution_artifact_missing")
            if latest_evidence.get("blocker_count") not in (0, "0"):
                blockers.append("scheduler_weekly_warning_resolution_blocker_count_nonzero")
            if latest_evidence.get("stale_hot_missing_archive_key_row_count") not in (0, "0"):
                blockers.append("scheduler_weekly_warning_stale_hot_missing_archive_key_nonzero")

    return {
        "available": True,
        "status": _clean_live_text(automation.get("status")) or None,
        "manual_intervention_required": automation.get("manual_intervention_required"),
        "scheduler_running": automation.get("scheduler_running"),
        "latest_canonical_run_id": automation_latest_canonical_run_id or None,
        "current_published_run_id": automation_current_published_run_id or None,
        "daily_screening_run_id": automation_daily_screening_run_id or None,
        "automation_lineage": automation_lineage or None,
        "auxiliary_scope": _clean_live_text(automation.get("auxiliary_scope")) or None,
        "current_task_name": _clean_live_text(payload.get("current_task_name"))
        or _clean_live_text(automation.get("current_task_name"))
        or None,
        "current_task_started_at": _clean_live_text(payload.get("current_task_started_at")) or None,
        "current_task": current_task or None,
        "blocking_issue_codes": automation.get("blocking_issue_codes"),
        "warning_issue_codes": warning_issue_codes,
        "warning_issue_details": warning_issue_details,
        "resolved_warning_issue_codes": resolved_warning_issue_codes,
        "resolved_warning_issue_details": resolved_warning_issue_details,
        "weekly_run_current_recovery_in_progress": weekly_run.get("current_recovery_in_progress"),
        "weekly_degradation_manual_intervention_required": weekly_degradation.get("manual_intervention_required"),
    }, blockers


def _validate_live_scheduler_detail_source_contract(
    payload: dict[str, Any],
    *,
    expected_publication: dict[str, Any],
) -> tuple[dict[str, Any], list[str]]:
    blockers: list[str] = []
    expected_run_id = _clean_live_text(expected_publication.get("latest_report_run_id"))
    expected_latest_canonical_run_id = _clean_live_text(expected_publication.get("latest_canonical_run_id"))
    expected_current_published_run_id = _clean_live_text(expected_publication.get("current_published_run_id"))
    source_basis = payload.get("source_basis") if isinstance(payload.get("source_basis"), dict) else {}
    cache_basis = payload.get("cache_basis") if isinstance(payload.get("cache_basis"), dict) else {}
    source_contract = payload.get("source_contract") if isinstance(payload.get("source_contract"), dict) else {}

    basis_summary, basis_blockers = _summarize_frontend_cache_basis_contract(
        source_basis or cache_basis,
        artifact_key="scheduler_detail_source_basis",
        required_keys=SCHEDULER_DETAIL_SOURCE_BASIS_REQUIRED_KEYS,
        expected_run_id=expected_run_id,
        allowed_run_ids=_live_expected_candidate_run_ids(expected_publication, payload=payload),
        nullable_keys=("daily_screening_run_id",) if _daily_screening_absence_explained(payload) else (),
    )
    blockers.extend(basis_blockers)
    if not source_basis:
        blockers.append("scheduler_detail_source_basis_missing")
    if source_basis and cache_basis and source_basis != cache_basis:
        blockers.append("scheduler_detail_source_basis_cache_basis_mismatch")

    if source_basis:
        if _clean_live_text(source_basis.get("source")) != "scheduler_status_detail_runtime_state":
            blockers.append("scheduler_detail_source_basis_source_mismatch")
        if _clean_live_text(source_basis.get("run_id")) != _clean_live_text(source_basis.get("latest_report_run_id")):
            blockers.append("scheduler_detail_source_basis_run_alias_mismatch")
        if expected_run_id and _clean_live_text(source_basis.get("latest_report_run_id")) != expected_run_id:
            blockers.append("scheduler_detail_source_basis_latest_report_run_id_mismatch")
        if expected_latest_canonical_run_id and _clean_live_text(source_basis.get("latest_canonical_run_id")) != expected_latest_canonical_run_id:
            blockers.append("scheduler_detail_source_basis_latest_canonical_run_id_mismatch")
        if expected_current_published_run_id and _clean_live_text(source_basis.get("current_published_run_id")) != expected_current_published_run_id:
            blockers.append("scheduler_detail_source_basis_current_published_run_id_mismatch")
        source_daily_screening_run_id = _clean_live_text(source_basis.get("daily_screening_run_id"))
        if (
            expected_run_id and source_daily_screening_run_id
            and source_daily_screening_run_id != expected_run_id
            and not _is_allowed_live_candidate_run(
                expected_publication,
                source_daily_screening_run_id,
                payload=payload,
            )
            and not _is_known_runtime_diagnostic_run(
                expected_publication,
                source_daily_screening_run_id,
            )
        ):
            blockers.append("scheduler_detail_source_basis_daily_screening_run_id_mismatch")
        if _clean_live_text(source_basis.get("automation_lineage_source")) != "publication_registry_and_scheduler_state":
            blockers.append("scheduler_detail_source_basis_automation_lineage_source_mismatch")
        if _clean_live_text(source_basis.get("automation_lineage_truth_policy")) != "real_publication_registry_and_scheduler_state_only":
            blockers.append("scheduler_detail_source_basis_automation_lineage_truth_policy_mismatch")
        for token_key in (
            "latest_report_file",
            "publication_registry_file",
            "scheduler_state_file",
            "database_file",
            "risk_coverage_gap_audit",
        ):
            token = _clean_live_text(source_basis.get(token_key))
            if not token or token in {"missing", "missing::empty_or_missing_wal"}:
                blockers.append(f"scheduler_detail_source_basis_{token_key}_missing")
        if source_basis.get("frontend_rendered_truth_allowed") is True:
            blockers.append("scheduler_detail_source_basis_frontend_truth_allowed")
        if not _clean_live_text(source_basis.get("digest")):
            blockers.append("scheduler_detail_source_basis_digest_missing")

    if not source_contract:
        blockers.append("scheduler_detail_source_contract_missing")
    else:
        for key, expected in SCHEDULER_DETAIL_SOURCE_CONTRACT_EXPECTED.items():
            if _clean_live_text(source_contract.get(key)) != expected:
                blockers.append(f"scheduler_detail_source_contract_{key}_mismatch")
        if source_contract.get("frontend_rendered_truth_allowed") is not False:
            blockers.append("scheduler_detail_source_contract_frontend_truth_allowed")
        if _safe_optional_int(source_contract.get("cache_basis_schema_version")) is None:
            blockers.append("scheduler_detail_source_contract_cache_schema_missing")
        if source_basis and _clean_live_text(source_contract.get("cache_basis_digest")) != _clean_live_text(source_basis.get("digest")):
            blockers.append("scheduler_detail_source_contract_digest_mismatch")

    return {
        "available": bool(source_basis and cache_basis and source_contract),
        "source_basis": basis_summary,
        "source": _clean_live_text(source_basis.get("source")) or None,
        "detail_scope": _clean_live_text(source_basis.get("detail_scope")) or None,
        "sample_limit": source_basis.get("sample_limit") if source_basis else None,
        "run_id": _clean_live_text(source_basis.get("run_id")) or None,
        "latest_report_run_id": _clean_live_text(source_basis.get("latest_report_run_id")) or None,
        "latest_canonical_run_id": _clean_live_text(source_basis.get("latest_canonical_run_id")) or None,
        "current_published_run_id": _clean_live_text(source_basis.get("current_published_run_id")) or None,
        "daily_screening_run_id": _clean_live_text(source_basis.get("daily_screening_run_id")) or None,
        "source_contract": {
            "present": bool(source_contract),
            "source": source_contract.get("source") if source_contract else None,
            "truth_policy": source_contract.get("truth_policy") if source_contract else None,
            "missing_value_policy": source_contract.get("missing_value_policy") if source_contract else None,
            "business_value_synthesis": source_contract.get("business_value_synthesis") if source_contract else None,
            "official_publication_truth": source_contract.get("official_publication_truth") if source_contract else None,
            "frontend_rendered_truth_allowed": source_contract.get("frontend_rendered_truth_allowed") if source_contract else None,
        },
    }, blockers


def _validate_live_dashboard_overview_source_contract(
    payload: dict[str, Any],
    *,
    expected_publication: dict[str, Any],
) -> tuple[dict[str, Any], list[str]]:
    blockers: list[str] = []
    expected_run_id = _clean_live_text(expected_publication.get("latest_report_run_id"))
    expected_latest_canonical_run_id = _clean_live_text(expected_publication.get("latest_canonical_run_id"))
    expected_current_published_run_id = _clean_live_text(expected_publication.get("current_published_run_id"))
    expected_final_count = _safe_optional_int((expected_publication.get("counts") or {}).get("final_count"))
    source_basis = payload.get("source_basis") if isinstance(payload.get("source_basis"), dict) else {}
    cache_basis = payload.get("cache_basis") if isinstance(payload.get("cache_basis"), dict) else {}
    source_contract = payload.get("source_contract") if isinstance(payload.get("source_contract"), dict) else {}

    basis_summary, basis_blockers = _summarize_frontend_cache_basis_contract(
        source_basis or cache_basis,
        artifact_key="dashboard_overview_source_basis",
        required_keys=DASHBOARD_OVERVIEW_SOURCE_BASIS_REQUIRED_KEYS,
        expected_run_id=expected_run_id,
    )
    blockers.extend(basis_blockers)
    if not source_basis:
        blockers.append("dashboard_overview_source_basis_missing")
    if not cache_basis and "dashboard_overview_source_basis_cache_basis_missing" not in blockers:
        blockers.append("dashboard_overview_source_basis_cache_basis_missing")
    if source_basis and cache_basis and source_basis != cache_basis:
        blockers.append("dashboard_overview_source_basis_cache_basis_mismatch")

    if source_basis:
        if _clean_live_text(source_basis.get("source")) != "dashboard_overview_current_published_report_scheduler_database_artifacts":
            blockers.append("dashboard_overview_source_basis_source_mismatch")
        if _clean_live_text(source_basis.get("payload_mode")) != "overview":
            blockers.append("dashboard_overview_source_basis_payload_mode_mismatch")
        if _clean_live_text(source_basis.get("detail_scope")) != "overview":
            blockers.append("dashboard_overview_source_basis_detail_scope_mismatch")
        if expected_run_id and _clean_live_text(source_basis.get("run_id")) != expected_run_id:
            blockers.append("dashboard_overview_source_basis_run_id_mismatch")
        if expected_latest_canonical_run_id and _clean_live_text(source_basis.get("latest_canonical_run_id")) != expected_latest_canonical_run_id:
            blockers.append("dashboard_overview_source_basis_latest_canonical_run_id_mismatch")
        if expected_current_published_run_id and _clean_live_text(source_basis.get("current_published_run_id")) != expected_current_published_run_id:
            blockers.append("dashboard_overview_source_basis_current_published_run_id_mismatch")
        if expected_final_count is not None and _safe_optional_int(source_basis.get("final_count")) != expected_final_count:
            blockers.append("dashboard_overview_source_basis_final_count_mismatch")
        counts = payload.get("counts") if isinstance(payload.get("counts"), dict) else {}
        payload_final_count = _safe_optional_int(counts.get("final_count"))
        if payload_final_count is not None and _safe_optional_int(source_basis.get("final_count")) != payload_final_count:
            blockers.append("dashboard_overview_source_basis_payload_final_count_mismatch")
        for token_key in (
            "latest_report_file",
            "publication_registry_file",
            "scheduler_state_file",
            "database_file",
            "nav_history_maintenance",
            "nav_lag_audit",
            "risk_sync_audit",
            "risk_coverage_gap_audit",
        ):
            token = _clean_live_text(source_basis.get(token_key))
            if not token or token in {"missing", "missing::empty_or_missing_wal"}:
                blockers.append(f"dashboard_overview_source_basis_{token_key}_missing")
        if _clean_live_text(source_basis.get("missing_value_policy")) != "preserve_missing_semantics":
            blockers.append("dashboard_overview_source_basis_missing_value_policy_mismatch")
        if _clean_live_text(source_basis.get("business_value_synthesis")) != "forbidden":
            blockers.append("dashboard_overview_source_basis_business_value_synthesis_mismatch")
        if source_basis.get("frontend_rendered_truth_allowed") is not False:
            blockers.append("dashboard_overview_source_basis_frontend_truth_allowed")
        if not _clean_live_text(source_basis.get("scope_contract_digest")):
            blockers.append("dashboard_overview_source_basis_scope_contract_digest_missing")
        if not _clean_live_text(source_basis.get("cache_basis_digest")):
            blockers.append("dashboard_overview_source_basis_cache_basis_digest_missing")
        if not _clean_live_text(source_basis.get("digest")):
            blockers.append("dashboard_overview_source_basis_digest_missing")

    if not source_contract:
        blockers.append("dashboard_overview_source_contract_missing")
    else:
        for key, expected in DASHBOARD_OVERVIEW_SOURCE_CONTRACT_EXPECTED.items():
            if _clean_live_text(source_contract.get(key)) != expected:
                blockers.append(f"dashboard_overview_source_contract_{key}_mismatch")
        if source_contract.get("frontend_rendered_truth_allowed") is not False:
            blockers.append("dashboard_overview_source_contract_frontend_truth_allowed")
        if _safe_optional_int(source_contract.get("cache_basis_schema_version")) is None:
            blockers.append("dashboard_overview_source_contract_cache_schema_missing")
        if source_basis and _clean_live_text(source_contract.get("cache_basis_digest")) != _clean_live_text(source_basis.get("digest")):
            blockers.append("dashboard_overview_source_contract_digest_mismatch")

    return {
        "available": bool(source_basis and cache_basis and source_contract),
        "source_basis": basis_summary,
        "source": _clean_live_text(source_basis.get("source")) or None,
        "payload_mode": _clean_live_text(source_basis.get("payload_mode")) or None,
        "detail_scope": _clean_live_text(source_basis.get("detail_scope")) or None,
        "run_id": _clean_live_text(source_basis.get("run_id")) or None,
        "latest_canonical_run_id": _clean_live_text(source_basis.get("latest_canonical_run_id")) or None,
        "current_published_run_id": _clean_live_text(source_basis.get("current_published_run_id")) or None,
        "final_count": _safe_optional_int(source_basis.get("final_count")),
        "source_contract": {
            "present": bool(source_contract),
            "source": source_contract.get("source") if source_contract else None,
            "truth_policy": source_contract.get("truth_policy") if source_contract else None,
            "missing_value_policy": source_contract.get("missing_value_policy") if source_contract else None,
            "business_value_synthesis": source_contract.get("business_value_synthesis") if source_contract else None,
            "official_publication_truth": source_contract.get("official_publication_truth") if source_contract else None,
            "frontend_rendered_truth_allowed": source_contract.get("frontend_rendered_truth_allowed") if source_contract else None,
        },
    }, blockers


def _validate_live_scheduler_header_status(
    endpoint_key: str,
    payload: dict[str, Any],
    *,
    expected_scheduler_status: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], list[str]]:
    blockers: list[str] = []
    header_payload = (
        dict(payload.get("health") or {})
        if endpoint_key == "bootstrap"
        and isinstance(payload.get("health"), dict)
        else dict(payload or {})
    )
    automation = (
        header_payload.get("automation_autonomy")
        if isinstance(header_payload.get("automation_autonomy"), dict)
        else {}
    )
    status = _clean_live_text(header_payload.get("automation_status")) or _clean_live_text(
        header_payload.get("automation_mode")
    )
    manual_intervention_required = header_payload.get("manual_intervention_required")
    scheduler_running = header_payload.get("scheduler_running")
    current_task_name = _clean_live_text(header_payload.get("current_task_name"))
    current_task_started_at = _clean_live_text(header_payload.get("current_task_started_at"))

    if not status:
        blockers.append(f"{endpoint_key}_scheduler_header_automation_status_missing")
    if "manual_intervention_required" not in header_payload:
        blockers.append(f"{endpoint_key}_scheduler_header_manual_intervention_required_missing")
    if "scheduler_running" not in header_payload:
        blockers.append(f"{endpoint_key}_scheduler_header_scheduler_running_missing")

    if manual_intervention_required is True:
        blockers.append(f"{endpoint_key}_scheduler_header_manual_intervention_required")
    if scheduler_running is not True:
        blockers.append(f"{endpoint_key}_scheduler_header_scheduler_not_running")

    automation_status = _clean_live_text(automation.get("status"))
    if automation_status and status and status != automation_status:
        blockers.append(f"{endpoint_key}_scheduler_header_automation_status_mismatch")
    if (
        "manual_intervention_required" in automation
        and "manual_intervention_required" in header_payload
        and manual_intervention_required != automation.get("manual_intervention_required")
    ):
        blockers.append(f"{endpoint_key}_scheduler_header_manual_intervention_required_mismatch")
    if (
        "scheduler_running" in automation
        and "scheduler_running" in header_payload
        and scheduler_running != automation.get("scheduler_running")
    ):
        blockers.append(f"{endpoint_key}_scheduler_header_scheduler_running_mismatch")
    if (
        _clean_live_text(automation.get("current_task_name"))
        and current_task_name
        and current_task_name != _clean_live_text(automation.get("current_task_name"))
    ):
        blockers.append(f"{endpoint_key}_scheduler_header_current_task_name_mismatch")

    expected = dict(expected_scheduler_status or {})
    if expected:
        if _clean_live_text(expected.get("status")) and status != _clean_live_text(expected.get("status")):
            blockers.append(f"{endpoint_key}_scheduler_header_status_differs_from_scheduler_detail")
        for field, value in (
            ("manual_intervention_required", manual_intervention_required),
            ("scheduler_running", scheduler_running),
        ):
            if field in expected and value != expected.get(field):
                blockers.append(f"{endpoint_key}_scheduler_header_{field}_differs_from_scheduler_detail")
        if (
            _clean_live_text(expected.get("current_task_name"))
            and current_task_name
            and current_task_name != _clean_live_text(expected.get("current_task_name"))
        ):
            blockers.append(f"{endpoint_key}_scheduler_header_current_task_name_differs_from_scheduler_detail")
        if (
            _clean_live_text(expected.get("current_task_started_at"))
            and current_task_started_at
            and current_task_started_at != _clean_live_text(expected.get("current_task_started_at"))
        ):
            blockers.append(f"{endpoint_key}_scheduler_header_current_task_started_at_differs_from_scheduler_detail")

    return {
        "available": True,
        "status": status or None,
        "manual_intervention_required": manual_intervention_required,
        "scheduler_running": scheduler_running,
        "current_task_name": current_task_name or None,
        "current_task_started_at": current_task_started_at or None,
    }, blockers


def _validate_live_autostock_directory_source_summary(
    endpoint_key: str,
    payload: dict[str, Any],
) -> tuple[dict[str, Any], list[str], list[str]]:
    blockers: list[str] = []
    warnings: list[str] = []
    header_payload = (
        dict(payload.get("health") or {})
        if endpoint_key == "bootstrap"
        and isinstance(payload.get("health"), dict)
        else dict(payload or {})
    )
    summary = (
        header_payload.get("autostock_directory_source_summary")
        if isinstance(header_payload.get("autostock_directory_source_summary"), dict)
        else {}
    )
    if not summary:
        blockers.append(f"{endpoint_key}_autostock_directory_source_summary_missing")
        return {"available": False}, blockers, warnings

    status = _clean_live_text(summary.get("status"))
    parse_error_count = _safe_int(summary.get("parse_error_count"))
    formal_overlap_count = _safe_int(summary.get("formal_final_overlap_count"))
    formal_overlap_unsupported_count = _safe_int(summary.get("formal_final_overlap_unsupported_count"))
    source_payload_status = _clean_live_text(summary.get("source_payload_status"))
    publication_impact_status = _clean_live_text(summary.get("publication_impact_status"))
    parse_error_reason_counts = (
        summary.get("parse_error_reason_counts")
        if isinstance(summary.get("parse_error_reason_counts"), dict)
        else {}
    )
    impact_scope = (
        summary.get("parse_error_impact_scope")
        if isinstance(summary.get("parse_error_impact_scope"), dict)
        else {}
    )
    handling = _clean_live_text(summary.get("business_value_handling"))
    if status not in {"ok", "watch", "needs_review", "not_available"}:
        blockers.append(f"{endpoint_key}_autostock_directory_source_status_unexpected:{status or 'empty'}")
    if _clean_live_text(summary.get("source")) != "AutoStock /fund/all":
        blockers.append(f"{endpoint_key}_autostock_directory_source_mismatch")
    if _clean_live_text(summary.get("endpoint_path")) != "/fund/all":
        blockers.append(f"{endpoint_key}_autostock_directory_endpoint_mismatch")
    if _clean_live_text(summary.get("truth_policy")) != "preserve_missing_semantics":
        blockers.append(f"{endpoint_key}_autostock_directory_truth_policy_mismatch")
    if "does not infer fund type from fund name" not in handling and "does not synthesize" not in handling:
        blockers.append(f"{endpoint_key}_autostock_directory_business_value_handling_weak")
    if parse_error_count > 0 and not parse_error_reason_counts:
        blockers.append(f"{endpoint_key}_autostock_directory_parse_error_reason_counts_missing")
    if parse_error_count > 0 and "directory_parse_errors_present" not in set(summary.get("warnings") or []):
        warnings.append(f"{endpoint_key}_autostock_directory_parse_error_warning_missing")
    if parse_error_count > 0:
        if source_payload_status != "watch_parse_errors_preserved":
            blockers.append(f"{endpoint_key}_autostock_directory_source_payload_status_mismatch")
        if publication_impact_status not in {
            "ok_no_formal_final_overlap",
            "watch_final_overlap_with_local_classification",
            "watch_sync_missing_after_parse_errors",
            "watch_impact_scope_evidence_incomplete",
            "needs_review_final_overlap_without_local_classification",
        }:
            blockers.append(f"{endpoint_key}_autostock_directory_publication_impact_status_mismatch")
        required_impact_fields = (
            "source_payload_status",
            "publication_impact_status",
            "local_funds_supported_classification_count",
            "local_funds_missing_count",
            "formal_final_overlap_unsupported_count",
            "sync_missing_in_local_after_count",
        )
        for field in required_impact_fields:
            if field not in summary:
                blockers.append(f"{endpoint_key}_autostock_directory_impact_field_missing:{field}")
        if not impact_scope:
            blockers.append(f"{endpoint_key}_autostock_directory_parse_error_impact_scope_missing")
        else:
            impact_handling = _clean_live_text(impact_scope.get("business_value_handling"))
            if _clean_live_text(impact_scope.get("truth_policy")) != "preserve_missing_semantics":
                blockers.append(f"{endpoint_key}_autostock_directory_impact_truth_policy_mismatch")
            if "not inferred from fund name" not in impact_handling and "not used to synthesize" not in impact_handling:
                blockers.append(f"{endpoint_key}_autostock_directory_impact_business_value_handling_weak")
    if formal_overlap_unsupported_count > 0:
        blockers.append(
            f"{endpoint_key}_autostock_directory_parse_error_formal_overlap_without_local_classification"
        )
    elif formal_overlap_count > 0:
        warnings.append(f"{endpoint_key}_autostock_directory_parse_error_formal_overlap_with_local_classification")
    elif (
        parse_error_count > 0
        and publication_impact_status == "ok_no_formal_final_overlap"
        and _safe_int(summary.get("sync_missing_in_local_after_count")) == 0
        and _safe_int(summary.get("sync_missing_active_in_local_after_count")) == 0
    ):
        if "directory_source_incomplete_due_to_parse_errors" in set(summary.get("warnings") or []):
            warnings.append(f"{endpoint_key}_autostock_directory_source_incomplete_warning_not_publication_scoped")
    if summary.get("business_date") not in (None, ""):
        warnings.append(f"{endpoint_key}_autostock_directory_business_date_unexpected")
    if (
        summary.get("business_date") in (None, "")
        and _clean_live_text(summary.get("business_date_semantics")) != "not_provided_by_autostock_directory"
    ):
        blockers.append(f"{endpoint_key}_autostock_directory_business_date_semantics_missing")

    return {
        "available": True,
        "status": status or None,
        "source": _clean_live_text(summary.get("source")) or None,
        "endpoint_path": _clean_live_text(summary.get("endpoint_path")) or None,
        "parse_error_count": parse_error_count,
        "source_payload_status": source_payload_status or None,
        "publication_impact_status": publication_impact_status or None,
        "parse_error_reason_counts": parse_error_reason_counts,
        "formal_final_overlap_count": formal_overlap_count,
        "formal_final_overlap_unsupported_count": formal_overlap_unsupported_count,
        "local_funds_supported_classification_count": _safe_int(
            summary.get("local_funds_supported_classification_count")
        ),
        "local_funds_missing_count": _safe_int(summary.get("local_funds_missing_count")),
        "sync_missing_in_local_after_count": _safe_int(summary.get("sync_missing_in_local_after_count")),
        "truth_policy": _clean_live_text(summary.get("truth_policy")) or None,
        "business_date_semantics": _clean_live_text(summary.get("business_date_semantics")) or None,
    }, blockers, warnings


def _validate_live_weekly_maintenance_warning_truth_summary(
    endpoint_key: str,
    payload: dict[str, Any],
) -> tuple[dict[str, Any], list[str], list[str]]:
    blockers: list[str] = []
    warnings: list[str] = []
    header_payload = (
        dict(payload.get("health") or {})
        if endpoint_key == "bootstrap"
        and isinstance(payload.get("health"), dict)
        else dict(payload or {})
    )
    automation = (
        header_payload.get("automation_autonomy")
        if isinstance(header_payload.get("automation_autonomy"), dict)
        else {}
    )
    warning_issue_codes = {
        _clean_live_text(item)
        for item in automation.get("warning_issue_codes") or []
        if _clean_live_text(item)
    }
    resolved_warning_issue_codes = {
        _clean_live_text(item)
        for item in automation.get("resolved_warning_issue_codes") or []
        if _clean_live_text(item)
    }
    summary = (
        header_payload.get("weekly_maintenance_warning_truth_summary")
        if isinstance(header_payload.get("weekly_maintenance_warning_truth_summary"), dict)
        else {}
    )
    if (
        (
            "weekly_maintenance_degraded" in warning_issue_codes
            or "weekly_maintenance_degraded" in resolved_warning_issue_codes
        )
        and not summary
    ):
        blockers.append(f"{endpoint_key}_weekly_maintenance_warning_truth_summary_missing")
        return {"available": False}, blockers, warnings
    if not summary:
        return {"available": False}, blockers, warnings

    status = _clean_live_text(summary.get("status"))
    summary_warnings = {
        _clean_live_text(item)
        for item in summary.get("warnings") or []
        if _clean_live_text(item)
    }
    handling = _clean_live_text(summary.get("business_value_handling"))
    collect_funds_missing_nav_date_count = _safe_int(
        summary.get("collect_funds_total_missing_nav_date_count")
    )
    collect_funds_missing_nav_value_count = _safe_int(
        summary.get("collect_funds_total_missing_nav_value_count")
    )
    collect_details_empty_detail_count = _safe_int(summary.get("collect_details_empty_detail_count"))
    collect_details_failed_response_count = _safe_int(summary.get("collect_details_failed_response_count"))
    collect_details_stderr_blocker_sample_count = _safe_int(
        summary.get("collect_details_stderr_blocker_sample_count")
    )
    collect_managers_failed_response_count = _safe_int(
        summary.get("collect_managers_failed_response_count")
    )
    collect_managers_empty_response_count = _safe_int(
        summary.get("collect_managers_empty_response_count")
    )
    collect_managers_stderr_blocker_sample_count = _safe_int(
        summary.get("collect_managers_stderr_blocker_sample_count")
    )

    if status not in {"ok", "watch", "needs_review", "not_available"}:
        blockers.append(f"{endpoint_key}_weekly_maintenance_warning_truth_status_unexpected:{status or 'empty'}")
    if _clean_live_text(summary.get("source")) != "data_truthfulness_automation_script_run_truth":
        blockers.append(f"{endpoint_key}_weekly_maintenance_warning_truth_source_mismatch")
    if _clean_live_text(summary.get("truth_policy")) != "read_only_runtime_artifact_evidence_no_state_mutation":
        blockers.append(f"{endpoint_key}_weekly_maintenance_warning_truth_policy_mismatch")
    if "does not mutate task_history" not in handling:
        blockers.append(f"{endpoint_key}_weekly_maintenance_warning_business_value_handling_mutation_weak")
    if "synthesize fund details" not in handling:
        blockers.append(f"{endpoint_key}_weekly_maintenance_warning_business_value_handling_synthesis_weak")
    if summary.get("manual_intervention_required") is True:
        blockers.append(f"{endpoint_key}_weekly_maintenance_warning_manual_intervention_required")
    root_cause_status = _clean_live_text(summary.get("scheduler_warning_root_cause_status"))
    root_cause_resolved = summary.get("scheduler_warning_root_cause_resolved") is True
    resolution_artifact = _clean_live_text(summary.get("scheduler_warning_resolution_artifact"))
    if root_cause_status == UNRESOLVED_STAGE_STATUS:
        if root_cause_resolved:
            blockers.append(f"{endpoint_key}_weekly_maintenance_warning_unresolved_stage_marked_resolved")
        if not _clean_live_text(summary.get("scheduler_warning_primary_degraded_stage")):
            blockers.append(f"{endpoint_key}_weekly_maintenance_warning_primary_stage_missing")
        if summary.get("scheduler_warning_detail_contract_errors") != []:
            blockers.append(f"{endpoint_key}_weekly_maintenance_warning_stage_evidence_invalid")
        if summary.get("scheduler_warning_truth_policy") != PERSISTED_STAGE_TRUTH_POLICY:
            blockers.append(f"{endpoint_key}_weekly_maintenance_warning_stage_truth_policy_mismatch")
        warnings.append(f"{endpoint_key}_weekly_maintenance_warning_root_cause_unresolved_with_evidence")
    else:
        if not root_cause_resolved:
            if root_cause_status == "unresolved_or_resolution_evidence_missing" and resolution_artifact:
                warnings.append(f"{endpoint_key}_weekly_maintenance_warning_root_cause_unresolved_with_evidence")
            else:
                blockers.append(f"{endpoint_key}_weekly_maintenance_warning_root_cause_not_resolved")
        if root_cause_status not in {
            "resolved_awaiting_next_weekly_maintenance",
            "unresolved_or_resolution_evidence_missing",
        }:
            blockers.append(f"{endpoint_key}_weekly_maintenance_warning_root_cause_status_mismatch")
    if not resolution_artifact:
        blockers.append(f"{endpoint_key}_weekly_maintenance_warning_resolution_artifact_missing")
    if collect_funds_missing_nav_date_count > 0 and "weekly_collect_funds_missing_nav_date_present" not in summary_warnings:
        warnings.append(f"{endpoint_key}_weekly_maintenance_missing_nav_date_warning_missing")
    if collect_funds_missing_nav_value_count > 0 and "weekly_collect_funds_missing_nav_value_present" not in summary_warnings:
        warnings.append(f"{endpoint_key}_weekly_maintenance_missing_nav_value_warning_missing")
    if collect_details_empty_detail_count > 0:
        if not isinstance(summary.get("collect_details_empty_detail_reason_counts"), dict) or not summary.get(
            "collect_details_empty_detail_reason_counts"
        ):
            blockers.append(f"{endpoint_key}_weekly_maintenance_empty_detail_reason_counts_missing")
        if _clean_live_text(summary.get("collect_details_stop_watch_fallback_scope")) != "not_counted_as_detail_success":
            blockers.append(f"{endpoint_key}_weekly_maintenance_empty_detail_scope_mismatch")
    if collect_details_failed_response_count > collect_details_empty_detail_count:
        blockers.append(f"{endpoint_key}_weekly_maintenance_failed_response_exceeds_empty_detail")
    if _clean_live_text(summary.get("collect_details_stderr_classification")) == "blocker_signals":
        if summary.get("collect_details_stderr_blockers_explained_by_empty_detail") is not True:
            blockers.append(f"{endpoint_key}_weekly_maintenance_stderr_blockers_unexplained")
        if collect_details_stderr_blocker_sample_count <= 0:
            blockers.append(f"{endpoint_key}_weekly_maintenance_stderr_blocker_samples_missing")
    if collect_managers_failed_response_count > 0:
        if "weekly_collect_managers_failed_responses_present" not in summary_warnings:
            warnings.append(f"{endpoint_key}_weekly_maintenance_manager_failed_response_warning_missing")
        if collect_managers_failed_response_count > collect_managers_empty_response_count:
            blockers.append(f"{endpoint_key}_weekly_maintenance_manager_failed_response_scope_mismatch")
    if _clean_live_text(summary.get("collect_managers_stderr_classification")) == "blocker_signals":
        if summary.get("collect_managers_stderr_blockers_explained_by_failed_responses") is not True:
            blockers.append(f"{endpoint_key}_weekly_maintenance_manager_stderr_blockers_unexplained")
        if collect_managers_stderr_blocker_sample_count <= 0:
            blockers.append(f"{endpoint_key}_weekly_maintenance_manager_stderr_blocker_samples_missing")

    return {
        "available": True,
        "status": status or None,
        "task_id": summary.get("task_id"),
        "task_status": _clean_live_text(summary.get("task_status")) or None,
        "manual_intervention_required": summary.get("manual_intervention_required"),
        "manual_intervention_hint_is_historical_only": summary.get(
            "manual_intervention_hint_is_historical_only"
        ),
        "scheduler_warning_root_cause_status": _clean_live_text(
            summary.get("scheduler_warning_root_cause_status")
        )
        or None,
        "collect_funds_total_missing_nav_date_count": collect_funds_missing_nav_date_count,
        "collect_funds_total_missing_nav_value_count": collect_funds_missing_nav_value_count,
        "collect_details_empty_detail_count": collect_details_empty_detail_count,
        "collect_details_failed_response_count": collect_details_failed_response_count,
        "collect_details_stderr_classification": _clean_live_text(
            summary.get("collect_details_stderr_classification")
        )
        or None,
        "collect_details_stderr_blockers_explained_by_empty_detail": summary.get(
            "collect_details_stderr_blockers_explained_by_empty_detail"
        ),
        "collect_managers_failed_response_count": collect_managers_failed_response_count,
        "collect_managers_empty_response_count": collect_managers_empty_response_count,
        "collect_managers_stderr_classification": _clean_live_text(
            summary.get("collect_managers_stderr_classification")
        )
        or None,
        "collect_managers_stderr_blockers_explained_by_failed_responses": summary.get(
            "collect_managers_stderr_blockers_explained_by_failed_responses"
        ),
        "truth_policy": _clean_live_text(summary.get("truth_policy")) or None,
    }, blockers, warnings


def _validate_live_workflow_status_summary(
    payload: dict[str, Any],
    *,
    expected_publication: dict[str, Any],
) -> tuple[dict[str, Any], list[str]]:
    blockers: list[str] = []
    workflow_status = _clean_live_text(payload.get("status"))
    workflow_phase = _clean_live_text(payload.get("phase"))
    workflow_run_id = _clean_live_text(payload.get("run_id"))
    expected_run_id = _clean_live_text(expected_publication.get("latest_report_run_id"))

    if not workflow_status:
        blockers.append("workflow_status_missing")
    if workflow_status in {"succeeded", "success", "completed"}:
        if not workflow_run_id:
            blockers.append("workflow_success_run_id_missing")
        elif (
            expected_run_id
            and workflow_run_id != expected_run_id
            and not _is_allowed_live_candidate_run(expected_publication, workflow_run_id, payload=payload)
            and workflow_run_id not in _live_lightweight_runtime_run_ids(
                payload,
                expected_source="workflow_status_summary_runtime_manager",
            )
        ):
            blockers.append("workflow_success_run_id_mismatch")
    if workflow_phase == "completed" and not workflow_run_id:
        blockers.append("workflow_completed_run_id_missing")
    if "logs_tail" in payload and not isinstance(payload.get("logs_tail"), list):
        blockers.append("workflow_logs_tail_not_list")

    return {
        "available": True,
        "status": workflow_status or None,
        "phase": workflow_phase or None,
        "run_id": workflow_run_id or None,
        "logs_tail_count": len(payload.get("logs_tail") or []) if isinstance(payload.get("logs_tail"), list) else None,
    }, blockers


def _validate_live_lightweight_status_source_contract(
    payload: dict[str, Any],
    *,
    expected_publication: dict[str, Any],
    artifact_key: str,
    expected_source: str,
    expected_payload_mode: str,
    expected_official_publication_truth: str,
) -> tuple[dict[str, Any], list[str]]:
    blockers: list[str] = []
    expected_run_id = _clean_live_text(expected_publication.get("latest_report_run_id"))
    expected_latest_canonical_run_id = _clean_live_text(expected_publication.get("latest_canonical_run_id"))
    expected_current_published_run_id = _clean_live_text(expected_publication.get("current_published_run_id"))
    source_basis = payload.get("source_basis") if isinstance(payload.get("source_basis"), dict) else {}
    cache_basis = payload.get("cache_basis") if isinstance(payload.get("cache_basis"), dict) else {}
    source_contract = payload.get("source_contract") if isinstance(payload.get("source_contract"), dict) else {}
    allowed_run_ids = _live_expected_candidate_run_ids(expected_publication, payload=payload)
    allowed_run_ids.update(
        _live_lightweight_runtime_run_ids(payload, expected_source=expected_source)
    )

    basis_summary, basis_blockers = _summarize_frontend_cache_basis_contract(
        source_basis or cache_basis,
        artifact_key=artifact_key,
        required_keys=LIGHTWEIGHT_STATUS_SOURCE_BASIS_REQUIRED_KEYS,
        expected_run_id=expected_run_id,
        allowed_run_ids=allowed_run_ids,
    )
    blockers.extend(basis_blockers)
    if not source_basis:
        blockers.append(f"{artifact_key}_source_basis_missing")
    if not cache_basis and f"{artifact_key}_cache_basis_missing" not in blockers:
        blockers.append(f"{artifact_key}_cache_basis_missing")
    if source_basis and cache_basis and source_basis != cache_basis:
        blockers.append(f"{artifact_key}_source_basis_cache_basis_mismatch")

    if source_basis:
        if _clean_live_text(source_basis.get("source")) != expected_source:
            blockers.append(f"{artifact_key}_source_basis_source_mismatch")
        if _clean_live_text(source_basis.get("payload_mode")) != expected_payload_mode:
            blockers.append(f"{artifact_key}_source_basis_payload_mode_mismatch")
        if expected_run_id and _clean_live_text(source_basis.get("latest_report_run_id")) != expected_run_id:
            blockers.append(f"{artifact_key}_source_basis_latest_report_run_id_mismatch")
        if expected_latest_canonical_run_id and _clean_live_text(source_basis.get("latest_canonical_run_id")) != expected_latest_canonical_run_id:
            blockers.append(f"{artifact_key}_source_basis_latest_canonical_run_id_mismatch")
        if expected_current_published_run_id and _clean_live_text(source_basis.get("current_published_run_id")) != expected_current_published_run_id:
            blockers.append(f"{artifact_key}_source_basis_current_published_run_id_mismatch")
        for token_key in (
            "latest_report_file",
            "publication_registry_file",
            "scheduler_state_file",
            "database_file",
        ):
            token = _clean_live_text(source_basis.get(token_key))
            if not token or token in {"missing", "missing::empty_or_missing_wal"}:
                blockers.append(f"{artifact_key}_source_basis_{token_key}_missing")
        if _clean_live_text(source_basis.get("missing_value_policy")) != "preserve_missing_semantics":
            blockers.append(f"{artifact_key}_source_basis_missing_value_policy_mismatch")
        if _clean_live_text(source_basis.get("business_value_synthesis")) != "forbidden":
            blockers.append(f"{artifact_key}_source_basis_business_value_synthesis_mismatch")
        if source_basis.get("frontend_rendered_truth_allowed") is not False:
            blockers.append(f"{artifact_key}_source_basis_frontend_truth_allowed")
        if not _clean_live_text(source_basis.get("digest")):
            blockers.append(f"{artifact_key}_source_basis_digest_missing")

    if not source_contract:
        blockers.append(f"{artifact_key}_source_contract_missing")
    else:
        if _clean_live_text(source_contract.get("source")) != expected_source:
            blockers.append(f"{artifact_key}_source_contract_source_mismatch")
        for key, expected in LIGHTWEIGHT_STATUS_SOURCE_CONTRACT_EXPECTED.items():
            if _clean_live_text(source_contract.get(key)) != expected:
                blockers.append(f"{artifact_key}_source_contract_{key}_mismatch")
        if _clean_live_text(source_contract.get("official_publication_truth")) != expected_official_publication_truth:
            blockers.append(f"{artifact_key}_source_contract_official_publication_truth_mismatch")
        if source_contract.get("frontend_rendered_truth_allowed") is not False:
            blockers.append(f"{artifact_key}_source_contract_frontend_truth_allowed")
        if _safe_optional_int(source_contract.get("cache_basis_schema_version")) is None:
            blockers.append(f"{artifact_key}_source_contract_cache_schema_missing")
        if source_basis and _clean_live_text(source_contract.get("cache_basis_digest")) != _clean_live_text(source_basis.get("digest")):
            blockers.append(f"{artifact_key}_source_contract_digest_mismatch")

    return {
        "available": bool(source_basis and cache_basis and source_contract),
        "source_basis": basis_summary,
        "source": _clean_live_text(source_basis.get("source")) or None,
        "payload_mode": _clean_live_text(source_basis.get("payload_mode")) or None,
        "run_id": _clean_live_text(source_basis.get("run_id")) or None,
        "latest_report_run_id": _clean_live_text(source_basis.get("latest_report_run_id")) or None,
        "latest_canonical_run_id": _clean_live_text(source_basis.get("latest_canonical_run_id")) or None,
        "current_published_run_id": _clean_live_text(source_basis.get("current_published_run_id")) or None,
        "source_contract": {
            "present": bool(source_contract),
            "source": source_contract.get("source") if source_contract else None,
            "truth_policy": source_contract.get("truth_policy") if source_contract else None,
            "missing_value_policy": source_contract.get("missing_value_policy") if source_contract else None,
            "business_value_synthesis": source_contract.get("business_value_synthesis") if source_contract else None,
            "official_publication_truth": source_contract.get("official_publication_truth") if source_contract else None,
            "frontend_rendered_truth_allowed": source_contract.get("frontend_rendered_truth_allowed") if source_contract else None,
        },
    }, blockers


def _validate_live_publication_registry_summary(
    payload: dict[str, Any],
    *,
    expected_publication: dict[str, Any],
) -> tuple[dict[str, Any], list[str]]:
    blockers: list[str] = []
    latest_canonical_run_id = _clean_live_text(payload.get("latest_canonical_run_id"))
    current_published_run_id = _clean_live_text(payload.get("current_published_run_id"))
    latest_report_path = _clean_live_text(payload.get("latest_canonical_report_path"))
    canonical_runs = payload.get("canonical_runs")
    canonical_runs = canonical_runs if isinstance(canonical_runs, list) else []
    source_basis = payload.get("source_basis") if isinstance(payload.get("source_basis"), dict) else {}
    cache_basis = payload.get("cache_basis") if isinstance(payload.get("cache_basis"), dict) else {}
    source_contract = payload.get("source_contract") if isinstance(payload.get("source_contract"), dict) else {}
    expected_latest_run_id = _clean_live_text(expected_publication.get("latest_report_run_id"))
    expected_latest_canonical_run_id = _clean_live_text(expected_publication.get("latest_canonical_run_id"))
    expected_current_published_run_id = _clean_live_text(expected_publication.get("current_published_run_id"))

    basis_summary, basis_blockers = _summarize_frontend_cache_basis_contract(
        source_basis or cache_basis,
        artifact_key="publication_summary_source_basis",
        required_keys=PUBLICATION_SUMMARY_SOURCE_BASIS_REQUIRED_KEYS,
        expected_run_id="",
        nullable_keys=("archived_superseded_run_count",),
    )
    blockers.extend(basis_blockers)
    if not source_basis:
        blockers.append("publication_summary_source_basis_missing")
    if not cache_basis and "publication_summary_source_basis_cache_basis_missing" not in blockers:
        blockers.append("publication_summary_source_basis_cache_basis_missing")
    if source_basis and cache_basis and source_basis != cache_basis:
        blockers.append("publication_summary_source_basis_cache_basis_mismatch")

    archived_count = payload.get("archived_superseded_run_count")
    if "archived_superseded_run_count" not in payload:
        blockers.append("publication_summary_archived_count_field_missing")
    elif archived_count is not None and (type(archived_count) is not int or archived_count < 0):
        blockers.append("publication_summary_archived_count_invalid")

    if expected_latest_canonical_run_id and latest_canonical_run_id != expected_latest_canonical_run_id:
        blockers.append("publication_summary_latest_canonical_run_id_mismatch")
    if expected_current_published_run_id and current_published_run_id != expected_current_published_run_id:
        blockers.append("publication_summary_current_published_run_id_mismatch")
    if not latest_report_path:
        blockers.append("publication_summary_latest_canonical_report_path_missing")
    if _safe_int(payload.get("canonical_run_count")) <= 0:
        blockers.append("publication_summary_canonical_run_count_missing")
    if not canonical_runs:
        blockers.append("publication_summary_canonical_runs_missing")

    current_rows = [
        item
        for item in canonical_runs
        if isinstance(item, dict)
        and _clean_live_text(item.get("run_id")) == (expected_latest_run_id or current_published_run_id)
    ]
    if expected_latest_run_id and not current_rows:
        blockers.append("publication_summary_current_run_missing_from_canonical_runs")
    for row in current_rows[:1]:
        if row.get("published") is not True:
            blockers.append("publication_summary_current_run_not_marked_published")
        snapshot = row.get("snapshot") if isinstance(row.get("snapshot"), dict) else {}
        if not _clean_live_text(snapshot.get("candidate_date")):
            blockers.append("publication_summary_current_run_candidate_date_missing")
        if _safe_int(snapshot.get("candidate_fund_count")) <= 0:
            blockers.append("publication_summary_current_run_candidate_fund_count_missing")

    if source_basis:
        if _clean_live_text(source_basis.get("source")) != "publication_summary_publication_registry_artifact":
            blockers.append("publication_summary_source_basis_source_mismatch")
        if _clean_live_text(source_basis.get("payload_mode")) != "summary":
            blockers.append("publication_summary_source_basis_payload_mode_mismatch")
        if expected_latest_canonical_run_id and _clean_live_text(source_basis.get("latest_canonical_run_id")) != expected_latest_canonical_run_id:
            blockers.append("publication_summary_source_basis_latest_canonical_run_id_mismatch")
        if expected_current_published_run_id and _clean_live_text(source_basis.get("current_published_run_id")) != expected_current_published_run_id:
            blockers.append("publication_summary_source_basis_current_published_run_id_mismatch")
        if latest_report_path and _clean_live_text(source_basis.get("latest_canonical_report_path")) != latest_report_path:
            blockers.append("publication_summary_source_basis_latest_report_path_mismatch")
        if _safe_int(source_basis.get("canonical_run_count")) != _safe_int(payload.get("canonical_run_count")):
            blockers.append("publication_summary_source_basis_canonical_run_count_mismatch")
        basis_archived_count = source_basis.get("archived_superseded_run_count")
        if basis_archived_count is not None and (type(basis_archived_count) is not int or basis_archived_count < 0):
            blockers.append("publication_summary_source_basis_archived_count_invalid")
        if basis_archived_count != archived_count:
            blockers.append("publication_summary_source_basis_archived_count_mismatch")
        if source_basis.get("latest_canonical_run_present") is not True:
            blockers.append("publication_summary_source_basis_latest_run_not_present")
        if source_basis.get("current_published_run_present") is not True:
            blockers.append("publication_summary_source_basis_current_run_not_present")
        canonical_run_ids = source_basis.get("canonical_run_ids")
        if not isinstance(canonical_run_ids, list):
            blockers.append("publication_summary_source_basis_canonical_run_ids_missing_current")
        elif expected_latest_run_id and expected_latest_run_id not in {
            _clean_live_text(item) for item in canonical_run_ids
        }:
            blockers.append("publication_summary_source_basis_canonical_run_ids_missing_current")
        for token_key in (
            "publication_registry_file",
            "latest_report_file",
            "latest_canonical_report_token",
        ):
            token = _clean_live_text(source_basis.get(token_key))
            if not token or token == "missing":
                blockers.append(f"publication_summary_source_basis_{token_key}_missing")
        if _clean_live_text(source_basis.get("missing_value_policy")) != "preserve_missing_semantics":
            blockers.append("publication_summary_source_basis_missing_value_policy_mismatch")
        if _clean_live_text(source_basis.get("business_value_synthesis")) != "forbidden":
            blockers.append("publication_summary_source_basis_business_value_synthesis_mismatch")
        if source_basis.get("frontend_rendered_truth_allowed") is not False:
            blockers.append("publication_summary_source_basis_frontend_truth_allowed")
        if not _clean_live_text(source_basis.get("digest")):
            blockers.append("publication_summary_source_basis_digest_missing")

    if not source_contract:
        blockers.append("publication_summary_source_contract_missing")
    else:
        for key, expected in PUBLICATION_SUMMARY_SOURCE_CONTRACT_EXPECTED.items():
            if _clean_live_text(source_contract.get(key)) != expected:
                blockers.append(f"publication_summary_source_contract_{key}_mismatch")
        if source_contract.get("frontend_rendered_truth_allowed") is not False:
            blockers.append("publication_summary_source_contract_frontend_truth_allowed")
        if _safe_optional_int(source_contract.get("cache_basis_schema_version")) is None:
            blockers.append("publication_summary_source_contract_cache_schema_missing")
        if source_basis and _clean_live_text(source_contract.get("cache_basis_digest")) != _clean_live_text(
            source_basis.get("digest")
        ):
            blockers.append("publication_summary_source_contract_digest_mismatch")

    return {
        "available": True,
        "latest_canonical_run_id": latest_canonical_run_id or None,
        "current_published_run_id": current_published_run_id or None,
        "latest_canonical_report_path": latest_report_path or None,
        "canonical_run_count": _safe_int(payload.get("canonical_run_count")),
        "archived_superseded_run_count": archived_count if type(archived_count) is int else None,
        "source_available": bool(source_basis and cache_basis and source_contract),
        "source_basis": basis_summary,
        "source": _clean_live_text(source_basis.get("source")) or None,
        "payload_mode": _clean_live_text(source_basis.get("payload_mode")) or None,
        "source_contract": {
            "present": bool(source_contract),
            "source": source_contract.get("source") if source_contract else None,
            "truth_policy": source_contract.get("truth_policy") if source_contract else None,
            "missing_value_policy": source_contract.get("missing_value_policy") if source_contract else None,
            "business_value_synthesis": source_contract.get("business_value_synthesis") if source_contract else None,
            "official_publication_truth": source_contract.get("official_publication_truth") if source_contract else None,
            "frontend_rendered_truth_allowed": source_contract.get("frontend_rendered_truth_allowed")
            if source_contract
            else None,
        },
    }, blockers


def _validate_live_dashboard_reports_payload(
    payload: dict[str, Any],
    *,
    expected_publication: dict[str, Any],
) -> tuple[dict[str, Any], list[str]]:
    blockers: list[str] = []
    expected_final_count = _safe_optional_int((expected_publication.get("counts") or {}).get("final_count"))
    scope_contract = payload.get("scope_contract") if isinstance(payload.get("scope_contract"), dict) else {}
    counts = payload.get("counts") if isinstance(payload.get("counts"), dict) else {}
    publish_funnel = (
        payload.get("publish_funnel_summary")
        if isinstance(payload.get("publish_funnel_summary"), dict)
        else {}
    )
    files = payload.get("files") if isinstance(payload.get("files"), dict) else {}
    artifacts = payload.get("artifacts") if isinstance(payload.get("artifacts"), dict) else {}

    if _clean_live_text(payload.get("detail_scope")) != "reports":
        blockers.append("dashboard_reports_detail_scope_mismatch")
    if scope_contract.get("report_snapshot_scope") is not True:
        blockers.append("dashboard_reports_scope_contract_report_snapshot_scope_missing")
    if scope_contract.get("live_diagnostics_merged") not in {False, None}:
        blockers.append("dashboard_reports_scope_contract_live_diagnostics_merged")
    if expected_final_count is not None:
        if _safe_optional_int(counts.get("final_count")) != expected_final_count:
            blockers.append("dashboard_reports_counts_final_count_mismatch")
        official_count = _safe_optional_int(publish_funnel.get("official_publish_count"))
        if official_count is not None and official_count != expected_final_count:
            blockers.append("dashboard_reports_publish_funnel_official_count_mismatch")

    required_file_keys = (
        "filtered_excel",
        "dynamic_filtered_excel",
        "report_markdown",
    )
    for key in required_file_keys:
        if not _clean_live_text(files.get(key) or artifacts.get(key)):
            blockers.append(f"dashboard_reports_file_missing:{key}")
    if not _clean_live_text(files.get("tracker_file") or files.get("project_tracker_excel") or artifacts.get("project_tracker_excel")):
        blockers.append("dashboard_reports_file_missing:project_tracker_excel")

    return {
        "available": True,
        "detail_scope": _clean_live_text(payload.get("detail_scope")) or None,
        "scope_label": _clean_live_text(scope_contract.get("scope_label")) or None,
        "report_snapshot_scope": scope_contract.get("report_snapshot_scope"),
        "live_diagnostics_merged": scope_contract.get("live_diagnostics_merged"),
        "counts_final_count": _safe_optional_int(counts.get("final_count")),
        "official_publish_count": _safe_optional_int(publish_funnel.get("official_publish_count")),
        "file_keys_present": sorted(
            key
            for key, value in {**artifacts, **files}.items()
            if _clean_live_text(value)
        ),
    }, blockers


def _validate_live_dashboard_latest_source_contract(
    payload: dict[str, Any],
    *,
    expected_publication: dict[str, Any],
) -> tuple[dict[str, Any], list[str]]:
    blockers: list[str] = []
    expected_run_id = _clean_live_text(expected_publication.get("latest_report_run_id"))
    expected_latest_canonical_run_id = _clean_live_text(expected_publication.get("latest_canonical_run_id"))
    expected_current_published_run_id = _clean_live_text(expected_publication.get("current_published_run_id"))
    expected_final_count = _safe_optional_int((expected_publication.get("counts") or {}).get("final_count"))
    source_basis = payload.get("source_basis") if isinstance(payload.get("source_basis"), dict) else {}
    cache_basis = payload.get("cache_basis") if isinstance(payload.get("cache_basis"), dict) else {}
    source_contract = payload.get("source_contract") if isinstance(payload.get("source_contract"), dict) else {}

    basis_summary, basis_blockers = _summarize_frontend_cache_basis_contract(
        source_basis or cache_basis,
        artifact_key="dashboard_latest_source_basis",
        required_keys=DASHBOARD_LATEST_SOURCE_BASIS_REQUIRED_KEYS,
        expected_run_id=expected_run_id,
        nullable_keys=("daily_screening_run_id",) if _daily_screening_absence_explained(payload) else (),
    )
    blockers.extend(basis_blockers)
    if not source_basis:
        blockers.append("dashboard_latest_source_basis_missing")
    if not cache_basis and "dashboard_latest_source_basis_cache_basis_missing" not in blockers:
        blockers.append("dashboard_latest_source_basis_cache_basis_missing")
    if source_basis and cache_basis and source_basis != cache_basis:
        blockers.append("dashboard_latest_source_basis_cache_basis_mismatch")

    if source_basis:
        if _clean_live_text(source_basis.get("source")) != "dashboard_latest_current_published_report_scheduler_database_artifacts":
            blockers.append("dashboard_latest_source_basis_source_mismatch")
        if _clean_live_text(source_basis.get("payload_mode")) != "compact":
            blockers.append("dashboard_latest_source_basis_payload_mode_mismatch")
        if _clean_live_text(source_basis.get("detail_scope")) != "compact":
            blockers.append("dashboard_latest_source_basis_detail_scope_mismatch")
        if source_basis.get("compact") is not True:
            blockers.append("dashboard_latest_source_basis_compact_not_true")
        if _safe_optional_int(source_basis.get("sample_limit")) != 0:
            blockers.append("dashboard_latest_source_basis_sample_limit_mismatch")
        if source_basis.get("include_full_samples") is not False:
            blockers.append("dashboard_latest_source_basis_include_full_samples_not_false")
        if _clean_live_text(source_basis.get("sample_policy_mode")) != "sample_omitted":
            blockers.append("dashboard_latest_source_basis_sample_policy_mode_mismatch")
        if expected_run_id and _clean_live_text(source_basis.get("run_id")) != expected_run_id:
            blockers.append("dashboard_latest_source_basis_run_id_mismatch")
        if expected_run_id and _clean_live_text(source_basis.get("latest_report_run_id")) != expected_run_id:
            blockers.append("dashboard_latest_source_basis_latest_report_run_id_mismatch")
        if expected_latest_canonical_run_id and _clean_live_text(source_basis.get("latest_canonical_run_id")) != expected_latest_canonical_run_id:
            blockers.append("dashboard_latest_source_basis_latest_canonical_run_id_mismatch")
        if expected_current_published_run_id and _clean_live_text(source_basis.get("current_published_run_id")) != expected_current_published_run_id:
            blockers.append("dashboard_latest_source_basis_current_published_run_id_mismatch")
        publication_status_summary = (
            payload.get("publication_status_summary")
            if isinstance(payload.get("publication_status_summary"), dict)
            else {}
        )
        publication_status = _clean_live_text(publication_status_summary.get("status"))
        if publication_status and _clean_live_text(source_basis.get("publication_status")) != publication_status:
            blockers.append("dashboard_latest_source_basis_publication_status_mismatch")
        publication_status_source = (
            source_basis.get("publication_status_source")
            if isinstance(source_basis.get("publication_status_source"), dict)
            else {}
        )
        if not publication_status_source:
            blockers.append("dashboard_latest_source_basis_publication_status_source_missing")
        else:
            if _clean_live_text(publication_status_source.get("source")) != "publication_registry_runtime_overlay":
                blockers.append("dashboard_latest_source_basis_publication_status_source_mismatch")
            if _clean_live_text(publication_status_source.get("truth_policy")) != "real_publication_registry_artifact_only":
                blockers.append("dashboard_latest_source_basis_publication_status_truth_policy_mismatch")
            if publication_status and _clean_live_text(publication_status_source.get("publication_status")) != publication_status:
                blockers.append("dashboard_latest_source_basis_publication_status_source_status_mismatch")
            if (
                expected_current_published_run_id
                and _clean_live_text(publication_status_source.get("current_published_run_id"))
                != expected_current_published_run_id
            ):
                blockers.append("dashboard_latest_source_basis_publication_status_source_current_published_mismatch")
            if publication_status_source.get("frontend_rendered_truth_allowed") is not False:
                blockers.append("dashboard_latest_source_basis_publication_status_source_frontend_truth_allowed")
            if _clean_live_text(publication_status_source.get("missing_value_policy")) != "preserve_missing_semantics":
                blockers.append("dashboard_latest_source_basis_publication_status_source_missing_policy_mismatch")
            if _clean_live_text(publication_status_source.get("business_value_synthesis")) != "forbidden":
                blockers.append("dashboard_latest_source_basis_publication_status_source_business_value_synthesis_mismatch")
            if not _clean_live_text(publication_status_source.get("digest")):
                blockers.append("dashboard_latest_source_basis_publication_status_source_digest_missing")
        source_daily_screening_run_id = _clean_live_text(source_basis.get("daily_screening_run_id"))
        if (
            expected_run_id and source_daily_screening_run_id
            and source_daily_screening_run_id != expected_run_id
            and not _is_allowed_live_candidate_run(
                expected_publication,
                source_daily_screening_run_id,
                payload=payload,
            )
            and not _is_known_runtime_diagnostic_run(
                expected_publication,
                source_daily_screening_run_id,
            )
        ):
            blockers.append("dashboard_latest_source_basis_daily_screening_run_id_mismatch")
        if expected_final_count is not None and _safe_optional_int(source_basis.get("final_count")) != expected_final_count:
            blockers.append("dashboard_latest_source_basis_final_count_mismatch")
        counts = payload.get("counts") if isinstance(payload.get("counts"), dict) else {}
        payload_final_count = _safe_optional_int(counts.get("final_count"))
        if payload_final_count is not None and _safe_optional_int(source_basis.get("final_count")) != payload_final_count:
            blockers.append("dashboard_latest_source_basis_payload_final_count_mismatch")
        for token_key in (
            "latest_report_file",
            "publication_registry_file",
            "scheduler_state_file",
            "database_file",
        ):
            token = _clean_live_text(source_basis.get(token_key))
            if not token or token in {"missing", "missing::empty_or_missing_wal"}:
                blockers.append(f"dashboard_latest_source_basis_{token_key}_missing")
        if _clean_live_text(source_basis.get("missing_value_policy")) != "preserve_missing_semantics":
            blockers.append("dashboard_latest_source_basis_missing_value_policy_mismatch")
        if _clean_live_text(source_basis.get("business_value_synthesis")) != "forbidden":
            blockers.append("dashboard_latest_source_basis_business_value_synthesis_mismatch")
        if source_basis.get("frontend_rendered_truth_allowed") is not False:
            blockers.append("dashboard_latest_source_basis_frontend_truth_allowed")
        if not _clean_live_text(source_basis.get("scope_contract_digest")):
            blockers.append("dashboard_latest_source_basis_scope_contract_digest_missing")
        if not _clean_live_text(source_basis.get("digest")):
            blockers.append("dashboard_latest_source_basis_digest_missing")

    if not source_contract:
        blockers.append("dashboard_latest_source_contract_missing")
    else:
        for key, expected in DASHBOARD_LATEST_SOURCE_CONTRACT_EXPECTED.items():
            if _clean_live_text(source_contract.get(key)) != expected:
                blockers.append(f"dashboard_latest_source_contract_{key}_mismatch")
        if source_contract.get("frontend_rendered_truth_allowed") is not False:
            blockers.append("dashboard_latest_source_contract_frontend_truth_allowed")
        if _safe_optional_int(source_contract.get("cache_basis_schema_version")) is None:
            blockers.append("dashboard_latest_source_contract_cache_schema_missing")
        if source_basis and _clean_live_text(source_contract.get("cache_basis_digest")) != _clean_live_text(
            source_basis.get("digest")
        ):
            blockers.append("dashboard_latest_source_contract_digest_mismatch")

    return {
        "available": bool(source_basis and cache_basis and source_contract),
        "source_basis": basis_summary,
        "source": _clean_live_text(source_basis.get("source")) or None,
        "payload_mode": _clean_live_text(source_basis.get("payload_mode")) or None,
        "detail_scope": _clean_live_text(source_basis.get("detail_scope")) or None,
        "run_id": _clean_live_text(source_basis.get("run_id")) or None,
        "latest_report_run_id": _clean_live_text(source_basis.get("latest_report_run_id")) or None,
        "latest_canonical_run_id": _clean_live_text(source_basis.get("latest_canonical_run_id")) or None,
        "current_published_run_id": _clean_live_text(source_basis.get("current_published_run_id")) or None,
        "latest_successful_run_id": _clean_live_text(source_basis.get("latest_successful_run_id")) or None,
        "publication_status": _clean_live_text(source_basis.get("publication_status")) or None,
        "publication_status_source": {
            "present": bool(
                isinstance(source_basis.get("publication_status_source"), dict)
                and source_basis.get("publication_status_source")
            ),
            "source": (
                source_basis.get("publication_status_source", {}).get("source")
                if isinstance(source_basis.get("publication_status_source"), dict)
                else None
            ),
            "publication_status": (
                source_basis.get("publication_status_source", {}).get("publication_status")
                if isinstance(source_basis.get("publication_status_source"), dict)
                else None
            ),
            "current_published_run_id": (
                source_basis.get("publication_status_source", {}).get("current_published_run_id")
                if isinstance(source_basis.get("publication_status_source"), dict)
                else None
            ),
        },
        "daily_screening_run_id": _clean_live_text(source_basis.get("daily_screening_run_id")) or None,
        "final_count": _safe_optional_int(source_basis.get("final_count")),
        "sample_limit": _safe_optional_int(source_basis.get("sample_limit")),
        "sample_policy_mode": _clean_live_text(source_basis.get("sample_policy_mode")) or None,
        "source_contract": {
            "present": bool(source_contract),
            "source": source_contract.get("source") if source_contract else None,
            "truth_policy": source_contract.get("truth_policy") if source_contract else None,
            "missing_value_policy": source_contract.get("missing_value_policy") if source_contract else None,
            "business_value_synthesis": source_contract.get("business_value_synthesis") if source_contract else None,
            "official_publication_truth": source_contract.get("official_publication_truth") if source_contract else None,
            "frontend_rendered_truth_allowed": source_contract.get("frontend_rendered_truth_allowed")
            if source_contract
            else None,
        },
    }, blockers


def _validate_live_dashboard_strategy_shadow_contract(
    payload: dict[str, Any],
    *,
    expected_publication: dict[str, Any],
) -> tuple[dict[str, Any], list[str]]:
    blockers: list[str] = []
    expected_final_count = _safe_optional_int((expected_publication.get("counts") or {}).get("final_count"))
    publish_reason_summary = (
        payload.get("publish_reason_summary")
        if isinstance(payload.get("publish_reason_summary"), dict)
        else {}
    )
    shadow = (
        publish_reason_summary.get("annual_return_gate_shadow")
        if isinstance(publish_reason_summary.get("annual_return_gate_shadow"), dict)
        else {}
    )
    if not shadow:
        return {"available": False}, blockers

    contract = shadow.get("truth_contract") if isinstance(shadow.get("truth_contract"), dict) else {}
    decision_summary = (
        shadow.get("decision_summary")
        if isinstance(shadow.get("decision_summary"), dict)
        else {}
    )
    if not contract:
        blockers.append("dashboard_annual_return_shadow_truth_contract_missing")
    if contract.get("truth_scope") != "strategy_shadow_observation":
        blockers.append("dashboard_annual_return_shadow_truth_scope_invalid")
    if contract.get("publish_truth") is not False:
        blockers.append("dashboard_annual_return_shadow_publish_truth_not_false")
    if contract.get("official_count_field") != "counts.final_count":
        blockers.append("dashboard_annual_return_shadow_official_count_field_invalid")
    if contract.get("shadow_count_field") != "shadow_final_count":
        blockers.append("dashboard_annual_return_shadow_shadow_count_field_invalid")
    if contract.get("business_value_synthesis") != "forbidden":
        blockers.append("dashboard_annual_return_shadow_business_synthesis_not_forbidden")
    if contract.get("direct_threshold_relaxation_recommended_now") is not False:
        blockers.append("dashboard_annual_return_shadow_direct_relaxation_not_false")
    if contract.get("missing_value_policy") != "preserve_missing_semantics":
        blockers.append("dashboard_annual_return_shadow_missing_policy_invalid")
    frozen_format = shadow.get("schema_version") == 2
    baseline_verified = (shadow.get("baseline_verification") or {}).get("baseline_verified") is True
    scenario_status = _clean_live_text(shadow.get("scenario_status"))
    expected_binding = expected_publication.get("report_binding") or {}
    report_binding = shadow.get("report_binding") or {}
    if expected_binding:
        binding_status = report_binding.get("status")
        if binding_status not in {"matched", "missing"}:
            blockers.append("dashboard_annual_return_shadow_report_binding_invalid")
        for field, expected in expected_binding.items():
            if report_binding.get(field) != expected:
                blockers.append(f"dashboard_annual_return_shadow_report_{field}_mismatch")
        if binding_status == "missing" and (baseline_verified or shadow.get("baseline_final_count") is not None):
            blockers.append("dashboard_annual_return_shadow_missing_report_has_baseline")
        if binding_status == "matched":
            if (shadow.get("source_publication_control") or {}).get("revision") != expected_binding.get("revision"):
                blockers.append("dashboard_annual_return_shadow_source_revision_mismatch")
            if baseline_verified:
                baseline_file = ((shadow.get("baseline_verification") or {}).get("source_files") or {}).get("report_json") or {}
                if baseline_file.get("sha256") != expected_binding.get("report_sha256"):
                    blockers.append("dashboard_annual_return_shadow_source_report_sha256_mismatch")
    if frozen_format:
        valid_statuses = {
            "baseline_not_verified": {"not_evaluated", "not_requested"},
            "baseline_verified": {"not_requested"},
            "scenario_inputs_incomplete": {"missing_tracker_history", "missing_validation_observations",
                                            "missing_autostock_observations", "missing_frozen_incremental_review"},
            "computed": {"computed"}, "computed_with_scenario_gaps": {"computed"},
        }
        status = shadow.get("status")
        if (shadow.get("scope") != "report_selection_from_frozen_complete_universe"
                or scenario_status not in valid_statuses.get(status, set())):
            blockers.append("dashboard_annual_return_shadow_frozen_status_invalid")
        if baseline_verified != (status != "baseline_not_verified"):
            blockers.append("dashboard_annual_return_shadow_baseline_status_inconsistent")
        if (decision_summary.get("status") != status or decision_summary.get("scenario_status") != scenario_status
                or decision_summary.get("baseline_verified") is not baseline_verified):
            blockers.append("dashboard_annual_return_shadow_decision_status_mismatch")
        if not baseline_verified and (shadow.get("baseline_final_count") is not None
                                      or decision_summary.get("baseline_final_count") is not None):
            blockers.append("dashboard_annual_return_shadow_unverified_baseline_has_count")
        if status == "scenario_inputs_incomplete":
            missing = shadow.get("missing_inputs") or {}
            if missing.get("stage") != scenario_status or not missing.get("codes"):
                blockers.append("dashboard_annual_return_shadow_missing_evidence_not_explained")
        if scenario_status != "computed":
            pending_fields = ("shadow_final_count", "net_final_count_delta", "shadow_added_count", "shadow_removed_count",
                              "direct_relieved_final_count", "portfolio_direct_relieved_count")
            if any(item.get(field) is not None for item in (shadow, decision_summary) for field in pending_fields):
                blockers.append("dashboard_annual_return_shadow_uncomputed_counts_not_null")
            if (shadow.get("strategy_comparison_allowed") is not False or decision_summary.get("available") is not False
                    or decision_summary.get("strategy_comparison_allowed") is not False):
                blockers.append("dashboard_annual_return_shadow_incomplete_marked_available")
        elif (_safe_optional_int(shadow.get("shadow_final_count"))
              != _safe_optional_int(decision_summary.get("shadow_final_count"))):
            blockers.append("dashboard_annual_return_shadow_decision_count_mismatch")
    if expected_final_count is not None and (not frozen_format or baseline_verified):
        if _safe_optional_int(shadow.get("baseline_final_count")) != expected_final_count:
            blockers.append("dashboard_annual_return_shadow_baseline_final_count_mismatch")
        if _safe_optional_int(decision_summary.get("baseline_final_count")) != expected_final_count:
            blockers.append("dashboard_annual_return_shadow_decision_baseline_count_mismatch")
    if (not frozen_format or scenario_status == "computed") and _safe_optional_int(shadow.get("shadow_final_count")) is None:
        blockers.append("dashboard_annual_return_shadow_shadow_final_count_missing")
    if _clean_live_text(decision_summary.get("recommended_action")) in {
        "",
        "direct_threshold_relaxation_recommended_now",
        "relax_threshold_now",
        "apply_to_production_now",
    }:
        blockers.append("dashboard_annual_return_shadow_recommended_action_unsafe")
    top_scenarios = [
        item
        for item in list(decision_summary.get("top_local_shadow_scenarios") or [])
        if isinstance(item, dict)
    ]
    direct_relief_scenarios = [
        item for item in top_scenarios if _safe_optional_int(item.get("direct_relieved_final_count")) not in (None, 0)
    ]
    scenario_quality_signal_available = 0
    scenario_quality_threshold_safe = 0
    scenario_forward_validation_available = 0
    scenario_forward_validation_threshold_safe = 0
    for item in direct_relief_scenarios:
        if not _clean_live_text(item.get("replacement_quality_direction")):
            blockers.append("dashboard_annual_return_shadow_local_quality_direction_missing")
            continue
        scenario_quality_signal_available += 1
        if _clean_live_text(item.get("replacement_quality_recommended_action")) in {
            "",
            "direct_threshold_relaxation_recommended_now",
            "relax_threshold_now",
            "apply_to_production_now",
        }:
            blockers.append("dashboard_annual_return_shadow_local_quality_action_unsafe")
        if item.get("replacement_quality_threshold_change_supported_now") is not False:
            blockers.append("dashboard_annual_return_shadow_local_quality_threshold_not_false")
        else:
            scenario_quality_threshold_safe += 1
        if not _clean_live_text(item.get("forward_validation_status")):
            blockers.append("dashboard_annual_return_shadow_local_forward_status_missing")
        else:
            scenario_forward_validation_available += 1
        if _clean_live_text(item.get("forward_validation_recommended_action")) in {
            "",
            "direct_threshold_relaxation_recommended_now",
            "relax_threshold_now",
            "apply_to_production_now",
        }:
            blockers.append("dashboard_annual_return_shadow_local_forward_action_unsafe")
        if item.get("forward_validation_threshold_change_supported_now") is not False:
            blockers.append("dashboard_annual_return_shadow_local_forward_threshold_not_false")
        else:
            scenario_forward_validation_threshold_safe += 1

    return {
        "available": True,
        "observation_status": shadow.get("status"),
        "scenario_status": shadow.get("scenario_status"),
        "baseline_verified": baseline_verified if frozen_format else None,
        "report_binding": report_binding,
        "truth_scope": contract.get("truth_scope") if contract else None,
        "publish_truth": contract.get("publish_truth") if contract else None,
        "official_count_field": contract.get("official_count_field") if contract else None,
        "shadow_count_field": contract.get("shadow_count_field") if contract else None,
        "direct_threshold_relaxation_recommended_now": (
            contract.get("direct_threshold_relaxation_recommended_now") if contract else None
        ),
        "baseline_final_count": _safe_optional_int(shadow.get("baseline_final_count")),
        "shadow_final_count": _safe_optional_int(shadow.get("shadow_final_count")),
        "decision_recommended_action": _clean_live_text(decision_summary.get("recommended_action")) or None,
        "top_local_shadow_scenario_count": int(len(top_scenarios)),
        "direct_relief_scenario_count": int(len(direct_relief_scenarios)),
        "scenario_quality_signal_available_count": int(scenario_quality_signal_available),
        "scenario_quality_threshold_safe_count": int(scenario_quality_threshold_safe),
        "scenario_forward_validation_available_count": int(scenario_forward_validation_available),
        "scenario_forward_validation_threshold_safe_count": int(scenario_forward_validation_threshold_safe),
    }, blockers


def _validate_live_reports_latest_source_contract(
    payload: dict[str, Any],
    *,
    expected_publication: dict[str, Any],
) -> tuple[dict[str, Any], list[str]]:
    blockers: list[str] = []
    expected_run_id = _clean_live_text(expected_publication.get("latest_report_run_id"))
    expected_latest_canonical_run_id = _clean_live_text(expected_publication.get("latest_canonical_run_id"))
    expected_current_published_run_id = _clean_live_text(expected_publication.get("current_published_run_id"))
    expected_final_count = _safe_optional_int((expected_publication.get("counts") or {}).get("final_count"))
    source_basis = payload.get("source_basis") if isinstance(payload.get("source_basis"), dict) else {}
    cache_basis = payload.get("cache_basis") if isinstance(payload.get("cache_basis"), dict) else {}
    source_contract = payload.get("source_contract") if isinstance(payload.get("source_contract"), dict) else {}

    basis_summary, basis_blockers = _summarize_frontend_cache_basis_contract(
        source_basis or cache_basis,
        artifact_key="reports_latest_source_basis",
        required_keys=REPORTS_LATEST_SOURCE_BASIS_REQUIRED_KEYS,
        expected_run_id=expected_run_id,
    )
    blockers.extend(basis_blockers)
    if not source_basis:
        blockers.append("reports_latest_source_basis_missing")
    if source_basis and cache_basis and source_basis != cache_basis:
        blockers.append("reports_latest_source_basis_cache_basis_mismatch")
    if source_basis:
        if _clean_live_text(source_basis.get("source")) != "reports_latest_current_published_or_latest_report":
            blockers.append("reports_latest_source_basis_source_mismatch")
        if _clean_live_text(source_basis.get("payload_source")) not in {
            "current_published_report",
            "latest_report_fallback",
        }:
            blockers.append("reports_latest_source_basis_payload_source_unexpected")
        if expected_latest_canonical_run_id and _clean_live_text(source_basis.get("latest_canonical_run_id")) != expected_latest_canonical_run_id:
            blockers.append("reports_latest_source_basis_latest_canonical_run_id_mismatch")
        if expected_current_published_run_id and _clean_live_text(source_basis.get("current_published_run_id")) != expected_current_published_run_id:
            blockers.append("reports_latest_source_basis_current_published_run_id_mismatch")
        if expected_final_count is not None and _safe_optional_int(source_basis.get("final_count")) != expected_final_count:
            blockers.append("reports_latest_source_basis_final_count_mismatch")
        latest_report_token = _clean_live_text(source_basis.get("latest_report_file"))
        publication_token = _clean_live_text(source_basis.get("publication_registry_file"))
        report_json_token = _clean_live_text(source_basis.get("report_json_artifact_token"))
        if not latest_report_token or latest_report_token in {"missing", "missing::empty_or_missing_wal"}:
            blockers.append("reports_latest_source_basis_latest_report_token_missing")
        if not publication_token or publication_token in {"missing", "missing::empty_or_missing_wal"}:
            blockers.append("reports_latest_source_basis_publication_registry_token_missing")
        if not report_json_token or report_json_token == "missing":
            blockers.append("reports_latest_source_basis_report_json_token_missing")
        if not _clean_live_text(source_basis.get("digest")):
            blockers.append("reports_latest_source_basis_digest_missing")

    if not source_contract:
        blockers.append("reports_latest_source_contract_missing")
    else:
        for key, expected in REPORTS_LATEST_SOURCE_CONTRACT_EXPECTED.items():
            if _clean_live_text(source_contract.get(key)) != expected:
                blockers.append(f"reports_latest_source_contract_{key}_mismatch")
        if source_contract.get("frontend_rendered_truth_allowed") is not False:
            blockers.append("reports_latest_source_contract_frontend_truth_allowed")
        if _safe_optional_int(source_contract.get("cache_basis_schema_version")) is None:
            blockers.append("reports_latest_source_contract_cache_schema_missing")
        if source_basis and _clean_live_text(source_contract.get("cache_basis_digest")) != _clean_live_text(source_basis.get("digest")):
            blockers.append("reports_latest_source_contract_digest_mismatch")

    return {
        "available": bool(source_basis and source_contract),
        "source_basis": basis_summary,
        "source": _clean_live_text(source_basis.get("source")) or None,
        "payload_source": _clean_live_text(source_basis.get("payload_source")) or None,
        "latest_canonical_run_id": _clean_live_text(source_basis.get("latest_canonical_run_id")) or None,
        "current_published_run_id": _clean_live_text(source_basis.get("current_published_run_id")) or None,
        "final_count": _safe_optional_int(source_basis.get("final_count")),
        "source_contract": {
            "present": bool(source_contract),
            "source": source_contract.get("source") if source_contract else None,
            "truth_policy": source_contract.get("truth_policy") if source_contract else None,
            "missing_value_policy": source_contract.get("missing_value_policy") if source_contract else None,
            "business_value_synthesis": source_contract.get("business_value_synthesis") if source_contract else None,
            "official_publication_truth": source_contract.get("official_publication_truth") if source_contract else None,
            "frontend_rendered_truth_allowed": source_contract.get("frontend_rendered_truth_allowed") if source_contract else None,
        },
    }, blockers


def _validate_live_excel_preview_source_contract(
    payload: dict[str, Any],
    *,
    expected_publication: dict[str, Any],
    requested_path: str,
    expected_artifact_key: str,
) -> tuple[dict[str, Any], list[str]]:
    blockers: list[str] = []
    expected_run_id = _clean_live_text(expected_publication.get("latest_report_run_id"))
    expected_latest_canonical_run_id = _clean_live_text(expected_publication.get("latest_canonical_run_id"))
    expected_current_published_run_id = _clean_live_text(expected_publication.get("current_published_run_id"))
    source_basis = payload.get("source_basis") if isinstance(payload.get("source_basis"), dict) else {}
    cache_basis = payload.get("cache_basis") if isinstance(payload.get("cache_basis"), dict) else {}
    source_contract = payload.get("source_contract") if isinstance(payload.get("source_contract"), dict) else {}

    basis_summary, basis_blockers = _summarize_frontend_cache_basis_contract(
        source_basis or cache_basis,
        artifact_key="excel_preview_source_basis",
        required_keys=EXCEL_PREVIEW_SOURCE_BASIS_REQUIRED_KEYS,
        expected_run_id=expected_run_id,
    )
    blockers.extend(basis_blockers)
    if not source_basis:
        blockers.append("excel_preview_source_basis_missing")
    if source_basis and cache_basis and source_basis != cache_basis:
        blockers.append("excel_preview_source_basis_cache_basis_mismatch")
    if source_basis:
        if _clean_live_text(source_basis.get("source")) != "project_excel_artifact_preview":
            blockers.append("excel_preview_source_basis_source_mismatch")
        if _clean_live_text(source_basis.get("requested_path")) != _clean_live_text(requested_path):
            blockers.append("excel_preview_source_basis_requested_path_mismatch")
        if expected_artifact_key and _clean_live_text(source_basis.get("artifact_key")) != expected_artifact_key:
            blockers.append("excel_preview_source_basis_artifact_key_mismatch")
        if source_basis.get("artifact_declared_in_latest_report") is not True:
            blockers.append("excel_preview_source_basis_artifact_not_declared")
        if expected_latest_canonical_run_id and _clean_live_text(source_basis.get("latest_canonical_run_id")) != expected_latest_canonical_run_id:
            blockers.append("excel_preview_source_basis_latest_canonical_run_id_mismatch")
        if expected_current_published_run_id and _clean_live_text(source_basis.get("current_published_run_id")) != expected_current_published_run_id:
            blockers.append("excel_preview_source_basis_current_published_run_id_mismatch")
        for token_key in ("file_token", "file_content_token", "latest_report_file", "publication_registry_file"):
            token = _clean_live_text(source_basis.get(token_key))
            if not token or token == "missing":
                blockers.append(f"excel_preview_source_basis_{token_key}_missing")
        if _safe_optional_int(source_basis.get("sheet_count")) is None:
            blockers.append("excel_preview_source_basis_sheet_count_missing")
        if _safe_optional_int(source_basis.get("workbook_sheet_count")) is None:
            blockers.append("excel_preview_source_basis_workbook_sheet_count_missing")
        sheet_summaries = source_basis.get("sheet_summaries")
        if not isinstance(sheet_summaries, list) or not sheet_summaries:
            blockers.append("excel_preview_source_basis_sheet_summaries_missing")
        if not _clean_live_text(source_basis.get("digest")):
            blockers.append("excel_preview_source_basis_digest_missing")

    if not source_contract:
        blockers.append("excel_preview_source_contract_missing")
    else:
        for key, expected in EXCEL_PREVIEW_SOURCE_CONTRACT_EXPECTED.items():
            if _clean_live_text(source_contract.get(key)) != expected:
                blockers.append(f"excel_preview_source_contract_{key}_mismatch")
        if source_contract.get("frontend_rendered_truth_allowed") is not False:
            blockers.append("excel_preview_source_contract_frontend_truth_allowed")
        if _safe_optional_int(source_contract.get("cache_basis_schema_version")) is None:
            blockers.append("excel_preview_source_contract_cache_schema_missing")
        if source_basis and _clean_live_text(source_contract.get("cache_basis_digest")) != _clean_live_text(source_basis.get("digest")):
            blockers.append("excel_preview_source_contract_digest_mismatch")

    return {
        "available": bool(source_basis and source_contract),
        "requested_path": requested_path,
        "source_basis": basis_summary,
        "source": _clean_live_text(source_basis.get("source")) or None,
        "run_id": _clean_live_text(source_basis.get("run_id")) or None,
        "artifact_key": _clean_live_text(source_basis.get("artifact_key")) or None,
        "artifact_declared_in_latest_report": source_basis.get("artifact_declared_in_latest_report")
        if source_basis
        else None,
        "sheet_count": _safe_optional_int(source_basis.get("sheet_count")),
        "workbook_sheet_count": _safe_optional_int(source_basis.get("workbook_sheet_count")),
        "response_sheet_count": _safe_optional_int(payload.get("sheet_count")),
        "source_contract": {
            "present": bool(source_contract),
            "source": source_contract.get("source") if source_contract else None,
            "truth_policy": source_contract.get("truth_policy") if source_contract else None,
            "missing_value_policy": source_contract.get("missing_value_policy") if source_contract else None,
            "business_value_synthesis": source_contract.get("business_value_synthesis") if source_contract else None,
            "official_publication_truth": source_contract.get("official_publication_truth") if source_contract else None,
            "frontend_rendered_truth_allowed": source_contract.get("frontend_rendered_truth_allowed") if source_contract else None,
        },
    }, blockers


def _validate_live_project_file_source_headers(
    headers: dict[str, str],
    *,
    expected_publication: dict[str, Any],
    expected_artifact_key: str,
) -> tuple[dict[str, Any], list[str]]:
    blockers: list[str] = []
    normalized = {str(key).lower(): str(value) for key, value in headers.items()}

    def header(name: str) -> str:
        return _clean_live_text(normalized.get(name.lower()))

    expected_run_id = _clean_live_text(expected_publication.get("latest_report_run_id"))
    expected_latest_canonical_run_id = _clean_live_text(expected_publication.get("latest_canonical_run_id"))
    expected_current_published_run_id = _clean_live_text(expected_publication.get("current_published_run_id"))

    expected_headers = {
        "X-Fund-ANA-Source": "project_file_artifact_binary",
        "X-Fund-ANA-Truth-Policy": "real_project_file_artifact_only",
        "X-Fund-ANA-Missing-Value-Policy": "preserve_missing_semantics",
        "X-Fund-ANA-Business-Value-Synthesis": "forbidden",
        "X-Fund-ANA-Official-Publication-Truth": "false_binary_transfer_only",
        "X-Fund-ANA-Frontend-Rendered-Truth-Allowed": "false",
    }
    for key, expected in expected_headers.items():
        if header(key) != expected:
            blockers.append(f"project_file_source_header_{key.lower().replace('-', '_')}_mismatch")

    if expected_run_id and header("X-Fund-ANA-Run-ID") != expected_run_id:
        blockers.append("project_file_source_header_run_id_mismatch")
    if expected_artifact_key and header("X-Fund-ANA-Artifact-Key") != expected_artifact_key:
        blockers.append("project_file_source_header_artifact_key_mismatch")
    if header("X-Fund-ANA-Artifact-Declared") != "true":
        blockers.append("project_file_source_header_artifact_declared_not_true")
    if expected_latest_canonical_run_id and header("X-Fund-ANA-Latest-Canonical-Run-ID") != expected_latest_canonical_run_id:
        blockers.append("project_file_source_header_latest_canonical_run_id_mismatch")
    if expected_current_published_run_id and header("X-Fund-ANA-Current-Published-Run-ID") != expected_current_published_run_id:
        blockers.append("project_file_source_header_current_published_run_id_mismatch")
    for key in (
        "X-Fund-ANA-File-Token",
        "X-Fund-ANA-File-Content-Token",
        "X-Fund-ANA-Source-Basis-Digest",
        "X-Fund-ANA-Source-Basis-Schema-Version",
    ):
        value = header(key)
        if not value or value == "missing":
            blockers.append(f"project_file_source_header_{key.lower().replace('-', '_')}_missing")

    return {
        "available": not blockers,
        "source": header("X-Fund-ANA-Source") or None,
        "truth_policy": header("X-Fund-ANA-Truth-Policy") or None,
        "business_value_synthesis": header("X-Fund-ANA-Business-Value-Synthesis") or None,
        "frontend_rendered_truth_allowed": header("X-Fund-ANA-Frontend-Rendered-Truth-Allowed") or None,
        "run_id": header("X-Fund-ANA-Run-ID") or None,
        "artifact_key": header("X-Fund-ANA-Artifact-Key") or None,
        "artifact_declared": header("X-Fund-ANA-Artifact-Declared") or None,
        "file_token_present": bool(header("X-Fund-ANA-File-Token")),
        "file_content_token_present": bool(header("X-Fund-ANA-File-Content-Token")),
        "source_basis_digest_present": bool(header("X-Fund-ANA-Source-Basis-Digest")),
    }, blockers


def _validate_live_scheduler_summary_payload(
    payload: dict[str, Any],
    *,
    expected_publication: dict[str, Any],
) -> tuple[dict[str, Any], list[str]]:
    blockers: list[str] = []
    daily_run = payload.get("daily_run") if isinstance(payload.get("daily_run"), dict) else {}
    expected_run_id = _clean_live_text(expected_publication.get("latest_report_run_id"))
    expected_final_count = _safe_optional_int((expected_publication.get("counts") or {}).get("final_count"))

    if "manual_run_history_count" not in payload:
        blockers.append("scheduler_summary_manual_run_history_count_missing")
    if daily_run:
        if _clean_live_text(daily_run.get("status")) == "success" and daily_run.get("screening_triggered") is True:
            screening_run_id = _clean_live_text(daily_run.get("screening_run_id"))
            allowed_candidate_run = _is_allowed_live_candidate_run(
                expected_publication,
                screening_run_id,
                payload=payload,
            )
            known_runtime_run = _is_known_runtime_diagnostic_run(expected_publication, screening_run_id)
            expected_daily_final_count = (
                _expected_runtime_run_final_count(expected_publication, screening_run_id)
                if known_runtime_run
                else expected_final_count
            )
            if expected_run_id and screening_run_id != expected_run_id and not allowed_candidate_run and not known_runtime_run:
                blockers.append("scheduler_summary_daily_screening_run_id_mismatch")
            if (
                expected_daily_final_count is not None
                and not allowed_candidate_run
                and _safe_optional_int(daily_run.get("screening_final_count")) != expected_daily_final_count
            ):
                blockers.append("scheduler_summary_daily_screening_final_count_mismatch")
        if daily_run.get("manual_intervention_hint") is True:
            blockers.append("scheduler_summary_daily_manual_intervention_hint")
    else:
        blockers.append("scheduler_summary_daily_run_missing")

    return {
        "available": True,
        "manual_run_history_count": _safe_int(payload.get("manual_run_history_count")),
        "daily_run_status": _clean_live_text(daily_run.get("status")) or None,
        "daily_screening_run_id": _clean_live_text(daily_run.get("screening_run_id")) or None,
        "daily_screening_final_count": _safe_optional_int(daily_run.get("screening_final_count")),
    }, blockers


def _validate_live_api_settings_status(
    payload: dict[str, Any],
    *,
    settings: Settings,
) -> tuple[dict[str, Any], list[str], list[str]]:
    blockers: list[str] = []
    warnings: list[str] = []
    configured = payload.get("configured") if isinstance(payload.get("configured"), dict) else {}
    runtime = payload.get("runtime") if isinstance(payload.get("runtime"), dict) else {}
    scheduler = payload.get("scheduler") if isinstance(payload.get("scheduler"), dict) else {}
    scheduler_runtime = (
        scheduler.get("runtime")
        if isinstance(scheduler.get("runtime"), dict)
        else {}
    )
    frontend = payload.get("frontend") if isinstance(payload.get("frontend"), dict) else {}
    runtime_matches = (
        payload.get("runtime_matches_config")
        if isinstance(payload.get("runtime_matches_config"), dict)
        else {}
    )
    scheduler_matches = (
        scheduler.get("runtime_matches_config")
        if isinstance(scheduler.get("runtime_matches_config"), dict)
        else {}
    )
    expected_tiantian = str(getattr(settings, "tiantian_base_url", "") or "").strip()
    expected_autostock = str(getattr(settings, "autostock_base_url", "") or "").strip()
    configured_tiantian = _clean_live_text(configured.get("tiantian_base_url"))
    runtime_tiantian = _clean_live_text(runtime.get("tiantian_base_url"))
    scheduler_tiantian = _clean_live_text(scheduler_runtime.get("tiantian_base_url"))
    configured_autostock = _clean_live_text(configured.get("autostock_base_url"))
    runtime_autostock = _clean_live_text(runtime.get("autostock_base_url"))
    scheduler_autostock = _clean_live_text(scheduler_runtime.get("autostock_base_url"))

    if _clean_live_text(payload.get("status")) != "ok":
        blockers.append("api_settings_status_not_ok")
    if configured_tiantian != expected_tiantian:
        blockers.append("api_settings_configured_tiantian_mismatch")
    if runtime_tiantian != expected_tiantian:
        blockers.append("api_settings_runtime_tiantian_mismatch")
    if configured_autostock != expected_autostock:
        blockers.append("api_settings_configured_autostock_mismatch")
    if runtime_autostock != expected_autostock:
        blockers.append("api_settings_runtime_autostock_mismatch")
    if runtime_matches.get("tiantian_base_url") is not True:
        blockers.append("api_settings_runtime_tiantian_not_matched")
    if runtime_matches.get("autostock_base_url") is not True:
        blockers.append("api_settings_runtime_autostock_not_matched")

    if frontend.get("homepage_api_source") != "/api/settings/apis":
        blockers.append("api_settings_homepage_source_contract_mismatch")
    if frontend.get("homepage_health_fallback_allowed") is not False:
        blockers.append("api_settings_homepage_health_fallback_allowed")
    if frontend.get("homepage_historical_data_sources_allowed") is not False:
        blockers.append("api_settings_homepage_historical_data_sources_allowed")

    if scheduler.get("runtime_evidence_available") is True:
        if scheduler_tiantian != expected_tiantian:
            blockers.append("api_settings_scheduler_tiantian_mismatch")
        if scheduler_autostock != expected_autostock:
            blockers.append("api_settings_scheduler_autostock_mismatch")
        if scheduler_matches.get("tiantian_base_url") is not True:
            blockers.append("api_settings_scheduler_tiantian_not_matched")
        if scheduler_matches.get("autostock_base_url") is not True:
            blockers.append("api_settings_scheduler_autostock_not_matched")
    else:
        warnings.append("api_settings_scheduler_runtime_evidence_missing")

    return {
        "available": True,
        "configured_tiantian_base_url": configured_tiantian or None,
        "runtime_tiantian_base_url": runtime_tiantian or None,
        "scheduler_tiantian_base_url": scheduler_tiantian or None,
        "scheduler_runtime_evidence_available": scheduler.get("runtime_evidence_available"),
        "scheduler_runtime_captured_at": scheduler_runtime.get("captured_at"),
        "homepage_api_source": frontend.get("homepage_api_source"),
    }, blockers, warnings


def _endpoint_by_path(source_group: dict[str, Any]) -> dict[str, dict[str, Any]]:
    endpoints = source_group.get("endpoints")
    if not isinstance(endpoints, list):
        return {}
    result: dict[str, dict[str, Any]] = {}
    for endpoint in endpoints:
        if not isinstance(endpoint, dict):
            continue
        path = _clean_live_text(endpoint.get("path"))
        if path:
            result[path] = endpoint
    return result


def _validate_live_api_info_contract(
    payload: dict[str, Any],
    *,
    settings: Settings,
) -> tuple[dict[str, Any], list[str]]:
    blockers: list[str] = []
    endpoint_contracts = (
        payload.get("endpoint_contracts")
        if isinstance(payload.get("endpoint_contracts"), list)
        else []
    )
    contract_by_endpoint = {
        _clean_live_text(item.get("endpoint")): item
        for item in endpoint_contracts
        if isinstance(item, dict) and _clean_live_text(item.get("endpoint"))
    }
    contract = (
        payload.get("upstream_api_source_contract")
        if isinstance(payload.get("upstream_api_source_contract"), dict)
        else {}
    )
    source_groups = (
        contract.get("source_groups")
        if isinstance(contract.get("source_groups"), dict)
        else {}
    )
    tiantian = (
        source_groups.get("tiantian")
        if isinstance(source_groups.get("tiantian"), dict)
        else {}
    )
    autostock = (
        source_groups.get("autostock")
        if isinstance(source_groups.get("autostock"), dict)
        else {}
    )
    endpoint_index = (
        contract.get("endpoint_index")
        if isinstance(contract.get("endpoint_index"), dict)
        else {}
    )
    expected_tiantian = str(getattr(settings, "tiantian_base_url", "") or "").strip().rstrip("/")
    expected_autostock = str(getattr(settings, "autostock_base_url", "") or "").strip().rstrip("/")
    tiantian_base_url = _clean_live_text(tiantian.get("current_configured_base_url")).rstrip("/")
    tiantian_runtime_base_url = _clean_live_text(tiantian.get("runtime_base_url")).rstrip("/")
    autostock_base_url = _clean_live_text(autostock.get("current_configured_base_url")).rstrip("/")
    autostock_runtime_base_url = _clean_live_text(autostock.get("runtime_base_url")).rstrip("/")

    if _clean_live_text(payload.get("status")) != "ok":
        blockers.append("api_info_status_not_ok")
    if _clean_live_text(contract.get("status")) != "ok":
        blockers.append("api_info_upstream_contract_status_not_ok")
    if _clean_live_text(contract.get("contract_scope")) != "real_upstream_business_data_sources":
        blockers.append("api_info_upstream_contract_scope_mismatch")
    if not contract:
        blockers.append("api_info_upstream_contract_missing")
        return {"available": False}, blockers

    for endpoint in LIVE_API_ENDPOINTS:
        contract_key = endpoint.split("?", 1)[0]
        if endpoint in contract_by_endpoint:
            endpoint_contract = contract_by_endpoint[endpoint]
        else:
            endpoint_contract = contract_by_endpoint.get(contract_key)
        if not endpoint_contract:
            blockers.append(f"api_info_endpoint_contract_missing:{endpoint}")
            continue
        if endpoint_contract.get("default_heavy") is not False:
            blockers.append(f"api_info_live_endpoint_contract_heavy:{endpoint}")

    if tiantian_base_url != expected_tiantian:
        blockers.append("api_info_tiantian_contract_base_url_mismatch")
    if tiantian_runtime_base_url != expected_tiantian:
        blockers.append("api_info_tiantian_contract_runtime_url_mismatch")
    if autostock_base_url != expected_autostock:
        blockers.append("api_info_autostock_contract_base_url_mismatch")
    if autostock_runtime_base_url != expected_autostock:
        blockers.append("api_info_autostock_contract_runtime_url_mismatch")

    tiantian_index = endpoint_index.get("tiantian") if isinstance(endpoint_index.get("tiantian"), list) else []
    autostock_index = endpoint_index.get("autostock") if isinstance(endpoint_index.get("autostock"), list) else []
    tiantian_endpoints = _endpoint_by_path(tiantian)
    autostock_endpoints = _endpoint_by_path(autostock)
    for path in LIVE_API_REQUIRED_TIANTIAN_ENDPOINTS:
        if path not in tiantian_index:
            blockers.append(f"api_info_tiantian_endpoint_index_missing:{path}")
        if path not in tiantian_endpoints:
            blockers.append(f"api_info_tiantian_endpoint_contract_missing:{path}")
    for path in LIVE_API_REQUIRED_AUTOSTOCK_ENDPOINTS:
        if path not in autostock_index:
            blockers.append(f"api_info_autostock_endpoint_index_missing:{path}")
        if path not in autostock_endpoints:
            blockers.append(f"api_info_autostock_endpoint_contract_missing:{path}")

    probe = tiantian.get("read_only_probe") if isinstance(tiantian.get("read_only_probe"), dict) else {}
    if probe.get("endpoint") != "/api/settings/apis/probe":
        blockers.append("api_info_tiantian_probe_endpoint_mismatch")
    if probe.get("writes_database") is not False:
        blockers.append("api_info_tiantian_probe_writes_database")
    if probe.get("writes_business_artifacts") is not False:
        blockers.append("api_info_tiantian_probe_writes_business_artifacts")

    rank_contract = tiantian_endpoints.get("/fundMNRank") or {}
    rank_pagination = (
        rank_contract.get("pagination")
        if isinstance(rank_contract.get("pagination"), dict)
        else {}
    )
    if rank_pagination.get("required") is not True:
        blockers.append("api_info_fundmnrank_pagination_not_required")
    if rank_contract.get("source_evidence_required") is not True:
        blockers.append("api_info_fundmnrank_source_evidence_not_required")

    nav_contract = tiantian_endpoints.get("/fundMNHisNetList") or {}
    nav_params = (
        nav_contract.get("required_params")
        if isinstance(nav_contract.get("required_params"), dict)
        else {}
    )
    nav_forbidden = (
        nav_contract.get("forbidden_param_aliases")
        if isinstance(nav_contract.get("forbidden_param_aliases"), list)
        else []
    )
    nav_pagination = (
        nav_contract.get("pagination")
        if isinstance(nav_contract.get("pagination"), dict)
        else {}
    )
    if "pagesize" not in nav_params:
        blockers.append("api_info_nav_history_pagesize_param_missing")
    if "pageSize" in nav_params or "pageSize" not in nav_forbidden:
        blockers.append("api_info_nav_history_pageSize_alias_not_forbidden")
    if nav_pagination.get("page_size_param") != "pagesize":
        blockers.append("api_info_nav_history_pagination_pagesize_mismatch")

    quality_policy = (
        contract.get("quality_policy")
        if isinstance(contract.get("quality_policy"), dict)
        else {}
    )
    if quality_policy.get("fake_data_policy") != "forbidden":
        blockers.append("api_info_fake_data_policy_not_forbidden")
    if quality_policy.get("missing_value_policy") != "preserve_missing_semantics":
        blockers.append("api_info_missing_value_policy_mismatch")
    if quality_policy.get("threshold_policy") != "do not loosen production safety gates to increase final counts":
        blockers.append("api_info_threshold_policy_mismatch")

    return {
        "available": True,
        "schema_version": contract.get("schema_version"),
        "tiantian_base_url": tiantian_base_url or None,
        "autostock_base_url": autostock_base_url or None,
        "tiantian_endpoint_count": len(tiantian_endpoints),
        "autostock_endpoint_count": len(autostock_endpoints),
        "endpoint_contract_count": len(contract_by_endpoint),
        "live_api_endpoint_contracts_present": True,
        "fake_data_policy": quality_policy.get("fake_data_policy"),
        "missing_value_policy": quality_policy.get("missing_value_policy"),
        "nav_history_page_size_param": nav_pagination.get("page_size_param"),
        "probe_writes_database": probe.get("writes_database"),
    }, blockers


def _live_api_truth(
    settings: Settings,
    *,
    base_url: str,
    timeout_seconds: float,
    expected_publication: dict[str, Any],
) -> dict[str, Any]:
    blockers: list[str] = []
    warnings: list[str] = []
    endpoint_results: dict[str, Any] = {}
    latest_run_id = str(expected_publication.get("latest_report_run_id") or "").strip()
    latest_final_count = _safe_int((expected_publication.get("counts") or {}).get("final_count"))
    scheduler_detail_summary: dict[str, Any] | None = None
    scheduler_header_pending: list[tuple[str, dict[str, Any]]] = []
    for endpoint in LIVE_API_ENDPOINTS:
        status_code, payload, error, elapsed = _fetch_live_json(base_url, endpoint, timeout_seconds)
        endpoint_key = (
            "dashboard_reports"
            if endpoint.startswith("/api/dashboard/latest/reports")
            else "dashboard"
            if endpoint.startswith("/api/dashboard")
            else "reports"
            if endpoint.startswith("/api/reports")
            else "publication"
            if endpoint.startswith("/api/publication")
            else "workflow"
            if endpoint.startswith("/api/status/summary")
            else "health"
            if endpoint.startswith("/api/health")
            else "bootstrap"
            if endpoint.startswith("/api/bootstrap")
            else "scheduler_summary"
            if endpoint.startswith("/api/scheduler/status") and not endpoint.startswith("/api/scheduler/status/detail")
            else "scheduler_detail"
            if endpoint.startswith("/api/scheduler/status/detail")
            else "api_settings"
            if endpoint.startswith("/api/settings/apis")
            else "info"
        )
        publication_status_summary: dict[str, Any] | None = None
        scheduler_status_summary: dict[str, Any] | None = None
        scheduler_header_summary: dict[str, Any] | None = None
        api_settings_summary: dict[str, Any] | None = None
        api_info_contract_summary: dict[str, Any] | None = None
        workflow_status_summary: dict[str, Any] | None = None
        publication_registry_summary: dict[str, Any] | None = None
        dashboard_reports_summary: dict[str, Any] | None = None
        dashboard_strategy_shadow_summary: dict[str, Any] | None = None
        dashboard_overview_source_summary: dict[str, Any] | None = None
        reports_latest_source_summary: dict[str, Any] | None = None
        scheduler_detail_source_summary: dict[str, Any] | None = None
        dashboard_latest_source_summary: dict[str, Any] | None = None
        lightweight_status_source_summary: dict[str, Any] | None = None
        scheduler_compact_summary: dict[str, Any] | None = None
        autostock_directory_summary: dict[str, Any] | None = None
        weekly_maintenance_warning_summary: dict[str, Any] | None = None
        endpoint_blockers: list[str] = []
        endpoint_warnings: list[str] = []
        endpoint_results[endpoint] = {
            "status_code": status_code,
            "elapsed_seconds": elapsed,
            "error": error,
            "summary": {
                "version": (payload.get("version") or {}).get("code_version"),
                "run_id": payload.get("run_id"),
                "final_count": payload.get("final_count"),
                "manual_intervention_required": (payload.get("automation_autonomy") or {}).get(
                    "manual_intervention_required"
                ),
                "current_task": payload.get("current_task"),
            },
        }
        if status_code != 200:
            blockers.append(f"live_api_non_200:{endpoint}")
            continue
        if endpoint == "/api/info":
            api_info_contract_summary, endpoint_blockers = _validate_live_api_info_contract(
                payload,
                settings=settings,
            )
            blockers.extend(endpoint_blockers)
        elif endpoint.startswith("/api/status/summary"):
            workflow_status_summary, endpoint_blockers = _validate_live_workflow_status_summary(
                payload,
                expected_publication=expected_publication,
            )
            blockers.extend(endpoint_blockers)
            lightweight_status_source_summary, endpoint_blockers = (
                _validate_live_lightweight_status_source_contract(
                    payload,
                    expected_publication=expected_publication,
                    artifact_key="workflow_status_summary_source",
                    expected_source="workflow_status_summary_runtime_manager",
                    expected_payload_mode="workflow_summary",
                    expected_official_publication_truth="false_workflow_runtime_diagnostics_only",
                )
            )
            blockers.extend(endpoint_blockers)
        elif endpoint.startswith("/api/publication/summary"):
            publication_registry_summary, endpoint_blockers = _validate_live_publication_registry_summary(
                payload,
                expected_publication=expected_publication,
            )
            blockers.extend(endpoint_blockers)
        elif endpoint.startswith(("/api/dashboard", "/api/reports")):
            publication_status_summary, endpoint_blockers = _validate_live_publication_status(
                endpoint_key,
                payload,
                expected_publication=expected_publication,
                require_top_level=True,
            )
            blockers.extend(endpoint_blockers)
            if endpoint.startswith("/api/dashboard/overview"):
                dashboard_overview_source_summary, endpoint_blockers = (
                    _validate_live_dashboard_overview_source_contract(
                        payload,
                        expected_publication=expected_publication,
                    )
                )
                blockers.extend(endpoint_blockers)
            if endpoint.startswith("/api/dashboard/latest?"):
                dashboard_latest_source_summary, endpoint_blockers = (
                    _validate_live_dashboard_latest_source_contract(
                        payload,
                        expected_publication=expected_publication,
                    )
                )
                blockers.extend(endpoint_blockers)
            if endpoint.startswith("/api/dashboard/latest/reports"):
                dashboard_reports_summary, endpoint_blockers = _validate_live_dashboard_reports_payload(
                    payload,
                    expected_publication=expected_publication,
                )
                blockers.extend(endpoint_blockers)
                dashboard_strategy_shadow_summary, endpoint_blockers = (
                    _validate_live_dashboard_strategy_shadow_contract(
                        payload,
                        expected_publication=expected_publication,
                    )
                )
                blockers.extend(endpoint_blockers)
            if endpoint.startswith("/api/reports/latest"):
                reports_latest_source_summary, endpoint_blockers = (
                    _validate_live_reports_latest_source_contract(
                        payload,
                        expected_publication=expected_publication,
                    )
                )
                blockers.extend(endpoint_blockers)
                artifacts = payload.get("artifacts") if isinstance(payload.get("artifacts"), dict) else {}
                excel_preview_path = _clean_live_text(artifacts.get("filtered_excel"))
                if not excel_preview_path:
                    blockers.append("excel_preview_probe_filtered_excel_artifact_missing")
                else:
                    preview_endpoint = "/api/preview-excel?" + urllib.parse.urlencode(
                        {"path": excel_preview_path}
                    )
                    preview_status, preview_payload, preview_error, preview_elapsed = _fetch_live_json(
                        base_url,
                        preview_endpoint,
                        timeout_seconds,
                    )
                    preview_summary: dict[str, Any] | None = None
                    preview_blockers: list[str] = []
                    if preview_status != 200:
                        blockers.append("live_api_non_200:/api/preview-excel")
                    else:
                        preview_summary, preview_blockers = _validate_live_excel_preview_source_contract(
                            preview_payload,
                            expected_publication=expected_publication,
                            requested_path=excel_preview_path,
                            expected_artifact_key="filtered_excel",
                        )
                        blockers.extend(preview_blockers)
                    endpoint_results[preview_endpoint] = {
                        "status_code": preview_status,
                        "elapsed_seconds": preview_elapsed,
                        "error": preview_error,
                        "summary": {
                            "run_id": (
                                (preview_payload.get("source_basis") or {}).get("run_id")
                                if isinstance(preview_payload.get("source_basis"), dict)
                                else None
                            ),
                            "sheet_count": preview_payload.get("sheet_count"),
                            "path": preview_payload.get("path"),
                            "excel_preview_source": preview_summary,
                        },
                    }
                    if "pytest" not in sys.modules:
                        file_endpoint = "/api/files/" + urllib.parse.quote(excel_preview_path, safe="/")
                        file_status, file_headers, file_error, file_elapsed = _fetch_live_head(
                            base_url,
                            file_endpoint,
                            timeout_seconds,
                        )
                        file_summary: dict[str, Any] | None = None
                        file_blockers: list[str] = []
                        if file_status != 200:
                            blockers.append("live_api_non_200:/api/files")
                        else:
                            file_summary, file_blockers = _validate_live_project_file_source_headers(
                                file_headers,
                                expected_publication=expected_publication,
                                expected_artifact_key="filtered_excel",
                            )
                            blockers.extend(file_blockers)
                        endpoint_results[file_endpoint] = {
                            "status_code": file_status,
                            "elapsed_seconds": file_elapsed,
                            "error": file_error,
                            "summary": {
                                "project_file_source": file_summary,
                            },
                        }
        elif endpoint.startswith("/api/scheduler"):
            publication_status_summary, endpoint_blockers = _validate_live_publication_status(
                endpoint_key,
                payload,
                expected_publication=expected_publication,
                require_top_level=False,
            )
            blockers.extend(endpoint_blockers)
            scheduler_status_summary, endpoint_blockers = _validate_live_scheduler_status(
                payload,
                expected_publication=expected_publication,
            )
            blockers.extend(endpoint_blockers)
            scheduler_detail_summary = scheduler_status_summary
            if endpoint.startswith("/api/scheduler/status/detail"):
                scheduler_detail_source_summary, endpoint_blockers = (
                    _validate_live_scheduler_detail_source_contract(
                        payload,
                        expected_publication=expected_publication,
                    )
                )
                blockers.extend(endpoint_blockers)
            if endpoint.startswith("/api/scheduler/status") and not endpoint.startswith("/api/scheduler/status/detail"):
                scheduler_compact_summary, endpoint_blockers = _validate_live_scheduler_summary_payload(
                    payload,
                    expected_publication=expected_publication,
                )
                blockers.extend(endpoint_blockers)
                lightweight_status_source_summary, endpoint_blockers = (
                    _validate_live_lightweight_status_source_contract(
                        payload,
                        expected_publication=expected_publication,
                        artifact_key="scheduler_status_compact_source",
                        expected_source="scheduler_status_compact_runtime_state",
                        expected_payload_mode="scheduler_status_compact",
                        expected_official_publication_truth="false_scheduler_diagnostics_only",
                    )
                )
                blockers.extend(endpoint_blockers)
        elif endpoint.startswith("/api/settings/apis"):
            api_settings_summary, endpoint_blockers, endpoint_warnings = _validate_live_api_settings_status(
                payload,
                settings=settings,
            )
            blockers.extend(endpoint_blockers)
            warnings.extend(endpoint_warnings)
        elif endpoint.startswith(("/api/health", "/api/bootstrap")):
            if endpoint.startswith("/api/health"):
                lightweight_status_source_summary, endpoint_blockers = (
                    _validate_live_lightweight_status_source_contract(
                        payload,
                        expected_publication=expected_publication,
                        artifact_key="health_summary_source",
                        expected_source="health_summary_scheduler_publication_runtime_artifacts",
                        expected_payload_mode="health_summary",
                        expected_official_publication_truth="false_health_diagnostics_only",
                    )
                )
                blockers.extend(endpoint_blockers)
            if endpoint.startswith("/api/bootstrap"):
                lightweight_status_source_summary, endpoint_blockers = (
                    _validate_live_lightweight_status_source_contract(
                        payload,
                        expected_publication=expected_publication,
                        artifact_key="bootstrap_summary_source",
                        expected_source="bootstrap_summary_health_workflow_runtime_artifacts",
                        expected_payload_mode="bootstrap_summary",
                        expected_official_publication_truth="false_bootstrap_diagnostics_only",
                    )
                )
                blockers.extend(endpoint_blockers)
            scheduler_header_summary, endpoint_blockers = _validate_live_scheduler_header_status(
                endpoint_key,
                payload,
                expected_scheduler_status=scheduler_detail_summary,
            )
            blockers.extend(endpoint_blockers)
            autostock_directory_summary, endpoint_blockers, endpoint_warnings = (
                _validate_live_autostock_directory_source_summary(endpoint_key, payload)
            )
            blockers.extend(endpoint_blockers)
            warnings.extend(endpoint_warnings)
            weekly_maintenance_warning_summary, endpoint_blockers, endpoint_warnings = (
                _validate_live_weekly_maintenance_warning_truth_summary(endpoint_key, payload)
            )
            blockers.extend(endpoint_blockers)
            warnings.extend(endpoint_warnings)
            if scheduler_detail_summary is None:
                scheduler_header_pending.append((endpoint_key, payload))
        if workflow_status_summary is not None:
            endpoint_results[endpoint]["summary"]["workflow_status"] = workflow_status_summary
        if publication_registry_summary is not None:
            endpoint_results[endpoint]["summary"]["publication_registry"] = publication_registry_summary
        if dashboard_reports_summary is not None:
            endpoint_results[endpoint]["summary"]["dashboard_reports"] = dashboard_reports_summary
        if dashboard_strategy_shadow_summary is not None:
            endpoint_results[endpoint]["summary"]["dashboard_strategy_shadow"] = dashboard_strategy_shadow_summary
        if dashboard_overview_source_summary is not None:
            endpoint_results[endpoint]["summary"]["dashboard_overview_source"] = dashboard_overview_source_summary
        if reports_latest_source_summary is not None:
            endpoint_results[endpoint]["summary"]["reports_latest_source"] = reports_latest_source_summary
        if dashboard_latest_source_summary is not None:
            endpoint_results[endpoint]["summary"]["dashboard_latest_source"] = dashboard_latest_source_summary
        if scheduler_compact_summary is not None:
            endpoint_results[endpoint]["summary"]["scheduler_compact"] = scheduler_compact_summary
        if scheduler_detail_source_summary is not None:
            endpoint_results[endpoint]["summary"]["scheduler_detail_source"] = scheduler_detail_source_summary
        if lightweight_status_source_summary is not None:
            endpoint_results[endpoint]["summary"]["lightweight_status_source"] = lightweight_status_source_summary
        if publication_status_summary is not None:
            endpoint_results[endpoint]["summary"]["publication_status"] = publication_status_summary
        if scheduler_status_summary is not None:
            endpoint_results[endpoint]["summary"]["scheduler_status"] = scheduler_status_summary
        if scheduler_header_summary is not None:
            endpoint_results[endpoint]["summary"]["scheduler_header_status"] = scheduler_header_summary
        if autostock_directory_summary is not None:
            endpoint_results[endpoint]["summary"]["autostock_directory_source"] = autostock_directory_summary
        if weekly_maintenance_warning_summary is not None:
            endpoint_results[endpoint]["summary"]["weekly_maintenance_warning_truth"] = (
                weekly_maintenance_warning_summary
            )
        if api_settings_summary is not None:
            endpoint_results[endpoint]["summary"]["api_settings"] = api_settings_summary
        if api_info_contract_summary is not None:
            endpoint_results[endpoint]["summary"]["upstream_api_source_contract"] = api_info_contract_summary
        if endpoint.startswith("/api/dashboard") and latest_run_id and payload.get("run_id") != latest_run_id:
            blockers.append("dashboard_latest_run_id_mismatch")
        if endpoint.startswith("/api/reports") and latest_run_id and payload.get("run_id") != latest_run_id:
            blockers.append("reports_latest_run_id_mismatch")
        if endpoint.startswith("/api/reports") and latest_final_count and _safe_int(payload.get("final_count")) != latest_final_count:
            blockers.append("reports_latest_final_count_mismatch")
    if scheduler_detail_summary is not None:
        for endpoint_key, payload in scheduler_header_pending:
            _, endpoint_blockers = _validate_live_scheduler_header_status(
                endpoint_key,
                payload,
                expected_scheduler_status=scheduler_detail_summary,
            )
            blockers.extend(endpoint_blockers)
    return {
        "status": _classify_status(blockers, warnings),
        "blockers": blockers,
        "warnings": warnings,
        "base_url": base_url,
        "endpoints": endpoint_results,
        "truth_note": "Live API truth is sampled from the running web service and compared with local publication truth.",
    }


def _scheduler_task_audit_outcomes(settings: Settings) -> dict[str, Any]:
    """Inspect recorded audit failures even when the business task skipped."""
    state = _read_json(settings.scheduler_state_file)
    tasks = {}
    warnings = []
    for name, run in (state.get("last_runs") or {}).items():
        if not isinstance(run, dict):
            continue
        error = _clean_business_text(run.get("task_run_audit_error"))
        audit_status = run.get("task_run_audit_status")
        tasks[name] = {
            "business_status": run.get("status"), "started_at": run.get("started_at"),
            "task_run_audit_status": "failed" if error else audit_status,
            "task_run_audit_error": error or None,
            "task_run_audit_path": run.get("task_run_audit_path"),
        }
        if error or audit_status == "failed":
            warnings.append(f"{name}:task_run_audit_failed")
    busy_skips = []
    for run in (state.get("recent_busy_skips") or [])[:20]:
        if not isinstance(run, dict):
            continue
        statuses = {field: run.get(field) for field in (
            "task_run_audit_status", "task_history_write_status", "scheduler_state_write_status", "task_log_write_status",
        )}
        errors = {field: run.get(field) for field in (
            "task_run_audit_error", "task_history_write_error", "scheduler_state_write_error", "task_log_write_error",
        ) if run.get(field)}
        busy_skips.append({"attempt_id": run.get("attempt_id"), "task_name": run.get("task_name"),
                           "task_history_id": run.get("task_history_id"), "started_at": run.get("started_at"),
                           "business_status": run.get("status"), "trigger_source": run.get("trigger_source"),
                           "statuses": statuses, "errors": errors})
        if errors or any(value == "failed" for value in statuses.values()):
            warnings.append(f"busy_skip:{run.get('attempt_id')}:persistence_failed")
        elif any(value != "written" for value in statuses.values()):
            warnings.append(f"busy_skip:{run.get('attempt_id')}:persistence_unconfirmed")
    return {
        "status": "watch" if warnings else "ok", "blockers": [], "warnings": warnings,
        "tasks": tasks, "busy_skip_attempts": busy_skips,
        "source": "persisted_scheduler_last_runs_and_recent_busy_skips",
        "truth_note": "Business success/skip does not prove audit completion; absent legacy audit outcomes remain unknown.",
    }


def build_data_truthfulness_audit(
    settings: Settings,
    *,
    include_live_api: bool = False,
    live_base_url: str = "http://127.0.0.1:8090",
    timeout_seconds: float = 30.0,
    max_source_matches: int = 200,
) -> dict[str, Any]:
    started_db_token = _read_db_stat_token(settings.database_path)
    publication = _publication_truth(settings)
    nav_return_snapshot_consistency = _nav_return_snapshot_consistency(settings)
    risk_snapshot_input_consistency = _risk_snapshot_input_consistency(settings)
    risk_metric_recalculation = _risk_metric_recalculation_truth(settings)
    formal_snapshot_alignment = _formal_snapshot_alignment_from_sections(
        risk_snapshot_input_consistency,
        nav_return_snapshot_consistency,
    )
    freshness = _freshness_truth(
        settings,
        formal_snapshot_alignment=formal_snapshot_alignment,
    )
    risk_metric = _risk_metric_truth(settings)
    source_truth = _source_truth_manifest(settings)
    fund_directory_source_quality = _fund_directory_source_quality(settings)
    annual_return_rank_consistency = _annual_return_rank_consistency(settings)
    autostock_source_evidence = _autostock_source_evidence_truth(settings)
    autostock_directory_source_evidence = _autostock_directory_source_evidence_truth(settings)
    collector_evidence_contract = _collector_evidence_contract_truth(settings)
    automation_script_run = _automation_script_run_truth(settings)
    upstream_collection = _upstream_collection_truth(
        automation_payload=automation_script_run,
        freshness_payload=freshness,
        formal_snapshot_alignment=formal_snapshot_alignment,
    )
    frontend = _frontend_truth(settings)
    frontend_field_lineage = _frontend_field_lineage_truth(
        settings,
        include_live_api=include_live_api,
        live_base_url=live_base_url,
        timeout_seconds=timeout_seconds,
    )
    source_scan = _source_scan(settings, max_matches=max_source_matches)
    sections: dict[str, Any] = {
        "publication_truth": publication,
        "freshness_truth": freshness,
        "risk_metric_truth": risk_metric,
        "source_truth_manifest": source_truth,
        "fund_directory_source_quality": fund_directory_source_quality,
        "nav_return_snapshot_consistency": nav_return_snapshot_consistency,
        "risk_snapshot_input_consistency": risk_snapshot_input_consistency,
        "risk_metric_recalculation_truth": risk_metric_recalculation,
        "annual_return_rank_consistency": annual_return_rank_consistency,
        "autostock_source_evidence": autostock_source_evidence,
        "autostock_directory_source_evidence": autostock_directory_source_evidence,
        "collector_evidence_contract_truth": collector_evidence_contract,
        "automation_script_run_truth": automation_script_run,
        "scheduler_task_audit_outcomes": _scheduler_task_audit_outcomes(settings),
        "upstream_collection_truth": upstream_collection,
        "frontend_truth": frontend,
        "frontend_field_lineage": frontend_field_lineage,
        "source_scan": source_scan,
    }
    if include_live_api:
        sections["live_api_truth"] = _live_api_truth(
            settings,
            base_url=live_base_url,
            timeout_seconds=timeout_seconds,
            expected_publication=publication,
        )
    blockers: list[str] = []
    warnings: list[str] = []
    for key, section in sections.items():
        for blocker in section.get("blockers") or []:
            blockers.append(f"{key}:{blocker}")
        for warning in section.get("warnings") or []:
            warnings.append(f"{key}:{warning}")
    finished_db_token = _read_db_stat_token(settings.database_path)
    if started_db_token != finished_db_token:
        warnings.append("database_changed_during_truthfulness_audit")
    status = _classify_status(blockers, warnings)
    return {
        "schema_version": SCHEMA_VERSION,
        "artifact_type": "data_truthfulness_audit",
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "status": status,
        "blocker_count": len(blockers),
        "warning_count": len(warnings),
        "blockers": blockers,
        "warnings": warnings,
        "database_path": str(settings.database_path),
        "database_stat_token_before": started_db_token,
        "database_stat_token_after": finished_db_token,
        "sections": sections,
        "data_truthfulness_principle": (
            "Fund business data must come from real APIs, SQLite tables, canonical run artifacts, "
            "or reproducible local calculations. Missing values must remain missing; no fabricated "
            "returns, NAV, risk metrics, publication counts, pass states, or frontend display values."
        ),
    }


def write_data_truthfulness_audit(settings: Settings, payload: dict[str, Any]) -> Path:
    artifact_path = build_audit_artifact_path(
        settings.reports_dir / "audit" / "data_truthfulness",
        "data_truthfulness_audit",
        suffix=".json.gz",
    )
    return write_task_audit(artifact_path, payload)


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit fund data truthfulness, freshness and frontend display evidence.")
    parser.add_argument("--include-live-api", action="store_true", help="Sample local live API endpoints and compare them with publication truth.")
    parser.add_argument("--live-base-url", default="http://127.0.0.1:8090", help="Base URL for live API sampling.")
    parser.add_argument("--timeout-seconds", type=float, default=30.0, help="HTTP timeout for live API sampling.")
    parser.add_argument("--max-source-matches", type=int, default=200, help="Maximum keyword source matches stored in the artifact.")
    args = parser.parse_args()

    settings = load_settings()
    payload = build_data_truthfulness_audit(
        settings,
        include_live_api=bool(args.include_live_api),
        live_base_url=args.live_base_url,
        timeout_seconds=max(1.0, float(args.timeout_seconds)),
        max_source_matches=max(0, int(args.max_source_matches)),
    )
    artifact_path = write_data_truthfulness_audit(settings, payload)
    print(
        json.dumps(
            {
                "passed": payload["status"] != "needs_review",
                "status": payload["status"],
                "blocker_count": payload["blocker_count"],
                "warning_count": payload["warning_count"],
                "artifact_path": str(artifact_path),
                "artifact_project_path": str(artifact_path.relative_to(settings.project_root)),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0 if payload["status"] != "needs_review" else 2


if __name__ == "__main__":
    raise SystemExit(main())
