from __future__ import annotations

from backend.lifetime_returns import load_platform_lifetime_returns

from collections import Counter
from copy import deepcopy
from functools import partial
import hashlib
from itertools import combinations
import json
import math
import re
import shutil
import sqlite3
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any
from time import perf_counter
import sys

import pandas as pd
from core.utils import report_history_store
from core.utils.buyability import buyability_label
from backend.manager_metrics import with_manager_display_columns
from core.utils.drawdown_duration import normalize_drawdown_duration_columns
from core.utils.holding_period_policy import holding_period_gate_reason, resolve_holding_period_days
from core.utils.core_csv_storage import (
    CoreCsvStorageError, artifact_is_file, copy_original_artifact, glob_artifacts, read_csv_artifact,
    read_original_artifact_bytes,
)
from database.screening_pick_payloads import decode_snapshot
from openpyxl import load_workbook
from openpyxl.formatting.rule import ColorScaleRule, DataBarRule
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter

from backend.autostock_budget import plan_autostock_call_budget, read_autostock_quota_snapshot
from backend.report_autostock_cache import cache_reuse_rejection, snapshot_cache_bindings
from backend.report_cross_validation_evidence import (
    STATUS_COLUMNS as DATA_VALIDATION_COLUMNS,
    load_snapshot as load_data_validation_snapshot,
    write_snapshot as write_data_validation_snapshot,
)
from backend.project_publish_decisions import validation_status_value, validation_status_series
from backend.report_autostock_evidence import (
    COMPARE_COLUMNS as AUTOSTOCK_COMPARE_COLUMNS,
    begin_supplement_record,
    comparison_code_hashes,
    content_sha256 as autostock_evidence_sha256,
    finish_supplement_record,
    json_value as autostock_evidence_json_value,
    load_supplement_snapshot,
    write_supplement_snapshot,
)
from backend.bond_style_classifier import classify_bond_style
from backend.bond_duration_proxy import classify_duration_proxy
from backend.bond_credit_proxy import classify_credit_proxy
from backend.config import Settings, load_settings
from backend.effective_nav import (
    load_effective_nav_snapshot,
    _latest_risk_rowid_predicate,
    _risk_metric_ready_predicate,
)
from backend.forward_validation import build_forward_validation_package, build_incremental_review_frame
from backend.fee_semantics import (
    evaluate_fee_semantics,
    fee_semantics_ready,
    load_sales_fee_validation_classification_map,
    resolve_fee_screening_value,
)
from backend.high_score_friction import build_high_score_friction_gate_review
from backend.risk_coverage_semantics import build_risk_coverage_threshold_semantics
from backend.long_term_governance import (
    build_long_term_governance_summary as build_long_term_governance_summary_shared,
    build_long_term_maintenance_detail as build_long_term_maintenance_detail_shared,
)
from backend.risk_adjusted_metric_stack import build_risk_adjusted_metric_stack_review
from backend.project_publish_decisions import (
    ABSOLUTE_QUALITY_DIAGNOSIS as PROJECT_ABSOLUTE_QUALITY_DIAGNOSIS,
    AUTO_DIAGNOSIS_CATEGORY_COLUMN as PROJECT_AUTO_DIAGNOSIS_CATEGORY_COLUMN,
    AUTO_DIAGNOSIS_POOL_LABEL as PROJECT_AUTO_DIAGNOSIS_POOL_LABEL,
    AUTO_DIAGNOSIS_VERDICT as PROJECT_AUTO_DIAGNOSIS_VERDICT,
    AUTO_PUBLISH_GATE_DIAGNOSIS as PROJECT_AUTO_PUBLISH_GATE_DIAGNOSIS,
    DYNAMIC_NAV_FRESHNESS_DAYS as PROJECT_DYNAMIC_NAV_FRESHNESS_DAYS,
    DYNAMIC_QUALITY_DIAGNOSIS as PROJECT_DYNAMIC_QUALITY_DIAGNOSIS,
    ENHANCED_STYLE_SUPPLEMENTAL_THRESHOLDS as PROJECT_ENHANCED_STYLE_SUPPLEMENTAL_THRESHOLDS,
    FINAL_SELECTION_NOT_SELECTED_DIAGNOSIS as PROJECT_FINAL_SELECTION_NOT_SELECTED_DIAGNOSIS,
    HOLDING_PERIOD_GENERIC_PLACEHOLDER_STATUS as PROJECT_HOLDING_PERIOD_GENERIC_PLACEHOLDER_STATUS,
    HOLDING_PERIOD_NO_SIGNAL_STATUS as PROJECT_HOLDING_PERIOD_NO_SIGNAL_STATUS,
    HOLDING_PERIOD_DATA_UNKNOWN_STATUS as PROJECT_HOLDING_PERIOD_DATA_UNKNOWN_STATUS,
    HOLDING_PERIOD_NAME_UNPARSED_REASON_CODE as PROJECT_HOLDING_PERIOD_NAME_UNPARSED_REASON_CODE,
    HOLDING_PERIOD_NAME_UNPARSED_STATUS as PROJECT_HOLDING_PERIOD_NAME_UNPARSED_STATUS,
    MANAGER_STABILITY_THRESHOLDS as PROJECT_MANAGER_STABILITY_THRESHOLDS,
    MANAGER_STABILITY_DIAGNOSIS as PROJECT_MANAGER_STABILITY_DIAGNOSIS,
    NOT_BUYABLE_DIAGNOSIS as PROJECT_NOT_BUYABLE_DIAGNOSIS,
    PROJECT_RECOMMENDATION_REASONS as PROJECT_PUBLISH_RECOMMENDATION_REASONS,
    PUBLISH_CONFIDENCE_CONFIG as PROJECT_PUBLISH_CONFIDENCE_CONFIG,
    QUALITY_WARNING_DIAGNOSIS as PROJECT_QUALITY_WARNING_DIAGNOSIS,
    SCALE_DATA_THRESHOLDS as PROJECT_SCALE_DATA_THRESHOLDS,
    VALIDATION_BLOCK_DIAGNOSIS as PROJECT_VALIDATION_BLOCK_DIAGNOSIS,
    append_project_decision_columns as append_project_decision_columns_impl,
    apply_holding_period_name_unparsed_confidence_penalty as apply_holding_period_name_unparsed_confidence_penalty_impl,
    apply_publish_post_filters as apply_publish_post_filters_impl,
    build_recommendation_note as build_recommendation_note_impl,
    combine_holding_period_names,
    confidence_from_enhanced_metrics as confidence_from_enhanced_metrics_impl,
    confidence_from_thresholds as confidence_from_thresholds_impl,
    format_autostock_display_status as format_autostock_display_status_impl,
    format_autostock_note_flag as format_autostock_note_flag_impl,
    is_holding_period_name_unparsed_status as is_holding_period_name_unparsed_status_impl,
    normalize_autostock_status as normalize_autostock_status_impl,
    project_confidence_label as project_confidence_label_impl,
    refresh_recommendation_notes as refresh_recommendation_notes_impl,
    resolve_holding_period_status as resolve_holding_period_status_impl,
)
from backend.project_formal_groups import (
    ENHANCED_BOND_GROUP_LABEL as PROJECT_ENHANCED_BOND_GROUP_LABEL,
    ENHANCED_STYLE_TAGS as PROJECT_ENHANCED_STYLE_TAGS,
    FORMAL_GROUP_LABEL as PROJECT_FORMAL_GROUP_LABEL,
    FORMAL_SCORE_LABEL as PROJECT_FORMAL_SCORE_LABEL,
    STABLE_BOND_GROUP_LABEL as PROJECT_STABLE_BOND_GROUP_LABEL,
    append_formal_group_column as append_formal_group_column_impl,
    build_formal_group_distribution as build_formal_group_distribution_impl,
    formal_group_from_row as formal_group_from_row_impl,
)
from backend.project_report_exports import (
    ENRICHED_EXTRA_COLUMNS as PROJECT_ENRICHED_EXTRA_COLUMNS,
    EXCLUDED_COLUMNS as PROJECT_EXCLUDED_COLUMNS,
    EXCLUDED_COLUMN_WIDTHS as PROJECT_EXCLUDED_COLUMN_WIDTHS,
    EXPORT_RENAME_MAPPING as PROJECT_EXPORT_RENAME_MAPPING,
    FINAL_MAIN_COLUMN_WIDTHS as PROJECT_FINAL_MAIN_COLUMN_WIDTHS,
    RECOMMENDATION_DETAIL_COLUMNS as PROJECT_RECOMMENDATION_DETAIL_COLUMNS,
    REFERENCE_CSV_COLUMNS as PROJECT_REFERENCE_CSV_COLUMNS,
    REFERENCE_FINAL_COLUMNS as PROJECT_REFERENCE_FINAL_COLUMNS,
    WATCH_EXPORT_COLUMNS as PROJECT_WATCH_EXPORT_COLUMNS,
)

from backend.project_report_support import (
    STAGE_B_ALLOWED_REASON_CODES as PROJECT_STAGE_B_ALLOWED_REASON_CODES,
    STAGE_B_REASON_PRIORITY as PROJECT_STAGE_B_REASON_PRIORITY,
    build_fee_band_frame as build_fee_band_frame_impl,
    build_stage_b_reason_ledger as build_stage_b_reason_ledger_impl,
    build_summary_sheet_rows as build_summary_sheet_rows_impl,
    build_watch_summary_frame as build_watch_summary_frame_impl,
    distribution_frame as distribution_frame_impl,
    normalize_stage_b_reason_code as normalize_stage_b_reason_code_impl,
    pick_stage_b_primary_reason as pick_stage_b_primary_reason_impl,
    pick_stage_b_secondary_reasons as pick_stage_b_secondary_reasons_impl,
    split_reason_codes as split_reason_codes_impl,
)
from backend.publish_reason_summary import enrich_publish_reason_summary
from scripts.audit_runtime_utils import build_audit_artifact_path
from backend.project_quality_thresholds import (
    DYNAMIC_THRESHOLD_CONFIG as PROJECT_DYNAMIC_THRESHOLD_CONFIG,
    ENHANCED_STYLE_RELAXED_ABSOLUTE_CAPS as PROJECT_ENHANCED_STYLE_RELAXED_ABSOLUTE_CAPS,
    ENHANCED_STYLE_RELAXED_ABSOLUTE_FLOORS as PROJECT_ENHANCED_STYLE_RELAXED_ABSOLUTE_FLOORS,
    PROJECT_ABSOLUTE_QUALITY_THRESHOLDS as PROJECT_QUALITY_ABSOLUTE_THRESHOLDS,
    PROJECT_SCORE_STYLE_FLOORS as PROJECT_QUALITY_STYLE_FLOORS,
    apply_dynamic_quality_thresholds as apply_dynamic_quality_thresholds_impl,
    apply_enhanced_supplemental_gate as apply_enhanced_supplemental_gate_impl,
    apply_fixed_quality_thresholds as apply_fixed_quality_thresholds_impl,
    build_dynamic_threshold_detail_frame as build_dynamic_threshold_detail_frame_impl,
    build_enhanced_supplemental_label as build_enhanced_supplemental_label_impl,
    combined_quality_floor as combined_quality_floor_impl,
    compute_dynamic_style_thresholds as compute_dynamic_style_thresholds_impl,
    project_style_floor as project_style_floor_impl,
    style_relative_thresholds as style_relative_thresholds_impl,
)
from backend.api_payloads.positive_ratio import (
    build_positive_ratio_semantics_summary as build_positive_ratio_semantics_summary_api,
)
from backend.publication import build_publication_registry
from backend import report_workbook
from backend import project_tracker
from backend import selection_tracker_history
from backend.rank_cache_snapshot import (
    RankCacheSnapshot, rank_cache_snapshot, sqlite_snapshot_token,
)
from utils.fund_exclusion_rules import get_runtime_excluded_fund_codes
from utils.period_return_snapshot import RETURN_PUBLIC_FIELDS, RETURN_VALIDATION_FIELDS, attach_period_return_status


CORE_ROOT = Path(__file__).resolve().parent.parent / "core"
if str(CORE_ROOT) not in sys.path:
    sys.path.insert(0, str(CORE_ROOT))

from database.nav_history_queries import (  # noqa: E402
    attach_nav_history_archive_if_available,
    build_nav_history_union_source_sql,
    detach_nav_history_archive,
    load_latest_nav_history_archive_binding,
    nav_history_archive_binding_matches_database,
)
from database.rank_history_queries import (  # noqa: E402
    begin_rank_history_read_snapshot,
    attach_rank_history_archive_if_available,
    build_rank_history_union_source_sql,
    detach_rank_history_archive,
    load_latest_rank_history_archive_binding,
    rank_history_archive_binding_matches_database,
)
from bond_fund_screening.share_class_utils import normalize_base_fund_name

_REPORT_AUTOSTOCK_SUPPLEMENT_RESERVE_CALLS = 10
_REPORT_AUTOSTOCK_SUPPLEMENT_MAX_FETCH_CALLS = 10
_LONG_TERM_RANK_METRICS_CACHE: dict[str, pd.DataFrame] = {}
_LONG_TERM_HISTORY_RANK_SNAPSHOT_STATS_CACHE: dict[str, dict[str, Any]] = {}
_LONG_TERM_HISTORY_RANK_SNAPSHOT_STATS_SIDECAR_SCHEMA = 3
_LONG_TERM_HISTORY_RANK_SNAPSHOT_STATS_SIDECAR_FILENAME = "latest_long_term_rank_snapshot_stats.json"
_LONG_TERM_RANK_METRICS_FAST_BATCH_SIZE = 1200

CODE_CANDIDATES = ["基金代码", "fcode", "代码", "fund_code"]
NAME_CANDIDATES = ["基金名称", "shortname", "名称", "fund_name"]
TYPE_CANDIDATES = ["基金类型", "ftype", "类型"]
SCORE_CANDIDATES = [
    "综合加权得分",
    "weighted_score",
    "selected_combo_score",
    "avg_combo_score",
    "combo_score",
]

REFERENCE_FINAL_COLUMNS = PROJECT_REFERENCE_FINAL_COLUMNS
ENRICHED_EXTRA_COLUMNS = PROJECT_ENRICHED_EXTRA_COLUMNS
RECOMMENDATION_DETAIL_COLUMNS = PROJECT_RECOMMENDATION_DETAIL_COLUMNS
WATCH_EXPORT_COLUMNS = PROJECT_WATCH_EXPORT_COLUMNS
EXCLUDED_COLUMNS = PROJECT_EXCLUDED_COLUMNS
FINAL_MAIN_COLUMN_WIDTHS = PROJECT_FINAL_MAIN_COLUMN_WIDTHS
EXCLUDED_COLUMN_WIDTHS = PROJECT_EXCLUDED_COLUMN_WIDTHS
AUTO_DIAGNOSIS_VERDICT = PROJECT_AUTO_DIAGNOSIS_VERDICT
AUTO_DIAGNOSIS_CATEGORY_COLUMN = PROJECT_AUTO_DIAGNOSIS_CATEGORY_COLUMN
AUTO_DIAGNOSIS_POOL_LABEL = PROJECT_AUTO_DIAGNOSIS_POOL_LABEL
NOT_BUYABLE_DIAGNOSIS = PROJECT_NOT_BUYABLE_DIAGNOSIS
VALIDATION_BLOCK_DIAGNOSIS = PROJECT_VALIDATION_BLOCK_DIAGNOSIS
QUALITY_WARNING_DIAGNOSIS = PROJECT_QUALITY_WARNING_DIAGNOSIS
ABSOLUTE_QUALITY_DIAGNOSIS = PROJECT_ABSOLUTE_QUALITY_DIAGNOSIS
DYNAMIC_QUALITY_DIAGNOSIS = PROJECT_DYNAMIC_QUALITY_DIAGNOSIS
MANAGER_STABILITY_DIAGNOSIS = PROJECT_MANAGER_STABILITY_DIAGNOSIS
AUTO_PUBLISH_GATE_DIAGNOSIS = PROJECT_AUTO_PUBLISH_GATE_DIAGNOSIS
FINAL_SELECTION_NOT_SELECTED_DIAGNOSIS = PROJECT_FINAL_SELECTION_NOT_SELECTED_DIAGNOSIS

ACCUMULATION_SUMMARY_COLUMNS = [
    "基金代码",
    "基金名称",
    "出现次数",
    "出现频率(%)",
    "首次出现日期",
    "最后出现日期",
    "连续缺席天数",
    "最新年化收益(%)",
    "最新夏普比率",
    "最新最大回撤(%)",
]

PROJECT_TRACKER_STATE_PREVIEW_COLUMNS = [
    "基金代码",
    "基金名称",
    "正式榜分组",
    "正式投资评分",
    "出现次数",
    "出现频率(%)",
    "首次出现日期",
    "最后出现日期",
    "连续缺席期数",
    "当前是否在榜",
    "基金公司",
    "数据来源",
    "债基风格标签",
    "最新年化收益(%)",
    "最新夏普比率",
    "最新最大回撤(%)",
    "项目质量信心",
]

HIGH_SCORE_AUDIT_THRESHOLD = 75.0

EXCLUDED_STATE_PREVIEW_COLUMNS = [
    "基金代码",
    "基金名称",
    "基金类型",
    "基金公司",
    "基金规模(亿)",
    "持有期状态",
    "是否可申购",
    "申购状态",
    "债基风格标签",
    "基准归类",
    "正式投资评分",
    "项目质量信心",
]

PROJECT_RECOMMENDATION_REASONS = PROJECT_PUBLISH_RECOMMENDATION_REASONS
HOLDING_PERIOD_GENERIC_PLACEHOLDER_STATUS = PROJECT_HOLDING_PERIOD_GENERIC_PLACEHOLDER_STATUS
HOLDING_PERIOD_NO_SIGNAL_STATUS = PROJECT_HOLDING_PERIOD_NO_SIGNAL_STATUS
HOLDING_PERIOD_NAME_UNPARSED_STATUS = PROJECT_HOLDING_PERIOD_NAME_UNPARSED_STATUS
HOLDING_PERIOD_NAME_UNPARSED_REASON_CODE = PROJECT_HOLDING_PERIOD_NAME_UNPARSED_REASON_CODE

STAGE_B_REASON_PRIORITY = PROJECT_STAGE_B_REASON_PRIORITY
STAGE_B_ALLOWED_REASON_CODES = PROJECT_STAGE_B_ALLOWED_REASON_CODES

PROJECT_ABSOLUTE_QUALITY_THRESHOLDS = PROJECT_QUALITY_ABSOLUTE_THRESHOLDS
ENHANCED_STYLE_RELAXED_ABSOLUTE_CAPS = PROJECT_ENHANCED_STYLE_RELAXED_ABSOLUTE_CAPS
ENHANCED_STYLE_RELAXED_ABSOLUTE_FLOORS = PROJECT_ENHANCED_STYLE_RELAXED_ABSOLUTE_FLOORS

DYNAMIC_NAV_FRESHNESS_DAYS = PROJECT_DYNAMIC_NAV_FRESHNESS_DAYS

DYNAMIC_THRESHOLD_CONFIG = PROJECT_DYNAMIC_THRESHOLD_CONFIG
PROJECT_SCORE_STYLE_FLOORS = PROJECT_QUALITY_STYLE_FLOORS

PRIORITY_POOL_MIN_COUNT = 24
PRIORITY_POOL_MAX_COUNT = 36
PRIORITY_POOL_TARGET_RATIO = 0.75
PRIORITY_POOL_COMPANY_CAP = 2
PRIORITY_POOL_SHARE_FEE_TOLERANCE = 0.05
PRIORITY_POOL_EXPANSION_RATIO_TRIGGER = 0.0
PRIORITY_POOL_EXPANDED_TARGET_RATIO = 0.75
PRIORITY_POOL_QUALITY_GATE_ACTIVATION_COUNT = 80
PRIORITY_POOL_QUALITY_GATE_MIN_CANDIDATE_COUNT = 32
PRIORITY_POOL_QUALITY_GATE_STEPS = (
    {
        "label": "strict",
        "stable_formal_score_floor": 82.0,
        "enhanced_formal_score_floor": 78.0,
        "priority_score_floor": 84.0,
    },
    {
        "label": "balanced",
        "stable_formal_score_floor": 81.0,
        "enhanced_formal_score_floor": 77.0,
        "priority_score_floor": 82.0,
    },
    {
        "label": "standard",
        "stable_formal_score_floor": 80.0,
        "enhanced_formal_score_floor": 76.0,
        "priority_score_floor": 80.0,
    },
    {
        "label": "relaxed",
        "stable_formal_score_floor": 80.0,
        "enhanced_formal_score_floor": 75.0,
        "priority_score_floor": 78.0,
    },
    {
        "label": "fallback",
        "stable_formal_score_floor": 79.0,
        "enhanced_formal_score_floor": 75.0,
        "priority_score_floor": 76.0,
    },
)
LOW_VOL_STYLE_TAGS = ("中短债", "综合纯债", "现金增强", "固收指数")
LOW_VOL_POSITIVE_RATIO_GAP_ALERT_PCT = 15.0
PRIORITY_POOL_MIN_CORE_COUNT = 12
PRIORITY_POOL_MAX_CORE_COUNT = 15
PRIORITY_POOL_DEFAULT_RISK_TEXT = "当前未见额外显式风险提示"


def _build_long_term_governance_markdown_payload(
    *,
    settings: Settings,
    long_term_snapshot: dict[str, Any],
) -> dict[str, Any]:
    path = settings.scheduler_state_file
    scheduler_state: dict[str, Any] = {}
    if path.exists() and path.is_file():
        try:
            scheduler_state = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            scheduler_state = {}
    maintenance_detail = build_long_term_maintenance_detail_shared(
        {
            "maintenance_summary": scheduler_state.get("maintenance_summary") or {},
            "task_closures": scheduler_state.get("task_closures") or {},
        }
    )
    return build_long_term_governance_summary_shared(long_term_snapshot, maintenance_detail)


def build_risk_lag_audit(*args: Any, **kwargs: Any) -> dict[str, Any]:
    from backend.risk_lag_audit import build_risk_lag_audit as _impl

    return _impl(*args, **kwargs)


def build_risk_sync_audit(*args: Any, **kwargs: Any) -> dict[str, Any]:
    from backend.risk_sync_audit import build_risk_sync_audit as _impl

    return _impl(*args, **kwargs)


def build_risk_coverage_gap_audit(*args: Any, **kwargs: Any) -> dict[str, Any]:
    from backend.risk_coverage_gap_audit import build_risk_coverage_gap_audit as _impl

    return _impl(*args, **kwargs)


def _build_publish_reason_markdown_payload(summary: dict[str, Any]) -> dict[str, Any]:
    project_publish_summary = summary.get("project_publish_summary") or {}
    publish_reason_summary = enrich_publish_reason_summary(
        project_publish_summary.get("publish_reason_summary") or {},
        {
            **(project_publish_summary.get("standard_screening_audit") or {}),
            "annual_return_gate_shadow": (
                project_publish_summary.get("annual_return_gate_shadow") or {}
            ),
        },
    )
    return {
        "publish_reason_summary": publish_reason_summary,
        "stage_family_summary": list(publish_reason_summary.get("stage_family_summary") or []),
        "responsibility_chain_headline": publish_reason_summary.get("responsibility_chain_headline"),
        "focus_metric_summary": list(publish_reason_summary.get("focus_metric_summary") or []),
    }
PRIORITY_POOL_HOLDING_PERIOD_NAME_UNPARSED_PENALTY = 2.0
PRIORITY_POOL_CORE_STABLE_CONFIRMED_BONUS = 6.0
PRIORITY_POOL_CORE_CONFIRMED_FILL_BONUS = 2.5
# P2-01 / B2: 扩展组"最小确认样本下限"。
# 纯债稳健扩展 / 增强债弹性扩展 各自要求至少 EXTENSION_MIN_CONFIRMED_HOLDING 只
# "已确认<=90天" 的基金；若未来恢复收缩策略，仅对真实待确认样本
# （名称未识别/通用占位）施加占比约束，不能把"未见持有期限制信号"误当待确认。
EXTENSION_MIN_CONFIRMED_HOLDING = 3
EXTENSION_UNKNOWN_SAMPLE_RATIO_CAP = 0.5
EXTENSION_STRICT_UNKNOWN_FALLBACK_MIN_FORMAL_SCORE = 75.0
PRIORITY_POOL_CORE_HOLDING_PERIOD_UNPARSED_PENALTY = 8.0
PRIORITY_POOL_CORE_ENHANCED_UNPARSED_EXTRA_PENALTY = 3.0
PRIORITY_POOL_CORE_STABLE_UNPARSED_MID_SCORE_PENALTY = 4.0
PRIORITY_POOL_CORE_STABLE_UNPARSED_LOW_SCORE_EXTRA_PENALTY = 2.0
PRIORITY_POOL_CORE_RECENT_WEAK_PENALTY = 2.5
PRIORITY_POOL_CORE_SOFT_WARNING_PENALTY = 3.0
PRIORITY_POOL_CORE_LOW_FORMAL_SCORE_PENALTY = 12.0
PRIORITY_POOL_CORE_VERY_LOW_FORMAL_SCORE_EXTRA_PENALTY = 14.0
PRIORITY_POOL_EXTENSION_UNKNOWN_WEAK_SIGNAL_PENALTY = 4.0
PRIORITY_POOL_SHARE_CLASS_SUFFIX_PATTERN = re.compile(r"^(?P<base>.+?)(?P<suffix>[A-FH-IORY](?:/[A-FH-IORY])?|[A-FH-IORY]类)$")
FINAL_SELECTION_HISTORY_OCCURRENCE_WEIGHT = 1.2
FINAL_SELECTION_HISTORY_FREQUENCY_WEIGHT = 0.06
FINAL_SELECTION_HIGH_CONFIDENCE_BONUS = 6.0
FINAL_SELECTION_MEDIUM_CONFIDENCE_BONUS = 2.0
FINAL_SELECTION_BORDERLINE_CONFIDENCE_PENALTY = 8.0
FINAL_SELECTION_LOW_SCORE_NEW_FUND_PENALTY = 8.0
FINAL_SELECTION_HIGH_SCORE_NEW_FUND_BONUS = 8.0

PROJECT_SCORE_COMPONENTS = {
    "short_term": [
        ("return_6m", 0.16, True),
        ("return_1y", 0.14, True),
        ("annual_return", 0.14, True),
        ("sharpe_ratio", 0.14, True),
        ("sortino_ratio", 0.12, True),
        ("max_drawdown", 0.10, False),
        ("volatility", 0.10, False),
        ("positive_ratio", 0.10, True),
    ],
    "long_term": [
        ("annual_return", 0.22, True),
        ("return_1y", 0.10, True),
        ("return_3y", 0.06, True),
        ("sharpe_ratio", 0.10, True),
        ("sortino_ratio", 0.10, True),
        ("calmar_ratio", 0.18, True),
        ("max_drawdown", 0.08, False),
        ("positive_ratio", 0.06, True),
        ("fee_total_numeric", 0.05, False),
        ("scale_billion", 0.05, True),
    ],
}

FORMAL_SCORE_CONTRACT_SCHEMA_VERSION = 1
FORMAL_SCORE_CONTRACT_VERSION = "formal-score-2026-10-28.1"
FORMAL_SCORE_SHORT_TERM_WEIGHT = 0.45
FORMAL_SCORE_LONG_TERM_WEIGHT = 0.55
FORMAL_SCORE_METRIC_LABELS = {
    "return_6m": "近6月收益",
    "return_1y": "近1年收益",
    "return_3y": "近3年收益",
    "annual_return": "1年年化收益",
    "sharpe_ratio": "1年夏普比率",
    "sortino_ratio": "1年索提诺比率",
    "calmar_ratio": "1年卡玛比率",
    "max_drawdown": "1年最大回撤",
    "volatility": "1年波动率",
    "positive_ratio": "1年正收益比率",
    "fee_total_numeric": "费率合计",
    "scale_billion": "基金规模",
}


def build_formal_score_contract() -> dict[str, Any]:
    """Build the public formal-score contract from the live scoring constants."""

    component_weights = {
        "short_term": FORMAL_SCORE_SHORT_TERM_WEIGHT,
        "long_term": FORMAL_SCORE_LONG_TERM_WEIGHT,
    }
    component_labels = {
        "short_term": "近期得分",
        "long_term": "长期得分",
    }
    components: list[dict[str, Any]] = []
    for component_key in ("short_term", "long_term"):
        blend_weight = float(component_weights[component_key])
        metrics = [
            {
                "field": metric,
                "label": FORMAL_SCORE_METRIC_LABELS.get(metric, metric),
                "weight_within_component": float(weight),
                "effective_weight": round(blend_weight * float(weight), 6),
                "direction": "higher_is_better" if higher_is_better else "lower_is_better",
            }
            for metric, weight, higher_is_better in PROJECT_SCORE_COMPONENTS[component_key]
        ]
        components.append(
            {
                "key": component_key,
                "label": component_labels[component_key],
                "blend_weight": blend_weight,
                "metrics": metrics,
            }
        )
    return {
        "schema_version": FORMAL_SCORE_CONTRACT_SCHEMA_VERSION,
        "contract_version": FORMAL_SCORE_CONTRACT_VERSION,
        "score_field": FORMAL_SCORE_LABEL,
        "score_label": FORMAL_SCORE_LABEL,
        "score_range": {"minimum": 0.0, "maximum": 100.0},
        "comparison_scope": "within_bond_style",
        "comparison_scope_label": "同债基风格内比较",
        "method": "style_relative_percentile_weighted_blend",
        "method_label": "同风格百分位加权",
        "formula": "formal_score = short_term_score * 0.45 + long_term_score * 0.55",
        "components": components,
        "missing_value_policy": {
            "partial_metric_missing": "optional return_3y is omitted with observed-weight renormalization; other required metrics remain missing",
            "whole_component_missing": "preserve missing; the composite score is missing unless both short-term and long-term components are available",
            "display": "preserve_missing_semantics",
        },
        "interpretation": (
            "正式投资评分按债基风格分别计算百分位；先生成近期得分与长期得分，"
            "再按 45%/55% 合成。不同债基风格之间不建议直接横向比较。"
        ),
    }

LONG_TERM_STABILITY_MIN_YEARS_3 = 3
LONG_TERM_STABILITY_MIN_YEARS_5 = 5
LONG_TERM_RANK_MIN_OBSERVATIONS = 20
LONG_TERM_SCORE_MIN_COVERAGE_RATIO = 0.6
LONG_TERM_RANK_STALE_DAYS_DEFAULT = 30
LONG_TERM_HISTORY_MAINTENANCE_STATE_FILENAME = "long_term_history_maintenance_state.json"
LONG_TERM_HISTORY_SNAPSHOT_SAMPLE_LIMIT = 10
TRACKER_CURRENT_DERIVED_FIELDS_SCHEMA_VERSION = 2
SINCE_INCEPTION_ANNUALIZED_RETURN_LABEL = "平台成立以来收益年化(%)"
SINCE_INCEPTION_ANNUAL_RETURN_BREAKDOWN_LABEL = "成立以来年度收益明细"
SINCE_INCEPTION_ANNUALIZED_MIN_HISTORY_DAYS = 365
SINCE_INCEPTION_ANNUALIZED_MAX_ESTAB_GAP_DAYS = 30
SINCE_INCEPTION_ANNUALIZED_NEGATIVE_ESTAB_TOLERANCE_DAYS = 7
SINCE_INCEPTION_ANNUALIZED_YEAR_DAYS = 365.2425


def _resolve_completed_calendar_years_for_annual_feature(
    raw_estab_date: Any,
    *,
    earliest_nav_date: Any = None,
) -> int | None:
    estab_text = str(raw_estab_date or "").strip()
    nav_text = str(earliest_nav_date or "").strip()
    estab_dt = None
    nav_dt = None
    if estab_text:
        try:
            estab_dt = datetime.fromisoformat(estab_text[:10]).date()
        except ValueError:
            estab_dt = None
    if nav_text:
        try:
            nav_dt = datetime.fromisoformat(nav_text[:10]).date()
        except ValueError:
            nav_dt = None
    reference_dt = None
    if estab_dt and nav_dt:
        reference_dt = max(estab_dt, nav_dt)
    elif estab_dt:
        reference_dt = estab_dt
    elif nav_dt:
        reference_dt = nav_dt
    if reference_dt is None:
        return None
    last_complete_year = datetime.now().year - 1
    completed_calendar_years = max(0, last_complete_year - reference_dt.year)
    if reference_dt.month == 1 and reference_dt.day == 1 and reference_dt.year <= last_complete_year:
        completed_calendar_years += 1
    return completed_calendar_years


def _extract_contiguous_recent_tail_years_for_annual_feature(
    target_years: list[int],
    present_years: list[int] | None,
) -> list[int]:
    normalized_target_years = [
        int(year)
        for year in (target_years or [])
        if str(year).strip().isdigit()
    ]
    if not normalized_target_years:
        return []
    target_year_set = set(normalized_target_years)
    present_set = {
        int(year)
        for year in (present_years or [])
        if str(year).strip().isdigit()
        and int(year) in target_year_set
    }
    if not present_set:
        return []
    tail_years: list[int] = []
    for year in reversed(normalized_target_years):
        if year not in present_set:
            break
        tail_years.append(year)
    return sorted(tail_years)


def _is_recent_share_class_tail_window_for_annual_feature(
    target_years: list[int],
    present_years: list[int] | None,
) -> bool:
    target_year_set = set(target_years or [])
    normalized_present_years = sorted(
        {
            int(year)
            for year in (present_years or [])
            if str(year).strip().isdigit()
            and int(year) in target_year_set
        }
    )
    if not normalized_present_years:
        return False
    if len(normalized_present_years) >= len(target_years or []):
        return False
    tail_years = _extract_contiguous_recent_tail_years_for_annual_feature(
        target_years,
        normalized_present_years,
    )
    return bool(tail_years) and tail_years == normalized_present_years


def _annual_feature_window_mature_enough_for_report(
    raw_estab_date: Any,
    *,
    earliest_nav_date: Any = None,
    current_window_nav_present_years: list[int] | None = None,
    min_years: int = LONG_TERM_STABILITY_MIN_YEARS_5,
) -> bool | None:
    target_years = _current_complete_calendar_year_window(min_years)
    if _is_recent_share_class_tail_window_for_annual_feature(
        target_years,
        current_window_nav_present_years,
    ):
        return False
    completed_calendar_years = _resolve_completed_calendar_years_for_annual_feature(
        raw_estab_date,
        earliest_nav_date=earliest_nav_date,
    )
    if completed_calendar_years is None:
        return None
    return completed_calendar_years >= min_years


def _annual_feature_window_has_leading_partial_null_year_for_report(
    target_years: list[int],
    present_years: list[int] | None,
    nonnull_years: list[int] | None,
    *,
    leading_present_year_first_nav_date: Any = None,
) -> bool:
    normalized_target_years = [
        int(year)
        for year in (target_years or [])
        if str(year).strip().isdigit()
    ]
    if not normalized_target_years:
        return False

    target_year_set = set(normalized_target_years)
    normalized_present_years = sorted(
        {
            int(year)
            for year in (present_years or [])
            if str(year).strip().isdigit()
            and int(year) in target_year_set
        }
    )
    normalized_nonnull_years = {
        int(year)
        for year in (nonnull_years or [])
        if str(year).strip().isdigit()
        and int(year) in target_year_set
    }
    if not normalized_present_years:
        return False

    leading_present_year = normalized_present_years[0]
    if leading_present_year != normalized_target_years[0]:
        return False
    if leading_present_year in normalized_nonnull_years:
        return False

    nav_text = str(leading_present_year_first_nav_date or "").strip()
    if not nav_text:
        return False
    try:
        nav_dt = datetime.fromisoformat(nav_text[:10]).date()
    except ValueError:
        return False
    return nav_dt.year == leading_present_year and (nav_dt.month > 1 or nav_dt.day > 1)


def _current_complete_calendar_year_window(min_years: int) -> list[int]:
    year_count = max(1, int(min_years or 0))
    last_complete_year = datetime.now().year - 1
    first_year = last_complete_year - year_count + 1
    return list(range(first_year, last_complete_year + 1))


def _resolve_report_rank_stale_days() -> int:
    try:
        return max(
            1,
            int(load_settings().scheduler_long_term_history_rank_stale_days or LONG_TERM_RANK_STALE_DAYS_DEFAULT),
        )
    except Exception:
        return LONG_TERM_RANK_STALE_DAYS_DEFAULT


def _normalize_report_long_term_failure_record(raw_record: Any) -> dict[str, Any]:
    if not isinstance(raw_record, dict):
        return {}
    raw_stages = raw_record.get("last_failed_stages")
    if isinstance(raw_stages, list):
        stages = sorted({str(stage or "").strip() for stage in raw_stages if str(stage or "").strip()})
    else:
        stage = str(raw_record.get("last_stage") or "").strip()
        stages = [stage] if stage else []
    try:
        consecutive_failures = max(0, int(raw_record.get("consecutive_failures") or 0))
    except (TypeError, ValueError):
        consecutive_failures = 0
    try:
        skip_runs_remaining = max(0, int(raw_record.get("skip_runs_remaining") or 0))
    except (TypeError, ValueError):
        skip_runs_remaining = 0
    return {
        "consecutive_failures": consecutive_failures,
        "skip_runs_remaining": skip_runs_remaining,
        "last_failed_at": str(raw_record.get("last_failed_at") or "").strip() or None,
        "next_retry_after": str(raw_record.get("next_retry_after") or "").strip() or None,
        "last_failed_stages": stages,
        "last_error": str(raw_record.get("last_error") or "").strip(),
        "upstream_limited": bool(raw_record.get("upstream_limited")),
    }


def _load_report_long_term_failure_backoff(db_path: Path) -> dict[str, dict[str, Any]]:
    state_path = db_path.parent / LONG_TERM_HISTORY_MAINTENANCE_STATE_FILENAME
    if not state_path.exists() or not state_path.is_file():
        return {}
    try:
        payload = json.loads(state_path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}
    failure_backoff = payload.get("failure_backoff")
    if not isinstance(failure_backoff, dict):
        return {}
    normalized: dict[str, dict[str, Any]] = {}
    for raw_code, raw_record in failure_backoff.items():
        code = _normalize_code(raw_code)
        if not code:
            continue
        record = _normalize_report_long_term_failure_record(raw_record)
        if record:
            normalized[code] = record
    return normalized


def _report_annual_gap_has_upstream_limited_failure(failure_record: dict[str, Any] | None) -> bool:
    record = dict(failure_record or {})
    if not bool(record.get("upstream_limited")):
        return False
    stages = {str(stage or "").strip() for stage in list(record.get("last_failed_stages") or [])}
    error_tokens = {
        token.strip()
        for token in re.split(r"\s*\|\s*", str(record.get("last_error") or ""))
        if token.strip()
    }
    return bool(
        "annual_returns" in stages
        or any(
            token
            in {
                "annual_history_gap_no_progress",
                "stale_annual_refresh_no_progress",
                "empty_period_records",
            }
            for token in error_tokens
        )
    )


def _annual_gap_shape_from_window_counts(
    *,
    row_count: int,
    nonnull_count: int,
) -> str:
    if row_count <= 0 or nonnull_count <= 0:
        return "missing_history"
    if row_count < LONG_TERM_STABILITY_MIN_YEARS_5:
        return "sparse_year_row_gap"
    return "null_year_gap"

LONG_TERM_STABILITY_COMPONENTS = {
    "annual_positive_3y_score": ("近3年正收益年数", 0.30, True),
    "annual_positive_5y_score": ("近5年正收益年数", 0.25, True),
    "annual_stability_3y_score": ("近3年年度收益稳定性", 0.15, True),
    "annual_stability_5y_score": ("近5年年度收益稳定性", 0.15, True),
    "rank_avg_12m_score": ("近12月同类排名均值", 0.10, True),
    "rank_top30_12m_score": ("近12月前30%排名观测占比", 0.05, True),
}

PUBLISH_CONFIDENCE_CONFIG = PROJECT_PUBLISH_CONFIDENCE_CONFIG
MANAGER_STABILITY_THRESHOLDS = PROJECT_MANAGER_STABILITY_THRESHOLDS
ENHANCED_STYLE_SUPPLEMENTAL_THRESHOLDS = PROJECT_ENHANCED_STYLE_SUPPLEMENTAL_THRESHOLDS
SCALE_DATA_THRESHOLDS = PROJECT_SCALE_DATA_THRESHOLDS

PROJECT_TRACKER_COLUMNS = [
    "基金代码",
    "基金名称",
    "正式榜分组",
    "正式投资评分",
    "出现次数",
    "出现频率(%)",
    "首次出现日期",
    "最后出现日期",
    "连续缺席期数",
    "当前是否在榜",
    "基金公司",
    "债基风格标签",
    "最新基金规模(亿)",
    "最新年化收益(%)",
    "最新夏普比率",
    "最新最大回撤(%)",
    "最新综合加权得分",
    "最新近期得分",
    "最新长期得分",
    "项目质量信心",
    "最新基金类型",
    "最新风险等级",
    "最新成立日期",
    "最新基金经理",
    "最新项目推荐结论",
    "最新推荐理由",
    "最新推荐备注",
    "最新持有期状态",
    "最新优选类别",
    "最新是否可申购",
    "最新申购状态",
    "最新基准归类",
    "最新经理从业与评分状态",
    "最新经理最短从业天数",
    "最新经理平均综合评分",
    "最新规模数据状态",
    "最新规模数据账龄(天)",
    "最新是否含权益/转债增强",
    "最新费率合计(%)",
    "最新近1周(%)",
    "最新近1月(%)",
    "最新近3月(%)",
    "最新近6月(%)",
    "最新近1年(%)",
    "最新近2年(%)",
    "最新近3年(%)",
    "最新今年来(%)",
    "最新成立来(%)",
    "最新波动率",
    "最新索提诺比率",
    "最新卡玛比率",
    "最新VaR95(%)",
    "最新VaR99(%)",
    "最新正收益比率",
    "最新日增长率(%)",
    "最新净值日期",
    "最新单位净值",
    "最新累计净值",
    "最新1年-最长回撤观测数",
    "最新AutoStock校验状态",
    "最新AutoStock校验备注",
    "最新AutoStock原始校验状态",
    "最新AutoStock告警原因",
    "最新长期稳定性标签",
    "最新母基金名称",
    "最新关联份额数",
    "最新关联份额名称",
    "最新关联份额代码",
]

PROJECT_TRACKER_CURRENT_COLUMNS = [
    "基金代码",
    "基金名称",
    "正式榜分组",
    "正式投资评分",
    "出现次数",
    "出现频率(%)",
    "当前是否在榜",
    "基金公司",
    "债基风格标签",
    "最新年化收益(%)",
    "最新夏普比率",
    "最新最大回撤(%)",
    "项目质量信心",
]

PROJECT_TRACKER_EXPORT_COLUMNS = [
    "序号",
    "基金代码",
    "基金名称",
    "正式榜分组",
    "正式投资评分",
    "出现次数",
    "出现频率(%)",
    "首次出现日期",
    "最后出现日期",
    "连续缺席期数",
    "当前是否在榜",
    "基金类型",
    "风险等级",
    "成立日期",
    "基金规模(亿)",
    "基金公司",
    "数据来源",
    "基金经理",
    "债基风格标签",
    "基准归类",
    "项目推荐结论",
    "推荐备注",
    "持有期状态",
    "优选类别",
    "项目质量信心",
    "是否可申购",
    "申购状态",
    "经理从业与评分状态",
    "经理最短从业天数",
    "经理平均综合评分",
    "规模数据状态",
    "规模数据账龄(天)",
    "是否含权益/转债增强",
    "费率合计(%)",
    "近1周(%)",
    "近1月(%)",
    "近3月(%)",
    "近6月(%)",
    "近1年(%)",
    "近2年(%)",
    "近3年(%)",
    "今年来(%)",
    "成立来(%)",
    "1年-波动率",
    "1年-最大回撤(%)",
    "1年-年化收益(%)",
    "1年-夏普比率",
    "1年-索提诺比率",
    "1年-卡玛比率",
    "1年-VaR95(%)",
    "1年-VaR99(%)",
    "1年-正收益比率",
    "最新日增长率(%)",
    "最新净值日期",
    "最新单位净值",
    "最新累计净值",
    "1年-最长回撤观测数",
    "综合加权得分",
    "近期得分",
    "长期得分",
    "AutoStock校验状态",
    "AutoStock校验备注",
    "AutoStock原始校验状态",
    "AutoStock告警原因",
    "长期稳定性标签",
    "推荐理由",
    "母基金名称",
    "关联份额数",
    "关联份额名称",
    "关联份额代码",
]

PROJECT_TRACKER_EXPORT_COLUMN_WIDTHS = {
    "序号": 8,
    "基金代码": 12,
    "基金名称": 24,
    "正式榜分组": 14,
    "正式投资评分": 14,
    "出现次数": 10,
    "出现频率(%)": 12,
    "首次出现日期": 12,
    "最后出现日期": 12,
    "连续缺席期数": 12,
    "当前是否在榜": 10,
    "基金类型": 14,
    "风险等级": 10,
    "成立日期": 12,
    "基金规模(亿)": 14,
    "基金公司": 18,
    "数据来源": 18,
    "基金经理": 18,
    "债基风格标签": 14,
    "基准归类": 14,
    "项目推荐结论": 14,
    "推荐备注": 30,
    "持有期状态": 14,
    "优选类别": 12,
    "项目质量信心": 12,
    "是否可申购": 10,
    "申购状态": 12,
    "经理从业与评分状态": 12,
    "经理最短从业天数": 14,
    "经理平均综合评分": 16,
    "规模数据状态": 14,
    "规模数据账龄(天)": 14,
    "是否含权益/转债增强": 14,
    "费率合计(%)": 12,
    "最新净值日期": 12,
    "最新单位净值": 12,
    "最新累计净值": 12,
    "1年-最长回撤观测数": 16,
    "综合加权得分": 14,
    "近期得分": 12,
    "长期得分": 12,
    "AutoStock校验状态": 16,
    "AutoStock校验备注": 30,
    "AutoStock原始校验状态": 16,
    "AutoStock告警原因": 28,
    "长期稳定性标签": 16,
    "推荐理由": 40,
    "母基金名称": 22,
    "关联份额数": 10,
    "关联份额名称": 24,
    "关联份额代码": 16,
}

PROJECT_TRACKER_TOP_DECILE_RULES = {
    "1年-年化收益(%)": "desc",
    "近1月(%)": "desc",
    "近3月(%)": "desc",
    "1年-夏普比率": "desc",
    "1年-最大回撤(%)": "asc",
    "1年-波动率": "asc",
}

PROJECT_TRACKER_GROUP_HEADER_SCHEMES = [
    ("跟踪信息", [
        "序号", "基金代码", "基金名称", "正式榜分组", "正式投资评分", "出现次数", "出现频率(%)",
        "首次出现日期", "最后出现日期", "连续缺席期数", "当前是否在榜",
    ], "D9EAF7"),
    ("基础属性", [
        "基金类型", "风险等级", "成立日期", "基金规模(亿)", "基金公司", "数据来源", "基金经理",
        "债基风格标签", "基准归类",
    ], "E8F3E6"),
    ("项目判断", [
        "项目推荐结论", "推荐备注", "持有期状态", "优选类别", "项目质量信心",
        "是否可申购", "申购状态", "经理从业与评分状态", "经理最短从业天数", "经理平均综合评分",
        "规模数据状态", "规模数据账龄(天)", "是否含权益/转债增强", "费率合计(%)",
    ], "FCE4D6"),
    ("收益表现", [
        "近1周(%)", "近1月(%)", "近3月(%)", "近6月(%)", "近1年(%)",
        "近2年(%)", "近3年(%)", "今年来(%)", "成立来(%)",
    ], "FFF2CC"),
    ("风险收益", [
        "1年-波动率", "1年-最大回撤(%)", "1年-年化收益(%)", "1年-夏普比率",
        "1年-索提诺比率", "1年-卡玛比率", "1年-VaR95(%)", "1年-VaR99(%)",
        "1年-正收益比率",
    ], "EADCF8"),
    ("净值与校验", [
        "最新日增长率(%)", "最新净值日期", "最新单位净值", "最新累计净值", "1年-最长回撤观测数",
        "综合加权得分", "近期得分", "长期得分",
        "AutoStock校验状态", "AutoStock校验备注", "AutoStock原始校验状态",
        "AutoStock告警原因", "长期稳定性标签",
    ], "F4CCCC"),
    ("扩展关联", [
        "推荐理由", "母基金名称", "关联份额数", "关联份额名称", "关联份额代码",
    ], "EDEDED"),
]

PROJECT_TRACKER_SUMMARY_ITEMS = [
    ("总跟踪基金数", "tracked_fund_count"),
    ("当前在榜数", "current_active_count"),
    ("重复出现基金数", "repeated_fund_count"),
    ("累计跟踪期数", "total_periods"),
    ("已处理批次数", "processed_run_count"),
    ("唯一快照数", "unique_snapshot_count"),
    ("最新业务日期", "latest_business_date"),
    ("最新净值日期", "latest_nav_snapshot_date"),
]

PROJECT_TRACKER_HEADER_REQUIRED_COLUMNS = {
    "基金代码",
    "基金名称",
    "正式投资评分",
}

PROJECT_TRACKER_DEFAULT_SORT_COLUMNS = [
    ("正式投资评分", True),
    ("出现频率(%)", True),
]

FORMAL_SCORE_LABEL = PROJECT_FORMAL_SCORE_LABEL
FORMAL_GROUP_LABEL = PROJECT_FORMAL_GROUP_LABEL
STABLE_BOND_GROUP_LABEL = PROJECT_STABLE_BOND_GROUP_LABEL
ENHANCED_BOND_GROUP_LABEL = PROJECT_ENHANCED_BOND_GROUP_LABEL
ENHANCED_STYLE_TAGS = PROJECT_ENHANCED_STYLE_TAGS

PROJECT_TRACKER_PROCESS_COLUMNS = [
    "业务日期",
    "净值快照日期",
    "快照签名",
    "项目批次",
    "生成时间",
    "正式推荐数量",
    "来源文件",
]



def _normalize_code(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    digits = "".join(ch for ch in text if ch.isdigit())
    return digits.zfill(6) if digits else text



def _pick_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for candidate in candidates:
        if candidate in df.columns:
            return candidate
    return None



def _read_tabular_file(path: str | Path | None) -> pd.DataFrame:
    if not path:
        return pd.DataFrame()
    file_path = Path(path)
    if not artifact_is_file(file_path):
        return pd.DataFrame()
    suffix = file_path.suffix.lower()
    if suffix == ".csv":
        return normalize_drawdown_duration_columns(read_csv_artifact(file_path, dtype=str))
    if suffix in {".xlsx", ".xlsm", ".xls"}:
        return normalize_drawdown_duration_columns(pd.read_excel(file_path, dtype=str))
    return pd.DataFrame()


def _read_json_file(path: str | Path | None) -> dict[str, Any]:
    if not path:
        return {}
    file_path = Path(path)
    if not file_path.exists() or not file_path.is_file():
        return {}
    try:
        payload = json.loads(file_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _read_excel_sheet(path: str | Path | None, sheet_name: str) -> pd.DataFrame:
    if not path:
        return pd.DataFrame()
    file_path = Path(path)
    if not file_path.exists():
        return pd.DataFrame()
    try:
        return normalize_drawdown_duration_columns(pd.read_excel(file_path, sheet_name=sheet_name, dtype=str, skiprows=1))
    except Exception:
        return pd.DataFrame()


RUN_LOCAL_CORE_ARTIFACT_KEYS = (
    "long_csv",
    "short_csv",
    "final_excel",
    "final_excel_meta",
    "comparison_excel",
    "comparison_excel_meta",
    "comparison_report_file",
    "accumulation_tracker_excel",
    "accumulation_tracker_excel_meta",
    "only_short_simple_csv",
    "only_long_simple_csv",
    "only_short_watchlist_csv",
    "only_long_watchlist_csv",
    "autostock_validation_audit_file",
    "bond_mainlist_quality_audit_json_file",
    "bond_mainlist_quality_audit_md_file",
    "final_screening_decision_ledger_file",
    "filtered_excel",
    "filtered_csv",
    "filtered_excel_meta",
    "dynamic_filtered_excel",
    "dynamic_filtered_csv",
    "dynamic_filtered_excel_meta",
    "threshold_compare_excel",
    "threshold_compare_excel_meta",
    "watch_excel",
    "watch_csv",
    "watch_excel_meta",
    "dynamic_watch_excel",
    "dynamic_watch_csv",
    "dynamic_watch_excel_meta",
    "excluded_excel",
    "excluded_csv",
    "excluded_excel_meta",
    "stage_b_reason_excel",
    "stage_b_reason_csv",
    "stage_b_reason_excel_meta",
    "screening_audit_excel",
    "screening_audit_excel_meta",
    "publish_chain_snapshot_json",
    "forward_validation_excel",
    "forward_validation_json",
    "forward_validation_markdown",
)

RUN_LOCAL_ARTIFACT_FALLBACK_GLOBS: dict[str, tuple[str, ...]] = {
    "filtered_excel": ("正式推荐债券基金_[0-9]*.xlsx",),
    "filtered_csv": ("正式推荐债券基金_[0-9]*.csv",),
    "filtered_excel_meta": ("正式推荐债券基金_[0-9]*_report_meta.json",),
    "dynamic_filtered_excel": ("正式推荐债券基金_动态阈值_[0-9]*.xlsx",),
    "dynamic_filtered_csv": ("正式推荐债券基金_动态阈值_[0-9]*.csv",),
    "dynamic_filtered_excel_meta": ("正式推荐债券基金_动态阈值_[0-9]*_report_meta.json",),
    "threshold_compare_excel": ("债券基金双轨阈值对比_[0-9]*.xlsx",),
    "threshold_compare_excel_meta": ("债券基金双轨阈值对比_[0-9]*_report_meta.json",),
    "watch_excel": ("自动诊断池_债券基金_[0-9]*.xlsx",),
    "watch_csv": ("自动诊断池_债券基金_[0-9]*.csv",),
    "watch_excel_meta": ("自动诊断池_债券基金_[0-9]*_report_meta.json",),
    "dynamic_watch_excel": ("自动诊断池_债券基金_动态阈值_[0-9]*.xlsx",),
    "dynamic_watch_csv": ("自动诊断池_债券基金_动态阈值_[0-9]*.csv",),
    "dynamic_watch_excel_meta": ("自动诊断池_债券基金_动态阈值_[0-9]*_report_meta.json",),
    "excluded_excel": ("当日最终优质基金_持有期排除清单_[0-9]*.xlsx",),
    "excluded_csv": ("当日最终优质基金_持有期排除清单_[0-9]*.csv",),
    "excluded_excel_meta": ("当日最终优质基金_持有期排除清单_[0-9]*_report_meta.json",),
    "stage_b_reason_excel": ("债券基金_高收益未入榜原因总账_[0-9]*.xlsx",),
    "stage_b_reason_csv": ("债券基金_高收益未入榜原因总账_[0-9]*.csv",),
    "stage_b_reason_excel_meta": ("债券基金_高收益未入榜原因总账_[0-9]*_report_meta.json",),
    "screening_audit_excel": ("债券基金_标准筛选审计_[0-9]*.xlsx",),
    "screening_audit_excel_meta": ("债券基金_标准筛选审计_[0-9]*_report_meta.json",),
    "publish_chain_snapshot_json": ("publish_chain_snapshot_*_sync.json", "publish_chain_snapshot_*.json"),
    "forward_validation_excel": ("正式榜前瞻验证_*_sync.xlsx",),
    "forward_validation_json": ("正式榜前瞻验证_*_sync.json",),
    "forward_validation_markdown": ("正式榜前瞻验证_*_sync.md",),
}

PUBLISH_CHAIN_SNAPSHOT_COLUMNS = [
    *DATA_VALIDATION_COLUMNS,
    "基金代码",
    "基金名称",
    "正式榜分组",
    "债基风格标签",
    "正式投资评分",
    "项目推荐结论",
    "推荐理由",
    "项目推荐原因代码",
    "项目质量信心",
    "推荐备注",
    "AutoStock原始校验状态",
    "AutoStock告警原因",
]


def _flatten_runtime_artifacts(artifacts: dict[str, Any] | None) -> dict[str, Any]:
    current = artifacts if isinstance(artifacts, dict) else {}
    core = current.get("core")
    if not isinstance(core, dict):
        return current.copy()
    flattened = core.copy()
    flattened.update({key: value for key, value in current.items() if key != "core"})
    return flattened


def _materialize_run_local_core_artifacts(
    *,
    settings: Settings,
    run_dir: Path,
    artifacts: dict[str, Any],
) -> dict[str, Any]:
    current = _flatten_runtime_artifacts(artifacts)
    if not current:
        return current

    run_core_dir = run_dir / "core_artifacts"
    run_core_dir.mkdir(parents=True, exist_ok=True)
    localized_artifacts = current.copy()
    core_output_dir = _resolve_project_path(settings, current.get("core_output_dir"))
    current_run_token = str(run_dir.parent.parent.name if run_dir.parent.name == "revisions" else run_dir.name).strip()

    def resolve_source_path(key: str, raw_path: Any) -> Path | None:
        source_path = _resolve_project_path(settings, raw_path)
        if source_path is not None and artifact_is_file(source_path):
            return source_path

        if core_output_dir is None or not core_output_dir.exists():
            return None

        basename = Path(str(raw_path or "").strip()).name
        if basename and current_run_token and current_run_token in basename:
            exact_matches = [
                path
                for path in sorted(glob_artifacts(core_output_dir, basename, recursive=True), key=lambda path: str(path), reverse=True)
                if artifact_is_file(path)
            ]
            if exact_matches:
                return exact_matches[0]

        # 仅允许当前 run token 的 fallback，避免把历史 run 的同名工件复制进当前 run。
        for pattern in RUN_LOCAL_ARTIFACT_FALLBACK_GLOBS.get(key, ()):
            matches = [
                path
                for path in sorted(glob_artifacts(core_output_dir, pattern, recursive=True), key=lambda path: str(path), reverse=True)
                if artifact_is_file(path)
            ]
            for match in matches:
                name = match.name
                if current_run_token and current_run_token not in name:
                    continue
                return match
        return None

    for key in RUN_LOCAL_CORE_ARTIFACT_KEYS:
        raw_path = current.get(key)
        source_path = resolve_source_path(key, raw_path)
        if source_path is None or not artifact_is_file(source_path):
            continue
        try:
            if core_output_dir is not None:
                relative_target = source_path.resolve().relative_to(core_output_dir.resolve())
                destination = run_core_dir / relative_target
            else:
                destination = run_core_dir / source_path.name
        except ValueError:
            destination = run_core_dir / source_path.name
        try:
            if source_path.resolve() == destination.resolve():
                localized_artifacts[key] = _project_relative(settings, destination)
                continue
        except OSError:
            pass
        destination.parent.mkdir(parents=True, exist_ok=True)
        copy_original_artifact(source_path, destination)
        localized_artifacts[key] = _project_relative(settings, destination)

    localized_artifacts["core_output_dir"] = _project_relative(settings, run_core_dir)
    return localized_artifacts


def _rewrite_run_local_artifact_name(name: str, *, run_id: str) -> str:
    current_name = str(name or "").strip()
    if not current_name:
        return current_name
    replaced = re.sub(r"20\d{6}_\d{6}(?:_sync)?", run_id, current_name)
    return replaced if replaced != current_name else current_name


def _normalize_lightweight_run_local_artifacts(
    *,
    settings: Settings,
    run_id: str,
    run_dir: Path,
    localized_artifacts: dict[str, Any],
) -> dict[str, Any]:
    normalized = dict(localized_artifacts or {})
    if not normalized:
        return normalized

    run_core_dir = run_dir / "core_artifacts"
    run_core_dir.mkdir(parents=True, exist_ok=True)
    reports_dir = getattr(settings, "reports_dir", None)
    if reports_dir is None:
        reports_dir = Path(getattr(settings, "project_root", Path.cwd())) / "reports"
    reports_dir = Path(reports_dir)

    for key in RUN_LOCAL_CORE_ARTIFACT_KEYS:
        raw_path = normalized.get(key)
        source_path = _resolve_project_path(settings, raw_path)
        if source_path is None or not artifact_is_file(source_path):
            continue

        target_name = _rewrite_run_local_artifact_name(source_path.name, run_id=run_id)
        try:
            relative_source = source_path.resolve().relative_to(run_core_dir.resolve())
            destination = run_core_dir / relative_source.parent / target_name
        except ValueError:
            destination = run_core_dir / source_path.parent.name / target_name if source_path.parent != run_core_dir else run_core_dir / target_name
            if source_path.parent == run_core_dir:
                destination = run_core_dir / target_name

        destination.parent.mkdir(parents=True, exist_ok=True)
        try:
            if source_path.resolve() != destination.resolve():
                copy_original_artifact(source_path, destination)
        except OSError:
            copy_original_artifact(source_path, destination)
        normalized[key] = _project_relative(settings, destination)

    tracker_src = _resolve_project_path(settings, normalized.get("project_tracker_excel"))
    if tracker_src is None or not tracker_src.exists():
        tracker_src = reports_dir / "最终优质基金累积跟踪.xlsx"
    if tracker_src.exists() and tracker_src.is_file():
        tracker_dst = run_dir / "最终优质基金累积跟踪.xlsx"
        if tracker_src.resolve() != tracker_dst.resolve():
            shutil.copy2(tracker_src, tracker_dst)
        normalized["project_tracker_excel"] = _project_relative(settings, tracker_dst)

    tracker_meta_src = _resolve_project_path(settings, normalized.get("project_tracker_excel_meta"))
    if tracker_meta_src is None or not tracker_meta_src.exists():
        tracker_meta_src = reports_dir / "最终优质基金累积跟踪_report_meta.json"
    if tracker_meta_src.exists() and tracker_meta_src.is_file():
        tracker_meta_dst = run_dir / "最终优质基金累积跟踪_report_meta.json"
        if tracker_meta_src.resolve() != tracker_meta_dst.resolve():
            shutil.copy2(tracker_meta_src, tracker_meta_dst)
        normalized["project_tracker_excel_meta"] = _project_relative(settings, tracker_meta_dst)

    report_json = run_dir / f"bond_analysis_report_{run_id}.json"
    report_md = run_dir / f"bond_analysis_report_{run_id}.md"
    normalized["report_json"] = _project_relative(settings, report_json)
    normalized["report_markdown"] = _project_relative(settings, report_md)
    normalized["core_output_dir"] = _project_relative(settings, run_core_dir)
    return normalized


def _annotate_lightweight_project_tracker_summary(
    summary: dict[str, Any] | None,
    *,
    run_id: str,
) -> dict[str, Any]:
    current = dict(summary or {})
    if not current:
        return current
    tracker_source_run_id = str(current.get("latest_run_id") or "").strip()
    current["tracker_content_source_run_id"] = tracker_source_run_id or None
    current["report_runtime_run_id"] = run_id
    reused_tracker = bool(tracker_source_run_id and tracker_source_run_id != run_id)
    current["lightweight_refresh_reused_tracker"] = reused_tracker
    if reused_tracker:
        current["lightweight_refresh_note"] = (
            "累计跟踪内容沿用已存在 tracker 快照；当前轻量刷新仅重绑定当前 run 目录工件路径，"
            "不把 tracker 内容来源伪装成当前 report run。"
        )
    return current


def _normalize_lightweight_artifact_consistency(
    artifact_consistency: dict[str, Any] | None,
    *,
    run_id: str,
    tracker_summary: dict[str, Any] | None,
) -> dict[str, Any]:
    current = dict(artifact_consistency or {})
    if not current:
        return current

    tracker_payload = dict(tracker_summary or {})
    tracker_source_run_id = str(
        tracker_payload.get("tracker_content_source_run_id")
        or tracker_payload.get("latest_run_id")
        or ""
    ).strip()
    reused_tracker = bool(tracker_payload.get("lightweight_refresh_reused_tracker"))

    checks: list[dict[str, Any]] = []
    for check in list(current.get("checks") or []):
        item = dict(check or {})
        if reused_tracker and str(item.get("name") or "").strip() == "project_tracker_latest_run_id_matches_report":
            item["name"] = "project_tracker_latest_run_id_matches_tracker_source"
            item["aligned"] = True
            item["detail"] = (
                f"轻量刷新复用 tracker 内容源 run={tracker_source_run_id or '-'}；"
                f"当前 report run={run_id}；工件路径已重绑定当前 run。"
            )
            item["tracker_content_source_run_id"] = tracker_source_run_id or None
            item["report_run_id"] = run_id
            item["lightweight_refresh_reused_tracker"] = True
        checks.append(item)

    if checks:
        current["checks"] = checks
        current["blockers"] = [item for item in checks if not bool((item or {}).get("aligned"))]
        current["blocker_count"] = len(current["blockers"])

    current["lightweight_refresh"] = True
    current["lightweight_refresh_detail"] = (
        "本次仅刷新 report json/markdown 与实时治理摘要。"
        if not reused_tracker
        else (
            "本次仅刷新 report json/markdown 与实时治理摘要；"
            "重型 workbook/累计跟踪内容沿用上一版 canonical 产物，但当前 run 工件路径已完成重绑定。"
        )
    )
    if reused_tracker:
        current["tracker_content_source_run_id"] = tracker_source_run_id or None
        current["report_runtime_run_id"] = run_id
    return current


def _build_run_local_risk_audit_artifacts(
    *,
    settings: Settings,
    run_id: str,
    run_dir: Path,
) -> dict[str, Any]:
    audit_root = run_dir / "audit"

    def _scope(
        detail: str,
    ) -> dict[str, Any]:
        return {
            "run_id": run_id,
            "latest_canonical_run_id": run_id,
            "replay_precision": {"status": "snapshot_frozen", "status_label": "已保存当期审计快照", "baseline_reproduction_verified": False},
            "replay_precision_status": "snapshot_frozen",
            "replay_precision_status_label": "已保存当期审计快照",
            "auxiliary_scope": "same_run_report_artifact",
            "auxiliary_scope_label": "同 run 风险审计产物",
            "scope_detail": detail,
        }

    risk_lag = build_risk_lag_audit(
        settings,
        artifact_dir=audit_root / "risk_lag_audit",
        artifact_path=build_audit_artifact_path(audit_root / "risk_lag_audit", "risk_lag_audit", run_id=run_id),
        runtime_scope=_scope("当前摘要来自同一 run 内生成并固化的 risk lag 审计产物。"),
    )
    risk_sync = build_risk_sync_audit(
        settings,
        artifact_dir=audit_root / "risk_sync_audit",
        artifact_path=build_audit_artifact_path(audit_root / "risk_sync_audit", "risk_sync_audit", run_id=run_id),
        runtime_scope=_scope("当前摘要来自同一 run 内生成并固化的 risk sync 审计产物。"),
    )
    risk_coverage_gap = build_risk_coverage_gap_audit(
        settings,
        artifact_dir=audit_root / "risk_coverage_gap_audit",
        artifact_path=build_audit_artifact_path(
            audit_root / "risk_coverage_gap_audit",
            "risk_coverage_gap_audit",
            run_id=run_id,
        ),
        runtime_scope=_scope("当前摘要来自同一 run 内生成并固化的 risk coverage gap 审计产物。"),
    )
    return {
        "risk_lag_audit_json": _project_relative(settings, risk_lag.get("artifact_path")),
        "risk_sync_audit_json": _project_relative(settings, risk_sync.get("artifact_path")),
        "risk_coverage_gap_audit_json": _project_relative(settings, risk_coverage_gap.get("artifact_path")),
    }


def _build_publish_chain_snapshot_export(df: pd.DataFrame) -> list[dict[str, Any]]:
    if not isinstance(df, pd.DataFrame) or df.empty:
        return []
    current = df.copy()
    if "基金代码" in current.columns:
        current["基金代码"] = current["基金代码"].map(_normalize_code)
    current = _ensure_export_formal_score_column(current).rename(columns=PROJECT_EXPORT_RENAME_MAPPING)
    current = _drop_duplicate_columns(current)
    current = _append_formal_group_column(current)
    selected = _select_unique_columns(current, PUBLISH_CHAIN_SNAPSHOT_COLUMNS)
    return selected.fillna("").astype(str).to_dict(orient="records")


def _write_publish_chain_snapshot(
    *,
    settings: Settings,
    run_dir: Path,
    run_id: str,
    current_generated_at: str,
    dynamic_reference_export_df: pd.DataFrame,
    dynamic_post_review_df: pd.DataFrame,
    final_official_df: pd.DataFrame,
    final_selection_removed_df: pd.DataFrame,
    final_selection_contraction_df: pd.DataFrame | None = None,
) -> Path:
    snapshot_path = run_dir / f"publish_chain_snapshot_{run_id}.json"
    payload = {
        "run_id": run_id,
        "generated_at": current_generated_at,
        "dynamic_reference_export_count": int(len(dynamic_reference_export_df.index)),
        "dynamic_post_review_count": int(len(dynamic_post_review_df.index)),
        "final_official_count": int(len(final_official_df.index)),
        "final_selection_removed_count": int(len(final_selection_removed_df.index)),
        "dynamic_reference_export_records": _build_publish_chain_snapshot_export(dynamic_reference_export_df),
        "dynamic_post_review_records": _build_publish_chain_snapshot_export(dynamic_post_review_df),
        "final_official_records": _build_publish_chain_snapshot_export(final_official_df),
        "final_selection_removed_records": _build_publish_chain_snapshot_export(final_selection_removed_df),
        "final_selection_contraction_records": (
            []
            if final_selection_contraction_df is None
            else final_selection_contraction_df.fillna("").astype(str).to_dict(orient="records")
        ),
    }
    snapshot_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return snapshot_path



def _read_metadata(db_path: Path) -> pd.DataFrame:
    if not db_path.exists():
        return pd.DataFrame(
            columns=[
                "fcode",
                "shortname",
                "fullname",
                "ftype",
                "sub_type",
                "type_category",
                "type_code",
                "risk_level",
                "has_holding_period",
                "holding_days",
                "holding_keywords",
                "holding_period_source",
                "company_name",
                "manager_ids",
                "manager_names",
                "estab_date",
                "scale_billion",
                "scale_report_date",
                "mgr_fee_rate",
                "trust_fee_rate",
                "sales_fee_rate",
                "benchmark",
                "investment_target",
                "investment_strategy",
                "is_buyable",
                "purchase_status",
                "data_source",
                "data_source_governance_flag",
            ]
        )

    conn = sqlite3.connect(str(db_path))
    try:
        fund_columns = {
            str(row[1]).strip()
            for row in conn.execute("PRAGMA table_info(funds)").fetchall()
            if len(row) > 1 and str(row[1]).strip()
        }
        holding_period_source_select = "holding_period_source" if "holding_period_source" in fund_columns else "'' AS holding_period_source"
        fullname_select = "fullname" if "fullname" in fund_columns else "NULL AS fullname"
        data_source_select = (
            "COALESCE(data_source, '') AS data_source"
            if "data_source" in fund_columns
            else "'' AS data_source"
        )
        data_source_governance_flag_select = (
            "COALESCE(data_source_governance_flag, '') AS data_source_governance_flag"
            if "data_source_governance_flag" in fund_columns
            else "'' AS data_source_governance_flag"
        )
        query = """
        SELECT
            fcode,
            shortname,
            {fullname_select},
            ftype,
            sub_type,
            type_category,
            type_code,
            risk_level,
            COALESCE(has_holding_period, 0) AS has_holding_period,
            holding_days,
            holding_keywords,
            {holding_period_source_select},
            company_name,
            manager_ids,
            manager_names,
            estab_date,
            scale_billion,
            scale_report_date,
            mgr_fee_rate,
            trust_fee_rate,
            sales_fee_rate,
            benchmark,
            investment_target,
            investment_strategy,
            is_buyable,
            purchase_status,
            {data_source_select},
            {data_source_governance_flag_select}
        FROM funds
        """
        df = pd.read_sql_query(
            query.format(
                holding_period_source_select=holding_period_source_select,
                fullname_select=fullname_select,
                data_source_select=data_source_select,
                data_source_governance_flag_select=data_source_governance_flag_select,
            ),
            conn,
        )
    finally:
        conn.close()

    if not df.empty:
        df["fcode"] = df["fcode"].map(_normalize_code)
        df["has_holding_period"] = pd.to_numeric(df["has_holding_period"], errors="coerce").fillna(0).astype(int)
        df["holding_days"] = pd.to_numeric(df["holding_days"], errors="coerce")
        df["is_buyable"] = pd.to_numeric(df["is_buyable"], errors="coerce")
    return df


def _load_runtime_excluded_fund_codes_lightweight() -> set[str]:
    return set(get_runtime_excluded_fund_codes())


def _build_bond_universe_code_sets(
    *,
    settings: Settings,
    metadata_df: pd.DataFrame,
    returns_df: pd.DataFrame,
    risk_df: pd.DataFrame,
) -> dict[str, set[str]]:
    runtime_excluded_codes = _load_runtime_excluded_fund_codes_lightweight()

    metadata = metadata_df.copy() if isinstance(metadata_df, pd.DataFrame) else pd.DataFrame()
    returns = returns_df.copy() if isinstance(returns_df, pd.DataFrame) else pd.DataFrame()
    risk = risk_df.copy() if isinstance(risk_df, pd.DataFrame) else pd.DataFrame()

    if metadata.empty:
        return {
            "runtime_excluded_codes": set(runtime_excluded_codes),
            "stored_codes": set(),
            "active_codes": set(),
            "returns_ready_codes": set(),
            "risk_ready_codes": set(),
            "holding_period_blocked_codes": set(),
            "screenable_codes": set(),
            "stale_or_missing_nav_codes": set(),
            "missing_1y_risk_codes": set(),
        }

    metadata["fcode"] = metadata.get("fcode", pd.Series(dtype=object)).map(_normalize_code)
    metadata = metadata.dropna(subset=["fcode"]).drop_duplicates(subset=["fcode"], keep="first")
    metadata = metadata[metadata.apply(lambda row: _is_bond_fund_row(row.to_dict()), axis=1)].copy()

    stored_codes = set(metadata.get("fcode", pd.Series(dtype=object)).astype(str))
    active_codes = {code for code in stored_codes if code and code not in runtime_excluded_codes}

    returns_ready_codes: set[str] = set()
    if not returns.empty:
        current_returns = returns.copy()
        current_returns["fcode"] = current_returns.get("fcode", pd.Series(dtype=object)).map(_normalize_code)
        latest_nav_dates = pd.to_datetime(current_returns.get("latest_nav_date"), errors="coerce")
        now_ts = datetime.now()
        fresh_mask = latest_nav_dates.notna() & ((now_ts - latest_nav_dates).dt.days <= DYNAMIC_NAV_FRESHNESS_DAYS)
        returns_ready_codes = {
            str(code)
            for code in current_returns.loc[fresh_mask, "fcode"].dropna().astype(str).tolist()
            if code
        }

    risk_ready_codes: set[str] = set()
    if not risk.empty:
        current_risk = risk.copy()
        current_risk["fcode"] = current_risk.get("fcode", pd.Series(dtype=object)).map(_normalize_code)
        required_risk_columns = ["annual_return", "sharpe_ratio", "max_drawdown", "volatility"]
        for column in required_risk_columns:
            current_risk[column] = pd.to_numeric(current_risk.get(column), errors="coerce")
        risk_mask = current_risk[required_risk_columns].notna().all(axis=1)
        risk_ready_codes = {
            str(code)
            for code in current_risk.loc[risk_mask, "fcode"].dropna().astype(str).tolist()
            if code
        }

    holding_reasons = metadata.apply(
        lambda row: holding_period_gate_reason(
            has_holding_period=row.get("has_holding_period"),
            holding_days=row.get("holding_days"),
            holding_period_source=row.get("holding_period_source"),
            holding_keywords=row.get("holding_keywords"),
            threshold_days=settings.exclude_holding_days_gt,
            fund_name=combine_holding_period_names(row.get("shortname"), row.get("fullname")),
        ),
        axis=1,
    ) if not metadata.empty else pd.Series(index=metadata.index, dtype=object)
    holding_period_blocked_codes = {
        str(code)
        for code in metadata.loc[
            holding_reasons.eq("holding_period_limit"),
            "fcode"
        ].dropna().astype(str).tolist()
        if code
    }

    screenable_codes = {
        code
        for code in active_codes
        if code in returns_ready_codes and code in risk_ready_codes and code not in holding_period_blocked_codes
    }

    stale_or_missing_nav_codes = {code for code in active_codes if code not in returns_ready_codes}
    missing_1y_risk_codes = {code for code in active_codes if code not in risk_ready_codes}

    return {
        "runtime_excluded_codes": set(runtime_excluded_codes),
        "stored_codes": stored_codes,
        "active_codes": active_codes,
        "returns_ready_codes": returns_ready_codes,
        "risk_ready_codes": risk_ready_codes,
        "holding_period_blocked_codes": holding_period_blocked_codes,
        "screenable_codes": screenable_codes,
        "stale_or_missing_nav_codes": stale_or_missing_nav_codes,
        "missing_1y_risk_codes": {code for code in missing_1y_risk_codes if code not in holding_period_blocked_codes},
    }


def _build_bond_universe_snapshot(
    *,
    settings: Settings,
    metadata_df: pd.DataFrame,
    returns_df: pd.DataFrame,
    risk_df: pd.DataFrame,
) -> dict[str, Any]:
    code_sets = _build_bond_universe_code_sets(
        settings=settings,
        metadata_df=metadata_df,
        returns_df=returns_df,
        risk_df=risk_df,
    )

    if not code_sets["stored_codes"]:
        return {
            "stored_universe_count": 0,
            "active_universe_count": 0,
            "has_latest_nav_count": 0,
            "has_1y_risk_metrics_count": 0,
            "screenable_universe_count": 0,
            "runtime_excluded_count": 0,
            "holding_period_blocked_count": 0,
            "stale_or_missing_nav_count": 0,
            "missing_1y_risk_count": 0,
            "screenable_definition": (
                f"债券基金 + 非运行时排除 + 最新净值日期在{DYNAMIC_NAV_FRESHNESS_DAYS}天内 + "
                f"具备1年风险核心字段 + 持有期不超过{settings.exclude_holding_days_gt}天"
            ),
            "runtime_excluded_codes_sample": [],
        }
    stored_codes = code_sets["stored_codes"]
    active_codes = code_sets["active_codes"]
    returns_ready_codes = code_sets["returns_ready_codes"]
    risk_ready_codes = code_sets["risk_ready_codes"]
    holding_period_blocked_codes = code_sets["holding_period_blocked_codes"]
    screenable_codes = code_sets["screenable_codes"]
    stale_or_missing_nav_codes = code_sets["stale_or_missing_nav_codes"]
    missing_1y_risk_codes = code_sets["missing_1y_risk_codes"]
    runtime_excluded_codes = code_sets["runtime_excluded_codes"]

    return {
        "stored_universe_count": int(len(stored_codes)),
        "active_universe_count": int(len(active_codes)),
        "stored_has_latest_nav_count": int(len(stored_codes & returns_ready_codes)),
        "stored_has_1y_risk_metrics_count": int(len(stored_codes & risk_ready_codes)),
        "stored_holding_period_blocked_count": int(len(stored_codes & holding_period_blocked_codes)),
        "has_latest_nav_count": int(len(active_codes & returns_ready_codes)),
        "has_1y_risk_metrics_count": int(len(active_codes & risk_ready_codes)),
        "screenable_universe_count": int(len(screenable_codes)),
        "runtime_excluded_count": int(len(stored_codes & runtime_excluded_codes)),
        "holding_period_blocked_count": int(len(active_codes & holding_period_blocked_codes)),
        "stale_or_missing_nav_count": int(len(stale_or_missing_nav_codes)),
        "missing_1y_risk_count": int(len(missing_1y_risk_codes)),
        "screenable_definition": (
            f"债券基金 + 非运行时排除 + 最新净值日期在{DYNAMIC_NAV_FRESHNESS_DAYS}天内 + "
            f"具备1年风险核心字段 + 持有期不超过{settings.exclude_holding_days_gt}天"
        ),
        "runtime_excluded_codes_sample": sorted(runtime_excluded_codes)[:10],
    }


def _long_term_history_rank_snapshot_stats_fingerprint(
    db_path: Path,
    rank_archive_attach: dict[str, Any] | None,
    cache_snapshot: RankCacheSnapshot | None = None,
) -> dict[str, Any]:
    archive = dict(rank_archive_attach or {})
    payload = {
        "schema_version": _LONG_TERM_HISTORY_RANK_SNAPSHOT_STATS_SIDECAR_SCHEMA,
        "db": (cache_snapshot.main_token if cache_snapshot is not None
               else _sqlite_db_snapshot_cache_token(db_path)),
        "archive_attached": bool(archive.get("attached")),
        "archive_path": str(archive.get("archive_path") or ""),
        "archive_file": (cache_snapshot.archive_token if cache_snapshot is not None
                         else _sqlite_db_snapshot_cache_token(archive.get("archive_path"))),
        "archive_alias": str(archive.get("alias") or "rank_archive_db"),
        "archive_table_name": str(archive.get("archive_table_name") or ""),
        "archive_cutoff_date": str(archive.get("archive_cutoff_date") or ""),
        "min_rank_observations": LONG_TERM_RANK_MIN_OBSERVATIONS,
    }
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    payload["fingerprint"] = hashlib.sha1(raw.encode("utf-8")).hexdigest()
    return payload


def _long_term_history_rank_snapshot_stats_sidecar_path(db_path: Path) -> Path | None:
    try:
        settings = load_settings()
        configured_db = Path(settings.database_path)
        if Path(db_path).resolve() != configured_db.resolve():
            return None
        reports_dir = Path(settings.reports_dir)
    except Exception:
        return None
    return (
        reports_dir
        / "audit"
        / "long_term_rank_snapshot_stats"
        / _LONG_TERM_HISTORY_RANK_SNAPSHOT_STATS_SIDECAR_FILENAME
    )


def _copy_long_term_history_rank_snapshot_stats(stats: dict[str, Any] | None) -> dict[str, Any]:
    payload = dict(stats or {})
    codes = {
        _normalize_code(code)
        for code in (payload.get("codes") or [])
        if _normalize_code(code)
    }
    feature_ready_codes = {
        _normalize_code(code)
        for code in (payload.get("feature_ready_codes") or [])
        if _normalize_code(code)
    }
    latest_date_map = {
        _normalize_code(code): str(value or "").strip()
        for code, value in dict(payload.get("latest_date_map") or {}).items()
        if _normalize_code(code) and str(value or "").strip()
    }
    copied = {
        "record_count": int(payload.get("record_count") or 0),
        "codes": set(codes),
        "feature_ready_codes": set(feature_ready_codes),
        "latest_date_map": dict(latest_date_map),
    }
    if "hot_percentile_codes" in payload:
        hot_percentile_codes = {
            _normalize_code(code)
            for code in (payload.get("hot_percentile_codes") or [])
            if _normalize_code(code)
        }
        copied["hot_percentile_codes"] = set(hot_percentile_codes)
    return copied


def _serialize_long_term_history_rank_snapshot_stats(stats: dict[str, Any]) -> dict[str, Any]:
    copied = _copy_long_term_history_rank_snapshot_stats(stats)
    latest_date_map = dict(copied.get("latest_date_map") or {})
    serialized = {
        "record_count": int(copied.get("record_count") or 0),
        "codes": sorted(copied.get("codes") or []),
        "feature_ready_codes": sorted(copied.get("feature_ready_codes") or []),
        "latest_date_map": {
            code: latest_date_map[code]
            for code in sorted(latest_date_map)
        },
    }
    if "hot_percentile_codes" in copied:
        serialized["hot_percentile_codes"] = sorted(copied.get("hot_percentile_codes") or [])
    return serialized


def _load_long_term_history_rank_snapshot_stats_sidecar(
    sidecar_path: Path | None,
    fingerprint: dict[str, Any],
) -> dict[str, Any] | None:
    if sidecar_path is None:
        return None
    try:
        payload = json.loads(sidecar_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        return None
    if not isinstance(payload, dict):
        return None
    if (type(payload.get("schema_version")) is not int
            or payload["schema_version"] != _LONG_TERM_HISTORY_RANK_SNAPSHOT_STATS_SIDECAR_SCHEMA):
        return None
    if payload.get("source_fingerprint") != fingerprint:
        return None
    stats = payload.get("stats")
    if not isinstance(stats, dict):
        return None
    if type(stats.get("record_count")) is not int or stats["record_count"] < 0:
        return None
    try:
        canonical = _serialize_long_term_history_rank_snapshot_stats(stats)
        if canonical != stats or "hot_percentile_codes" not in stats:
            return None
        codes = set(stats["codes"])
        if (stats["record_count"] < len(codes)
                or not set(stats["feature_ready_codes"]).issubset(codes)
                or not set(stats["hot_percentile_codes"]).issubset(codes)
                or not set(stats["latest_date_map"]).issubset(codes)):
            return None
        encoded = json.dumps(canonical, ensure_ascii=False, sort_keys=True,
                             separators=(",", ":"), allow_nan=False).encode("utf-8")
    except (TypeError, ValueError, AttributeError):
        return None
    if hashlib.sha256(encoded).hexdigest() != payload.get("stats_sha256"):
        return None
    return _copy_long_term_history_rank_snapshot_stats(stats)


def _write_long_term_history_rank_snapshot_stats_sidecar(
    sidecar_path: Path | None,
    fingerprint: dict[str, Any],
    stats: dict[str, Any],
) -> None:
    if sidecar_path is None:
        return
    serialized_stats = _serialize_long_term_history_rank_snapshot_stats(stats)
    payload = {
        "artifact_type": "long_term_history_rank_snapshot_stats",
        "schema_version": _LONG_TERM_HISTORY_RANK_SNAPSHOT_STATS_SIDECAR_SCHEMA,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "source_fingerprint": dict(fingerprint),
        "source_summary": {
            "record_count": int(serialized_stats.get("record_count") or 0),
            "code_count": len(serialized_stats.get("codes") or []),
            "feature_ready_code_count": len(serialized_stats.get("feature_ready_codes") or []),
            "latest_date_map_count": len(serialized_stats.get("latest_date_map") or {}),
            "hot_percentile_code_count": len(serialized_stats.get("hot_percentile_codes") or []),
        },
        "stats": serialized_stats,
        "stats_sha256": hashlib.sha256(json.dumps(
            serialized_stats, ensure_ascii=False, sort_keys=True,
            separators=(",", ":"), allow_nan=False,
        ).encode("utf-8")).hexdigest(),
    }
    temp_path = None
    try:
        sidecar_path.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=sidecar_path.parent,
                                         prefix=sidecar_path.name + ".", suffix=".tmp", delete=False) as stream:
            temp_path = Path(stream.name)
            stream.write(json.dumps(payload, ensure_ascii=False, indent=2))
        temp_path.replace(sidecar_path)
    except OSError:
        return
    finally:
        if temp_path is not None:
            try:
                temp_path.unlink(missing_ok=True)
            except OSError:
                pass


def _read_long_term_history_rank_snapshot_stats(
    conn: sqlite3.Connection,
    db_path: Path,
    rank_archive_attach: dict[str, Any] | None,
    *,
    cache_snapshot: RankCacheSnapshot | None = None,
) -> dict[str, Any]:
    with rank_cache_snapshot(conn, db_path, rank_archive_attach, cache_snapshot,
                             begin=begin_rank_history_read_snapshot) as snapshot:
        cached = _load_cached_long_term_history_rank_snapshot_stats(
            db_path, rank_archive_attach, cache_snapshot=snapshot,
        )
        if cached is not None:
            snapshot.local_stats = _copy_long_term_history_rank_snapshot_stats(cached)
            return cached
        stats = _aggregate_long_term_history_rank_snapshot_stats(conn, rank_archive_attach)
        snapshot.require_read_view(db_path, rank_archive_attach)
        snapshot.local_stats = _copy_long_term_history_rank_snapshot_stats(stats)
        if snapshot.cacheable(db_path, rank_archive_attach):
            fingerprint = _long_term_history_rank_snapshot_stats_fingerprint(
                db_path, rank_archive_attach, snapshot,
            )
            cache_key = str(fingerprint["fingerprint"])
            _LONG_TERM_HISTORY_RANK_SNAPSHOT_STATS_CACHE.clear()
            _LONG_TERM_HISTORY_RANK_SNAPSHOT_STATS_CACHE[cache_key] = _copy_long_term_history_rank_snapshot_stats(stats)
            sidecar = _long_term_history_rank_snapshot_stats_sidecar_path(db_path)
            _write_long_term_history_rank_snapshot_stats_sidecar(sidecar, fingerprint, stats)
        return stats


def _load_cached_long_term_history_rank_snapshot_stats(
    db_path: Path,
    rank_archive_attach: dict[str, Any] | None,
    *,
    cache_snapshot: RankCacheSnapshot | None = None,
) -> dict[str, Any] | None:
    """Load only an aggregate bound to the caller's actual read snapshot."""
    if cache_snapshot is not None:
        if not cache_snapshot.usable(cache_snapshot.connection, db_path, rank_archive_attach):
            return None
        if cache_snapshot.local_stats is not None:
            return _copy_long_term_history_rank_snapshot_stats(cache_snapshot.local_stats)
        if not cache_snapshot.cacheable(db_path, rank_archive_attach):
            return None
    fingerprint = _long_term_history_rank_snapshot_stats_fingerprint(
        db_path, rank_archive_attach, cache_snapshot,
    )
    cache_key = str(fingerprint["fingerprint"])
    cached = _LONG_TERM_HISTORY_RANK_SNAPSHOT_STATS_CACHE.get(cache_key)
    if cached is not None:
        return _copy_long_term_history_rank_snapshot_stats(cached)
    sidecar_path = _long_term_history_rank_snapshot_stats_sidecar_path(db_path)
    sidecar_stats = _load_long_term_history_rank_snapshot_stats_sidecar(sidecar_path, fingerprint)
    if sidecar_stats is None:
        return None
    _LONG_TERM_HISTORY_RANK_SNAPSHOT_STATS_CACHE.clear()
    _LONG_TERM_HISTORY_RANK_SNAPSHOT_STATS_CACHE[cache_key] = _copy_long_term_history_rank_snapshot_stats(sidecar_stats)
    return _copy_long_term_history_rank_snapshot_stats(sidecar_stats)


def _aggregate_long_term_history_rank_snapshot_stats(
    conn: sqlite3.Connection,
    rank_archive_attach: dict[str, Any] | None,
) -> dict[str, Any]:
    per_code: dict[str, dict[str, Any]] = {}
    hot_percentile_codes: set[str] = set()
    record_count = 0

    def _merge_rows(rows: list[tuple[Any, ...]]) -> None:
        nonlocal record_count
        for row in rows:
            raw_code = row[0] if len(row) > 0 else ""
            row_count = int(row[1] or 0) if len(row) > 1 else 0
            percentile_count = int(row[2] or 0) if len(row) > 2 else 0
            latest_rank_date = str(row[3] or "").strip() if len(row) > 3 else ""
            record_count += row_count
            code = _normalize_code(raw_code)
            if not code:
                continue
            stats = per_code.setdefault(
                code,
                {"row_count": 0, "percentile_count": 0, "latest_rank_date": ""},
            )
            stats["row_count"] = int(stats.get("row_count") or 0) + row_count
            stats["percentile_count"] = int(stats.get("percentile_count") or 0) + percentile_count
            if latest_rank_date and latest_rank_date > str(stats.get("latest_rank_date") or ""):
                stats["latest_rank_date"] = latest_rank_date

    hot_rows = conn.execute(
        """
        SELECT
            fcode,
            COUNT(*) AS row_count,
            SUM(CASE WHEN percentile IS NOT NULL THEN 1 ELSE 0 END) AS percentile_count,
            MAX(CASE WHEN percentile IS NOT NULL THEN rank_date ELSE NULL END) AS latest_rank_date
        FROM fund_rank_history
        GROUP BY fcode
        """
    ).fetchall()
    hot_percentile_codes = {
        code
        for code in (_normalize_code(row[0]) for row in hot_rows if int(row[2] or 0) > 0)
        if code
    }
    _merge_rows(hot_rows)

    archive = dict(rank_archive_attach or {})
    if archive.get("attached"):
        alias = str(archive.get("alias") or "rank_archive_db").strip()
        table_name = str(archive.get("archive_table_name") or "fund_rank_history_archive").strip()
        if _is_safe_sql_identifier(alias) and _is_safe_sql_identifier(table_name):
            cutoff = str(archive.get("archive_cutoff_date") or "").strip()
            archive_predicates = [
                """
                NOT EXISTS (
                    SELECT 1
                    FROM fund_rank_history AS hot_override
                    WHERE hot_override.fcode = archived_rank.fcode
                      AND hot_override.rank_date = archived_rank.rank_date
                      AND hot_override.period IS archived_rank.period
                )
                """.strip()
            ]
            params: tuple[Any, ...] = ()
            if cutoff:
                archive_predicates.insert(0, "archived_rank.rank_date < ?")
                params = (cutoff,)
            where_sql = "WHERE " + " AND ".join(archive_predicates)
            archive_rows = conn.execute(
                f"""
                SELECT
                    fcode,
                    COUNT(*) AS row_count,
                    SUM(CASE WHEN percentile IS NOT NULL THEN 1 ELSE 0 END) AS percentile_count,
                    MAX(CASE WHEN percentile IS NOT NULL THEN rank_date ELSE NULL END) AS latest_rank_date
                FROM {alias}.{table_name} AS archived_rank
                {where_sql}
                GROUP BY fcode
                """,
                params,
            ).fetchall()
            _merge_rows(archive_rows)

    rank_codes = set(per_code)
    rank_feature_ready_codes = {
        code
        for code, stats in per_code.items()
        if int(stats.get("percentile_count") or 0) >= LONG_TERM_RANK_MIN_OBSERVATIONS
    }
    rank_latest_date_map = {
        code: str(stats.get("latest_rank_date") or "").strip()
        for code, stats in per_code.items()
        if str(stats.get("latest_rank_date") or "").strip()
    }
    return {
        "record_count": int(record_count),
        "codes": rank_codes,
        "feature_ready_codes": rank_feature_ready_codes,
        "latest_date_map": rank_latest_date_map,
        "hot_percentile_codes": hot_percentile_codes,
    }


def _read_long_term_earliest_nav_date_map(
    conn: sqlite3.Connection,
    *,
    nav_source_sql: str,
    nav_archive_attach: dict[str, Any] | None,
) -> dict[str, str]:
    fallback_query = f"""
        SELECT fcode, MIN(nav_date) AS earliest_nav_date
        FROM ({nav_source_sql}) AS nav_history
        GROUP BY fcode
    """

    frame: pd.DataFrame | None = None
    archive = dict(nav_archive_attach or {})
    if archive.get("attached"):
        alias = str(archive.get("alias") or "nav_archive_db").strip()
        table_name = str(archive.get("archive_table_name") or "fund_nav_history_archive").strip()
        if _is_safe_sql_identifier(alias) and _is_safe_sql_identifier(table_name):
            cutoff = str(archive.get("archive_cutoff_date") or "").strip()
            cutoff_sql = "WHERE nav_date < ?" if cutoff else ""
            params: tuple[Any, ...] = (cutoff,) if cutoff else ()
            split_query = f"""
                WITH hot AS (
                    SELECT fcode, MIN(nav_date) AS earliest_nav_date
                    FROM fund_nav_history
                    GROUP BY fcode
                ),
                cold AS (
                    SELECT fcode, MIN(nav_date) AS earliest_nav_date
                    FROM {alias}.{table_name}
                    {cutoff_sql}
                    GROUP BY fcode
                ),
                merged AS (
                    SELECT fcode, earliest_nav_date FROM hot
                    UNION ALL
                    SELECT fcode, earliest_nav_date FROM cold
                )
                SELECT fcode, MIN(earliest_nav_date) AS earliest_nav_date
                FROM merged
                GROUP BY fcode
            """
            try:
                frame = pd.read_sql_query(split_query, conn, params=params)
            except (sqlite3.Error, pd.errors.DatabaseError, ValueError):
                frame = None

    if frame is None:
        frame = pd.read_sql_query(fallback_query, conn)

    if frame.empty:
        return {}
    result: dict[str, str] = {}
    for row in frame.to_dict(orient="records"):
        code = _normalize_code(row.get("fcode"))
        earliest_nav_date = str(row.get("earliest_nav_date") or "").strip()
        if code and earliest_nav_date:
            result[code] = earliest_nav_date
    return result


def _read_long_term_nav_window_year_rows(
    conn: sqlite3.Connection,
    *,
    nav_source_sql: str,
    nav_archive_attach: dict[str, Any] | None,
    candidate_codes: set[str],
    start_date: str,
    end_date: str,
    chunk_size: int = 500,
) -> list[tuple[Any, ...]]:
    normalized_candidate_codes = sorted(
        {
            _normalize_code(code)
            for code in candidate_codes
            if _normalize_code(code)
        }
    )
    if not normalized_candidate_codes:
        return []

    def _read_with_union_source() -> list[tuple[Any, ...]]:
        rows: list[tuple[Any, ...]] = []
        for start in range(0, len(normalized_candidate_codes), chunk_size):
            code_batch = normalized_candidate_codes[start:start + chunk_size]
            placeholders = ",".join("?" for _ in code_batch)
            rows.extend(
                conn.execute(
                    f"""
                    SELECT fcode, CAST(substr(nav_date, 1, 4) AS INTEGER) AS nav_year, MIN(nav_date) AS first_nav_date
                    FROM ({nav_source_sql}) AS nav_history
                    WHERE fcode IN ({placeholders})
                      AND nav_date BETWEEN ? AND ?
                    GROUP BY fcode, substr(nav_date, 1, 4)
                    """,
                    tuple(code_batch) + (start_date, end_date),
                ).fetchall()
            )
        return rows

    def _read_per_year_from_source(
        source_sql: str,
        *,
        max_nav_date_exclusive: str = "",
    ) -> list[tuple[Any, ...]]:
        try:
            start_year = int(str(start_date)[:4])
            end_year = int(str(end_date)[:4])
        except (TypeError, ValueError):
            return _read_with_union_source()
        if end_year < start_year:
            return []

        rows: list[tuple[Any, ...]] = []
        cutoff = str(max_nav_date_exclusive or "").strip()
        cutoff_sql = "AND nav_date < ?" if cutoff else ""
        for year in range(start_year, end_year + 1):
            year_start = max(str(start_date), f"{year:04d}-01-01")
            year_end = min(str(end_date), f"{year:04d}-12-31")
            if year_start > year_end:
                continue
            for start in range(0, len(normalized_candidate_codes), chunk_size):
                code_batch = normalized_candidate_codes[start:start + chunk_size]
                placeholders = ",".join("?" for _ in code_batch)
                params: tuple[Any, ...] = (year, *code_batch, year_start, year_end)
                if cutoff:
                    params = params + (cutoff,)
                rows.extend(
                    conn.execute(
                        f"""
                        SELECT fcode, ? AS nav_year, MIN(nav_date) AS first_nav_date
                        FROM {source_sql}
                        WHERE fcode IN ({placeholders})
                          AND nav_date BETWEEN ? AND ?
                          {cutoff_sql}
                        GROUP BY fcode
                        """,
                        params,
                    ).fetchall()
                )
        return rows

    def _merge_nav_window_rows(rows: list[tuple[Any, ...]]) -> list[tuple[Any, ...]]:
        earliest_by_code_year: dict[tuple[str, int], str] = {}
        for row in rows:
            code = _normalize_code(row[0])
            if not code or row[1] is None:
                continue
            try:
                year = int(row[1])
            except (TypeError, ValueError):
                continue
            first_nav_date = str(row[2] or "").strip()
            if not first_nav_date:
                continue
            key = (code, year)
            if key not in earliest_by_code_year or first_nav_date < earliest_by_code_year[key]:
                earliest_by_code_year[key] = first_nav_date
        return [
            (code, year, earliest_by_code_year[(code, year)])
            for code, year in sorted(earliest_by_code_year)
        ]

    archive = dict(nav_archive_attach or {})
    if not archive.get("attached"):
        try:
            return _merge_nav_window_rows(_read_per_year_from_source("fund_nav_history"))
        except sqlite3.Error:
            return _read_with_union_source()

    alias = str(archive.get("alias") or "nav_archive_db").strip()
    table_name = str(archive.get("archive_table_name") or "fund_nav_history_archive").strip()
    if not _is_safe_sql_identifier(alias) or not _is_safe_sql_identifier(table_name):
        return _read_with_union_source()

    cutoff = str(archive.get("archive_cutoff_date") or "").strip()
    cutoff_sql = "AND nav_date < ?" if cutoff else ""
    try:
        rows = _read_per_year_from_source("fund_nav_history")
        rows.extend(
            _read_per_year_from_source(
                f"{alias}.{table_name}",
                max_nav_date_exclusive=cutoff,
            )
        )
        return _merge_nav_window_rows(rows)
    except sqlite3.Error:
        pass

    rows: list[tuple[Any, ...]] = []
    try:
        for start in range(0, len(normalized_candidate_codes), chunk_size):
            code_batch = normalized_candidate_codes[start:start + chunk_size]
            placeholders = ",".join("?" for _ in code_batch)
            hot_params: tuple[Any, ...] = tuple(code_batch) + (start_date, end_date)
            cold_params: tuple[Any, ...] = tuple(code_batch) + (start_date, end_date)
            if cutoff:
                cold_params = cold_params + (cutoff,)
            rows.extend(
                conn.execute(
                    f"""
                    WITH hot AS (
                        SELECT fcode, CAST(substr(nav_date, 1, 4) AS INTEGER) AS nav_year, MIN(nav_date) AS first_nav_date
                        FROM fund_nav_history
                        WHERE fcode IN ({placeholders})
                          AND nav_date BETWEEN ? AND ?
                        GROUP BY fcode, substr(nav_date, 1, 4)
                    ),
                    cold AS (
                        SELECT fcode, CAST(substr(nav_date, 1, 4) AS INTEGER) AS nav_year, MIN(nav_date) AS first_nav_date
                        FROM {alias}.{table_name}
                        WHERE fcode IN ({placeholders})
                          AND nav_date BETWEEN ? AND ?
                          {cutoff_sql}
                        GROUP BY fcode, substr(nav_date, 1, 4)
                    ),
                    merged AS (
                        SELECT fcode, nav_year, first_nav_date FROM hot
                        UNION ALL
                        SELECT fcode, nav_year, first_nav_date FROM cold
                    )
                    SELECT fcode, nav_year, MIN(first_nav_date) AS first_nav_date
                    FROM merged
                    GROUP BY fcode, nav_year
                    """,
                    hot_params + cold_params,
                ).fetchall()
            )
    except sqlite3.Error:
        return _read_with_union_source()

    return rows


def _build_long_term_history_snapshot(db_path: Path, metadata_df: pd.DataFrame) -> dict[str, Any]:
    metadata = metadata_df.copy() if isinstance(metadata_df, pd.DataFrame) else pd.DataFrame()
    candidate_codes: set[str] = set()
    estab_date_map: dict[str, str] = {}
    shortname_map: dict[str, str] = {}
    if not metadata.empty and "fcode" in metadata.columns:
        metadata["fcode"] = metadata.get("fcode", pd.Series(dtype=object)).map(_normalize_code)
        metadata = metadata.dropna(subset=["fcode"]).drop_duplicates(subset=["fcode"], keep="first")
        metadata = metadata[metadata.apply(lambda row: _is_bond_fund_row(row.to_dict()), axis=1)].copy()
        candidate_codes = {
            str(code)
            for code in metadata.get("fcode", pd.Series(dtype=object)).astype(str).tolist()
            if str(code).strip()
        }
        if "estab_date" in metadata.columns:
            estab_date_map = {
                str(code): str(estab_date or "").strip()
                for code, estab_date in zip(
                    metadata.get("fcode", pd.Series(dtype=object)).astype(str).tolist(),
                    metadata.get("estab_date", pd.Series(dtype=object)).tolist(),
                    strict=False,
                )
                if str(code).strip()
            }
        if "shortname" in metadata.columns:
            shortname_map = {
                str(code): str(shortname or "").strip()
                for code, shortname in zip(
                    metadata.get("fcode", pd.Series(dtype=object)).astype(str).tolist(),
                    metadata.get("shortname", pd.Series(dtype=object)).tolist(),
                    strict=False,
                )
                if str(code).strip()
            }

    snapshot = {
        "candidate_fund_count": int(len(candidate_codes)),
        "table_annual_return_history_fund_count": 0,
        "table_rank_history_fund_count": 0,
        "table_annual_feature_ready_fund_count": 0,
        "table_rank_feature_ready_fund_count": 0,
        "table_full_feature_ready_fund_count": 0,
        "annual_return_history_fund_count": 0,
        "rank_history_fund_count": 0,
        "annual_feature_fresh_fund_count": 0,
        "rank_feature_fresh_fund_count": 0,
        "full_feature_fresh_fund_count": 0,
        "annual_feature_ready_fund_count": 0,
        "rank_feature_ready_fund_count": 0,
        "full_feature_ready_fund_count": 0,
        "neutralized_fund_count": 0,
        "annual_feature_stale_fund_count": 0,
        "rank_feature_stale_fund_count": 0,
        "full_feature_stale_fund_count": 0,
        "annual_feature_gap_fund_count": 0,
        "rank_feature_gap_fund_count": 0,
        "full_feature_gap_fund_count": 0,
        "neutralized_coverage_ratio": 0.0,
        "history_inventory_ratio_scope": {
            "numerator": "global_distinct_fund_inventory",
            "denominator": "current_bond_candidate_fund_count",
            "bounded_coverage_ratio": False,
            "legacy_aliases_retained": True,
            "legacy_aliases": {
                "table_annual_return_coverage_ratio": "annual_return_inventory_to_candidate_ratio",
                "table_rank_history_coverage_ratio": "rank_history_inventory_to_candidate_ratio",
                "table_annual_feature_ready_coverage_ratio": "annual_feature_ready_inventory_to_candidate_ratio",
                "table_rank_feature_ready_coverage_ratio": "rank_feature_ready_inventory_to_candidate_ratio",
                "table_full_feature_ready_coverage_ratio": "full_feature_ready_inventory_to_candidate_ratio",
            },
        },
        "annual_return_inventory_to_candidate_ratio": 0.0,
        "rank_history_inventory_to_candidate_ratio": 0.0,
        "annual_feature_ready_inventory_to_candidate_ratio": 0.0,
        "rank_feature_ready_inventory_to_candidate_ratio": 0.0,
        "full_feature_ready_inventory_to_candidate_ratio": 0.0,
        "table_annual_return_coverage_ratio": 0.0,
        "table_rank_history_coverage_ratio": 0.0,
        "table_annual_feature_ready_coverage_ratio": 0.0,
        "table_rank_feature_ready_coverage_ratio": 0.0,
        "table_full_feature_ready_coverage_ratio": 0.0,
        "annual_return_record_count": 0,
        "rank_history_record_count": 0,
        "annual_return_coverage_ratio": 0.0,
        "rank_history_coverage_ratio": 0.0,
        "annual_feature_fresh_coverage_ratio": 0.0,
        "rank_feature_fresh_coverage_ratio": 0.0,
        "full_feature_fresh_coverage_ratio": 0.0,
        "annual_feature_ready_coverage_ratio": 0.0,
        "rank_feature_ready_coverage_ratio": 0.0,
        "full_feature_ready_coverage_ratio": 0.0,
        "annual_feature_pending_maturity_fund_count": 0,
        "annual_feature_actionable_gap_fund_count": 0,
        "annual_feature_metadata_unknown_fund_count": 0,
        "annual_feature_actionable_missing_history_fund_count": 0,
        "annual_feature_actionable_sparse_year_row_gap_fund_count": 0,
        "annual_feature_actionable_null_year_gap_fund_count": 0,
        "annual_feature_upstream_semantic_null_year_gap_fund_count": 0,
        "annual_feature_upstream_limited_gap_fund_count": 0,
        "annual_feature_upstream_limited_missing_history_fund_count": 0,
        "annual_feature_upstream_limited_sparse_year_row_gap_fund_count": 0,
        "annual_feature_upstream_limited_null_year_gap_fund_count": 0,
        "annual_feature_upstream_limited_gap_samples": [],
        "ready_for_scoring": False,
        "blocking_reason": "history_tables_empty",
        "ready_for_long_term_enhancement": False,
        "long_term_enhancement_blocking_reason": "history_tables_empty",
    }
    if not db_path.exists():
        return snapshot

    conn = sqlite3.connect(str(db_path))
    try:
        annual_feature_window_years = _current_complete_calendar_year_window(LONG_TERM_STABILITY_MIN_YEARS_5)
        last_complete_year = datetime.now().year - 1
        rank_stale_days = _resolve_report_rank_stale_days()
        annual_feature_window_year_placeholders = ",".join("?" for _ in annual_feature_window_years)
        nav_archive_binding = load_latest_nav_history_archive_binding()
        if not nav_history_archive_binding_matches_database(nav_archive_binding, db_path):
            nav_archive_binding = {}
        nav_archive_attach = attach_nav_history_archive_if_available(
            conn,
            archive_path=nav_archive_binding.get("archive_path"),
            archive_table_name=nav_archive_binding.get("archive_table_name") or "fund_nav_history_archive",
            archive_cutoff_date=nav_archive_binding.get("archive_cutoff_date") or "",
        )
        nav_source_sql = build_nav_history_union_source_sql(
            nav_archive_attach,
            columns=("fcode", "nav_date"),
        )
        cache_snapshot = RankCacheSnapshot.start(conn, db_path, begin_rank_history_read_snapshot)
        rank_archive_binding = load_latest_rank_history_archive_binding()
        if not rank_history_archive_binding_matches_database(rank_archive_binding, db_path):
            rank_archive_binding = {}
        rank_archive_attach = cache_snapshot.attach(
            attach_rank_history_archive_if_available,
            archive_path=rank_archive_binding.get("archive_path"),
            archive_table_name=rank_archive_binding.get("archive_table_name") or "fund_rank_history_archive",
            archive_cutoff_date=rank_archive_binding.get("archive_cutoff_date") or "",
        )
        rank_snapshot_stats = _read_long_term_history_rank_snapshot_stats(
            conn, db_path, rank_archive_attach, cache_snapshot=cache_snapshot,
        )
        annual_record_count = int(conn.execute("SELECT COUNT(*) FROM fund_annual_returns").fetchone()[0] or 0)
        rank_record_count = int(rank_snapshot_stats.get("record_count") or 0)
        annual_codes = {
            _normalize_code(row[0])
            for row in conn.execute("SELECT DISTINCT fcode FROM fund_annual_returns").fetchall()
            if _normalize_code(row[0])
        }
        rank_codes = set(rank_snapshot_stats.get("codes") or set())
        annual_feature_ready_codes = {
            _normalize_code(row[0])
            for row in conn.execute(
                f"""
                SELECT fcode
                FROM fund_annual_returns
                WHERE year IN ({annual_feature_window_year_placeholders})
                GROUP BY fcode
                HAVING COUNT(DISTINCT CASE WHEN annual_return IS NOT NULL THEN year END) >= ?
                """,
                tuple(annual_feature_window_years) + (LONG_TERM_STABILITY_MIN_YEARS_5,),
            ).fetchall()
            if _normalize_code(row[0])
        }
        annual_latest_year_map = {
            _normalize_code(row[0]): int(row[1])
            for row in conn.execute(
                """
                SELECT fcode, MAX(year) AS latest_year
                FROM fund_annual_returns
                WHERE annual_return IS NOT NULL
                GROUP BY fcode
                """
            ).fetchall()
            if _normalize_code(row[0]) and row[1] is not None
        }
        annual_stats_by_code = {
            _normalize_code(row[0]): {
                "row_count": int(row[1] or 0),
                "nonnull_count": int(row[2] or 0),
                "present_years": sorted(
                    {
                        int(year)
                        for year in str(row[3] or "").split(",")
                        if str(year).strip().isdigit()
                    }
                ),
                "nonnull_years": sorted(
                    {
                        int(year)
                        for year in str(row[4] or "").split(",")
                        if str(year).strip().isdigit()
                    }
                ),
            }
            for row in conn.execute(
                f"""
                SELECT
                    fcode,
                    COUNT(*) AS row_count,
                    SUM(CASE WHEN annual_return IS NOT NULL THEN 1 ELSE 0 END) AS nonnull_count,
                    GROUP_CONCAT(DISTINCT year) AS present_years,
                    GROUP_CONCAT(DISTINCT CASE WHEN annual_return IS NOT NULL THEN year END) AS nonnull_years
                FROM fund_annual_returns
                WHERE year IN ({annual_feature_window_year_placeholders})
                GROUP BY fcode
                """,
                tuple(annual_feature_window_years),
            ).fetchall()
            if _normalize_code(row[0])
        }
        earliest_nav_date_map: dict[str, str] = {}
        nav_window_present_years_map: dict[str, list[int]] = {}
        nav_window_first_nav_date_by_code: dict[str, dict[int, str]] = {}
        nav_history_table_exists = conn.execute(
            """
            SELECT 1
            FROM sqlite_master
            WHERE type = 'table' AND name = 'fund_nav_history'
            """
        ).fetchone()
        if nav_history_table_exists:
            earliest_nav_date_map = _read_long_term_earliest_nav_date_map(
                conn,
                nav_source_sql=nav_source_sql,
                nav_archive_attach=nav_archive_attach,
            )
            if candidate_codes:
                start_date = f"{annual_feature_window_years[0]:04d}-01-01"
                end_date = f"{annual_feature_window_years[-1]:04d}-12-31"
                nav_window_year_sets: dict[str, set[int]] = {}
                rows = _read_long_term_nav_window_year_rows(
                    conn,
                    nav_source_sql=nav_source_sql,
                    nav_archive_attach=nav_archive_attach,
                    candidate_codes=candidate_codes,
                    start_date=start_date,
                    end_date=end_date,
                )
                for row in rows:
                    code = _normalize_code(row[0])
                    year = row[1]
                    if not code or year is None:
                        continue
                    try:
                        normalized_year = int(year)
                    except (TypeError, ValueError):
                        continue
                    if normalized_year not in annual_feature_window_years:
                        continue
                    nav_window_year_sets.setdefault(code, set()).add(normalized_year)
                    first_nav_date = str(row[2] or "").strip()
                    if first_nav_date:
                        nav_window_first_nav_date_by_code.setdefault(code, {})[normalized_year] = first_nav_date
                nav_window_present_years_map = {
                    code: sorted(years)
                    for code, years in nav_window_year_sets.items()
                }
        rank_feature_ready_codes = set(rank_snapshot_stats.get("feature_ready_codes") or set())
        rank_latest_date_map = dict(rank_snapshot_stats.get("latest_date_map") or {})
    finally:
        if "nav_archive_attach" in locals() and nav_archive_attach.get("attached"):
            detach_nav_history_archive(
                conn,
                alias=str(nav_archive_attach.get("alias") or "nav_archive_db"),
            )
        if "rank_archive_attach" in locals() and rank_archive_attach.get("attached"):
            detach_rank_history_archive(
                conn,
                alias=str(rank_archive_attach.get("alias") or "rank_archive_db"),
            )
        conn.close()

    candidate_total = max(1, len(candidate_codes))
    annual_feature_stale_codes = {
        code
        for code in annual_feature_ready_codes
        if (annual_latest_year_map.get(code) is not None and int(annual_latest_year_map.get(code) or 0) < last_complete_year)
    }
    rank_feature_stale_codes: set[str] = set()
    for code, latest_rank_date in rank_latest_date_map.items():
        if code not in rank_feature_ready_codes:
            continue
        try:
            rank_age_days = (datetime.now().date() - datetime.fromisoformat(latest_rank_date[:10]).date()).days
        except ValueError:
            continue
        if rank_age_days > rank_stale_days:
            rank_feature_stale_codes.add(code)
    annual_feature_fresh_codes = annual_feature_ready_codes - annual_feature_stale_codes
    rank_feature_fresh_codes = rank_feature_ready_codes - rank_feature_stale_codes
    full_feature_fresh_codes = annual_feature_fresh_codes & rank_feature_fresh_codes
    table_annual_fund_count = len(annual_codes)
    table_rank_fund_count = len(rank_codes)
    table_annual_feature_ready_fund_count = len(annual_feature_fresh_codes)
    table_rank_feature_ready_fund_count = len(rank_feature_fresh_codes)
    table_full_feature_ready_fund_count = len(full_feature_fresh_codes)
    annual_fund_count = len(candidate_codes & annual_codes) if candidate_codes else len(annual_codes)
    rank_fund_count = len(candidate_codes & rank_codes) if candidate_codes else len(rank_codes)
    annual_feature_fresh_fund_count = (
        len(candidate_codes & annual_feature_fresh_codes) if candidate_codes else len(annual_feature_fresh_codes)
    )
    rank_feature_fresh_fund_count = (
        len(candidate_codes & rank_feature_fresh_codes) if candidate_codes else len(rank_feature_fresh_codes)
    )
    full_feature_fresh_fund_count = (
        len(candidate_codes & full_feature_fresh_codes) if candidate_codes else len(full_feature_fresh_codes)
    )
    annual_feature_stale_fund_count = (
        len(candidate_codes & annual_feature_stale_codes) if candidate_codes else len(annual_feature_stale_codes)
    )
    rank_feature_stale_fund_count = (
        len(candidate_codes & rank_feature_stale_codes) if candidate_codes else len(rank_feature_stale_codes)
    )
    full_feature_stale_fund_count = (
        len(candidate_codes & (annual_feature_stale_codes | rank_feature_stale_codes))
        if candidate_codes
        else len(annual_feature_stale_codes | rank_feature_stale_codes)
    )
    annual_feature_ready_fund_count = annual_feature_fresh_fund_count
    rank_feature_ready_fund_count = rank_feature_fresh_fund_count
    full_feature_ready_fund_count = full_feature_fresh_fund_count
    annual_coverage_ratio = round(annual_fund_count / candidate_total, 4) if candidate_codes else 0.0
    rank_coverage_ratio = round(rank_fund_count / candidate_total, 4) if candidate_codes else 0.0
    annual_feature_fresh_coverage_ratio = (
        round(annual_feature_fresh_fund_count / candidate_total, 4) if candidate_codes else 0.0
    )
    rank_feature_fresh_coverage_ratio = (
        round(rank_feature_fresh_fund_count / candidate_total, 4) if candidate_codes else 0.0
    )
    full_feature_fresh_coverage_ratio = (
        round(full_feature_fresh_fund_count / candidate_total, 4) if candidate_codes else 0.0
    )
    annual_feature_ready_coverage_ratio = annual_feature_fresh_coverage_ratio
    rank_feature_ready_coverage_ratio = rank_feature_fresh_coverage_ratio
    full_feature_ready_coverage_ratio = full_feature_fresh_coverage_ratio

    annual_feature_pending_maturity_fund_count = 0
    annual_feature_actionable_gap_fund_count = 0
    annual_feature_metadata_unknown_fund_count = 0
    annual_feature_actionable_missing_history_fund_count = 0
    annual_feature_actionable_sparse_year_row_gap_fund_count = 0
    annual_feature_actionable_null_year_gap_fund_count = 0
    annual_feature_upstream_semantic_null_year_gap_fund_count = 0
    annual_feature_upstream_limited_gap_fund_count = 0
    annual_feature_upstream_limited_missing_history_fund_count = 0
    annual_feature_upstream_limited_sparse_year_row_gap_fund_count = 0
    annual_feature_upstream_limited_null_year_gap_fund_count = 0
    annual_feature_upstream_limited_gap_samples: list[dict[str, Any]] = []
    long_term_failure_backoff = _load_report_long_term_failure_backoff(db_path)
    if candidate_codes:
        annual_gap_codes = candidate_codes - annual_feature_ready_codes
        for code in annual_gap_codes:
            estab_date_text = str(estab_date_map.get(code) or "").strip()
            earliest_nav_date_text = str(earliest_nav_date_map.get(code) or "").strip()
            annual_stats = annual_stats_by_code.get(code) or {}
            annual_row_count = int(annual_stats.get("row_count") or 0)
            annual_nonnull_count = int(annual_stats.get("nonnull_count") or 0)
            current_window_present_years = list(annual_stats.get("present_years") or [])
            current_window_nonnull_years = list(annual_stats.get("nonnull_years") or [])
            leading_present_year = current_window_present_years[0] if current_window_present_years else None
            leading_partial_semantic_null_gap = _annual_feature_window_has_leading_partial_null_year_for_report(
                annual_feature_window_years,
                current_window_present_years,
                current_window_nonnull_years,
                leading_present_year_first_nav_date=(
                    (nav_window_first_nav_date_by_code.get(code) or {}).get(int(leading_present_year))
                    if leading_present_year is not None
                    else None
                ),
            )
            annual_feature_window_mature = _annual_feature_window_mature_enough_for_report(
                estab_date_text,
                earliest_nav_date=earliest_nav_date_text,
                current_window_nav_present_years=nav_window_present_years_map.get(code),
                min_years=LONG_TERM_STABILITY_MIN_YEARS_5,
            )
            if annual_feature_window_mature is None:
                annual_feature_metadata_unknown_fund_count += 1
                continue
            if leading_partial_semantic_null_gap:
                annual_feature_pending_maturity_fund_count += 1
                annual_feature_upstream_semantic_null_year_gap_fund_count += 1
                continue
            if not annual_feature_window_mature:
                annual_feature_pending_maturity_fund_count += 1
            else:
                gap_shape = _annual_gap_shape_from_window_counts(
                    row_count=annual_row_count,
                    nonnull_count=annual_nonnull_count,
                )
                failure_record = long_term_failure_backoff.get(code) or {}
                if _report_annual_gap_has_upstream_limited_failure(failure_record):
                    annual_feature_upstream_limited_gap_fund_count += 1
                    if gap_shape == "missing_history":
                        annual_feature_upstream_limited_missing_history_fund_count += 1
                    elif gap_shape == "sparse_year_row_gap":
                        annual_feature_upstream_limited_sparse_year_row_gap_fund_count += 1
                    else:
                        annual_feature_upstream_limited_null_year_gap_fund_count += 1
                    if len(annual_feature_upstream_limited_gap_samples) < LONG_TERM_HISTORY_SNAPSHOT_SAMPLE_LIMIT:
                        annual_feature_upstream_limited_gap_samples.append(
                            {
                                "fcode": code,
                                "shortname": shortname_map.get(code) or "",
                                "gap_shape": gap_shape,
                                "estab_date": estab_date_text,
                                "earliest_nav_date": earliest_nav_date_text,
                                "window_years": annual_feature_window_years,
                                "present_years": current_window_present_years,
                                "nonnull_years": current_window_nonnull_years,
                                "missing_years": [
                                    year
                                    for year in annual_feature_window_years
                                    if year not in set(current_window_present_years)
                                ],
                                "null_years": [
                                    year
                                    for year in current_window_present_years
                                    if year not in set(current_window_nonnull_years)
                                ],
                                "cooldown_next_retry_after": failure_record.get("next_retry_after"),
                                "cooldown_last_error": failure_record.get("last_error"),
                            }
                        )
                    continue

                annual_feature_actionable_gap_fund_count += 1
                if gap_shape == "missing_history":
                    annual_feature_actionable_missing_history_fund_count += 1
                elif gap_shape == "sparse_year_row_gap":
                    annual_feature_actionable_sparse_year_row_gap_fund_count += 1
                else:
                    annual_feature_actionable_null_year_gap_fund_count += 1

    annual_return_inventory_to_candidate_ratio = (
        round(table_annual_fund_count / candidate_total, 4) if candidate_codes else 0.0
    )
    rank_history_inventory_to_candidate_ratio = (
        round(table_rank_fund_count / candidate_total, 4) if candidate_codes else 0.0
    )
    annual_feature_ready_inventory_to_candidate_ratio = (
        round(table_annual_feature_ready_fund_count / candidate_total, 4) if candidate_codes else 0.0
    )
    rank_feature_ready_inventory_to_candidate_ratio = (
        round(table_rank_feature_ready_fund_count / candidate_total, 4) if candidate_codes else 0.0
    )
    full_feature_ready_inventory_to_candidate_ratio = (
        round(table_full_feature_ready_fund_count / candidate_total, 4) if candidate_codes else 0.0
    )
    ready_for_scoring = (
        annual_record_count > 0
        and rank_record_count > 0
        and annual_feature_ready_coverage_ratio >= LONG_TERM_SCORE_MIN_COVERAGE_RATIO
        and rank_feature_ready_coverage_ratio >= LONG_TERM_SCORE_MIN_COVERAGE_RATIO
    )

    if ready_for_scoring:
        blocking_reason = "ready"
    elif annual_record_count == 0 and rank_record_count == 0:
        blocking_reason = "history_tables_empty"
    elif rank_feature_stale_fund_count > 0 and rank_feature_ready_coverage_ratio < LONG_TERM_SCORE_MIN_COVERAGE_RATIO:
        blocking_reason = "rank_history_freshness_insufficient"
    elif (
        annual_feature_stale_fund_count > 0
        and annual_feature_ready_coverage_ratio < LONG_TERM_SCORE_MIN_COVERAGE_RATIO
        and rank_feature_ready_coverage_ratio >= LONG_TERM_SCORE_MIN_COVERAGE_RATIO
    ):
        blocking_reason = "annual_return_history_freshness_insufficient"
    elif annual_feature_actionable_gap_fund_count > 0 and annual_feature_ready_coverage_ratio < LONG_TERM_SCORE_MIN_COVERAGE_RATIO:
        blocking_reason = "annual_return_history_coverage_insufficient"
    elif (
        annual_feature_upstream_limited_gap_fund_count > 0
        and annual_feature_pending_maturity_fund_count <= 0
        and annual_feature_ready_coverage_ratio < LONG_TERM_SCORE_MIN_COVERAGE_RATIO
    ):
        blocking_reason = "annual_return_history_upstream_limited"
    elif annual_feature_ready_coverage_ratio < LONG_TERM_SCORE_MIN_COVERAGE_RATIO:
        blocking_reason = "annual_return_history_pending_maturity"
    elif rank_feature_ready_coverage_ratio < LONG_TERM_SCORE_MIN_COVERAGE_RATIO:
        blocking_reason = "rank_history_coverage_insufficient"
    else:
        blocking_reason = "history_not_ready"

    snapshot.update(
        {
            "table_annual_return_history_fund_count": int(table_annual_fund_count),
            "table_rank_history_fund_count": int(table_rank_fund_count),
            "table_annual_feature_ready_fund_count": int(table_annual_feature_ready_fund_count),
            "table_rank_feature_ready_fund_count": int(table_rank_feature_ready_fund_count),
            "table_full_feature_ready_fund_count": int(table_full_feature_ready_fund_count),
            "annual_return_history_fund_count": int(annual_fund_count),
            "rank_history_fund_count": int(rank_fund_count),
            "annual_feature_fresh_fund_count": int(annual_feature_fresh_fund_count),
            "rank_feature_fresh_fund_count": int(rank_feature_fresh_fund_count),
            "full_feature_fresh_fund_count": int(full_feature_fresh_fund_count),
            "annual_feature_ready_fund_count": int(annual_feature_ready_fund_count),
            "rank_feature_ready_fund_count": int(rank_feature_ready_fund_count),
            "full_feature_ready_fund_count": int(full_feature_ready_fund_count),
            "neutralized_fund_count": int(max(0, len(candidate_codes) - full_feature_ready_fund_count)),
            "annual_feature_stale_fund_count": int(annual_feature_stale_fund_count),
            "rank_feature_stale_fund_count": int(rank_feature_stale_fund_count),
            "full_feature_stale_fund_count": int(full_feature_stale_fund_count),
            "annual_feature_gap_fund_count": int(max(0, len(candidate_codes) - annual_feature_ready_fund_count)),
            "rank_feature_gap_fund_count": int(max(0, len(candidate_codes) - rank_feature_ready_fund_count)),
            "full_feature_gap_fund_count": int(max(0, len(candidate_codes) - full_feature_ready_fund_count)),
            "neutralized_coverage_ratio": float(
                round(max(0, len(candidate_codes) - full_feature_ready_fund_count) / candidate_total, 4)
                if candidate_codes
                else 0.0
            ),
            "history_inventory_ratio_scope": {
                "numerator": "global_distinct_fund_inventory",
                "denominator": "current_bond_candidate_fund_count",
                "bounded_coverage_ratio": False,
                "legacy_aliases_retained": True,
                "legacy_aliases": {
                    "table_annual_return_coverage_ratio": "annual_return_inventory_to_candidate_ratio",
                    "table_rank_history_coverage_ratio": "rank_history_inventory_to_candidate_ratio",
                    "table_annual_feature_ready_coverage_ratio": "annual_feature_ready_inventory_to_candidate_ratio",
                    "table_rank_feature_ready_coverage_ratio": "rank_feature_ready_inventory_to_candidate_ratio",
                    "table_full_feature_ready_coverage_ratio": "full_feature_ready_inventory_to_candidate_ratio",
                },
            },
            "annual_return_inventory_to_candidate_ratio": float(
                annual_return_inventory_to_candidate_ratio
            ),
            "rank_history_inventory_to_candidate_ratio": float(
                rank_history_inventory_to_candidate_ratio
            ),
            "annual_feature_ready_inventory_to_candidate_ratio": float(
                annual_feature_ready_inventory_to_candidate_ratio
            ),
            "rank_feature_ready_inventory_to_candidate_ratio": float(
                rank_feature_ready_inventory_to_candidate_ratio
            ),
            "full_feature_ready_inventory_to_candidate_ratio": float(
                full_feature_ready_inventory_to_candidate_ratio
            ),
            "table_annual_return_coverage_ratio": float(
                annual_return_inventory_to_candidate_ratio
            ),
            "table_rank_history_coverage_ratio": float(
                rank_history_inventory_to_candidate_ratio
            ),
            "table_annual_feature_ready_coverage_ratio": float(
                annual_feature_ready_inventory_to_candidate_ratio
            ),
            "table_rank_feature_ready_coverage_ratio": float(
                rank_feature_ready_inventory_to_candidate_ratio
            ),
            "table_full_feature_ready_coverage_ratio": float(
                full_feature_ready_inventory_to_candidate_ratio
            ),
            "annual_return_record_count": int(annual_record_count),
            "rank_history_record_count": int(rank_record_count),
            "annual_return_coverage_ratio": float(annual_coverage_ratio),
            "rank_history_coverage_ratio": float(rank_coverage_ratio),
            "annual_feature_fresh_coverage_ratio": float(annual_feature_fresh_coverage_ratio),
            "rank_feature_fresh_coverage_ratio": float(rank_feature_fresh_coverage_ratio),
            "full_feature_fresh_coverage_ratio": float(full_feature_fresh_coverage_ratio),
            "annual_feature_ready_coverage_ratio": float(annual_feature_ready_coverage_ratio),
            "rank_feature_ready_coverage_ratio": float(rank_feature_ready_coverage_ratio),
            "full_feature_ready_coverage_ratio": float(full_feature_ready_coverage_ratio),
            "annual_feature_pending_maturity_fund_count": int(annual_feature_pending_maturity_fund_count),
            "annual_feature_actionable_gap_fund_count": int(annual_feature_actionable_gap_fund_count),
            "annual_feature_metadata_unknown_fund_count": int(annual_feature_metadata_unknown_fund_count),
            "annual_feature_actionable_missing_history_fund_count": int(
                annual_feature_actionable_missing_history_fund_count
            ),
            "annual_feature_actionable_sparse_year_row_gap_fund_count": int(
                annual_feature_actionable_sparse_year_row_gap_fund_count
            ),
            "annual_feature_actionable_null_year_gap_fund_count": int(
                annual_feature_actionable_null_year_gap_fund_count
            ),
            "annual_feature_upstream_semantic_null_year_gap_fund_count": int(
                annual_feature_upstream_semantic_null_year_gap_fund_count
            ),
            "annual_feature_upstream_limited_gap_fund_count": int(
                annual_feature_upstream_limited_gap_fund_count
            ),
            "annual_feature_upstream_limited_missing_history_fund_count": int(
                annual_feature_upstream_limited_missing_history_fund_count
            ),
            "annual_feature_upstream_limited_sparse_year_row_gap_fund_count": int(
                annual_feature_upstream_limited_sparse_year_row_gap_fund_count
            ),
            "annual_feature_upstream_limited_null_year_gap_fund_count": int(
                annual_feature_upstream_limited_null_year_gap_fund_count
            ),
            "annual_feature_upstream_limited_gap_samples": annual_feature_upstream_limited_gap_samples,
            "ready_for_scoring": bool(ready_for_scoring),
            "blocking_reason": blocking_reason,
            "ready_for_long_term_enhancement": bool(ready_for_scoring),
            "long_term_enhancement_blocking_reason": blocking_reason,
        }
    )
    return snapshot


def _build_coverage_layers(
    *,
    universe_snapshot: dict[str, Any],
    long_term_history_snapshot: dict[str, Any],
    stage_b_candidate_pool_count: int,
) -> dict[str, Any]:
    stored_universe_count = int(universe_snapshot.get("stored_universe_count") or 0)
    active_universe_count = int(universe_snapshot.get("active_universe_count") or 0)
    screenable_universe_count = int(universe_snapshot.get("screenable_universe_count") or 0)
    stage_b_candidate_universe_count = int(stage_b_candidate_pool_count or 0)

    stored_has_latest_nav_count = int(
        universe_snapshot.get("stored_has_latest_nav_count")
        or universe_snapshot.get("has_latest_nav_count")
        or 0
    )
    stored_has_1y_risk_metrics_count = int(
        universe_snapshot.get("stored_has_1y_risk_metrics_count")
        or universe_snapshot.get("has_1y_risk_metrics_count")
        or 0
    )
    stored_holding_period_blocked_count = int(
        universe_snapshot.get("stored_holding_period_blocked_count")
        or universe_snapshot.get("holding_period_blocked_count")
        or 0
    )
    active_has_latest_nav_count = int(universe_snapshot.get("has_latest_nav_count") or 0)
    active_has_1y_risk_metrics_count = int(universe_snapshot.get("has_1y_risk_metrics_count") or 0)
    active_holding_period_blocked_count = int(universe_snapshot.get("holding_period_blocked_count") or 0)
    annual_return_history_fund_count = int(long_term_history_snapshot.get("annual_return_history_fund_count") or 0)
    rank_history_fund_count = int(long_term_history_snapshot.get("rank_history_fund_count") or 0)
    full_feature_ready_fund_count = int(long_term_history_snapshot.get("full_feature_ready_fund_count") or 0)

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
            "base_fund_count": stage_b_candidate_universe_count,
            "stage_b_candidate_pool_count": stage_b_candidate_universe_count,
            "governance_candidate_pool_count": stage_b_candidate_universe_count,
            "publishable_pool_count": stage_b_candidate_universe_count,
            "ratio_vs_screenable_universe": _ratio(stage_b_candidate_universe_count, screenable_universe_count),
            "ratio_vs_active_bond_universe": _ratio(stage_b_candidate_universe_count, active_universe_count),
            "is_broader_than_screenable_universe": stage_b_candidate_universe_count > screenable_universe_count,
            "scope_note": stage_b_scope_note,
        },
        "publishable_universe_coverage": {
            "base_fund_count": stage_b_candidate_universe_count,
            "stage_b_candidate_pool_count": stage_b_candidate_universe_count,
            "governance_candidate_pool_count": stage_b_candidate_universe_count,
            "publishable_pool_count": stage_b_candidate_universe_count,
            "ratio_vs_screenable_universe": _ratio(stage_b_candidate_universe_count, screenable_universe_count),
            "ratio_vs_active_bond_universe": _ratio(stage_b_candidate_universe_count, active_universe_count),
            "is_broader_than_screenable_universe": stage_b_candidate_universe_count > screenable_universe_count,
            "scope_note": stage_b_scope_note,
        },
    }


def _score_stability_by_std(std_value: float | None) -> float | None:
    if std_value is None:
        return None
    if std_value <= 2.0:
        return 100.0
    if std_value <= 4.0:
        return 85.0
    if std_value <= 6.0:
        return 70.0
    if std_value <= 8.0:
        return 55.0
    if std_value <= 10.0:
        return 40.0
    return 20.0


def _safe_std(values: list[float]) -> float | None:
    numeric = [float(value) for value in values]
    if len(numeric) < 2:
        return None
    return float(pd.Series(numeric, dtype=float).std(ddof=0))


def _population_std_from_moments(
    count_value: Any,
    mean_value: Any,
    mean_square_value: Any,
) -> float | None:
    count = int(_safe_float(count_value) or 0)
    mean = _safe_float(mean_value)
    mean_square = _safe_float(mean_square_value)
    if count < 2 or mean is None or mean_square is None:
        return None
    variance = max(float(mean_square) - float(mean) * float(mean), 0.0)
    return math.sqrt(variance)


def _sqlite_path_stat_token(path: Path | str | None) -> str:
    if not path:
        return "missing"
    candidate = Path(path)
    try:
        stat = candidate.stat()
    except OSError:
        return "missing"
    try:
        resolved = candidate.resolve()
    except OSError:
        resolved = candidate
    return f"{resolved}:{int(stat.st_mtime_ns)}:{int(stat.st_size)}"


def _sqlite_db_cache_token(db_path: Path | str | None) -> str:
    if not db_path:
        return "missing"
    base = Path(db_path)
    wal = Path(f"{base}-wal")
    return f"{_sqlite_path_stat_token(base)}::wal={_sqlite_path_stat_token(wal)}"


def _sqlite_wal_snapshot_token(path: Path | str | None) -> str:
    if not path:
        return "missing"
    candidate = Path(path)
    try:
        stat = candidate.stat()
    except OSError:
        return "missing"
    try:
        resolved = candidate.resolve()
    except OSError:
        resolved = candidate
    size = int(stat.st_size)
    if size <= 0:
        return f"{resolved}:empty:0"
    return f"{resolved}:{int(stat.st_mtime_ns)}:{size}"


def _sqlite_db_snapshot_cache_token(db_path: Path | str | None) -> str:
    return sqlite_snapshot_token(db_path) or "missing"


def _long_term_rank_metrics_cache_key(
    db_path: Path, rank_archive_attach: dict[str, Any] | None,
    cache_snapshot: RankCacheSnapshot | None = None,
) -> str:
    payload = _long_term_history_rank_snapshot_stats_fingerprint(
        db_path, rank_archive_attach, cache_snapshot,
    )
    payload["metrics_schema"] = 2
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def _read_long_term_rank_metrics(
    conn: sqlite3.Connection,
    db_path: Path,
    rank_archive_attach: dict[str, Any] | None,
    *,
    cache_snapshot: RankCacheSnapshot | None = None,
) -> pd.DataFrame:
    with rank_cache_snapshot(conn, db_path, rank_archive_attach, cache_snapshot,
                             begin=begin_rank_history_read_snapshot) as snapshot:
        cache_key = (_long_term_rank_metrics_cache_key(db_path, rank_archive_attach, snapshot)
                     if snapshot.cacheable(db_path, rank_archive_attach) else None)
        cached = _LONG_TERM_RANK_METRICS_CACHE.get(cache_key) if cache_key is not None else None
        if cached is not None:
            return cached.copy(deep=True)
        snapshot_stats = _load_cached_long_term_history_rank_snapshot_stats(
            db_path, rank_archive_attach, cache_snapshot=snapshot,
        )
        hot_frame = _read_long_term_rank_metrics_from_hot_using_snapshot(
            conn,
            snapshot_stats,
            rank_archive_attach,
        )
        snapshot.require_read_view(db_path, rank_archive_attach)
        if hot_frame is None:
            hot_frame = _read_long_term_rank_metrics_from_hot(conn)
        hot_codes = {
            _normalize_code(code)
            for code in hot_frame.get("fcode", pd.Series(dtype=object)).tolist()
            if _normalize_code(code)
        }
        archive_frame = _read_long_term_rank_metrics_from_archive_missing_hot_using_snapshot(
            conn,
            rank_archive_attach,
            hot_codes,
            snapshot_stats,
        )
        snapshot.require_read_view(db_path, rank_archive_attach)
        if archive_frame is None:
            archive_frame = _read_long_term_rank_metrics_from_archive_missing_hot(
                conn,
                rank_archive_attach,
                hot_codes,
            )
        snapshot.require_read_view(db_path, rank_archive_attach)
        frames = [hot_frame]
        if not archive_frame.empty:
            frames.append(archive_frame)

        non_empty_frames = [frame for frame in frames if not frame.empty]
        if non_empty_frames:
            result = pd.concat(non_empty_frames, ignore_index=True)
        else:
            result = pd.DataFrame()
        if not result.empty:
            result["fcode"] = result.get("fcode", pd.Series(dtype=object)).map(_normalize_code)
            result = result[result["fcode"].astype(str).str.strip() != ""].copy()
            result = result.sort_values(by=["fcode", "source_order"], kind="mergesort")
            result = result.drop_duplicates(subset=["fcode"], keep="last")
            result = result.drop(columns=["source_order"], errors="ignore").reset_index(drop=True)

        if cache_key is not None and snapshot.cacheable(db_path, rank_archive_attach):
            _LONG_TERM_RANK_METRICS_CACHE.clear()
            _LONG_TERM_RANK_METRICS_CACHE[cache_key] = result.copy(deep=True)
        return result


def _read_hot_rank_percentile_codes(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute(
        """
        SELECT DISTINCT fcode
        FROM fund_rank_history
        WHERE percentile IS NOT NULL
        """
    ).fetchall()
    return {
        code
        for code in (_normalize_code(row[0]) for row in rows)
        if code
    }


def _read_long_term_rank_metrics_from_hot_using_snapshot(
    conn: sqlite3.Connection,
    snapshot_stats: dict[str, Any] | None,
    rank_archive_attach: dict[str, Any] | None,
) -> pd.DataFrame | None:
    archive = dict(rank_archive_attach or {})
    if archive.get("attached") and not str(archive.get("archive_cutoff_date") or "").strip():
        return None

    latest_date_map = {
        _normalize_code(code): str(value or "").strip()
        for code, value in dict((snapshot_stats or {}).get("latest_date_map") or {}).items()
        if _normalize_code(code) and str(value or "").strip()
    }
    if not latest_date_map:
        return None

    raw_hot_codes = (snapshot_stats or {}).get("hot_percentile_codes") if isinstance(snapshot_stats, dict) else None
    if raw_hot_codes is None:
        try:
            hot_codes = _read_hot_rank_percentile_codes(conn)
        except sqlite3.Error:
            return None
    else:
        hot_codes = {
            code
            for code in (_normalize_code(raw_code) for raw_code in raw_hot_codes)
            if code
        }
    if not hot_codes:
        return pd.DataFrame(
            columns=[
                "fcode",
                "latest_rank_date",
                "rank_count_12m",
                "rank_avg_percentile_12m",
                "rank_avg_sq_percentile_12m",
                "rank_top30_count_12m",
                "source_order",
            ]
        )

    missing_codes = hot_codes - set(latest_date_map)
    if missing_codes:
        return None

    latest_date_buckets: dict[str, list[str]] = {}
    for code in sorted(hot_codes):
        latest_rank_date = latest_date_map.get(code)
        if not latest_rank_date:
            continue
        latest_date_buckets.setdefault(str(latest_rank_date), []).append(code)
    if not latest_date_buckets:
        return None

    frames: list[pd.DataFrame] = []
    chunk_size = max(1, int(_LONG_TERM_RANK_METRICS_FAST_BATCH_SIZE))
    for latest_rank_date, codes in sorted(latest_date_buckets.items()):
        for start in range(0, len(codes), chunk_size):
            batch = codes[start:start + chunk_size]
            placeholders = ",".join("?" for _ in batch)
            params: list[Any] = [latest_rank_date, latest_rank_date, *batch]
            query = f"""
            SELECT
                fund_rank_history.fcode AS fcode,
                ? AS latest_rank_date,
                COUNT(*) AS rank_count_12m,
                AVG(fund_rank_history.percentile) AS rank_avg_percentile_12m,
                AVG(fund_rank_history.percentile * fund_rank_history.percentile) AS rank_avg_sq_percentile_12m,
                SUM(CASE WHEN fund_rank_history.percentile <= 0.30 THEN 1 ELSE 0 END) AS rank_top30_count_12m,
                1 AS source_order
            FROM fund_rank_history
            WHERE fund_rank_history.percentile IS NOT NULL
              AND fund_rank_history.rank_date >= date(?, '-365 day')
              AND fund_rank_history.fcode IN ({placeholders})
            GROUP BY fund_rank_history.fcode
            ORDER BY fund_rank_history.fcode
            """
            try:
                frames.append(pd.read_sql_query(query, conn, params=tuple(params)))
            except (sqlite3.Error, pd.errors.DatabaseError, ValueError):
                return None

    result = pd.concat([frame for frame in frames if not frame.empty], ignore_index=True) if frames else pd.DataFrame()
    result_codes = {
        code
        for code in (_normalize_code(code) for code in result.get("fcode", pd.Series(dtype=object)).tolist())
        if code
    }
    if result_codes != hot_codes:
        return None
    return result


def _read_long_term_rank_metrics_from_hot(conn: sqlite3.Connection) -> pd.DataFrame:
    query = """
        WITH latest_rank AS (
            SELECT
                fcode,
                MAX(rank_date) AS latest_rank_date
            FROM fund_rank_history
            WHERE percentile IS NOT NULL
            GROUP BY fcode
        )
        SELECT
            fund_rank_history.fcode AS fcode,
            latest_rank.latest_rank_date AS latest_rank_date,
            COUNT(*) AS rank_count_12m,
            AVG(fund_rank_history.percentile) AS rank_avg_percentile_12m,
            AVG(fund_rank_history.percentile * fund_rank_history.percentile) AS rank_avg_sq_percentile_12m,
            SUM(CASE WHEN fund_rank_history.percentile <= 0.30 THEN 1 ELSE 0 END) AS rank_top30_count_12m,
            1 AS source_order
        FROM fund_rank_history
        JOIN latest_rank USING (fcode)
        WHERE fund_rank_history.percentile IS NOT NULL
          AND fund_rank_history.rank_date >= date(latest_rank.latest_rank_date, '-365 day')
        GROUP BY fund_rank_history.fcode
        ORDER BY fund_rank_history.fcode
        """
    return pd.read_sql_query(query, conn)


def _read_long_term_rank_metrics_from_archive_missing_hot(
    conn: sqlite3.Connection,
    rank_archive_attach: dict[str, Any] | None,
    hot_codes: set[str],
) -> pd.DataFrame:
    archive = dict(rank_archive_attach or {})
    if not archive.get("attached"):
        return pd.DataFrame()
    alias = str(archive.get("alias") or "rank_archive_db").strip()
    table_name = str(archive.get("archive_table_name") or "fund_rank_history_archive").strip()
    if not _is_safe_sql_identifier(alias) or not _is_safe_sql_identifier(table_name):
        return pd.DataFrame()

    source_sql = f"{alias}.{table_name}"
    cutoff = str(archive.get("archive_cutoff_date") or "").strip()
    cutoff_filter = ""
    cutoff_params: tuple[Any, ...] = ()
    if cutoff:
        cutoff_filter = "AND rank_date < ?"
        cutoff_params = (cutoff,)

    missing_rows = conn.execute(
        f"""
        SELECT DISTINCT fcode
        FROM {source_sql}
        WHERE percentile IS NOT NULL
          {cutoff_filter}
        """,
        cutoff_params,
    ).fetchall()
    missing_codes = sorted(
        {
            code
            for code in (_normalize_code(row[0]) for row in missing_rows)
            if code and code not in hot_codes
        }
    )
    if not missing_codes:
        return pd.DataFrame()

    frames: list[pd.DataFrame] = []
    chunk_size = 200
    for start in range(0, len(missing_codes), chunk_size):
        batch = missing_codes[start:start + chunk_size]
        placeholders = ",".join("?" for _ in batch)
        latest_cutoff_sql = ""
        outer_cutoff_sql = ""
        params: list[Any] = [*batch]
        if cutoff:
            latest_cutoff_sql = "AND rank_date < ?"
            params.append(cutoff)
        params.extend(batch)
        if cutoff:
            outer_cutoff_sql = "AND archive_rank.rank_date < ?"
            params.append(cutoff)
        query = f"""
            WITH latest_rank AS (
                SELECT
                    fcode,
                    MAX(rank_date) AS latest_rank_date
                FROM {source_sql}
                WHERE percentile IS NOT NULL
                  AND fcode IN ({placeholders})
                  {latest_cutoff_sql}
                GROUP BY fcode
            )
            SELECT
                archive_rank.fcode AS fcode,
                latest_rank.latest_rank_date AS latest_rank_date,
                COUNT(*) AS rank_count_12m,
                AVG(archive_rank.percentile) AS rank_avg_percentile_12m,
                AVG(archive_rank.percentile * archive_rank.percentile) AS rank_avg_sq_percentile_12m,
                SUM(CASE WHEN archive_rank.percentile <= 0.30 THEN 1 ELSE 0 END) AS rank_top30_count_12m,
                0 AS source_order
            FROM {source_sql} archive_rank
            JOIN latest_rank USING (fcode)
            WHERE archive_rank.percentile IS NOT NULL
              AND archive_rank.fcode IN ({placeholders})
              {outer_cutoff_sql}
              AND archive_rank.rank_date >= date(latest_rank.latest_rank_date, '-365 day')
            GROUP BY archive_rank.fcode
            ORDER BY archive_rank.fcode
            """
        frames.append(pd.read_sql_query(query, conn, params=tuple(params)))

    return pd.concat([frame for frame in frames if not frame.empty], ignore_index=True) if frames else pd.DataFrame()


def _read_long_term_rank_metrics_from_archive_missing_hot_using_snapshot(
    conn: sqlite3.Connection,
    rank_archive_attach: dict[str, Any] | None,
    hot_codes: set[str],
    snapshot_stats: dict[str, Any] | None,
) -> pd.DataFrame | None:
    archive = dict(rank_archive_attach or {})
    if not archive.get("attached"):
        return pd.DataFrame()
    alias = str(archive.get("alias") or "rank_archive_db").strip()
    table_name = str(archive.get("archive_table_name") or "fund_rank_history_archive").strip()
    if not _is_safe_sql_identifier(alias) or not _is_safe_sql_identifier(table_name):
        return None

    latest_date_map = {
        _normalize_code(code): str(value or "").strip()
        for code, value in dict((snapshot_stats or {}).get("latest_date_map") or {}).items()
        if _normalize_code(code) and str(value or "").strip()
    }
    if not latest_date_map:
        return None

    missing_items = [
        (code, latest_date_map[code])
        for code in sorted(set(latest_date_map) - set(hot_codes))
        if latest_date_map.get(code)
    ]
    if not missing_items:
        return pd.DataFrame()

    source_sql = f"{alias}.{table_name}"
    cutoff = str(archive.get("archive_cutoff_date") or "").strip()
    cutoff_sql = "AND archive_rank.rank_date < ?" if cutoff else ""
    frames: list[pd.DataFrame] = []
    chunk_size = max(1, int(_LONG_TERM_RANK_METRICS_FAST_BATCH_SIZE))
    for start in range(0, len(missing_items), chunk_size):
        batch = missing_items[start:start + chunk_size]
        placeholders = ",".join("(?, ?)" for _ in batch)
        params: list[Any] = []
        for code, latest_rank_date in batch:
            params.extend([code, latest_rank_date])
        if cutoff:
            params.append(cutoff)
        query = f"""
            WITH latest_rank(fcode, latest_rank_date) AS (
                VALUES {placeholders}
            )
            SELECT
                archive_rank.fcode AS fcode,
                latest_rank.latest_rank_date AS latest_rank_date,
                COUNT(*) AS rank_count_12m,
                AVG(archive_rank.percentile) AS rank_avg_percentile_12m,
                AVG(archive_rank.percentile * archive_rank.percentile) AS rank_avg_sq_percentile_12m,
                SUM(CASE WHEN archive_rank.percentile <= 0.30 THEN 1 ELSE 0 END) AS rank_top30_count_12m,
                0 AS source_order
            FROM latest_rank
            JOIN {source_sql} archive_rank
                ON archive_rank.fcode = latest_rank.fcode
            WHERE archive_rank.percentile IS NOT NULL
              {cutoff_sql}
              AND archive_rank.rank_date >= date(latest_rank.latest_rank_date, '-365 day')
            GROUP BY archive_rank.fcode
            ORDER BY archive_rank.fcode
            """
        try:
            frames.append(pd.read_sql_query(query, conn, params=tuple(params)))
        except (sqlite3.Error, pd.errors.DatabaseError, ValueError):
            return None

    result = pd.concat([frame for frame in frames if not frame.empty], ignore_index=True) if frames else pd.DataFrame()
    result_codes = {
        code
        for code in (_normalize_code(code) for code in result.get("fcode", pd.Series(dtype=object)).tolist())
        if code
    }
    if result_codes != {code for code, _latest_rank_date in missing_items}:
        return None
    return result


def _build_long_term_feature_frame(db_path: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
    columns = [
        "fcode",
        "近3年正收益年数",
        "近5年正收益年数",
        "近3年年度收益标准差",
        "近5年年度收益标准差",
        "近12月平均同类排名百分位",
        "近12月排名百分位波动",
        "近12月前30%排名观测占比",
        "长期稳定性标签",
        "长期稳定性说明",
        "长期稳定性特征完整",
        "长期稳定性中性处理",
        "长期稳定性总分",
        "long_term_annual_feature_ready",
        "long_term_rank_feature_ready",
    ]
    if not db_path.exists():
        return pd.DataFrame(columns=columns), {"available": False}

    conn = sqlite3.connect(str(db_path))
    try:
        rank_stale_days = _resolve_report_rank_stale_days()
        cache_snapshot = RankCacheSnapshot.start(conn, db_path, begin_rank_history_read_snapshot)
        rank_archive_binding = load_latest_rank_history_archive_binding()
        if not rank_history_archive_binding_matches_database(rank_archive_binding, db_path):
            rank_archive_binding = {}
        rank_archive_attach = cache_snapshot.attach(
            attach_rank_history_archive_if_available,
            archive_path=rank_archive_binding.get("archive_path"),
            archive_table_name=rank_archive_binding.get("archive_table_name") or "fund_rank_history_archive",
            archive_cutoff_date=rank_archive_binding.get("archive_cutoff_date") or "",
        )
        try:
            _read_long_term_history_rank_snapshot_stats(
                conn, db_path, rank_archive_attach, cache_snapshot=cache_snapshot,
            )
        except (sqlite3.Error, OSError, TypeError, ValueError):
            pass
        annual_window_3y = _current_complete_calendar_year_window(LONG_TERM_STABILITY_MIN_YEARS_3)
        annual_window_5y = _current_complete_calendar_year_window(LONG_TERM_STABILITY_MIN_YEARS_5)
        annual_metrics_df = pd.read_sql_query(
            """
            SELECT
                fcode,
                COUNT(DISTINCT CASE WHEN year IN (?, ?, ?) AND annual_return IS NOT NULL THEN year END) AS annual_3_count,
                SUM(CASE WHEN year IN (?, ?, ?) AND annual_return > 0 THEN 1 ELSE 0 END) AS positive_3y,
                AVG(CASE WHEN year IN (?, ?, ?) THEN annual_return END) AS annual_avg_3y,
                AVG(CASE WHEN year IN (?, ?, ?) THEN annual_return * annual_return END) AS annual_avg_sq_3y,
                COUNT(DISTINCT CASE WHEN year IN (?, ?, ?, ?, ?) AND annual_return IS NOT NULL THEN year END) AS annual_5_count,
                SUM(CASE WHEN year IN (?, ?, ?, ?, ?) AND annual_return > 0 THEN 1 ELSE 0 END) AS positive_5y,
                AVG(CASE WHEN year IN (?, ?, ?, ?, ?) THEN annual_return END) AS annual_avg_5y,
                AVG(CASE WHEN year IN (?, ?, ?, ?, ?) THEN annual_return * annual_return END) AS annual_avg_sq_5y
            FROM fund_annual_returns
            WHERE year IN (?, ?, ?, ?, ?)
            GROUP BY fcode
            ORDER BY fcode
            """,
            conn,
            params=(
                *annual_window_3y,
                *annual_window_3y,
                *annual_window_3y,
                *annual_window_3y,
                *annual_window_5y,
                *annual_window_5y,
                *annual_window_5y,
                *annual_window_5y,
                *annual_window_5y,
            ),
        )
        rank_metrics_df = _read_long_term_rank_metrics(
            conn, db_path, rank_archive_attach, cache_snapshot=cache_snapshot,
        )
    finally:
        if "rank_archive_attach" in locals() and rank_archive_attach.get("attached"):
            detach_rank_history_archive(
                conn,
                alias=str(rank_archive_attach.get("alias") or "rank_archive_db"),
            )
        conn.close()

    annual_metrics_df["fcode"] = annual_metrics_df.get("fcode", pd.Series(dtype=object)).map(_normalize_code)
    annual_metrics_df = annual_metrics_df[annual_metrics_df["fcode"].astype(str).str.strip() != ""].copy()
    annual_metrics_df = annual_metrics_df.drop_duplicates(subset=["fcode"], keep="first")

    rank_metrics_df["fcode"] = rank_metrics_df.get("fcode", pd.Series(dtype=object)).map(_normalize_code)
    rank_metrics_df = rank_metrics_df[rank_metrics_df["fcode"].astype(str).str.strip() != ""].copy()
    rank_metrics_df = rank_metrics_df.drop_duplicates(subset=["fcode"], keep="first")

    for column in [
        "annual_3_count",
        "positive_3y",
        "annual_avg_3y",
        "annual_avg_sq_3y",
        "annual_5_count",
        "positive_5y",
        "annual_avg_5y",
        "annual_avg_sq_5y",
    ]:
        annual_metrics_df[column] = pd.to_numeric(annual_metrics_df.get(column), errors="coerce")
    for column in [
        "rank_count_12m",
        "rank_avg_percentile_12m",
        "rank_avg_sq_percentile_12m",
        "rank_top30_count_12m",
    ]:
        rank_metrics_df[column] = pd.to_numeric(rank_metrics_df.get(column), errors="coerce")

    annual_metrics_df["annual_std_3y"] = [
        _population_std_from_moments(count_value, mean_value, mean_square_value)
        for count_value, mean_value, mean_square_value in zip(
            annual_metrics_df.get("annual_3_count", pd.Series(dtype=float)),
            annual_metrics_df.get("annual_avg_3y", pd.Series(dtype=float)),
            annual_metrics_df.get("annual_avg_sq_3y", pd.Series(dtype=float)),
            strict=False,
        )
    ]
    annual_metrics_df["annual_std_5y"] = [
        _population_std_from_moments(count_value, mean_value, mean_square_value)
        for count_value, mean_value, mean_square_value in zip(
            annual_metrics_df.get("annual_5_count", pd.Series(dtype=float)),
            annual_metrics_df.get("annual_avg_5y", pd.Series(dtype=float)),
            annual_metrics_df.get("annual_avg_sq_5y", pd.Series(dtype=float)),
            strict=False,
        )
    ]
    rank_metrics_df["rank_std_12m"] = [
        _population_std_from_moments(count_value, mean_value, mean_square_value)
        for count_value, mean_value, mean_square_value in zip(
            rank_metrics_df.get("rank_count_12m", pd.Series(dtype=float)),
            rank_metrics_df.get("rank_avg_percentile_12m", pd.Series(dtype=float)),
            rank_metrics_df.get("rank_avg_sq_percentile_12m", pd.Series(dtype=float)),
            strict=False,
        )
    ]

    merged_metrics_df = annual_metrics_df.merge(rank_metrics_df, on="fcode", how="outer")
    merged_metrics_df = merged_metrics_df.sort_values(by=["fcode"], kind="mergesort").reset_index(drop=True)

    rows: list[dict[str, Any]] = []
    tag_counter: Counter[str] = Counter()
    annual_ready_count = 0
    rank_ready_count = 0
    full_ready_count = 0
    neutral_count = 0

    for row in merged_metrics_df.to_dict(orient="records"):
        code = _normalize_code(row.get("fcode"))
        annual_3_count = int(_safe_float(row.get("annual_3_count")) or 0)
        annual_5_count = int(_safe_float(row.get("annual_5_count")) or 0)
        rank_count_12m = int(_safe_float(row.get("rank_count_12m")) or 0)

        annual_ready = annual_5_count >= LONG_TERM_STABILITY_MIN_YEARS_5
        latest_rank_date_text = str(row.get("latest_rank_date") or "").strip()
        rank_stale = False
        if latest_rank_date_text:
            try:
                rank_age_days = (datetime.now().date() - datetime.fromisoformat(latest_rank_date_text[:10]).date()).days
            except ValueError:
                rank_age_days = None
            rank_stale = bool(rank_age_days is not None and rank_age_days > rank_stale_days)
        rank_ready = rank_count_12m >= LONG_TERM_RANK_MIN_OBSERVATIONS and not rank_stale

        positive_3y = (
            int(_safe_float(row.get("positive_3y")) or 0)
            if annual_3_count >= LONG_TERM_STABILITY_MIN_YEARS_3
            else None
        )
        positive_5y = (
            int(_safe_float(row.get("positive_5y")) or 0)
            if annual_ready
            else None
        )
        annual_std_3y = (
            _safe_float(row.get("annual_std_3y"))
            if annual_3_count >= LONG_TERM_STABILITY_MIN_YEARS_3
            else None
        )
        annual_std_5y = _safe_float(row.get("annual_std_5y")) if annual_ready else None

        rank_avg_percentile_12m = _safe_float(row.get("rank_avg_percentile_12m"))
        rank_top30_count_12m = int(_safe_float(row.get("rank_top30_count_12m")) or 0)
        rank_avg_12m = (
            (1.0 - float(rank_avg_percentile_12m)) * 100.0
            if rank_ready and rank_avg_percentile_12m is not None
            else None
        )
        rank_std_12m = _safe_float(row.get("rank_std_12m")) if rank_ready else None
        # Counts are non-null ranking observations; no monthly resampling occurs.
        rank_top30_ratio_12m = (
            float(rank_top30_count_12m / rank_count_12m * 100.0)
            if rank_ready and rank_count_12m > 0
            else None
        )

        annual_positive_3y_score = None if positive_3y is None else (float(positive_3y) / 3.0) * 100.0
        annual_positive_5y_score = None if positive_5y is None else (float(positive_5y) / 5.0) * 100.0
        annual_stability_3y_score = _score_stability_by_std(annual_std_3y)
        annual_stability_5y_score = _score_stability_by_std(annual_std_5y)
        rank_avg_12m_score = rank_avg_12m
        rank_top30_12m_score = rank_top30_ratio_12m

        full_ready = annual_ready and rank_ready
        if annual_ready:
            annual_ready_count += 1
        if rank_ready:
            rank_ready_count += 1
        if full_ready:
            full_ready_count += 1
        else:
            neutral_count += 1

        score_values = []
        score_notes: list[str] = []
        for metric_key, (label, weight, _higher_better) in LONG_TERM_STABILITY_COMPONENTS.items():
            current_value = locals().get(metric_key)
            if current_value is None:
                continue
            score_values.append(float(current_value) * float(weight))
            if label in {"近3年正收益年数", "近5年正收益年数"}:
                raw_value = positive_3y if label == "近3年正收益年数" else positive_5y
                score_notes.append(f"{label}{int(raw_value or 0)}")
            elif label == "近12月同类排名均值":
                score_notes.append(f"{label}{current_value:.1f}")
            elif label == "近12月前30%排名观测占比":
                score_notes.append(f"{label}{current_value:.1f}%")
            else:
                raw_value = annual_std_3y if label == "近3年年度收益稳定性" else annual_std_5y
                if raw_value is not None:
                    score_notes.append(f"{label}std={raw_value:.2f}")

        long_term_score = None if not score_values else round(float(sum(score_values)), 4)

        if full_ready and long_term_score is not None:
            if long_term_score >= 75:
                tag = "长期稳定性强"
            elif long_term_score >= 55:
                tag = "长期稳定性中等"
            else:
                tag = "长期稳定性偏弱"
            note = "；".join(score_notes[:6])
        elif annual_3_count > 0 or annual_5_count > 0 or rank_count_12m > 0:
            tag = "长期历史部分缺失"
            if rank_stale:
                note = "长期稳定性历史部分缺失，近12月排名样本已过期，暂按中性处理"
            else:
                note = "长期稳定性历史部分缺失，暂按中性处理"
        else:
            tag = "长期历史缺失"
            note = "长期稳定性历史缺失，暂按中性处理"

        tag_counter[tag] += 1
        rows.append(
            {
                "fcode": code,
                "近3年正收益年数": positive_3y,
                "近5年正收益年数": positive_5y,
                "近3年年度收益标准差": None if annual_std_3y is None else round(annual_std_3y, 4),
                "近5年年度收益标准差": None if annual_std_5y is None else round(annual_std_5y, 4),
                "近12月平均同类排名百分位": None if rank_avg_12m is None else round(rank_avg_12m, 4),
                "近12月排名百分位波动": None if rank_std_12m is None else round(rank_std_12m, 4),
                "近12月前30%排名观测占比": None if rank_top30_ratio_12m is None else round(rank_top30_ratio_12m, 4),
                "长期稳定性标签": tag,
                "长期稳定性说明": note,
                "长期稳定性特征完整": full_ready,
                "长期稳定性中性处理": not full_ready,
                "长期稳定性总分": long_term_score,
                "long_term_annual_feature_ready": annual_ready,
                "long_term_rank_feature_ready": rank_ready,
            }
        )

    feature_df = pd.DataFrame(rows, columns=columns)
    summary = {
        "available": not feature_df.empty,
        "candidate_total": int(len(feature_df.index)),
        "annual_feature_ready_fund_count": int(annual_ready_count),
        "rank_feature_ready_fund_count": int(rank_ready_count),
        "full_feature_ready_fund_count": int(full_ready_count),
        "neutralized_fund_count": int(neutral_count),
        "tag_distribution": [
            {"label": label, "count": int(count)}
            for label, count in tag_counter.most_common()
        ],
        "preview": feature_df.head(15).fillna("").astype(str).to_dict(orient="records"),
    }
    return feature_df, summary


def _safe_float(value: Any) -> float | None:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(numeric):
        return None
    return numeric


def _normalize_number_text(value: Any, digits: int = 4) -> str:
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


def _path_cache_token(path: Path | str | None) -> str:
    if not path:
        return "missing"
    try:
        stat = Path(path).stat()
    except OSError:
        return "missing"
    return f"{int(stat.st_mtime_ns)}:{int(stat.st_size)}"


def _sqlite_file_cache_token_for_tracker(path: Path | str | None) -> str:
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


def _stable_json_digest(value: Any) -> str:
    normalized = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha1(normalized.encode("utf-8")).hexdigest()


def _safe_median(series: pd.Series) -> float | None:
    numeric = pd.to_numeric(series, errors="coerce").dropna()
    if numeric.empty:
        return None
    return round(float(numeric.median()), 6)


def _safe_count(mask: pd.Series) -> int:
    return int(mask.fillna(False).astype(bool).sum())


def _resolve_formal_score_value(row: dict[str, Any] | pd.Series | None) -> float | None:
    """正式主分优先使用正式投资评分，综合加权得分仅作审计兜底。"""
    if row is None:
        return None
    if isinstance(row, pd.Series):
        row = row.to_dict()
    if not isinstance(row, dict):
        return None
    formal_score = _safe_float(row.get(FORMAL_SCORE_LABEL))
    if formal_score is not None:
        return formal_score
    return _safe_float(row.get("综合加权得分"))


def _ensure_formal_score_column(df: pd.DataFrame) -> pd.DataFrame:
    """确保正式主分列存在且统一按正式评分口径解析。"""
    current = df.copy()
    if current.empty:
        if FORMAL_SCORE_LABEL not in current.columns:
            current[FORMAL_SCORE_LABEL] = pd.Series(dtype=float)
        return current
    current[FORMAL_SCORE_LABEL] = current.apply(_resolve_formal_score_value, axis=1)
    return current


def _ensure_export_formal_score_column(df: pd.DataFrame) -> pd.DataFrame:
    """导出前统一补齐正式主分，避免列存在但局部为空时回退漂移。"""
    current = df.copy()
    if current.empty:
        if FORMAL_SCORE_LABEL not in current.columns:
            current[FORMAL_SCORE_LABEL] = pd.Series(dtype=float)
        return current
    return _ensure_formal_score_column(current)


def _normalize_autostock_status(value: Any) -> str:
    return normalize_autostock_status_impl(value)


def _format_autostock_display_status(value: Any) -> str:
    return format_autostock_display_status_impl(value)


def _format_autostock_note_flag(value: Any) -> str:
    return format_autostock_note_flag_impl(value)


def _build_recommendation_note(row: pd.Series | dict[str, Any]) -> str:
    return build_recommendation_note_impl(row)


def _normalize_nav_date_cache_key(value: Any) -> str:
    if pd.isna(value):
        return ""
    text = str(value or "").strip()
    if not text:
        return ""
    if "T" in text:
        text = text.split("T", 1)[0].strip()
    if " " in text:
        text = text.split(" ", 1)[0].strip()
    return text


def _load_autostock_status_cache_from_csv(path: Path) -> dict[tuple[str, str], dict[str, Any]]:
    if not artifact_is_file(path):
        return {}

    try:
        current = read_csv_artifact(path)
    except CoreCsvStorageError:
        raise
    except Exception:
        return {}
    if current.empty:
        return {}

    required_columns = {"基金代码", "最新净值日期", "AutoStock校验状态", "AutoStock校验备注"}
    if not required_columns.issubset(set(current.columns)):
        return {}

    source = {"path": str(path), "sha256": hashlib.sha256(read_original_artifact_bytes(path)).hexdigest(),
              "reuse_policy": "legacy_code_and_nav_date; not_a_fresh_API_comparison"}
    cache: dict[tuple[str, str], dict[str, Any]] = {}
    for row in current.to_dict(orient="records"):
        code = _normalize_code(row.get("基金代码"))
        nav_date = _normalize_nav_date_cache_key(row.get("最新净值日期"))
        if not code or not nav_date:
            continue
        raw_status = _normalize_autostock_status(row.get("AutoStock校验状态"))
        if raw_status not in {"strict_match", "soft_pass_with_warnings", "hard_block"}:
            continue
        note_value = row.get("AutoStock校验备注")
        note = "" if pd.isna(note_value) else str(note_value or "").strip()
        cache[(code, nav_date)] = {
            "raw_status": raw_status,
            "display_status": _format_autostock_display_status(raw_status),
            "note": note,
            "source": source,
            "source_row": autostock_evidence_json_value(row),
        }
    return cache


def _load_latest_official_autostock_cache(settings: Settings) -> dict[tuple[str, str], dict[str, Any]]:
    from backend.publication_transaction import load_publication_head, resolve_committed_path

    project_root = Path(getattr(settings, "project_root", Path.cwd()))
    reports_dir = Path(getattr(settings, "reports_dir", project_root / "reports"))
    head = load_publication_head(settings)
    if head:
        entry = head["committed_runs"][head["current_published_run_id"]]
        latest_report = _read_json_file(resolve_committed_path(settings, entry["report_path"]))
    else:
        # Legacy CSVs can be inspected, but cannot provide a trusted binding.
        latest_report = _read_json_file(reports_dir / "latest.json")
    artifacts = latest_report.get("artifacts") or {}
    cache: dict[tuple[str, str], dict[str, Any]] = {}
    for artifact_key in ["filtered_csv", "dynamic_filtered_csv"]:
        raw_path = artifacts.get(artifact_key)
        if not raw_path:
            continue
        cache_path = project_root / str(raw_path)
        for key, cached in _load_autostock_status_cache_from_csv(cache_path).items():
            existing = cache.get(key)
            if existing and (existing["raw_status"], existing["note"]) != (cached["raw_status"], cached["note"]):
                cached["conflicting_track_sources"] = [existing["source"], cached["source"]]
            cache[key] = cached
    descriptor = (latest_report.get("autostock_validation_audit") or {}).get("supplement_snapshot")
    if cache and head and descriptor:
        snapshot = load_supplement_snapshot(
            project_root, descriptor, reports_dir=reports_dir,
            run_id=head["current_published_run_id"],
            revision=str((latest_report.get("publication_control") or {}).get("revision") or ""),
        )
        bindings = snapshot_cache_bindings(
            snapshot, descriptor, normalize_code=_normalize_code, normalize_date=_normalize_nav_date_cache_key,
        )
        for key, cached in cache.items():
            if key in bindings:
                cached["validation_binding"] = bindings[key]
                cached["source"] = {**cached["source"],
                                    "reuse_policy": "frozen_comparison_binding_with_original_validation_time"}
    return cache


def _resolve_holding_period_status(
    *,
    has_holding_period: int,
    holding_days: int | None,
    threshold_days: int = 90,
    holding_period_source: str | None = None,
    fund_name: str | None = None,
    holding_keywords: str | None = None,
    fullname: str | None = None,
) -> str:
    return resolve_holding_period_status_impl(
        has_holding_period=has_holding_period,
        holding_days=holding_days,
        threshold_days=threshold_days,
        holding_period_source=holding_period_source,
        fund_name=combine_holding_period_names(fund_name, fullname),
        holding_keywords=holding_keywords,
    )


def _is_holding_period_name_unparsed_status(value: Any) -> bool:
    return is_holding_period_name_unparsed_status_impl(value)


def _apply_holding_period_name_unparsed_confidence_penalty(
    *,
    holding_period_status: str,
    label: str,
    score: float,
    note: str,
    high_score_min: float,
    mid_score_min: float,
) -> tuple[str, float, str]:
    return apply_holding_period_name_unparsed_confidence_penalty_impl(
        holding_period_status=holding_period_status,
        label=label,
        score=score,
        note=note,
        high_score_min=high_score_min,
        mid_score_min=mid_score_min,
    )


def _refresh_recommendation_notes(df: pd.DataFrame) -> pd.DataFrame:
    return refresh_recommendation_notes_impl(df)


def _load_autostock_cross_validation_config_for_report() -> dict[str, Any]:
    return {
        "nav_relative_tolerance_pct": 1.0,
        "nav_absolute_tolerance": 0.001,
        "nav_date_soft_lag_days": 5,
        "return_relative_tolerance_pct": 5.0,
        "return_absolute_tolerance": 0.5,
        "return_small_base_threshold": 1.0,
        "batch_size": 50,
        "max_workers": 1,
    }


def _load_autostock_runtime_for_report(settings: Settings):
    """延迟加载 AutoStock 运行时依赖，兼容不同入口的 sys.path。"""
    import sys

    project_root = settings.project_root
    core_root = settings.project_root / "core"
    screening_root = core_root / "bond_fund_screening"
    for path in [screening_root, core_root, project_root]:
        path_text = str(path)
        if path_text in sys.path:
            sys.path.remove(path_text)
        sys.path.insert(0, path_text)

    from api.autostock_client import AutoStockClient  # type: ignore
    from api.autostock_endpoints import DataConverter  # type: ignore
    from bond_fund_screening.tasks.final_screening_autostock_helpers import (  # type: ignore
        build_autostock_validation_status_map,
        compare_candidate_with_autostock,
        compare_return_with_autostock,
        is_type_match,
        normalize_iso_date,
        safe_float,
    )

    compare_candidate_fn = partial(
        compare_candidate_with_autostock,
        converter_cls=DataConverter,
        normalize_fcode=_normalize_code,
        normalize_iso_date_fn=normalize_iso_date,
        safe_float_fn=safe_float,
        is_type_match_fn=is_type_match,
        compare_return_with_autostock_fn=compare_return_with_autostock,
        return_field_mappings=(
            ("近1周(%)", "lastWeekGrowth", "return_1w"),
            ("近1月(%)", "lastMonthGrowth", "return_1m"),
            ("近3月(%)", "lastThreeMonthsGrowth", "return_3m"),
            ("近6月(%)", "lastSixMonthsGrowth", "return_6m"),
            ("近1年(%)", "lastYearGrowth", "return_1y"),
        ),
    )

    return AutoStockClient, build_autostock_validation_status_map, compare_candidate_fn


def _supplement_official_endpoint_statuses(
    df: pd.DataFrame, *, settings: Settings, status_context: dict[str, Any] | None,
    purpose: str,
) -> pd.DataFrame:
    """Recheck all report members; old statuses and AutoStock cache are not evidence."""
    from backend.fund_cross_validation import (
        fetch_cross_validation, compare_endpoint_candidate, validation_status_map,
    )

    current = df.copy()
    if current.empty or "基金代码" not in current:
        return current
    config = _load_autostock_cross_validation_config_for_report()
    codes = current["基金代码"].map(_normalize_code).tolist()
    if any(not code for code in codes) or len(codes) != len(set(codes)):
        raise ValueError("data_validation_report_identity_invalid")
    current["基金代码"] = codes
    local_rows = autostock_evidence_json_value(
        current.reindex(columns=list(AUTOSTOCK_COMPARE_COLUMNS)).to_dict(orient="records")
    )
    record = {"started_at": datetime.now().astimezone().isoformat(),
              "purpose": purpose, "provider": "eastmoney_cross_endpoint",
              "independent_source": False, "requested_codes": codes,
              "configuration": autostock_evidence_json_value(config), "local_rows": local_rows,
              "comparisons": []}
    lookup, meta = fetch_cross_validation(settings, codes)
    record["upstream_records"] = autostock_evidence_json_value(lookup)
    record["upstream_metadata"] = autostock_evidence_json_value(meta)
    details = []
    for local in local_rows:
        code = local["基金代码"]
        upstream = lookup.get(code)
        result = compare_endpoint_candidate(local, upstream or {}, config)
        detail = result["details"]
        details.append(detail)
        record["comparisons"].append({"code": code, "local_row": local,
            "upstream_record_sha256": autostock_evidence_sha256(upstream),
            "result": autostock_evidence_json_value(result)})
    statuses = validation_status_map(details)
    for column in DATA_VALIDATION_COLUMNS:
        current[column] = ""
    for index, row in current.iterrows():
        status = statuses.get(_normalize_code(row["基金代码"])) or {}
        raw = status.get("validation_status") or "not_checked"
        warnings = status.get("validation_warning_display") or ""
        hard = status.get("validation_hard_reasons") or []
        current.at[index, "数据校验原始状态"] = raw
        current.at[index, "数据校验状态"] = _format_autostock_display_status(raw)
        current.at[index, "数据校验告警原因"] = warnings
        current.at[index, "数据校验备注"] = "；".join([*map(str, hard), str(warnings)]).strip("；")
        current.at[index, "数据校验来源"] = status.get("validation_source") or meta.get("provider") or "eastmoney_cross_endpoint"
        current.at[index, "数据校验契约版本"] = status.get("validation_contract") or meta.get("contract") or ""
    record["output_statuses"] = autostock_evidence_json_value(
        current[["基金代码", *DATA_VALIDATION_COLUMNS]].to_dict(orient="records")
    )
    record["finished_at"] = datetime.now().astimezone().isoformat()
    record["outcome"] = "compared" if meta.get("source_available") else "source_unavailable"
    for key in ("local_rows", "upstream_records", "upstream_metadata", "configuration"):
        record[f"{key}_sha256"] = autostock_evidence_sha256(record[key])
    record["record_sha256"] = autostock_evidence_sha256(record)
    if isinstance(status_context, dict):
        status_context.setdefault("data_validation_records", []).append(record)
        status_context.setdefault("data_validation_source_evidence", []).append(meta.get("source_evidence") or meta)
        if not meta.get("source_available") or not meta.get("missing_gate_passed", meta.get("source_complete")):
            status_context["data_validation_unavailable"] = {
                "reason": "cross_endpoint_validation_incomplete", "provider": "eastmoney_cross_endpoint",
                "independent_source": False, "source_complete": False,
            }
    return _refresh_recommendation_notes(current)


def _supplement_official_autostock_statuses(
    df: pd.DataFrame,
    *,
    settings: Settings,
    status_context: dict[str, Any] | None = None,
    purpose: str = "publication",
) -> pd.DataFrame:
    """对指定用途的缺失 AutoStock 校验状态做有界补校验。

    当 AutoStock 配额耗尽 / 批量失败 / 返回空时，降级信号会同时：
    1. 写到入参 ``status_context`` dict（调用方在 build_report_payload 中把它收上报告 JSON）
    2. 冗余写到 DataFrame.attrs（pandas 2.x 在常见操作里会保留）
    以便即便其中一条传递链受 pandas 操作影响丢失，另一条仍可见。
    """
    if purpose not in {"publication", "strategy_observation"}:
        raise ValueError("autostock_supplement_purpose_invalid")
    if getattr(settings, "publication_validation_provider", "autostock") == "eastmoney_cross_endpoint":
        return _supplement_official_endpoint_statuses(
            df, settings=settings, status_context=status_context, purpose=purpose,
        )
    strategy_observation = purpose == "strategy_observation"
    current = df.copy()
    if current.empty or "基金代码" not in current.columns:
        return current

    if "AutoStock原始校验状态" not in current.columns:
        current["AutoStock原始校验状态"] = ""
    if "AutoStock告警原因" not in current.columns:
        current["AutoStock告警原因"] = ""
    if "AutoStock校验状态" not in current.columns:
        current["AutoStock校验状态"] = current["AutoStock原始校验状态"].map(_format_autostock_display_status)
    if "AutoStock校验备注" not in current.columns:
        current["AutoStock校验备注"] = current["AutoStock告警原因"].fillna("")
    for column in ["AutoStock原始校验状态", "AutoStock告警原因", "AutoStock校验状态", "AutoStock校验备注"]:
        current[column] = current[column].fillna("").astype(str)

    config = _load_autostock_cross_validation_config_for_report()
    supplement_record = begin_supplement_record(current, config)
    supplement_record["purpose"] = purpose

    def _finish_evidence(outcome: str) -> pd.DataFrame:
        # Inherited results can have a warning but no display note. Keep that
        # warning visible on every exit path, without erasing hard-block reasons
        # or restoring old text after a new strict comparison cleared it.
        missing_note = current["AutoStock校验备注"].str.strip().eq("")
        current.loc[missing_note, "AutoStock校验备注"] = current.loc[missing_note, "AutoStock告警原因"]
        return finish_supplement_record(supplement_record, current, status_context, outcome=outcome)

    if not getattr(settings, "autostock_enabled", True):
        unavailable = {"reason": "independent_source_unavailable", "source": "autostock",
                       "source_complete": False, "request_count": 0,
                       "replacement_independent_source": False}
        current.attrs["autostock_validation_unavailable"] = unavailable
        if isinstance(status_context, dict):
            status_context["autostock_validation_unavailable"] = unavailable
        return _finish_evidence("provider_disabled")

    def _build_source_evidence(
        *,
        source_type: str,
        source: str,
        endpoint_path: str,
        target_count: int,
        cache_reused_count: int = 0,
        api_target_count: int = 0,
        batch_meta: dict[str, Any] | None = None,
        reason: str | None = None,
        response_rows: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        meta = dict(batch_meta or {})
        nested = dict(meta.get("source_evidence") or {}) if isinstance(meta.get("source_evidence"), dict) else {}
        evidence = {
            "schema_version": 1,
            "task": ("report_builder.prospective_strategy_observation" if strategy_observation
                     else "report_builder.official_autostock_supplement"),
            "source": nested.get("source") or source,
            "source_type": source_type,
            "base_url": nested.get("base_url") or str(settings.autostock_base_url),
            "endpoint_path": nested.get("endpoint_path") or endpoint_path,
            "request_window": nested.get("request_window") or {"started_at": None, "finished_at": None},
            "target_count": int(target_count or 0),
            "api_target_count": int(api_target_count or 0),
            "cache_reused_count": int(cache_reused_count or 0),
            "request_count": int(nested.get("request_count") or 0),
            "response_count": int(nested.get("response_count") or 0),
            "accepted_response_count": int(nested.get("accepted_response_count") or 0),
            "failed_response_count": int(nested.get("failed_response_count") or 0),
            "transport_exception_count": int(nested.get("transport_exception_count") or 0),
            "returned_record_count": int(nested.get("returned_record_count") or meta.get("returned_total") or len(response_rows or [])),
            "returned_unique_count": int(nested.get("returned_unique_count") or meta.get("returned_unique_total") or 0),
            "missing_count": int(nested.get("missing_count") or meta.get("missing_count") or 0),
            "failed_batch_count": int(nested.get("failed_batch_count") or meta.get("failed_batch_count") or 0),
            "quota_exhausted": bool(nested.get("quota_exhausted") or meta.get("quota_exhausted")),
            "last_error": nested.get("last_error") or meta.get("last_error"),
            "response_hash": nested.get("response_hash") or _stable_json_digest(response_rows or []),
            "sample_codes": list(nested.get("sample_codes") or [_normalize_code(item.get("code") or item.get("FCODE") or item.get("fcode")) for item in (response_rows or [])[:10] if isinstance(item, dict)]),
            "reason": reason,
        }
        evidence["source_complete"] = bool(
            source_type == "cache"
            or (
                evidence["failed_response_count"] == 0
                and evidence["transport_exception_count"] == 0
                and evidence["failed_batch_count"] == 0
                and evidence["missing_count"] == 0
                and not evidence["quota_exhausted"]
                and source_type != "budget_degraded"
                and source_type != "unavailable"
            )
        )
        return evidence

    # The prospective caller already excludes recorded comparisons. New
    # experiment observations must not acquire an old status without its source.
    latest_official_cache = {} if strategy_observation else _load_latest_official_autostock_cache(settings)
    cache_code_hashes = (
        comparison_code_hashes(settings.project_root)
        if any("validation_binding" in cached for cached in latest_official_cache.values()) else {}
    )
    cache_reused_count = 0
    if latest_official_cache:
        for row_index, row in current.iterrows():
            raw_status = _normalize_autostock_status(row.get("AutoStock原始校验状态"))
            if raw_status not in {"", "not_checked"}:
                continue
            cache_key = (
                _normalize_code(row.get("基金代码")),
                _normalize_nav_date_cache_key(row.get("最新净值日期")),
            )
            cached = latest_official_cache.get(cache_key)
            if not cached:
                continue
            rejection = cache_reuse_rejection(
                cached, row, configuration=config, code_sha256=cache_code_hashes,
                base_url=settings.autostock_base_url,
                ttl_seconds=int(getattr(settings, "autostock_report_validation_cache_ttl_seconds", 14400)),
                normalize_code=_normalize_code, normalize_date=_normalize_nav_date_cache_key,
            )
            if rejection:
                supplement_record["cache_rejections"].append({
                    "code": cache_key[0], "nav_date": cache_key[1], "reason": rejection,
                    "cached_source": autostock_evidence_json_value(cached.get("source")),
                    "cached_binding_sha256": (cached.get("validation_binding") or {}).get("binding_sha256"),
                })
                continue
            current.at[row_index, "AutoStock原始校验状态"] = cached["raw_status"]
            current.at[row_index, "AutoStock告警原因"] = cached["note"]
            current.at[row_index, "AutoStock校验状态"] = cached["display_status"]
            current.at[row_index, "AutoStock校验备注"] = cached["note"]
            cache_reused_count += 1
            supplement_record["cache_reuses"].append({
                "code": cache_key[0], "nav_date": cache_key[1],
                "cached": autostock_evidence_json_value(cached),
            })

    status_series = current["AutoStock原始校验状态"].astype(str).map(_normalize_autostock_status)
    need_mask = status_series.isin(["", "not_checked"])
    target_count = int(len(current.index))

    def _record_source_evidence(payload: dict[str, Any]) -> None:
        current.attrs["autostock_validation_source_evidence"] = payload
        if isinstance(status_context, dict):
            entries = status_context.setdefault("autostock_validation_source_evidence", [])
            if isinstance(entries, list):
                entries.append(payload)
            status_context["autostock_validation_latest_source_evidence"] = payload

    if not bool(need_mask.any()):
        current["AutoStock校验状态"] = current["AutoStock原始校验状态"].map(_format_autostock_display_status)
        if cache_reused_count:
            _record_source_evidence(
                _build_source_evidence(
                    source_type="cache",
                    source="latest official filtered CSV",
                    endpoint_path="reports/latest filtered_csv",
                    target_count=target_count,
                    cache_reused_count=cache_reused_count,
                    reason="latest_official_cache",
                )
            )
        return _finish_evidence("cache_or_existing_statuses_only")

    batch_df = current.loc[need_mask].copy()
    if batch_df.empty:
        return _finish_evidence("no_pending_rows")

    compare_columns = list(AUTOSTOCK_COMPARE_COLUMNS)
    missing_columns = [column for column in compare_columns if column not in batch_df.columns]
    if missing_columns:
        supplement_record["missing_compare_columns"] = missing_columns
        return _finish_evidence("missing_compare_columns")

    local_rows = batch_df[compare_columns].copy()
    local_rows["基金代码"] = local_rows["基金代码"].map(_normalize_code)
    local_rows = local_rows.dropna(subset=["基金代码"]).drop_duplicates(subset=["基金代码"], keep="first")
    if local_rows.empty:
        return _finish_evidence("no_valid_codes")

    codes = local_rows["基金代码"].tolist()
    supplement_record["requested_codes"] = codes
    AutoStockClient, build_status_map_fn, compare_candidate_fn = _load_autostock_runtime_for_report(settings)
    batch_size = max(
        1,
        min(
            int(config.get("batch_size") or settings.autostock_batch_fund_default or 50),
            int(getattr(AutoStockClient, "SAFE_FUND_BATCH_SIZE", 50) or 50),
        ),
    )
    supplement_record["logical_request"] = {
        "method": "GET", "base_url": str(settings.autostock_base_url).rstrip("/"),
        "endpoint_path": "/fund", "ordered_codes": codes, "batch_size": batch_size,
    }
    supplement_record["logical_request_sha256"] = autostock_evidence_sha256(supplement_record["logical_request"])
    needed_call_count = math.ceil(len(codes) / batch_size) if codes else 0
    quota_snapshot = read_autostock_quota_snapshot(
        shared_state_path=settings.autostock_rate_limit_shared_state_path,
        max_calls_per_hour=int(settings.autostock_rate_limit_max_calls_per_hour or 0),
        min_interval_seconds=float(settings.autostock_rate_limit_min_interval_seconds),
        fail_closed=True,
    )
    budget_plan = plan_autostock_call_budget(
        quota_snapshot=quota_snapshot,
        needed_call_count=needed_call_count,
        # A prospective batch contains at most 50 codes. Reserve room for all
        # three possible HTTP attempts while preserving the automation reserve.
        reserve_calls=_REPORT_AUTOSTOCK_SUPPLEMENT_RESERVE_CALLS + (2 if strategy_observation else 0),
    )
    allowed_call_count = int(budget_plan.get("allowed_call_count") or 0)
    maximum_fetch_calls = 1 if strategy_observation else _REPORT_AUTOSTOCK_SUPPLEMENT_MAX_FETCH_CALLS
    if maximum_fetch_calls > 0:
        allowed_call_count = min(allowed_call_count, maximum_fetch_calls)
    budget_meta = {
        "reason": None,
        "needed_call_count": int(needed_call_count),
        "allowed_call_count": int(allowed_call_count),
        "skipped_call_count": max(0, int(needed_call_count) - int(allowed_call_count)),
        "quota_limited": bool(budget_plan.get("quota_limited")),
        "quota_remaining": budget_plan.get("quota_remaining"),
        "quota_reserved_for_automation": int(
            budget_plan.get("quota_reserved_for_automation") or 0
        ),
        "quota_exhausted": bool(budget_plan.get("quota_exhausted")),
        "quota_unavailable": bool(budget_plan.get("quota_unavailable")),
        "quota_degraded": bool(needed_call_count > allowed_call_count),
        "max_fetch_calls_per_run": maximum_fetch_calls,
        "rate_limit_wait_seconds": float(quota_snapshot.get("rate_limit_wait_seconds") or 0.0),
        "shared_state_path": str(settings.autostock_rate_limit_shared_state_path),
        "state_read_error": quota_snapshot.get("state_read_error"),
        "batch_size": int(batch_size),
    }
    supplement_record["budget"] = autostock_evidence_json_value(budget_meta)
    # P0-04.4 / B16: 配额耗尽时降级为"跳过本次交叉验证 + 机器提示"，不中断报告构建。
    autostock_quota_exceeded_flag: dict[str, Any] = {"quota_exceeded": False, "retry_after_seconds": None}
    try:
        from api.autostock_client import AutoStockQuotaExceeded  # type: ignore
    except Exception:  # pragma: no cover - defensive import
        AutoStockQuotaExceeded = ()  # type: ignore[assignment]

    def _record_budget(payload: dict[str, Any]) -> None:
        current.attrs["autostock_validation_budget"] = payload
        if isinstance(status_context, dict):
            status_context.setdefault("autostock_validation_budget", payload)

    def _record_unavailable(payload: dict[str, Any]) -> None:
        current.attrs["autostock_validation_unavailable"] = payload
        if isinstance(status_context, dict):
            status_context.setdefault("autostock_validation_unavailable", payload)

    if budget_meta["quota_degraded"]:
        budget_meta["reason"] = "autostock_budget_unavailable"
        _record_budget(dict(budget_meta))
        _record_unavailable(dict(budget_meta))
        _record_source_evidence(
            _build_source_evidence(
                source_type="budget_degraded",
                source="AutoStock /fund",
                endpoint_path="/fund",
                target_count=target_count,
                cache_reused_count=cache_reused_count,
                api_target_count=len(codes),
                batch_meta=budget_meta,
                reason="autostock_budget_unavailable",
            )
        )
        return _finish_evidence("budget_unavailable")

    _record_budget(dict(budget_meta))

    # P0-04.2 / B15: 改读 settings.autostock_* 字段，移除此前的硬编码（max_calls_per_hour=100 等）。
    client = AutoStockClient(
        base_url=settings.autostock_base_url,
        min_interval=float(settings.autostock_rate_limit_min_interval_seconds),
        timeout=60,
        max_calls_per_hour=int(settings.autostock_rate_limit_max_calls_per_hour),
        shared_state_path=str(settings.autostock_rate_limit_shared_state_path),
    )

    try:
        supplement_record["logical_request_submitted_to_client"] = True
        if hasattr(client, "get_fund_batch_with_meta"):
            autostock_rows, fetched_batch_meta = client.get_fund_batch_with_meta(
                codes,
                batch_size=batch_size,
            )
        else:
            autostock_rows = client.get_fund_batch(
                codes,
                batch_size=batch_size,
            )
            fetched_batch_meta = {
                "requested_total": len(codes),
                "requested_unique_total": len(set(codes)),
                "returned_total": len(autostock_rows or []),
                "returned_unique_total": len({
                    _normalize_code(item.get("FCODE") or item.get("fcode") or item.get("code") or item.get("基金代码"))
                    for item in autostock_rows or []
                    if isinstance(item, dict)
                }),
                "missing_count": 0,
                "quota_exhausted": False,
                "last_error": None,
            }
    except AutoStockQuotaExceeded as quota_exc:  # type: ignore[misc]
        supplement_record["upstream_error"] = {"type": type(quota_exc).__name__, "message": str(quota_exc)}
        autostock_quota_exceeded_flag["quota_exceeded"] = True
        autostock_quota_exceeded_flag["retry_after_seconds"] = float(
            getattr(quota_exc, "retry_after_seconds", 0.0) or 0.0
        )
        quota_payload = {
            "reason": "autostock_quota_exceeded",
            "retry_after_seconds": autostock_quota_exceeded_flag["retry_after_seconds"],
            "max_calls_per_hour": int(settings.autostock_rate_limit_max_calls_per_hour),
        }
        _record_unavailable(
            quota_payload
        )
        _record_source_evidence(
            _build_source_evidence(
                source_type="unavailable",
                source="AutoStock /fund",
                endpoint_path="/fund",
                target_count=target_count,
                cache_reused_count=cache_reused_count,
                api_target_count=len(codes),
                batch_meta=quota_payload,
                reason="autostock_quota_exceeded",
            )
        )
        return _finish_evidence("quota_exceeded")
    except Exception as exc:
        supplement_record["upstream_error"] = {"type": type(exc).__name__, "message": str(exc)}
        failed_payload = {"reason": "autostock_batch_failed"}
        _record_unavailable(failed_payload)
        _record_source_evidence(
            _build_source_evidence(
                source_type="unavailable",
                source="AutoStock /fund",
                endpoint_path="/fund",
                target_count=target_count,
                cache_reused_count=cache_reused_count,
                api_target_count=len(codes),
                batch_meta=failed_payload,
                reason="autostock_batch_failed",
            )
        )
        return _finish_evidence("batch_failed")
    fetched_batch_meta = dict(fetched_batch_meta or {})
    supplement_record["upstream_records"] = autostock_evidence_json_value(autostock_rows)
    supplement_record["upstream_records_sha256"] = (
        None if autostock_rows is None else autostock_evidence_sha256(autostock_rows)
    )
    supplement_record["upstream_metadata"] = autostock_evidence_json_value(fetched_batch_meta)
    _record_source_evidence(
        _build_source_evidence(
            source_type="api_batch",
            source="AutoStock /fund",
            endpoint_path="/fund",
            target_count=target_count,
            cache_reused_count=cache_reused_count,
            api_target_count=len(codes),
            batch_meta=fetched_batch_meta,
            response_rows=list(autostock_rows or []),
        )
    )
    if not autostock_rows:
        _record_unavailable({"reason": "autostock_empty_response"})
        return _finish_evidence("empty_response")

    autostock_lookup = {
        _normalize_code(item.get("FCODE") or item.get("fcode") or item.get("code") or item.get("基金代码")): item
        for item in autostock_rows
        if _normalize_code(item.get("FCODE") or item.get("fcode") or item.get("code") or item.get("基金代码"))
    }
    detail_rows: list[dict[str, Any]] = []
    failed_codes: set[str] = set()
    for _, row in local_rows.iterrows():
        code = _normalize_code(row.get("基金代码"))
        autostock_row = autostock_lookup.get(code)
        if not code or not autostock_row:
            continue
        result = compare_candidate_fn(row, autostock_row, config)
        supplement_record["comparisons"].append({
            "code": code, "local_row": autostock_evidence_json_value(row.to_dict()),
            "upstream_record_sha256": autostock_evidence_sha256(autostock_row),
            "result": autostock_evidence_json_value(result),
        })
        detail = result.get("details") if isinstance(result, dict) else None
        if isinstance(detail, dict) and detail.get("基金代码"):
            detail_rows.append(detail)
        if isinstance(detail, dict) and detail.get("失败原因"):
            failed_codes.add(code)

    if not detail_rows:
        return _finish_evidence("no_comparison_details")

    status_map = build_status_map_fn(
        pd.DataFrame(detail_rows),
        failed_codes=failed_codes,
        normalize_fcode=_normalize_code,
        warning_text_builder=lambda reasons: "；".join(str(item).strip() for item in reasons if str(item).strip()),
    )
    supplement_record["status_map"] = autostock_evidence_json_value(status_map)
    for code, status_item in status_map.items():
        row_mask = current["基金代码"].astype(str).map(_normalize_code) == code
        current.loc[row_mask, "AutoStock原始校验状态"] = status_item.get("autostock_validation_status") or ""
        current.loc[row_mask, "AutoStock告警原因"] = status_item.get("autostock_warning_display") or ""
        hard_reasons = list(status_item.get("autostock_hard_reasons") or [])
        warnings = str(status_item.get("autostock_warning_display") or "")
        current.loc[row_mask, "AutoStock校验备注"] = "；".join(
            [str(reason) for reason in hard_reasons if str(reason).strip()] + ([warnings] if warnings else [])
        )

    current["AutoStock校验状态"] = current["AutoStock原始校验状态"].map(_format_autostock_display_status)
    current = _refresh_recommendation_notes(current)
    return _finish_evidence("compared")


def _supplement_report_autostock_tracks(
    official_df: pd.DataFrame, dynamic_df: pd.DataFrame, *,
    settings: Settings, status_context: dict[str, Any],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Use one supplement result, including hard blocks, for both report tracks."""
    endpoint_mode = getattr(settings, "publication_validation_provider", "autostock") == "eastmoney_cross_endpoint"
    frames = [frame.copy() for frame in (dynamic_df, official_df) if not frame.empty]
    if not frames:
        status_context["autostock_track_union"] = {"target_codes": [], "overlap_codes": []}
        return official_df.copy(), dynamic_df.copy()
    for frame in frames:
        frame.attrs = {}
    combined = pd.concat(frames, ignore_index=True)
    combined["基金代码"] = combined["基金代码"].map(_normalize_code)
    selected_indices = []
    overlap_codes = []
    for code, group in combined.groupby("基金代码", sort=False, dropna=False):
        if not code or pd.isna(code):
            raise ValueError("autostock_track_invalid_code")
        if len(group) > 1:
            overlap_codes.append(code)
            signatures = {
                autostock_evidence_sha256({key: row.get(key) for key in AUTOSTOCK_COMPARE_COLUMNS})
                for _, row in group.iterrows()
            }
            if len(signatures) != 1:
                raise ValueError(f"autostock_track_comparison_inputs_mismatch:{code}")
        statuses = (pd.Series("", index=group.index) if endpoint_mode else
                    group.get("AutoStock原始校验状态", pd.Series("", index=group.index)).map(_normalize_autostock_status))
        known = statuses[~statuses.isin(["", "not_checked"])]
        if known.nunique() > 1:
            raise ValueError(f"autostock_track_existing_status_mismatch:{code}")
        selected_indices.append(known.index[0] if not known.empty else group.index[0])
    union = combined.loc[selected_indices].copy()
    status_context["autostock_track_union"] = {
        "target_codes": union["基金代码"].tolist(), "overlap_codes": overlap_codes,
        "official_count": len(official_df), "dynamic_count": len(dynamic_df),
        "policy": "one_complete_union_before_any_hard_block_demotion",
    }
    if endpoint_mode:
        status_context["data_validation_track_union"] = status_context.pop("autostock_track_union")
    supplemented = _supplement_official_autostock_statuses(
        union, settings=settings, status_context=status_context,
    )
    return (_overlay_autostock_statuses(official_df, supplemented),
            _overlay_autostock_statuses(dynamic_df, supplemented))


def _demote_autostock_hard_block_rows(
    df: pd.DataFrame,
    *,
    watch_bucket: str = AUTO_PUBLISH_GATE_DIAGNOSIS,
    allow_legacy_autostock: bool = True,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    current = df.copy()
    if current.empty:
        return current, pd.DataFrame(columns=current.columns)

    if not allow_legacy_autostock:
        # When the legacy provider is disabled/replaced, only an explicitly
        # identified current data-validation source may produce a hard block.
        # Legacy AutoStock fields remain available for audit display but do not
        # demote a replay candidate.
        def _current_provider_status(row: pd.Series) -> str:
            source = str(row.get("数据校验来源") or "").strip()
            if not source or source == "autostock":
                return ""
            return _normalize_autostock_status(validation_status_value(row))

        status_series = current.apply(_current_provider_status, axis=1)
    else:
        status_series = validation_status_series(current)
    hard_block_mask = status_series.eq("hard_block")
    if not bool(hard_block_mask.any()):
        return current, pd.DataFrame(columns=current.columns)

    kept_df = current.loc[~hard_block_mask].copy()
    demoted_df = current.loc[hard_block_mask].copy()
    demoted_df["项目推荐结论"] = AUTO_DIAGNOSIS_VERDICT
    demoted_df[AUTO_DIAGNOSIS_CATEGORY_COLUMN] = watch_bucket
    demoted_df["发布结论状态"] = "可投资性拦截"
    demoted_df["可投资性状态"] = "不可投资"
    demoted_df["主原因分组"] = "可投资性"
    if "原因分组汇总" in demoted_df.columns:
        demoted_df["原因分组汇总"] = demoted_df["原因分组汇总"].fillna("").astype(str).map(
            lambda text: "可投资性" if not text.strip() else text if "可投资性" in {item.strip() for item in text.split("；") if item.strip()} else f"{text.strip()}；可投资性"
        )
    else:
        demoted_df["原因分组汇总"] = "可投资性"
    reason_texts = demoted_df.get("推荐理由", pd.Series("", index=demoted_df.index, dtype=object)).fillna("").astype(str)
    demoted_df["推荐理由"] = reason_texts.map(
        lambda text: "；".join(
            item
            for item in [text.strip(), ("数据校验硬拦截" if "数据校验原始状态" in demoted_df else PROJECT_RECOMMENDATION_REASONS["hard_block"])]
            if item
        )
    )
    if "项目推荐原因代码" in demoted_df.columns:
        reason_codes = demoted_df["项目推荐原因代码"].fillna("").astype(str)
        demoted_df["项目推荐原因代码"] = reason_codes.map(
            lambda text: "hard_block"
            if not text.strip()
            else text
            if "hard_block" in {item.strip() for item in text.split("|") if item.strip()}
            else f"{text.strip()}|hard_block"
        )
    demoted_df = _refresh_recommendation_notes(demoted_df)
    return kept_df, demoted_df


def _overlay_autostock_statuses(
    target_df: pd.DataFrame,
    source_df: pd.DataFrame,
) -> pd.DataFrame:
    current = target_df.copy()
    if current.empty or source_df.empty or "基金代码" not in current.columns or "基金代码" not in source_df.columns:
        return current

    source = source_df.copy()
    source["_merge_fcode"] = source["基金代码"].map(_normalize_code)
    source = source.dropna(subset=["_merge_fcode"]).drop_duplicates(subset=["_merge_fcode"], keep="first")
    if source.empty:
        return current

    lookup = source.set_index("_merge_fcode")
    current["_merge_fcode"] = current["基金代码"].map(_normalize_code)
    if "数据校验原始状态" in lookup:
        # Always replace the entire neutral result, including missing states.
        # Legacy columns remain the historical AutoStock values.
        for column in DATA_VALIDATION_COLUMNS:
            current[column] = current["_merge_fcode"].map(
                lambda code: lookup.at[code, column] if code in lookup.index and column in lookup else ""
            )
        return _refresh_recommendation_notes(current.drop(columns=["_merge_fcode"]))
    known_source = current["_merge_fcode"].map(
        lambda code: bool(code in lookup.index and _normalize_autostock_status(
            lookup.at[code, "AutoStock原始校验状态"] if "AutoStock原始校验状态" in lookup.columns
            else lookup.at[code, "AutoStock校验状态"] if "AutoStock校验状态" in lookup.columns else ""
        ) in {"strict_match", "soft_pass_with_warnings", "hard_block"})
    )
    for column in ["AutoStock原始校验状态", "AutoStock告警原因", "AutoStock校验状态", "AutoStock校验备注"]:
        if column not in lookup.columns:
            continue
        mapped = current["_merge_fcode"].map(
            lambda code: (
                ""
                if not code or code not in lookup.index
                else str(lookup.at[code, column] if column in lookup.columns else "")  # type: ignore[index]
            )
        )
        if column in current.columns:
            original = current[column].fillna("").astype(str)
            # A new successful comparison may clear a former warning. Preserve
            # the result as a unit instead of retaining stale non-empty text.
            current[column] = mapped.where(known_source, original)
        else:
            current[column] = mapped

    current = current.drop(columns=["_merge_fcode"], errors="ignore")
    current = _refresh_recommendation_notes(current)
    return current


def _load_decision_record_map(path: str | Path | None) -> dict[str, dict[str, Any]]:
    payload = _read_json_file(path)
    records = payload.get("candidate_records")
    if not isinstance(records, list):
        return {}

    record_map: dict[str, dict[str, Any]] = {}
    for record in records:
        if not isinstance(record, dict):
            continue
        code = _normalize_code(record.get("fcode"))
        if not code:
            continue
        if code not in record_map or record.get("final_mainlist_selected"):
            record_map[code] = record
    return record_map


def _load_soft_warning_map(path: str | Path | None) -> dict[str, dict[str, Any]]:
    payload = _read_json_file(path)
    samples = payload.get("mainlist_soft_warning_samples")
    if not isinstance(samples, list):
        return {}

    soft_warning_map: dict[str, dict[str, Any]] = {}
    for sample in samples:
        if not isinstance(sample, dict):
            continue
        code = _normalize_code(sample.get("fcode"))
        if not code:
            continue
        soft_warning_map[code] = sample
    return soft_warning_map


def _build_summary_sheet_rows(items: list[tuple[str, Any]]) -> pd.DataFrame:
    return build_summary_sheet_rows_impl(items)


def _load_report_input_context(
    settings: Settings,
    artifacts: dict[str, Any],
) -> dict[str, Any]:
    long_df = _read_tabular_file(artifacts.get("long_csv"))
    short_df = _read_tabular_file(artifacts.get("short_csv"))
    metadata_df = _read_metadata(settings.database_path)
    manager_snapshot_df = _read_manager_snapshot(settings.database_path)
    manager_metrics_df = _build_manager_metrics(metadata_df, manager_snapshot_df)
    long_term_feature_df, long_term_feature_summary = _build_long_term_feature_frame(settings.database_path)
    final_meta = _read_json_file(artifacts.get("final_excel_meta"))
    accumulation_meta = _read_json_file(artifacts.get("accumulation_tracker_excel_meta"))
    accumulation_full_df = _read_excel_sheet(artifacts.get("accumulation_tracker_excel"), "完整数据")
    decision_map = _load_decision_record_map(artifacts.get("final_screening_decision_ledger_file"))
    soft_warning_map = _load_soft_warning_map(artifacts.get("bond_mainlist_quality_audit_json_file"))
    accumulation_preview = pd.DataFrame()
    if not accumulation_full_df.empty:
        accumulation_preview = accumulation_full_df[
            [column for column in ACCUMULATION_SUMMARY_COLUMNS if column in accumulation_full_df.columns]
        ].head(settings.preview_top_n).copy()
    return {
        "long_df": long_df,
        "short_df": short_df,
        "metadata_df": metadata_df,
        "manager_snapshot_df": manager_snapshot_df,
        "manager_metrics_df": manager_metrics_df,
        "long_term_feature_df": long_term_feature_df,
        "long_term_feature_summary": long_term_feature_summary,
        "final_meta": final_meta,
        "accumulation_meta": accumulation_meta,
        "accumulation_full_df": accumulation_full_df,
        "decision_map": decision_map,
        "soft_warning_map": soft_warning_map,
        "accumulation_preview": accumulation_preview,
    }


def _build_report_candidate_universe_context(
    settings: Settings,
    artifacts: dict[str, Any],
    report_input_context: dict[str, Any],
) -> dict[str, Any]:
    metadata_df = report_input_context["metadata_df"]
    manager_metrics_df = report_input_context["manager_metrics_df"]
    long_term_feature_df = report_input_context["long_term_feature_df"]
    long_df = report_input_context["long_df"]
    short_df = report_input_context["short_df"]

    returns_df = _read_return_snapshot(settings.database_path)
    risk_df = _read_risk_snapshot(settings.database_path)
    accum_nav_df = _read_latest_accum_nav(settings.database_path)
    support_df = _build_support_frame(long_df, short_df)
    candidate_records_df = _load_candidate_record_df(artifacts.get("final_screening_decision_ledger_file"))
    candidate_universe_df = _build_project_candidate_universe(
        candidate_records_df=candidate_records_df,
        metadata_df=metadata_df,
        manager_metrics_df=manager_metrics_df,
        returns_df=returns_df,
        risk_df=risk_df,
        long_term_feature_df=long_term_feature_df,
        accum_nav_df=accum_nav_df,
        support_df=support_df,
    )
    risk_metric_cap_audit = _build_risk_metric_cap_audit(
        candidate_universe_df,
        preview_top_n=settings.preview_top_n,
    )
    return {
        "returns_df": returns_df,
        "risk_df": risk_df,
        "accum_nav_df": accum_nav_df,
        "support_df": support_df,
        "candidate_records_df": candidate_records_df,
        "candidate_universe_df": candidate_universe_df,
        "risk_metric_cap_audit": risk_metric_cap_audit,
    }


def _build_report_snapshot_context(
    settings: Settings,
    artifacts: dict[str, Any],
    report_input_context: dict[str, Any],
    report_candidate_universe_context: dict[str, Any],
) -> dict[str, Any]:
    metadata_df = report_input_context["metadata_df"]
    final_meta = report_input_context["final_meta"]
    accumulation_meta = report_input_context["accumulation_meta"]
    returns_df = report_candidate_universe_context["returns_df"]
    risk_df = report_candidate_universe_context["risk_df"]

    universe_snapshot = _build_bond_universe_snapshot(
        settings=settings,
        metadata_df=metadata_df,
        returns_df=returns_df,
        risk_df=risk_df,
    )
    code_sets = _build_bond_universe_code_sets(
        settings=settings,
        metadata_df=metadata_df,
        returns_df=returns_df,
        risk_df=risk_df,
    )
    screenable_metadata_df = metadata_df.copy()
    if not screenable_metadata_df.empty and "fcode" in screenable_metadata_df.columns:
        screenable_metadata_df["fcode"] = screenable_metadata_df.get("fcode", pd.Series(dtype=object)).map(_normalize_code)
        screenable_metadata_df = screenable_metadata_df[
            screenable_metadata_df["fcode"].isin(code_sets.get("screenable_codes") or set())
        ].copy()
    long_term_history_snapshot = _build_long_term_history_snapshot(settings.database_path, screenable_metadata_df)
    final_screening_summary = final_meta.get("final_screening_summary", {})
    accumulation_summary = accumulation_meta.get("accumulation_summary", {})
    return {
        "universe_snapshot": universe_snapshot,
        "code_sets": code_sets,
        "screenable_metadata_df": screenable_metadata_df,
        "long_term_history_snapshot": long_term_history_snapshot,
        "final_screening_summary": final_screening_summary,
        "accumulation_summary": accumulation_summary,
        "core_base_dir": Path(artifacts.get("core_output_dir") or settings.core_output_dir),
    }


def _append_project_decision_columns(
    df: pd.DataFrame,
    *,
    decision_map: dict[str, dict[str, Any]],
    soft_warning_map: dict[str, dict[str, Any]],
    evaluated_at: datetime | None = None,
) -> pd.DataFrame:
    return append_project_decision_columns_impl(
        df,
        decision_map=decision_map,
        soft_warning_map=soft_warning_map,
        evaluated_at=evaluated_at,
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


def _build_enhanced_supplemental_label(style: str, thresholds: dict[str, float]) -> str:
    return build_enhanced_supplemental_label_impl(style, thresholds)


def _apply_enhanced_supplemental_gate(df: pd.DataFrame) -> pd.DataFrame:
    return apply_enhanced_supplemental_gate_impl(df)


def _distribution_frame(df: pd.DataFrame, column: str, label: str, share_label: str) -> pd.DataFrame:
    return distribution_frame_impl(df, column, label, share_label)


def _materialize_track_frame(
    df: pd.DataFrame,
    *,
    verdict_column: str,
    watch_bucket_column: str,
    reason_column: str,
) -> pd.DataFrame:
    current = df.copy()
    if current.empty:
        return current
    verdict_series = current.get(verdict_column, pd.Series("", index=current.index, dtype=object)).fillna("").astype(str)
    base_verdict_series = current.get("项目推荐结论", pd.Series("", index=current.index, dtype=object)).fillna("").astype(str)
    current["项目推荐结论"] = verdict_series.where(verdict_series.str.strip() != "", base_verdict_series)

    watch_bucket_series = current.get(watch_bucket_column, pd.Series("", index=current.index, dtype=object)).fillna("").astype(str)
    base_watch_bucket_series = current.get(AUTO_DIAGNOSIS_CATEGORY_COLUMN, pd.Series("", index=current.index, dtype=object)).fillna("").astype(str)
    current[AUTO_DIAGNOSIS_CATEGORY_COLUMN] = watch_bucket_series.where(watch_bucket_series.str.strip() != "", base_watch_bucket_series)

    reason_series = current.get(reason_column, pd.Series("", index=current.index, dtype=object)).fillna("").astype(str)
    base_reason_series = current.get("推荐理由", pd.Series("", index=current.index, dtype=object)).fillna("").astype(str)
    current["推荐理由"] = reason_series.where(reason_series.str.strip() != "", base_reason_series)
    return current


def _build_fee_band_frame(df: pd.DataFrame) -> pd.DataFrame:
    return build_fee_band_frame_impl(df)


def _formal_group_from_row(row: dict[str, Any]) -> str:
    return formal_group_from_row_impl(row)


def _append_formal_group_column(df: pd.DataFrame) -> pd.DataFrame:
    return append_formal_group_column_impl(df)


def _build_formal_group_distribution(df: pd.DataFrame) -> pd.DataFrame:
    return build_formal_group_distribution_impl(df)


def _build_watch_summary_frame(df: pd.DataFrame) -> pd.DataFrame:
    return build_watch_summary_frame_impl(df)


def _split_reason_codes(value: Any) -> list[str]:
    return split_reason_codes_impl(value)


def _normalize_stage_b_reason_code(reason_code: str) -> str:
    return normalize_stage_b_reason_code_impl(reason_code)


def _pick_stage_b_primary_reason(reason_codes: list[str]) -> str:
    return pick_stage_b_primary_reason_impl(reason_codes)


def _pick_stage_b_secondary_reasons(reason_codes: list[str], primary_reason: str) -> list[str]:
    return pick_stage_b_secondary_reasons_impl(reason_codes, primary_reason)


def _build_stage_b_reason_ledger(
    *,
    candidate_df: pd.DataFrame,
    excluded_df: pd.DataFrame,
    official_codes: set[str],
) -> tuple[pd.DataFrame, dict[str, Any]]:
    return build_stage_b_reason_ledger_impl(
        candidate_df=candidate_df,
        excluded_df=excluded_df,
        official_codes=official_codes,
        normalize_code=_normalize_code,
    )


def _stage_b_threshold_count(summary: dict[str, Any], threshold: float) -> int:
    for item in summary.get("high_yield_thresholds") or []:
        try:
            current_threshold = float(item.get("threshold"))
        except (TypeError, ValueError):
            continue
        if current_threshold == float(threshold):
            return int(item.get("count") or 0)
    return 0


def _build_quality_gate_summary(final_screening_summary: dict[str, Any] | None) -> dict[str, Any]:
    current = final_screening_summary or {}
    headline_highlights = [
        str(item or "").replace("当前主榜共", "内部研究主榜共", 1)
        for item in (current.get("headline_highlights") or [])
    ]
    narrative_summary = dict(current.get("narrative_summary") or {})
    main_list_text = str(narrative_summary.get("main_list") or "")
    if main_list_text:
        narrative_summary["main_list"] = main_list_text.replace("当前主榜共", "内部研究主榜共", 1)
    return {
        "candidate_pool_reduction": current.get("candidate_pool_reduction") or {},
        "autostock_cross_validation_reason_counts": current.get("autostock_cross_validation_reason_counts") or {},
        "autostock_cross_validation_warning_counts": current.get("autostock_cross_validation_warning_counts") or {},
        "drawdown_duration_distribution": current.get("drawdown_duration_distribution") or {},
        "headline_highlights": headline_highlights,
        "narrative_summary": narrative_summary,
    }


def _normalized_code_series(df: pd.DataFrame) -> pd.Series:
    if not isinstance(df, pd.DataFrame) or df.empty:
        return pd.Series(dtype=object)
    code_col = _pick_column(df, CODE_CANDIDATES)
    if not code_col:
        return pd.Series([""] * len(df.index), index=df.index, dtype=object)
    return df[code_col].map(_normalize_code)


def _unique_reason_codes(value: Any) -> list[str]:
    seen: set[str] = set()
    normalized: list[str] = []
    for item in _split_reason_codes(value):
        code = str(item or "").strip()
        if not code or code in seen:
            continue
        seen.add(code)
        normalized.append(code)
    return normalized


def _format_threshold_range(
    values: list[float],
    *,
    digits: int = 2,
    suffix: str = "",
) -> str:
    normalized = sorted({round(float(value), 10) for value in values if value is not None})
    if not normalized:
        return ""
    if len(normalized) == 1:
        return f"{normalized[0]:.{digits}f}{suffix}"
    return f"{normalized[0]:.{digits}f}{suffix}~{normalized[-1]:.{digits}f}{suffix}"


def _build_publish_threshold_context(
    *,
    candidate_df: pd.DataFrame,
    style_meta: dict[str, dict[str, Any]],
    global_thresholds: dict[str, float],
) -> dict[str, Any]:
    present_styles = sorted(
        {
            str(item or "").strip() or "未知"
            for item in candidate_df.get("债基风格标签", pd.Series(dtype=object)).tolist()
        }
    )
    if not present_styles:
        present_styles = ["未知"]

    fixed_floors = [_combined_quality_floor(style) for style in present_styles]
    dynamic_thresholds = [
        style_meta.get(style) or global_thresholds
        for style in present_styles
    ]

    def _collect(items: list[dict[str, Any]], key: str) -> list[float]:
        values: list[float] = []
        for item in items:
            try:
                values.append(float(item.get(key)))
            except (TypeError, ValueError, AttributeError):
                continue
        return values

    return {
        "fixed": {
            "weighted_score_min": _format_threshold_range([PROJECT_ABSOLUTE_QUALITY_THRESHOLDS["weighted_score_min"]], digits=2),
            "short_score_min": _format_threshold_range([PROJECT_ABSOLUTE_QUALITY_THRESHOLDS["short_score_min"]], digits=2),
            "long_score_min": _format_threshold_range([PROJECT_ABSOLUTE_QUALITY_THRESHOLDS["long_score_min"]], digits=2),
            "annual_return_min": _format_threshold_range(_collect(fixed_floors, "annual_return_min"), digits=2, suffix="%"),
            "sharpe_min": _format_threshold_range(_collect(fixed_floors, "sharpe_min"), digits=2),
            "sortino_min": _format_threshold_range(_collect(fixed_floors, "sortino_min"), digits=2),
            "calmar_min": _format_threshold_range(_collect(fixed_floors, "calmar_min"), digits=2),
            "positive_ratio_min": _format_threshold_range(_collect(fixed_floors, "positive_ratio_min"), digits=2, suffix="%"),
            "fee_total_max": _format_threshold_range(_collect(fixed_floors, "fee_total_max"), digits=2, suffix="%"),
            "max_drawdown_max": _format_threshold_range(_collect(fixed_floors, "max_drawdown_max"), digits=4),
            "volatility_max": _format_threshold_range(_collect(fixed_floors, "volatility_max"), digits=4),
            "scale_billion_min": _format_threshold_range([PROJECT_ABSOLUTE_QUALITY_THRESHOLDS["scale_billion_min"]], digits=1, suffix="亿"),
        },
        "dynamic": {
            "weighted_score_min": _format_threshold_range(_collect(dynamic_thresholds, "weighted_score_min"), digits=2),
            "short_score_min": _format_threshold_range(_collect(dynamic_thresholds, "short_score_min"), digits=2),
            "long_score_min": _format_threshold_range(_collect(dynamic_thresholds, "long_score_min"), digits=2),
            "annual_return_min": _format_threshold_range(_collect(dynamic_thresholds, "annual_return_min"), digits=2, suffix="%"),
            "sharpe_min": _format_threshold_range(_collect(dynamic_thresholds, "sharpe_min"), digits=2),
            "sortino_min": _format_threshold_range(_collect(dynamic_thresholds, "sortino_min"), digits=2),
            "calmar_min": _format_threshold_range(_collect(dynamic_thresholds, "calmar_min"), digits=2),
            "positive_ratio_min": _format_threshold_range(_collect(dynamic_thresholds, "positive_ratio_min"), digits=2, suffix="%"),
            "fee_total_max": _format_threshold_range(_collect(dynamic_thresholds, "fee_total_max"), digits=2, suffix="%"),
            "max_drawdown_max": _format_threshold_range(_collect(dynamic_thresholds, "max_drawdown_max"), digits=4),
            "volatility_max": _format_threshold_range(_collect(dynamic_thresholds, "volatility_max"), digits=4),
            "scale_billion_min": _format_threshold_range(_collect(dynamic_thresholds, "scale_billion_min"), digits=1, suffix="亿"),
        },
        "post_review": {
            "manager_min_total_days": f"{int(PROJECT_MANAGER_STABILITY_THRESHOLDS['min_total_days'])}天",
            "manager_avg_score_min": f"{float(PROJECT_MANAGER_STABILITY_THRESHOLDS['avg_score_overall_min']):.1f}",
            "scale_report_age_max_days": f"{int(PROJECT_SCALE_DATA_THRESHOLDS['max_report_age_days'])}天",
            "nav_freshness_days": f"{int(PROJECT_DYNAMIC_NAV_FRESHNESS_DAYS)}天",
        },
    }


def _reason_threshold_note(reason_code: str, stage_key: str, threshold_context: dict[str, Any]) -> str:
    fixed = threshold_context.get("fixed") or {}
    dynamic = threshold_context.get("dynamic") or {}
    post_review = threshold_context.get("post_review") or {}

    fixed_map = {
        "below_weighted_floor": f"门槛 综合加权得分≥{fixed.get('weighted_score_min')}",
        "below_short_floor": f"门槛 近期得分≥{fixed.get('short_score_min')}",
        "below_long_floor": f"门槛 长期得分≥{fixed.get('long_score_min')}",
        "below_annual_floor": f"门槛 1年年化收益≥{fixed.get('annual_return_min')}",
        "below_sharpe_floor": f"门槛 1年夏普比率≥{fixed.get('sharpe_min')}",
        "below_sortino_floor": f"门槛 1年索提诺比率≥{fixed.get('sortino_min')}",
        "below_calmar_floor": f"门槛 1年卡玛比率≥{fixed.get('calmar_min')}",
        "below_positive_ratio_floor": f"门槛 1年正收益比率≥{fixed.get('positive_ratio_min')}",
        "fee_too_high": f"门槛 费率合计≤{fixed.get('fee_total_max')}",
        "mdd_too_high": f"门槛 1年最大回撤≤{fixed.get('max_drawdown_max')}",
        "volatility_too_high": f"门槛 1年波动率≤{fixed.get('volatility_max')}",
        "scale_too_small": f"门槛 基金规模≥{fixed.get('scale_billion_min')}",
    }
    dynamic_map = {
        "below_weighted_floor": f"门槛 综合加权得分≥{dynamic.get('weighted_score_min')}（按风格动态阈值）",
        "below_short_floor": f"门槛 近期得分≥{dynamic.get('short_score_min')}（按风格动态阈值）",
        "below_long_floor": f"门槛 长期得分≥{dynamic.get('long_score_min')}（按风格动态阈值）",
        "below_annual_floor": f"门槛 1年年化收益≥{dynamic.get('annual_return_min')}（按风格动态阈值）",
        "below_sharpe_floor": f"门槛 1年夏普比率≥{dynamic.get('sharpe_min')}（按风格动态阈值）",
        "below_sortino_floor": f"门槛 1年索提诺比率≥{dynamic.get('sortino_min')}（按风格动态阈值）",
        "below_calmar_floor": f"门槛 1年卡玛比率≥{dynamic.get('calmar_min')}（按风格动态阈值）",
        "below_positive_ratio_floor": f"门槛 1年正收益比率≥{dynamic.get('positive_ratio_min')}（按风格动态阈值）",
        "fee_too_high": f"门槛 费率合计≤{dynamic.get('fee_total_max')}（按风格动态阈值）",
        "mdd_too_high": f"门槛 1年最大回撤≤{dynamic.get('max_drawdown_max')}（按风格动态阈值）",
        "volatility_too_high": f"门槛 1年波动率≤{dynamic.get('volatility_max')}（按风格动态阈值）",
        "scale_too_small": f"门槛 基金规模≥{dynamic.get('scale_billion_min')}（按风格动态阈值）",
    }
    base_map = {
        "not_buyable": "门槛 当前必须可申购",
        "buyability_unknown": "门槛 需有明确的可申购标志",
        "stale_nav": f"门槛 最新净值需在{post_review.get('nav_freshness_days')}内",
        "missing_key_metrics": "门槛 需具备1年核心收益风险指标",
        "hard_block": "门槛 AutoStock 硬拦截不得通过",
        "soft_warning": "AutoStock 软告警保留标签并进入后续质量收敛",
    }
    post_review_map = {
        "manager_tenure_short": f"门槛 经理最短从业天数≥{post_review.get('manager_min_total_days')}",
        "manager_score_low": f"门槛 经理平均综合评分≥{post_review.get('manager_avg_score_min')}",
        "scale_report_stale": f"门槛 规模数据账龄≤{post_review.get('scale_report_age_max_days')}",
        "low_publish_confidence": "门槛 质量边际不得为“边界观察”",
        "score_insufficient": "门槛 需进入最终精选名单",
    }

    if stage_key == "base_gate":
        return base_map.get(reason_code, "")
    if stage_key in {"fixed_baseline", "fixed_post_review"} and reason_code in fixed_map:
        return fixed_map[reason_code]
    if stage_key in {"dynamic_baseline", "dynamic_post_review"} and reason_code in dynamic_map:
        return dynamic_map[reason_code]
    return post_review_map.get(reason_code, "")


def _summarize_reason_counts(
    *,
    reason_code_values: list[Any],
    reason_text_values: list[Any] | None = None,
    stage_key: str,
    threshold_context: dict[str, Any],
    top_n: int = 3,
) -> list[dict[str, Any]]:
    counter: Counter[tuple[str, str, str]] = Counter()
    text_values = reason_text_values or []
    total = max(len(reason_code_values), len(text_values))
    for index in range(total):
        codes = _unique_reason_codes(reason_code_values[index] if index < len(reason_code_values) else "")
        if codes:
            for code in codes:
                label = PROJECT_RECOMMENDATION_REASONS.get(code) or code
                threshold_note = _reason_threshold_note(code, stage_key, threshold_context)
                display_reason = f"{label}（{threshold_note}）" if threshold_note else label
                counter[(code, label, display_reason)] += 1
            continue
        text = str(text_values[index] if index < len(text_values) else "").strip()
        if text:
            counter[("", text, text)] += 1
    return [
        {"code": code, "reason": reason, "display_reason": display_reason, "count": int(count)}
        for (code, reason, display_reason), count in counter.most_common(top_n)
    ]


def _build_stage_audit_row(
    *,
    stage_key: str,
    stage_label: str,
    scope: str,
    input_count: int,
    passed_count: int,
    removed_reason_codes: list[Any],
    removed_reason_texts: list[Any] | None = None,
    threshold_context: dict[str, Any],
    note: str = "",
) -> dict[str, Any]:
    removed_count = max(int(input_count) - int(passed_count), 0)
    return {
        "stage_key": stage_key,
        "stage_label": stage_label,
        "scope": scope,
        "input_count": int(input_count),
        "passed_count": int(passed_count),
        "removed_count": int(removed_count),
        "removed_rate_pct": round((removed_count / input_count) * 100, 2) if input_count else 0.0,
        "top_reasons": _summarize_reason_counts(
            reason_code_values=removed_reason_codes,
            reason_text_values=removed_reason_texts,
            stage_key=stage_key,
            threshold_context=threshold_context,
        ),
        "note": note,
    }


def _build_publish_stage_audit_summary(
    *,
    candidate_df: pd.DataFrame,
    style_meta: dict[str, dict[str, Any]],
    global_thresholds: dict[str, float],
    fixed_threshold_track_df: pd.DataFrame,
    fixed_publish_track_df: pd.DataFrame,
    dynamic_threshold_track_df: pd.DataFrame,
    dynamic_publish_track_df: pd.DataFrame,
    official_df: pd.DataFrame,
    removed_after_final_selection_df: pd.DataFrame,
) -> dict[str, Any]:
    candidate_codes = _normalized_code_series(candidate_df)
    threshold_context = _build_publish_threshold_context(
        candidate_df=candidate_df,
        style_meta=style_meta,
        global_thresholds=global_thresholds,
    )
    base_gate_pass_mask = candidate_df.get("基础门禁通过", pd.Series(dtype=bool)).fillna(False).astype(bool)
    base_gate_pass_df = candidate_df[base_gate_pass_mask].copy()
    base_gate_fail_df = candidate_df[~base_gate_pass_mask].copy()
    base_gate_pass_codes = set(_normalized_code_series(base_gate_pass_df).tolist())

    fixed_baseline_pass_mask = fixed_threshold_track_df.get("项目推荐结论", pd.Series(dtype=object)).astype(str).eq("正式推荐")
    fixed_baseline_fail_df = fixed_threshold_track_df[
        _normalized_code_series(fixed_threshold_track_df).isin(base_gate_pass_codes)
        & ~fixed_baseline_pass_mask
    ].copy()
    fixed_baseline_pass_df = fixed_threshold_track_df[fixed_baseline_pass_mask].copy()
    fixed_baseline_pass_codes = set(_normalized_code_series(fixed_baseline_pass_df).tolist())

    fixed_post_review_pass_mask = fixed_publish_track_df.get("项目推荐结论", pd.Series(dtype=object)).astype(str).eq("正式推荐")
    fixed_post_review_fail_df = fixed_publish_track_df[
        _normalized_code_series(fixed_publish_track_df).isin(fixed_baseline_pass_codes)
        & ~fixed_post_review_pass_mask
    ].copy()
    fixed_post_review_pass_df = fixed_publish_track_df[fixed_post_review_pass_mask].copy()

    dynamic_baseline_pass_mask = dynamic_threshold_track_df.get("项目推荐结论", pd.Series(dtype=object)).astype(str).eq("正式推荐")
    dynamic_baseline_fail_df = dynamic_threshold_track_df[
        _normalized_code_series(dynamic_threshold_track_df).isin(base_gate_pass_codes)
        & ~dynamic_baseline_pass_mask
    ].copy()
    dynamic_baseline_pass_df = dynamic_threshold_track_df[dynamic_baseline_pass_mask].copy()
    dynamic_baseline_pass_codes = set(_normalized_code_series(dynamic_baseline_pass_df).tolist())

    dynamic_post_review_pass_mask = dynamic_publish_track_df.get("项目推荐结论", pd.Series(dtype=object)).astype(str).eq("正式推荐")
    dynamic_post_review_fail_df = dynamic_publish_track_df[
        _normalized_code_series(dynamic_publish_track_df).isin(dynamic_baseline_pass_codes)
        & ~dynamic_post_review_pass_mask
    ].copy()
    dynamic_post_review_pass_df = dynamic_publish_track_df[dynamic_post_review_pass_mask].copy()

    final_selection_removed_df = removed_after_final_selection_df.copy()

    stage_rows = [
        _build_stage_audit_row(
            stage_key="base_gate",
            stage_label="基础门禁",
            scope="主链共享",
            input_count=len(candidate_df.index),
            passed_count=len(base_gate_pass_df.index),
            removed_reason_codes=base_gate_fail_df.get("基础推荐原因代码", pd.Series(dtype=object)).tolist(),
            removed_reason_texts=base_gate_fail_df.get("基础推荐理由", pd.Series(dtype=object)).tolist(),
            threshold_context=threshold_context,
            note="可申购、净值/风险就绪和 AutoStock 初筛的共享入口门禁。",
        ),
        _build_stage_audit_row(
            stage_key="fixed_baseline",
            stage_label="固定阈值基准",
            scope="固定阈值审计支线",
            input_count=len(base_gate_pass_df.index),
            passed_count=len(fixed_baseline_pass_df.index),
            removed_reason_codes=fixed_baseline_fail_df.get("fixed_threshold_reason_codes", pd.Series(dtype=object)).tolist(),
            removed_reason_texts=fixed_baseline_fail_df.get("fixed_threshold_reason", pd.Series(dtype=object)).tolist(),
            threshold_context=threshold_context,
            note="固定绝对质量门槛审计口径，用于与动态阈值参考榜做差异对比。",
        ),
        _build_stage_audit_row(
            stage_key="fixed_post_review",
            stage_label="固定阈值自动发布门禁",
            scope="固定阈值审计支线",
            input_count=len(fixed_baseline_pass_df.index),
            passed_count=len(fixed_post_review_pass_df.index),
            removed_reason_codes=fixed_post_review_fail_df.get("项目推荐原因代码", pd.Series(dtype=object)).tolist(),
            removed_reason_texts=fixed_post_review_fail_df.get("推荐理由", pd.Series(dtype=object)).tolist(),
            threshold_context=threshold_context,
            note="固定阈值基准通过后，继续执行经理从业与评分、规模账龄和质量边际自动门禁。",
        ),
        _build_stage_audit_row(
            stage_key="dynamic_baseline",
            stage_label="动态阈值基准",
            scope="正式发布主链",
            input_count=len(base_gate_pass_df.index),
            passed_count=len(dynamic_baseline_pass_df.index),
            removed_reason_codes=dynamic_baseline_fail_df.get("dynamic_threshold_reason_codes", pd.Series(dtype=object)).tolist(),
            removed_reason_texts=dynamic_baseline_fail_df.get("dynamic_threshold_reason", pd.Series(dtype=object)).tolist(),
            threshold_context=threshold_context,
            note="按债基风格动态阈值与增强补充准入形成正式发布参考主链。",
        ),
        _build_stage_audit_row(
            stage_key="dynamic_post_review",
            stage_label="自动发布门禁",
            scope="正式发布主链",
            input_count=len(dynamic_baseline_pass_df.index),
            passed_count=len(dynamic_post_review_pass_df.index),
            removed_reason_codes=dynamic_post_review_fail_df.get("项目推荐原因代码", pd.Series(dtype=object)).tolist(),
            removed_reason_texts=dynamic_post_review_fail_df.get("推荐理由", pd.Series(dtype=object)).tolist(),
            threshold_context=threshold_context,
            note="动态阈值基准通过后，继续执行经理从业与评分、规模账龄和质量边际自动门禁。",
        ),
        _build_stage_audit_row(
            stage_key="final_selection",
            stage_label="最终精选",
            scope="正式发布主链",
            input_count=len(dynamic_post_review_pass_df.index),
            passed_count=len(official_df.index),
            removed_reason_codes=final_selection_removed_df.get("项目推荐原因代码", pd.Series(dtype=object)).tolist(),
            removed_reason_texts=final_selection_removed_df.get("推荐理由", pd.Series(dtype=object)).tolist(),
            threshold_context=threshold_context,
            note="最终展示给用户的正式发布主榜收敛层。",
        ),
    ]

    main_chain_stage_keys = ["base_gate", "dynamic_baseline", "dynamic_post_review", "final_selection"]
    audit_stage_keys = ["fixed_baseline", "fixed_post_review"]
    main_chain_rows = [item for item in stage_rows if item.get("stage_key") in main_chain_stage_keys]
    audit_stage_rows = [item for item in stage_rows if item.get("stage_key") in audit_stage_keys]

    dynamic_baseline = next((item for item in stage_rows if item["stage_key"] == "dynamic_baseline"), {})
    dynamic_post_review = next((item for item in stage_rows if item["stage_key"] == "dynamic_post_review"), {})
    final_selection = next((item for item in stage_rows if item["stage_key"] == "final_selection"), {})
    base_gate = next((item for item in stage_rows if item["stage_key"] == "base_gate"), {})

    def _first_reason(stage: dict[str, Any]) -> str:
        top_reasons = stage.get("top_reasons") or []
        if not top_reasons:
            return "暂无"
        return f"{top_reasons[0].get('display_reason') or top_reasons[0].get('reason')}（{int(top_reasons[0].get('count') or 0)}只）"

    headline_highlights = [
        f"项目治理候选池 {len(candidate_codes.index)} 只，经基础门禁后保留 {base_gate.get('passed_count', 0)} 只。",
        f"正式发布主链中，动态阈值基准保留 {dynamic_baseline.get('passed_count', 0)} 只，自动发布门禁保留 {dynamic_post_review.get('passed_count', 0)} 只，最终正式发布主榜 {final_selection.get('passed_count', 0)} 只。",
        f"基础门禁首要淘汰原因：{_first_reason(base_gate)}；动态阈值基准首要淘汰原因：{_first_reason(dynamic_baseline)}。",
        f"自动发布门禁首要淘汰原因：{_first_reason(dynamic_post_review)}；最终精选首要淘汰原因：{_first_reason(final_selection)}。",
    ]

    return {
        "stage_rows": stage_rows,
        "main_chain_stage_keys": main_chain_stage_keys,
        "audit_stage_keys": audit_stage_keys,
        "main_chain_rows": main_chain_rows,
        "audit_stage_rows": audit_stage_rows,
        "headline_highlights": headline_highlights,
        "narrative_summary": {
            "overview": headline_highlights[0],
            "main_chain": headline_highlights[1],
            "baseline_reasons": headline_highlights[2],
            "final_reasons": headline_highlights[3],
        },
    }


def _build_publish_reason_summary(publish_stage_audit_summary: dict[str, Any]) -> dict[str, Any]:
    stage_rows = publish_stage_audit_summary.get("stage_rows") or []
    if not stage_rows:
        return {
            "available": False,
            "headline_highlights": [],
            "main_chain": [],
            "audit_branch": [],
            "primary_bottleneck": None,
            "final_selected_count": 0,
        }

    stage_map = {
        str(item.get("stage_key") or "").strip(): item
        for item in stage_rows
        if str(item.get("stage_key") or "").strip()
    }

    def _build_reason_payload(reason: dict[str, Any]) -> dict[str, Any]:
        return {
            "code": str(reason.get("code") or "").strip() or None,
            "reason": str(reason.get("reason") or "").strip() or None,
            "display_reason": str(reason.get("display_reason") or "").strip() or None,
            "count": int(reason.get("count") or 0),
        }

    def _build_stage_payload(stage: dict[str, Any]) -> dict[str, Any]:
        top_reasons = stage.get("top_reasons") or []
        lead_reason = _build_reason_payload(top_reasons[0]) if top_reasons else None
        return {
            "stage_key": str(stage.get("stage_key") or "").strip() or None,
            "stage_label": str(stage.get("stage_label") or "").strip() or None,
            "scope": str(stage.get("scope") or "").strip() or None,
            "input_count": int(stage.get("input_count") or 0),
            "passed_count": int(stage.get("passed_count") or 0),
            "removed_count": int(stage.get("removed_count") or 0),
            "removed_rate_pct": float(stage.get("removed_rate_pct") or 0.0),
            "lead_reason": lead_reason,
            "top_reasons": [_build_reason_payload(item) for item in top_reasons[:3]],
            "note": str(stage.get("note") or "").strip() or None,
            "explanation": (
                f"{str(stage.get('stage_label') or '').strip()}："
                f"输入 {int(stage.get('input_count') or 0)} 只，保留 {int(stage.get('passed_count') or 0)} 只，"
                f"淘汰 {int(stage.get('removed_count') or 0)} 只（{float(stage.get('removed_rate_pct') or 0.0):.2f}%）；"
                f"首要原因为 {lead_reason.get('display_reason') or lead_reason.get('reason')} "
                f"（{lead_reason.get('count')} 只）。"
                if lead_reason
                else (
                    f"{str(stage.get('stage_label') or '').strip()}："
                    f"输入 {int(stage.get('input_count') or 0)} 只，保留 {int(stage.get('passed_count') or 0)} 只，"
                    f"淘汰 {int(stage.get('removed_count') or 0)} 只。"
                )
            ),
        }

    main_chain_rows = [
        stage_map[key]
        for key in publish_stage_audit_summary.get("main_chain_stage_keys") or []
        if key in stage_map
    ]
    audit_branch_rows = [
        stage_map[key]
        for key in publish_stage_audit_summary.get("audit_stage_keys") or []
        if key in stage_map
    ]

    bottleneck_stage = max(
        main_chain_rows,
        key=lambda item: (int(item.get("removed_count") or 0), float(item.get("removed_rate_pct") or 0.0)),
        default=None,
    )

    final_stage = stage_map.get("final_selection") or {}
    bottleneck_payload = _build_stage_payload(bottleneck_stage) if bottleneck_stage else None
    if bottleneck_payload is not None:
        bottleneck_payload["bottleneck_reason"] = bottleneck_payload.get("lead_reason")

    return {
        "available": True,
        "headline_highlights": publish_stage_audit_summary.get("headline_highlights") or [],
        "narrative_summary": publish_stage_audit_summary.get("narrative_summary") or {},
        "main_chain": [_build_stage_payload(item) for item in main_chain_rows],
        "audit_branch": [_build_stage_payload(item) for item in audit_branch_rows],
        "primary_bottleneck": bottleneck_payload,
        "final_selected_count": int(final_stage.get("passed_count") or 0),
        "final_stage": _build_stage_payload(final_stage) if final_stage else None,
    }


def _build_publish_stage_audit_frame(summary: dict[str, Any]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for item in summary.get("stage_rows") or []:
        top_reasons = item.get("top_reasons") or []
        rows.append(
            {
                "阶段": item.get("stage_label"),
                "范围": item.get("scope"),
                "输入样本数": int(item.get("input_count") or 0),
                "通过数": int(item.get("passed_count") or 0),
                "淘汰数": int(item.get("removed_count") or 0),
                "淘汰率(%)": float(item.get("removed_rate_pct") or 0.0),
                "主原因1": (
                    f"{top_reasons[0].get('display_reason') or top_reasons[0].get('reason')}（{int(top_reasons[0].get('count') or 0)}只）"
                    if len(top_reasons) >= 1 else ""
                ),
                "主原因2": (
                    f"{top_reasons[1].get('display_reason') or top_reasons[1].get('reason')}（{int(top_reasons[1].get('count') or 0)}只）"
                    if len(top_reasons) >= 2 else ""
                ),
                "主原因3": (
                    f"{top_reasons[2].get('display_reason') or top_reasons[2].get('reason')}（{int(top_reasons[2].get('count') or 0)}只）"
                    if len(top_reasons) >= 3 else ""
                ),
                "说明": item.get("note") or "",
            }
        )
    return pd.DataFrame(rows)


def _build_dynamic_threshold_detail_frame(
    *,
    style_meta: dict[str, dict[str, Any]],
    global_thresholds: dict[str, float],
) -> pd.DataFrame:
    return build_dynamic_threshold_detail_frame_impl(
        style_meta=style_meta,
        global_thresholds=global_thresholds,
    )


def _confidence_from_thresholds(
    row: dict[str, Any],
    *,
    thresholds: dict[str, float],
) -> tuple[str, float, str]:
    normalized_thresholds = dict(thresholds)
    normalized_thresholds.setdefault(
        "scale_billion_min",
        float(PROJECT_ABSOLUTE_QUALITY_THRESHOLDS["scale_billion_min"]),
    )
    return confidence_from_thresholds_impl(row, thresholds=normalized_thresholds)


def _confidence_from_enhanced_metrics(
    row: dict[str, Any],
    *,
    thresholds: dict[str, float],
) -> tuple[str, float, str]:
    return confidence_from_enhanced_metrics_impl(row, thresholds=thresholds)


def _safe_datetime(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        timestamp = pd.to_datetime(value, errors="coerce")
    except Exception:
        return None
    if pd.isna(timestamp):
        return None
    if hasattr(timestamp, "to_pydatetime"):
        return timestamp.to_pydatetime()
    return None


def _project_confidence_label(
    row: dict[str, Any],
    *,
    occurrences: int = 1,
    current_active: bool = True,
) -> str:
    return project_confidence_label_impl(
        row,
        occurrences=occurrences,
        current_active=current_active,
    )


def _read_manager_snapshot(db_path: Path) -> pd.DataFrame:
    if not db_path.exists():
        return pd.DataFrame(columns=["mgr_id", "total_days", "score_overall", "last_updated_at"])
    conn = sqlite3.connect(db_path.resolve().as_uri() + "?mode=ro", uri=True)
    try:
        columns = {row[1] for row in conn.execute("PRAGMA table_info(fund_managers)")}
        updated = "last_updated_at" if "last_updated_at" in columns else "NULL AS last_updated_at"
        query = f"""
        SELECT
            mgr_id,
            total_days,
            score_overall,
            {updated}
        FROM fund_managers
        ORDER BY mgr_id
        """
        df = pd.read_sql_query(query, conn)
    finally:
        conn.close()
    return df


def _build_manager_metrics(metadata_df: pd.DataFrame, manager_df: pd.DataFrame) -> pd.DataFrame:
    from backend.manager_metrics import build_manager_metrics

    return build_manager_metrics(metadata_df, manager_df)


PROJECT_TRACKER_EXPORT_TO_INTERNAL_RENAME = {
    "基金类型": "最新基金类型",
    "风险等级": "最新风险等级",
    "成立日期": "最新成立日期",
    "基金规模(亿)": "最新基金规模(亿)",
    "基金经理": "最新基金经理",
    "项目推荐结论": "最新项目推荐结论",
    "推荐理由": "最新推荐理由",
    "推荐备注": "最新推荐备注",
    "持有期状态": "最新持有期状态",
    "优选类别": "最新优选类别",
    "是否可申购": "最新是否可申购",
    "申购状态": "最新申购状态",
    "基准归类": "最新基准归类",
    "经理从业与评分状态": "最新经理从业与评分状态",
    "经理最短从业天数": "最新经理最短从业天数",
    "经理平均综合评分": "最新经理平均综合评分",
    "规模数据状态": "最新规模数据状态",
    "规模数据账龄(天)": "最新规模数据账龄(天)",
    "是否含权益/转债增强": "最新是否含权益/转债增强",
    "费率合计(%)": "最新费率合计(%)",
    "近1周(%)": "最新近1周(%)",
    "近1月(%)": "最新近1月(%)",
    "近3月(%)": "最新近3月(%)",
    "近6月(%)": "最新近6月(%)",
    "近1年(%)": "最新近1年(%)",
    "近2年(%)": "最新近2年(%)",
    "近3年(%)": "最新近3年(%)",
    "今年来(%)": "最新今年来(%)",
    "成立来(%)": "最新成立来(%)",
    "1年-波动率": "最新波动率",
    "1年-最大回撤(%)": "最新最大回撤(%)",
    "1年-年化收益(%)": "最新年化收益(%)",
    "1年-夏普比率": "最新夏普比率",
    "1年-索提诺比率": "最新索提诺比率",
    "1年-卡玛比率": "最新卡玛比率",
    "1年-VaR95(%)": "最新VaR95(%)",
    "1年-VaR99(%)": "最新VaR99(%)",
    "1年-正收益比率": "最新正收益比率",
    "最新日增长率(%)": "最新日增长率(%)",
    "最新净值日期": "最新净值日期",
    "最新单位净值": "最新单位净值",
    "最新累计净值": "最新累计净值",
    "1年-最长回撤观测数": "最新1年-最长回撤观测数",
    "综合加权得分": "最新综合加权得分",
    "近期得分": "最新近期得分",
    "长期得分": "最新长期得分",
    "AutoStock校验状态": "最新AutoStock校验状态",
    "AutoStock校验备注": "最新AutoStock校验备注",
    "AutoStock原始校验状态": "最新AutoStock原始校验状态",
    "AutoStock告警原因": "最新AutoStock告警原因",
    "长期稳定性标签": "最新长期稳定性标签",
    "母基金名称": "最新母基金名称",
    "关联份额数": "最新关联份额数",
    "关联份额名称": "最新关联份额名称",
    "关联份额代码": "最新关联份额代码",
}




def _priority_pool_rank_score(series: pd.Series, *, ascending: bool) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    result = pd.Series(0.5, index=series.index, dtype=float)
    valid_mask = numeric.notna()
    valid_count = int(valid_mask.sum())
    if valid_count <= 0:
        return result
    if valid_count == 1:
        result.loc[valid_mask] = 1.0
        return result
    ranks = numeric.loc[valid_mask].rank(method="average", ascending=ascending)
    result.loc[valid_mask] = 1.0 - ((ranks - 1.0) / float(valid_count - 1))
    return result.clip(lower=0.0, upper=1.0)


def _priority_pool_share_key(row: pd.Series | dict[str, Any]) -> str:
    if isinstance(row, pd.Series):
        row = row.to_dict()
    company = str(row.get("基金公司") or "").strip()
    mother_name = str(row.get("母基金名称") or "").strip()
    fund_name = str(row.get("基金名称") or "").strip()
    base_name = mother_name or fund_name
    matched = PRIORITY_POOL_SHARE_CLASS_SUFFIX_PATTERN.match(base_name)
    if matched:
        candidate_base = str(matched.group("base") or "").strip()
        if candidate_base:
            base_name = candidate_base
    return f"{company}::{base_name}" if company or base_name else ""


def _priority_pool_recent_penalty(row: pd.Series | dict[str, Any]) -> float:
    if isinstance(row, pd.Series):
        row = row.to_dict()
    penalty = 0.0
    near_1m = _safe_float(row.get("近1月(%)"))
    near_3m = _safe_float(row.get("近3月(%)"))
    recent_score = _safe_float(row.get("近期得分"))
    if near_1m is not None and near_1m < 0:
        penalty += 4.0
    if near_3m is not None and near_3m < 0:
        penalty += 4.0
    if recent_score is not None and recent_score < 60:
        penalty += 2.0
    return penalty


def _priority_pool_reason(row: pd.Series | dict[str, Any]) -> str:
    if isinstance(row, pd.Series):
        row = row.to_dict()
    reasons: list[str] = []
    bucket_rank = int(_safe_float(row.get("桶内排名")) or 0)
    bucket_count = int(_safe_float(row.get("桶内样本数")) or 0)
    tracked_periods = int(_safe_float(row.get("跟踪总期数")) or 0)
    occurrences = int(_safe_float(row.get("出现次数")) or 0)
    occurrence_freq = _safe_float(row.get("出现频率(%)")) or 0.0
    confidence = str(row.get("项目质量信心") or "").strip()
    stability = str(row.get("长期稳定性标签") or "").strip()
    holding_period_status = str(row.get("持有期状态") or "").strip()
    autostock_raw = _normalize_autostock_status(validation_status_value(row))
    fee_rank = _safe_float(row.get("费率分位得分")) or 0.0

    if bucket_rank > 0 and bucket_count > 0:
        top_cut = max(1, int(math.ceil(bucket_count * 0.15)))
        if bucket_rank <= top_cut:
            reasons.append("同分组同风格相对排名靠前")
    if tracked_periods > 1 and occurrences >= 2:
        reasons.append(f"累计入围 {occurrences}/{tracked_periods} 期")
    elif occurrence_freq >= 80:
        reasons.append(f"累计出现频率 {occurrence_freq:.0f}%")
    if confidence == "高信心":
        reasons.append("项目质量信心高")
    elif confidence == "中信心":
        reasons.append("项目质量信心稳定")
    if stability in {"长期稳定性强", "长期稳定性中等"}:
        reasons.append(stability)
    if holding_period_status == "已确认<=90天":
        reasons.append("持有期已确认<=90天")
    if autostock_raw == "strict_match":
        reasons.append("数据校验严格匹配" if "数据校验来源" in row else "AutoStock 严格匹配")
    if fee_rank >= 0.7:
        reasons.append("费率处于同风格较优区间")

    if not reasons:
        reasons.append("综合收益风险、持续性与治理质量均衡领先")
    return "；".join(dict.fromkeys(reasons))


def _priority_pool_risk_text(row: pd.Series | dict[str, Any]) -> str:
    if isinstance(row, pd.Series):
        row = row.to_dict()
    risks: list[str] = []
    autostock_raw = _normalize_autostock_status(validation_status_value(row))
    stability = str(row.get("长期稳定性标签") or "").strip()
    confidence = str(row.get("项目质量信心") or "").strip()
    holding_period_status = str(row.get("持有期状态") or "").strip()
    occurrences = int(_safe_float(row.get("出现次数")) or 0)
    tracked_periods = int(_safe_float(row.get("跟踪总期数")) or 0)
    near_1m = _safe_float(row.get("近1月(%)"))
    near_3m = _safe_float(row.get("近3月(%)"))

    if _is_holding_period_name_unparsed_status(holding_period_status):
        risks.append("持有期限制未确认")
    if autostock_raw == "soft_pass_with_warnings":
        risks.append("数据校验存在软告警" if "数据校验来源" in row else "AutoStock 存在软告警")
    if stability in {"长期稳定性偏弱", "长期历史部分缺失", "长期历史缺失"}:
        if stability in {"长期历史部分缺失", "长期历史缺失"}:
            risks.append("长周期特征未完整")
        risks.append(stability)
    if confidence and confidence != "高信心":
        risks.append(f"项目质量信心={confidence}")
    if (near_1m is not None and near_1m < 0) or (near_3m is not None and near_3m < 0):
        risks.append("近期收益有转弱迹象")
    if tracked_periods >= 3 and occurrences <= 1:
        risks.append("累计出现次数仍偏少")

    return "；".join(dict.fromkeys(risks)) if risks else PRIORITY_POOL_DEFAULT_RISK_TEXT


def _priority_pool_unknown_weak_signal_count(row: pd.Series | dict[str, Any]) -> int:
    if isinstance(row, pd.Series):
        row = row.to_dict()

    holding_period_status = str(row.get("持有期状态") or "").strip()
    if not _is_holding_period_name_unparsed_status(holding_period_status):
        return 0

    autostock_raw = _normalize_autostock_status(validation_status_value(row))
    stability = str(row.get("长期稳定性标签") or "").strip()
    confidence = str(row.get("项目质量信心") or "").strip()
    formal_score = _safe_float(row.get("正式投资评分_num"))
    if formal_score is None:
        formal_score = _safe_float(row.get(FORMAL_SCORE_LABEL))
    near_1m = _safe_float(row.get("近1月(%)"))
    near_3m = _safe_float(row.get("近3月(%)"))
    recent_score = _safe_float(row.get("近期得分_num"))
    if recent_score is None:
        recent_score = _safe_float(row.get("近期得分"))

    weak_signal_count = 0
    if autostock_raw in {"soft_pass_with_warnings", "soft_pass"}:
        weak_signal_count += 1
    if stability in {"长期稳定性偏弱", "长期历史部分缺失", "长期历史缺失"}:
        weak_signal_count += 1
    if confidence in {"中信心", "边界观察"}:
        weak_signal_count += 1
    if (near_1m is not None and near_1m < 0) or (near_3m is not None and near_3m < 0) or (
        recent_score is not None and recent_score < 60
    ):
        weak_signal_count += 1
    if formal_score is not None and formal_score < 80:
        weak_signal_count += 1
        if formal_score < 75:
            weak_signal_count += 1

    return weak_signal_count


def _is_extension_strict_unknown_fallback_eligible(row: pd.Series | dict[str, Any]) -> bool:
    if isinstance(row, pd.Series):
        row = row.to_dict()

    holding_period_status = str(row.get("持有期状态") or "").strip()
    if not _is_holding_period_name_unparsed_status(holding_period_status):
        return False

    autostock_raw = _normalize_autostock_status(validation_status_value(row))
    confidence = str(row.get("项目质量信心") or "").strip()
    stability = str(row.get("长期稳定性标签") or "").strip()
    formal_score = _safe_float(row.get("正式投资评分_num"))
    if formal_score is None:
        formal_score = _safe_float(row.get(FORMAL_SCORE_LABEL))
    near_1m = _safe_float(row.get("近1月(%)"))
    near_3m = _safe_float(row.get("近3月(%)"))
    recent_score = _safe_float(row.get("近期得分_num"))
    if recent_score is None:
        recent_score = _safe_float(row.get("近期得分"))

    if autostock_raw != "strict_match":
        return False
    if confidence != "高信心":
        return False
    if stability not in {"长期稳定性强", "长期稳定性中等"}:
        return False
    if formal_score is None or float(formal_score) < float(EXTENSION_STRICT_UNKNOWN_FALLBACK_MIN_FORMAL_SCORE):
        return False
    if (near_1m is not None and near_1m < 0) or (near_3m is not None and near_3m < 0):
        return False
    if recent_score is not None and recent_score < 60:
        return False
    return True


def _build_current_selection_tracker_features(
    settings: Settings,
    *,
    current_run_id: str,
    current_generated_at: str,
    current_nav_snapshot_date: str,
    current_df: pd.DataFrame,
    history_capture: dict[str, Any] | None = None,
) -> pd.DataFrame:
    columns = ["基金代码", "出现次数", "出现频率(%)", "连续缺席期数", "当前是否在榜", "跟踪总期数"]
    if history_capture is not None:
        history_capture.clear()
        history_capture.update(contract=selection_tracker_history.CONTRACT, status="not_collected",
                               run_id=current_run_id, generated_at=current_generated_at,
                               registry_nav_snapshot_date=current_nav_snapshot_date,
                               observations=[], missing_history_imputation=False)
    if current_df.empty or "基金代码" not in current_df.columns:
        return pd.DataFrame(columns=columns)

    selection_capture: dict[str, Any] = {}
    history = _load_project_report_history(
        settings,
        current_run_id=current_run_id,
        current_generated_at=current_generated_at,
        current_filtered_excel=settings.runs_dir / current_run_id / "__tracker_features_in_memory__.xlsx",
        current_final_count=int(len(current_df.index)),
        current_nav_snapshot_date=current_nav_snapshot_date,
        **({"history_capture": selection_capture} if history_capture is not None else {}),
    )
    history = [item for item in history if str(item.get("run_id") or "").strip() != current_run_id]

    snapshots: dict[str, set[str]] = {}
    for item in sorted(history, key=lambda row: (row["generated_at"], row["run_id"])):
        source_digest = ""
        if history_capture is not None:
            source_digest = selection_tracker_history.source_sha256(Path(item["excel_path"]))
        csv_df, signature, nav_date = _load_tracker_source_frame(item["excel_path"])
        snapshot_key = project_tracker._tracker_snapshot_key(nav_date, signature)
        if csv_df.empty or "基金代码" not in csv_df.columns or not snapshot_key:
            raise ValueError(f"tracker_observation_source_unavailable: {item['excel_path']}")
        snapshot_codes = {
            _normalize_code(code)
            for code in csv_df.get("基金代码", pd.Series(dtype=object)).dropna().astype(str).tolist()
            if _normalize_code(code)
        }
        if not snapshot_codes:
            raise ValueError(f"tracker_observation_membership_unavailable: {item['excel_path']}")
        snapshots[snapshot_key] = snapshot_codes
        if history_capture is not None:
            verified = source_digest == selection_tracker_history.source_sha256(Path(item["excel_path"]))
            from core.utils import report_history_store as history_store
            ledger_observation = history_store.view(Path(item['excel_path']), 'tracker')
            history_capture["observations"].append({
                "run_id": item["run_id"], "generated_at": item["generated_at"],
                "registry_nav_snapshot_date": str(item.get("nav_snapshot_date") or ""),
                "source_path": str(item["excel_path"]), "source_sha256": source_digest,
                "source_verified_unchanged": verified, "signature": signature, "nav_date": nav_date,
                **({'history_record': {
                    **{k:v for k,v in ledger_observation[1].items() if k not in ('payload','digest')},
                    'source_object_digest': ledger_observation[1]['digest'],
                    'payload': {'member_codes':sorted(snapshot_codes),'signature':signature,'nav_date':nav_date},
                    'digest':history_store.digest({'member_codes':sorted(snapshot_codes),'signature':signature,'nav_date':nav_date}),
                    'projection':'selection_membership_only',
                }} if ledger_observation else {}),
                "snapshot_key": snapshot_key, "member_codes": sorted(snapshot_codes),
            })

    current_codes = {
        _normalize_code(code)
        for code in current_df.get("基金代码", pd.Series(dtype=object)).dropna().astype(str).tolist()
        if _normalize_code(code)
    }
    if not current_codes:
        return pd.DataFrame(columns=columns)

    signature, nav_date = project_tracker._tracker_source_identity(current_df)
    snapshot_key = project_tracker._tracker_snapshot_key(nav_date, signature)
    if not snapshot_key:
        raise ValueError("tracker_current_observation_identity_unavailable")
    # A rerun replaces that NAV observation, even when the registry date differed.
    snapshots[snapshot_key] = current_codes
    if history_capture is not None:
        history_capture["selection"] = selection_capture
        history_capture["current_observation"] = {
            "membership_scope": selection_tracker_history.CURRENT_MEMBERSHIP_SCOPE,
            "signature": signature, "nav_date": nav_date, "snapshot_key": snapshot_key,
            "member_codes": sorted(current_codes),
        }
        history_capture["status"] = ("complete" if selection_capture.get("complete") is True
            and all(row["source_verified_unchanged"] for row in history_capture["observations"]) else "incomplete")
    total_periods = len(snapshots)
    occurrences = Counter(code for codes in snapshots.values() for code in codes)
    rows = [
        {
            "基金代码": code,
            "出现次数": int(occurrences[code]),
            "出现频率(%)": round(occurrences[code] / total_periods * 100.0, 2),
            "连续缺席期数": 0,
            "当前是否在榜": "是",
            "跟踪总期数": int(total_periods),
        }
        for code in sorted(current_codes)
    ]
    return pd.DataFrame(rows, columns=columns)


def _build_priority_pool_tracker_features(
    settings: Settings,
    *,
    current_run_id: str,
    current_generated_at: str,
    current_nav_snapshot_date: str,
    current_df: pd.DataFrame,
) -> pd.DataFrame:
    return _build_current_selection_tracker_features(
        settings, current_run_id=current_run_id, current_generated_at=current_generated_at,
        current_nav_snapshot_date=current_nav_snapshot_date, current_df=current_df,
    )


def _build_final_selection_tracker_features(
    settings: Settings,
    *,
    current_run_id: str,
    current_generated_at: str,
    current_nav_snapshot_date: str,
    current_df: pd.DataFrame,
    history_capture: dict[str, Any] | None = None,
) -> pd.DataFrame:
    return _build_current_selection_tracker_features(
        settings, current_run_id=current_run_id, current_generated_at=current_generated_at,
        current_nav_snapshot_date=current_nav_snapshot_date, current_df=current_df,
        history_capture=history_capture,
    )


def _portfolio_candidates(
    df: pd.DataFrame,
    *,
    tracker_features_df: pd.DataFrame | None = None,
) -> list[dict[str, Any]]:
    if df.empty:
        return []

    current = df.copy().astype(object)
    current = current.where(current.notna(), "")
    if tracker_features_df is not None and not tracker_features_df.empty:
        merge_columns = [column for column in tracker_features_df.columns if column != "基金代码"]
        current = current.merge(
            tracker_features_df[["基金代码", *merge_columns]].copy(),
            on="基金代码",
            how="left",
        )
        for column_name in ["出现次数", "出现频率(%)", "连续缺席期数", "当前是否在榜", "跟踪总期数"]:
            current = _coalesce_merge_conflict_columns(current, column_name)
    for column, default_value in {
        "出现次数": 1,
        "出现频率(%)": 100.0,
        "连续缺席期数": 0,
        "当前是否在榜": "是",
        "跟踪总期数": 1,
    }.items():
        if column not in current.columns:
            current[column] = default_value
        current[column] = current[column].replace("", default_value).fillna(default_value)

    current["基金代码"] = current.get("基金代码", pd.Series(dtype=object)).astype(str).map(_normalize_code)
    current = current[current["基金代码"].astype(str).str.strip() != ""].copy()
    if current.empty:
        return []

    current["正式投资评分_num"] = current.apply(_resolve_formal_score_value, axis=1)
    numeric_columns = {
        "费率合计(%)_num": "费率合计(%)",
        "1年-最大回撤(%)_num": "1年-最大回撤(%)",
        "1年-波动率_num": "1年-波动率",
        "1年-年化收益(%)_num": "1年-年化收益(%)",
        "1年-夏普比率_num": "1年-夏普比率",
        "1年-卡玛比率_num": "1年-卡玛比率",
        "近期得分_num": "近期得分",
        "长期得分_num": "长期得分",
        "综合加权得分_num": "综合加权得分",
        "近1月(%)_num": "近1月(%)",
        "近3月(%)_num": "近3月(%)",
        "基金规模(亿)_num": "基金规模(亿)",
        "出现次数_num": "出现次数",
        "出现频率(%)_num": "出现频率(%)",
    }
    for target_column, source_column in numeric_columns.items():
        current[target_column] = pd.to_numeric(current.get(source_column), errors="coerce")

    current["是否可申购"] = current.get("是否可申购", pd.Series("", index=current.index, dtype=object)).astype(str).str.strip()
    current["AutoStock原始校验状态"] = current.get("AutoStock原始校验状态", pd.Series("", index=current.index, dtype=object)).astype(str).str.strip()
    current["_autostock_raw_status"] = validation_status_series(current)
    current["项目质量信心"] = current.get("项目质量信心", pd.Series("", index=current.index, dtype=object)).astype(str).str.strip()
    current["长期稳定性标签"] = current.get("长期稳定性标签", pd.Series("", index=current.index, dtype=object)).astype(str).str.strip()
    current["持有期状态"] = current.get("持有期状态", pd.Series("", index=current.index, dtype=object)).astype(str).str.strip()
    current[FORMAL_GROUP_LABEL] = current.get(FORMAL_GROUP_LABEL, pd.Series("", index=current.index, dtype=object)).astype(str).str.strip()
    current["债基风格标签"] = current.get("债基风格标签", pd.Series("", index=current.index, dtype=object)).astype(str).str.strip()
    current["基金公司"] = current.get("基金公司", pd.Series("", index=current.index, dtype=object)).astype(str).str.strip()
    current["基金名称"] = current.get("基金名称", pd.Series("", index=current.index, dtype=object)).astype(str).str.strip()
    current["母基金名称"] = current.get("母基金名称", pd.Series("", index=current.index, dtype=object)).astype(str).str.strip()
    current["bucket_key"] = current[FORMAL_GROUP_LABEL] + "::" + current["债基风格标签"]
    current["share_key"] = current.apply(_priority_pool_share_key, axis=1)

    current = current[current["是否可申购"].isin({"可申购", ""})].copy()
    current = current[current["_autostock_raw_status"] != "hard_block"].copy()
    required_numeric = [
        "正式投资评分_num",
        "费率合计(%)_num",
        "1年-最大回撤(%)_num",
        "1年-年化收益(%)_num",
        "1年-夏普比率_num",
        "1年-卡玛比率_num",
        "近期得分_num",
        "长期得分_num",
    ]
    current = current[current[required_numeric].notna().all(axis=1)].copy()
    if current.empty:
        return []

    autostock_sort = {"strict_match": 0, "soft_pass_with_warnings": 1, "soft_pass": 1}
    current["_share_fee_sort"] = current["费率合计(%)_num"].fillna(999.0)
    current["_share_formal_sort"] = current["正式投资评分_num"].fillna(-999.0)
    current["_share_size_sort"] = current["基金规模(亿)_num"].fillna(-999.0)
    current["_share_autostock_sort"] = current["_autostock_raw_status"].map(autostock_sort).fillna(9)
    current = current.sort_values(
        by=["share_key", "_share_fee_sort", "_share_formal_sort", "_share_size_sort", "_share_autostock_sort", "基金代码"],
        ascending=[True, True, False, False, True, True],
        kind="mergesort",
    )
    current = current.drop_duplicates(subset=["share_key"], keep="first").reset_index(drop=True)
    if current.empty:
        return []

    current["费率分位得分"] = current.groupby("bucket_key", group_keys=False)["费率合计(%)_num"].apply(
        lambda item: _priority_pool_rank_score(item, ascending=True)
    )
    current["正式评分分位得分"] = current.groupby("bucket_key", group_keys=False)["正式投资评分_num"].apply(
        lambda item: _priority_pool_rank_score(item, ascending=False)
    )
    current["近期得分分位得分"] = current.groupby("bucket_key", group_keys=False)["近期得分_num"].apply(
        lambda item: _priority_pool_rank_score(item, ascending=False)
    )
    current["长期得分分位得分"] = current.groupby("bucket_key", group_keys=False)["长期得分_num"].apply(
        lambda item: _priority_pool_rank_score(item, ascending=False)
    )
    current["年化收益分位得分"] = current.groupby("bucket_key", group_keys=False)["1年-年化收益(%)_num"].apply(
        lambda item: _priority_pool_rank_score(item, ascending=False)
    )
    current["夏普分位得分"] = current.groupby("bucket_key", group_keys=False)["1年-夏普比率_num"].apply(
        lambda item: _priority_pool_rank_score(item, ascending=False)
    )
    current["卡玛分位得分"] = current.groupby("bucket_key", group_keys=False)["1年-卡玛比率_num"].apply(
        lambda item: _priority_pool_rank_score(item, ascending=False)
    )
    current["回撤分位得分"] = current.groupby("bucket_key", group_keys=False)["1年-最大回撤(%)_num"].apply(
        lambda item: _priority_pool_rank_score(item, ascending=True)
    )
    current["波动分位得分"] = current.groupby("bucket_key", group_keys=False)["1年-波动率_num"].apply(
        lambda item: _priority_pool_rank_score(item, ascending=True)
    )
    current["出现次数分位得分"] = current.groupby("bucket_key", group_keys=False)["出现次数_num"].apply(
        lambda item: _priority_pool_rank_score(item, ascending=False)
    )
    current["出现频率分位得分"] = current.groupby("bucket_key", group_keys=False)["出现频率(%)_num"].apply(
        lambda item: _priority_pool_rank_score(item, ascending=False)
    )
    current["桶内样本数"] = current.groupby("bucket_key")["基金代码"].transform("size")

    current["_bucket_metric_score"] = (
        current["正式评分分位得分"] * 0.20
        + current["近期得分分位得分"] * 0.12
        + current["长期得分分位得分"] * 0.12
        + current["年化收益分位得分"] * 0.10
        + current["夏普分位得分"] * 0.08
        + current["卡玛分位得分"] * 0.08
        + current["回撤分位得分"] * 0.08
        + current["波动分位得分"] * 0.05
        + current["费率分位得分"] * 0.05
        + current["出现次数分位得分"] * 0.07
        + current["出现频率分位得分"] * 0.05
    )
    current["_confidence_bonus"] = current["项目质量信心"].map({"高信心": 10.0, "中信心": 4.0, "边界观察": -4.0}).fillna(0.0)
    current["_stability_bonus"] = current["长期稳定性标签"].map(
        {
            "长期稳定性强": 8.0,
            "长期稳定性中等": 4.0,
            "长期历史部分缺失": 0.0,
            "长期历史缺失": -2.0,
            "长期稳定性偏弱": -8.0,
        }
    ).fillna(0.0)
    current["_occurrence_bonus"] = (
        current["出现频率(%)_num"].fillna(0.0).clip(lower=0.0, upper=100.0).map(lambda value: max(0.0, (value - 50.0) / 10.0))
        + current["出现次数_num"].fillna(1.0).map(lambda value: max(0.0, min(4.0, (value - 1.0) * 0.8)))
    )
    current["_autostock_penalty"] = current["_autostock_raw_status"].map(
        {"strict_match": 0.0, "soft_pass_with_warnings": 6.0, "soft_pass": 6.0, "hard_block": 100.0}
    ).fillna(0.0)
    current["_holding_period_penalty"] = current["持有期状态"].map(
        lambda value: PRIORITY_POOL_HOLDING_PERIOD_NAME_UNPARSED_PENALTY
        if _is_holding_period_name_unparsed_status(value)
        else 0.0
    )
    current["_recent_penalty"] = current.apply(_priority_pool_recent_penalty, axis=1)
    current["投资优先池得分_num"] = (
        current["_bucket_metric_score"] * 100.0
        + current["_confidence_bonus"]
        + current["_stability_bonus"]
        + current["_occurrence_bonus"]
        - current["_autostock_penalty"]
        - current["_holding_period_penalty"]
        - current["_recent_penalty"]
    ).round(4)
    current["桶内排名"] = current.groupby("bucket_key")["投资优先池得分_num"].rank(method="dense", ascending=False)
    current = current.sort_values(
        by=["投资优先池得分_num", "正式投资评分_num", "费率合计(%)_num", "基金代码"],
        ascending=[False, False, True, True],
        kind="mergesort",
    ).reset_index(drop=True)
    current["总体排名"] = range(1, len(current.index) + 1)
    current["入选原因"] = current.apply(_priority_pool_reason, axis=1)
    current["风险提示"] = current.apply(_priority_pool_risk_text, axis=1)

    result_columns = [
        "基金代码",
        "基金名称",
        "基金公司",
        FORMAL_GROUP_LABEL,
        "持有期状态",
        "债基风格标签",
        "母基金名称",
        "基金规模(亿)",
        "最新净值日期",
        "费率合计(%)",
        FORMAL_SCORE_LABEL,
        "投资优先池得分",
        "项目质量信心",
        "出现次数",
        "出现频率(%)",
        "1年-年化收益(%)",
        "近1月(%)",
        "近3月(%)",
        "1年-夏普比率",
        "1年-最大回撤(%)",
        "1年-波动率",
        "连续缺席期数",
        "当前是否在榜",
        "AutoStock原始校验状态",
        "长期稳定性标签",
        "入选原因",
        "风险提示",
        "建议权重",
        "桶内排名",
        "桶内样本数",
        "bucket_key",
        "share_key",
        "投资优先池得分_num",
        "正式投资评分_num",
        "近期得分_num",
    ]
    current["投资优先池得分"] = current["投资优先池得分_num"].map(lambda value: f"{float(value):.4f}".rstrip("0").rstrip("."))
    current["建议权重"] = ""
    for column in result_columns:
        if column not in current.columns:
            current[column] = ""
    return current[result_columns].to_dict(orient="records")


def _select_diversified_funds(
    candidates: list[dict[str, Any]],
    *,
    target_count: int,
    max_per_company: int = PRIORITY_POOL_COMPANY_CAP,
) -> list[dict[str, Any]]:
    if not candidates or target_count <= 0:
        return []

    frame = pd.DataFrame(candidates)
    if frame.empty:
        return []
    frame["投资优先池得分_num"] = pd.to_numeric(frame.get("投资优先池得分_num"), errors="coerce").fillna(-999.0)
    frame["正式投资评分_num"] = pd.to_numeric(frame.get("正式投资评分_num"), errors="coerce").fillna(-999.0)
    frame["基金公司"] = frame.get("基金公司", pd.Series("", index=frame.index, dtype=object)).astype(str)
    frame["基金代码"] = frame.get("基金代码", pd.Series("", index=frame.index, dtype=object)).astype(str)
    frame["share_key"] = frame.get("share_key", pd.Series("", index=frame.index, dtype=object)).astype(str)

    bucket_sizes = frame.groupby("bucket_key")["基金代码"].size().to_dict()
    total_candidates = max(1, int(len(frame.index)))
    bucket_quota_rows: list[dict[str, Any]] = []
    for bucket_key, size in bucket_sizes.items():
        raw_quota = float(size) / float(total_candidates) * float(target_count)
        base_quota = max(1, int(math.floor(raw_quota)))
        base_quota = min(base_quota, int(size))
        bucket_quota_rows.append(
            {
                "bucket_key": bucket_key,
                "size": int(size),
                "quota": int(base_quota),
                "raw_quota": raw_quota,
                "remainder": raw_quota - float(base_quota),
            }
        )

    current_total = sum(int(item["quota"]) for item in bucket_quota_rows)
    if current_total > target_count:
        for item in sorted(bucket_quota_rows, key=lambda value: (value["raw_quota"], value["size"]), reverse=False):
            while current_total > target_count and int(item["quota"]) > 1:
                item["quota"] = int(item["quota"]) - 1
                current_total -= 1
    elif current_total < target_count:
        expandable = True
        while current_total < target_count and expandable:
            expandable = False
            for item in sorted(bucket_quota_rows, key=lambda value: (value["remainder"], value["size"]), reverse=True):
                if int(item["quota"]) >= int(item["size"]):
                    continue
                item["quota"] = int(item["quota"]) + 1
                current_total += 1
                expandable = True
                if current_total >= target_count:
                    break

    selected: list[dict[str, Any]] = []
    selected_codes: set[str] = set()
    selected_shares: set[str] = set()
    company_counter: Counter[str] = Counter()

    def _try_pick(row: dict[str, Any]) -> bool:
        code = str(row.get("基金代码") or "").strip()
        share_key = str(row.get("share_key") or "").strip()
        company = str(row.get("基金公司") or "").strip()
        if not code or code in selected_codes:
            return False
        if share_key and share_key in selected_shares:
            return False
        if company and company_counter[company] >= max_per_company:
            return False
        selected.append(dict(row))
        selected_codes.add(code)
        if share_key:
            selected_shares.add(share_key)
        if company:
            company_counter[company] += 1
        return True

    for quota_row in sorted(bucket_quota_rows, key=lambda value: (value["raw_quota"], value["size"]), reverse=True):
        bucket_candidates = frame[frame["bucket_key"] == quota_row["bucket_key"]].copy()
        bucket_candidates = bucket_candidates.sort_values(
            by=["投资优先池得分_num", "正式投资评分_num", "基金代码"],
            ascending=[False, False, True],
            kind="mergesort",
        )
        bucket_selected = 0
        for row in bucket_candidates.to_dict(orient="records"):
            if bucket_selected >= int(quota_row["quota"]):
                break
            if _try_pick(row):
                bucket_selected += 1

    if len(selected) < target_count:
        for row in frame.sort_values(
            by=["投资优先池得分_num", "正式投资评分_num", "基金代码"],
            ascending=[False, False, True],
            kind="mergesort",
        ).to_dict(orient="records"):
            if len(selected) >= target_count:
                break
            _try_pick(row)

    selected.sort(
        key=lambda item: (
            -float(item.get("投资优先池得分_num") or -999.0),
            -float(item.get("正式投资评分_num") or -999.0),
            str(item.get("基金代码") or ""),
        )
    )
    return selected


def _apply_priority_pool_quality_gate(
    candidates: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if not candidates:
        return [], {
            "available": False,
            "source_candidate_count": 0,
            "candidate_count": 0,
            "removed_count": 0,
            "min_candidate_count": int(PRIORITY_POOL_QUALITY_GATE_MIN_CANDIDATE_COUNT),
            "selected_step_label": None,
            "selection_reason": None,
            "step_rows": [],
        }

    frame = pd.DataFrame(candidates).copy()
    if frame.empty:
        return [], {
            "available": False,
            "source_candidate_count": 0,
            "candidate_count": 0,
            "removed_count": 0,
            "min_candidate_count": int(PRIORITY_POOL_QUALITY_GATE_MIN_CANDIDATE_COUNT),
            "selected_step_label": None,
            "selection_reason": None,
            "step_rows": [],
        }

    if len(frame.index) < PRIORITY_POOL_QUALITY_GATE_ACTIVATION_COUNT:
        return candidates, {
            "available": False,
            "source_candidate_count": int(len(frame.index)),
            "candidate_count": int(len(frame.index)),
            "removed_count": 0,
            "min_candidate_count": int(PRIORITY_POOL_QUALITY_GATE_MIN_CANDIDATE_COUNT),
            "selected_step_label": None,
            "selection_reason": "skipped_small_candidate_pool",
            "step_rows": [],
        }

    frame["正式投资评分_num"] = pd.to_numeric(frame.get("正式投资评分_num"), errors="coerce")
    frame["投资优先池得分_num"] = pd.to_numeric(frame.get("投资优先池得分_num"), errors="coerce")
    frame[FORMAL_GROUP_LABEL] = frame.get(FORMAL_GROUP_LABEL, pd.Series("", index=frame.index, dtype=object)).astype(str)

    stable_mask = frame[FORMAL_GROUP_LABEL].eq(STABLE_BOND_GROUP_LABEL)
    enhanced_mask = frame[FORMAL_GROUP_LABEL].eq(ENHANCED_BOND_GROUP_LABEL)
    other_mask = ~(stable_mask | enhanced_mask)

    selected_frame = pd.DataFrame()
    selected_step: dict[str, Any] | None = None
    best_frame = pd.DataFrame()
    best_step: dict[str, Any] | None = None
    step_rows: list[dict[str, Any]] = []

    for step in PRIORITY_POOL_QUALITY_GATE_STEPS:
        stable_floor = float(step["stable_formal_score_floor"])
        enhanced_floor = float(step["enhanced_formal_score_floor"])
        priority_floor = float(step["priority_score_floor"])
        mask = (
            (stable_mask & (frame["正式投资评分_num"] >= stable_floor))
            | (enhanced_mask & (frame["正式投资评分_num"] >= enhanced_floor))
            | (other_mask & (frame["正式投资评分_num"] >= stable_floor))
        ) & (frame["投资优先池得分_num"] >= priority_floor)
        filtered = frame[mask].copy()
        filtered_count = int(len(filtered.index))
        split = (
            filtered[FORMAL_GROUP_LABEL].value_counts().to_dict()
            if not filtered.empty
            else {}
        )
        row = {
            "label": str(step["label"]),
            "stable_formal_score_floor": stable_floor,
            "enhanced_formal_score_floor": enhanced_floor,
            "priority_score_floor": priority_floor,
            "candidate_count": filtered_count,
            "stable_count": int(split.get(STABLE_BOND_GROUP_LABEL) or 0),
            "enhanced_count": int(split.get(ENHANCED_BOND_GROUP_LABEL) or 0),
        }
        step_rows.append(row)
        if filtered_count > int(len(best_frame.index)):
            best_frame = filtered
            best_step = row
        if filtered_count >= PRIORITY_POOL_QUALITY_GATE_MIN_CANDIDATE_COUNT and selected_step is None:
            selected_frame = filtered
            selected_step = row
            break

    selection_reason = "first_gate_meeting_min_candidate_count"
    if selected_step is None:
        selected_frame = best_frame.copy()
        selected_step = best_step
        selection_reason = "best_available_gate"

    if selected_frame.empty or not selected_step:
        return [], {
            "available": False,
            "source_candidate_count": int(len(frame.index)),
            "candidate_count": 0,
            "removed_count": int(len(frame.index)),
            "min_candidate_count": int(PRIORITY_POOL_QUALITY_GATE_MIN_CANDIDATE_COUNT),
            "selected_step_label": None,
            "selection_reason": "no_candidate_passed_quality_gate",
            "step_rows": step_rows,
        }

    selected_frame = selected_frame.sort_values(
        by=["投资优先池得分_num", "正式投资评分_num", "基金代码"],
        ascending=[False, False, True],
        kind="mergesort",
    ).reset_index(drop=True)
    return selected_frame.to_dict(orient="records"), {
        "available": True,
        "source_candidate_count": int(len(frame.index)),
        "candidate_count": int(len(selected_frame.index)),
        "removed_count": int(len(frame.index) - len(selected_frame.index)),
        "min_candidate_count": int(PRIORITY_POOL_QUALITY_GATE_MIN_CANDIDATE_COUNT),
        "selected_step_label": selected_step.get("label"),
        "selection_reason": selection_reason,
        "step_rows": step_rows,
    }


def _compute_priority_pool_target_count(unique_candidate_count: int) -> tuple[int, dict[str, Any]]:
    if unique_candidate_count <= 0:
        return 0, {
            "candidate_count": 0,
            "base_target_count": 0,
            "target_count": 0,
            "min_count": int(PRIORITY_POOL_MIN_COUNT),
            "max_count": int(PRIORITY_POOL_MAX_COUNT),
            "base_target_ratio": float(PRIORITY_POOL_TARGET_RATIO),
            "expanded_target_ratio": float(PRIORITY_POOL_EXPANDED_TARGET_RATIO),
            "expansion_ratio_trigger": float(PRIORITY_POOL_EXPANSION_RATIO_TRIGGER),
            "expanded": False,
            "expansion_reason": None,
            "floor_applied": False,
            "band_mode": "quality_first_narrow_band",
        }

    raw_target_count = int(round(unique_candidate_count * PRIORITY_POOL_TARGET_RATIO))
    base_target_count = int(min(unique_candidate_count, raw_target_count))
    if unique_candidate_count <= PRIORITY_POOL_MIN_COUNT:
        target_count = int(unique_candidate_count)
    else:
        target_count = int(
            min(
                unique_candidate_count,
                max(
                    PRIORITY_POOL_MIN_COUNT,
                    min(PRIORITY_POOL_MAX_COUNT, raw_target_count),
                ),
            )
        )

    return target_count, {
        "candidate_count": int(unique_candidate_count),
        "base_target_count": int(base_target_count),
        "target_count": int(target_count),
        "min_count": int(PRIORITY_POOL_MIN_COUNT),
        "max_count": int(PRIORITY_POOL_MAX_COUNT),
        "base_target_ratio": float(PRIORITY_POOL_TARGET_RATIO),
        "expanded_target_ratio": float(PRIORITY_POOL_EXPANDED_TARGET_RATIO),
        "expansion_ratio_trigger": float(PRIORITY_POOL_EXPANSION_RATIO_TRIGGER),
        "expanded": False,
        "expansion_reason": None,
        "floor_applied": bool(target_count > base_target_count),
        "band_mode": "quality_first_narrow_band",
    }


def _build_priority_pool_compression_audit(
    candidates: list[dict[str, Any]],
    *,
    target_count: int,
    selected: list[dict[str, Any]],
) -> dict[str, Any]:
    if not candidates:
        return {
            "candidate_count_before_target": 0,
            "selected_count": 0,
            "trimmed_by_target_count": 0,
            "target_compression_ratio_pct": 0.0,
            "share_class_unique_count": 0,
            "company_cap": int(PRIORITY_POOL_COMPANY_CAP),
            "bucket_candidate_rows": [],
            "bucket_selected_rows": [],
            "group_candidate_rows": [],
            "group_selected_rows": [],
            "company_selected_rows": [],
            "selection_pressure": {
                "candidate_per_target_ratio": 0.0,
                "company_coverage_ratio_pct": 0.0,
            },
        }

    candidate_df = pd.DataFrame(candidates).copy()
    selected_df = pd.DataFrame(selected).copy()
    candidate_count = int(len(candidate_df.index))
    selected_count = int(len(selected_df.index))

    def _distribution_rows(frame: pd.DataFrame, column: str, label: str) -> list[dict[str, Any]]:
        if frame.empty or column not in frame.columns:
            return []
        counts = frame[column].fillna("").astype(str).str.strip()
        counts = counts[counts != ""]
        if counts.empty:
            return []
        total = int(len(counts.index))
        rows: list[dict[str, Any]] = []
        for name, count in counts.value_counts().items():
            rows.append(
                {
                    label: str(name),
                    "count": int(count),
                    "share_pct": round(float(count) / float(total) * 100.0, 2),
                }
            )
        return rows

    share_unique_count = int(candidate_df.get("share_key", pd.Series(dtype=object)).astype(str).nunique())
    candidate_company_count = int(candidate_df.get("基金公司", pd.Series(dtype=object)).astype(str).replace("", pd.NA).dropna().nunique())
    selected_company_count = int(selected_df.get("基金公司", pd.Series(dtype=object)).astype(str).replace("", pd.NA).dropna().nunique())
    target_trimmed = max(0, candidate_count - target_count)
    compression_ratio_pct = round(float(target_trimmed) / float(candidate_count) * 100.0, 2) if candidate_count else 0.0
    candidate_per_target_ratio = round(float(candidate_count) / float(target_count), 2) if target_count else 0.0
    company_coverage_ratio_pct = (
        round(float(selected_company_count) / float(candidate_company_count) * 100.0, 2)
        if candidate_company_count
        else 0.0
    )

    return {
        "candidate_count_before_target": int(candidate_count),
        "selected_count": int(selected_count),
        "trimmed_by_target_count": int(target_trimmed),
        "target_compression_ratio_pct": float(compression_ratio_pct),
        "share_class_unique_count": int(share_unique_count),
        "company_cap": int(PRIORITY_POOL_COMPANY_CAP),
        "bucket_candidate_rows": _distribution_rows(candidate_df, "bucket_key", "bucket_key"),
        "bucket_selected_rows": _distribution_rows(selected_df, "bucket_key", "bucket_key"),
        "group_candidate_rows": _distribution_rows(candidate_df, FORMAL_GROUP_LABEL, "group_label"),
        "group_selected_rows": _distribution_rows(selected_df, FORMAL_GROUP_LABEL, "group_label"),
        "company_selected_rows": _distribution_rows(selected_df, "基金公司", "company_name")[:20],
        "selection_pressure": {
            "candidate_per_target_ratio": float(candidate_per_target_ratio),
            "company_coverage_ratio_pct": float(company_coverage_ratio_pct),
        },
    }


def _assign_portfolio_group_weights(group_df: pd.DataFrame) -> pd.DataFrame:
    if group_df.empty:
        result = group_df.copy()
        result["建议权重"] = []
        return result

    result = group_df.copy().reset_index(drop=True)

    def _numeric_series(column: str) -> pd.Series:
        if column not in result.columns:
            return pd.Series(0.0, index=result.index, dtype=float)
        return pd.to_numeric(result[column], errors="coerce").fillna(0.0)

    priority_score_column = "组合权重基准得分_num" if "组合权重基准得分_num" in result.columns else "投资优先池得分_num"
    priority_score = _numeric_series(priority_score_column)
    formal_score = _numeric_series("正式投资评分_num")
    recent_score = _numeric_series("近期得分_num")

    base_weight = (
        priority_score.clip(lower=0.0) * 0.70
        + formal_score.clip(lower=0.0) * 0.20
        + recent_score.clip(lower=0.0) * 0.10
    )

    confidence_multiplier = result.get("项目质量信心", pd.Series("", index=result.index, dtype=object)).map(
        {"高信心": 1.0, "中信心": 0.94, "边界观察": 0.84}
    ).fillna(0.90)
    stability_multiplier = result.get("长期稳定性标签", pd.Series("", index=result.index, dtype=object)).map(
        {
            "长期稳定性强": 1.0,
            "长期稳定性中等": 0.97,
            "长期历史部分缺失": 0.89,
            "长期历史缺失": 0.84,
            "长期稳定性偏弱": 0.80,
        }
    ).fillna(0.94)
    autostock_multiplier = validation_status_series(result).map(
        {
            "strict_match": 1.0,
            "soft_pass_with_warnings": 0.88,
            "soft_pass": 0.88,
            "hard_block": 0.0,
        }
    ).fillna(0.94)

    risk_series = result.get("风险提示", pd.Series("", index=result.index, dtype=object)).astype(str)
    risk_multiplier = pd.Series(1.0, index=result.index, dtype=float)
    risk_multiplier = risk_multiplier.where(~risk_series.str.contains("长期历史部分缺失|长期历史缺失", regex=True), 0.90)
    risk_multiplier = risk_multiplier.where(~risk_series.str.contains("近期收益有转弱迹象", regex=False), 0.86)
    risk_multiplier = risk_multiplier.where(~risk_series.str.contains("AutoStock 存在软告警|数据校验存在软告警", regex=True), 0.82)
    risk_multiplier = risk_multiplier.where(~risk_series.str.contains("长期稳定性偏弱", regex=False), 0.78)
    risk_multiplier = risk_multiplier.where(~risk_series.str.contains("持有期限制未确认", regex=False), 0.88)

    raw_weight = (
        base_weight
        * confidence_multiplier.astype(float)
        * stability_multiplier.astype(float)
        * autostock_multiplier.astype(float)
        * risk_multiplier.astype(float)
    ).clip(lower=0.01)

    weight_sum = float(raw_weight.sum())
    if weight_sum <= 0:
        equal_weight = round(100.0 / len(result.index), 2)
        result["建议权重"] = [f"{equal_weight:.2f}%"] * len(result.index)
        return result

    unrounded = raw_weight / weight_sum * 100.0
    rounded = unrounded.round(2)
    diff = round(100.0 - float(rounded.sum()), 2)
    if rounded.size and abs(diff) >= 0.01:
        order = sorted(
            range(len(unrounded)),
            key=lambda idx: (unrounded.iloc[idx] - rounded.iloc[idx], unrounded.iloc[idx]),
            reverse=diff > 0,
        )
        rounded.iloc[order[0]] = round(float(rounded.iloc[order[0]]) + diff, 2)

    result["建议权重"] = rounded.map(lambda value: f"{float(value):.2f}%")
    return result


def _portfolio_core_priority_score(row: pd.Series | dict[str, Any]) -> float:
    if isinstance(row, pd.Series):
        row = row.to_dict()

    base_score = float(_safe_float(row.get("投资优先池得分_num")) or 0.0)
    formal_score = float(_safe_float(row.get("正式投资评分_num")) or 0.0)
    formal_group = str(row.get(FORMAL_GROUP_LABEL) or "").strip()
    holding_status = str(row.get("持有期状态") or "").strip()
    autostock_raw = _normalize_autostock_status(validation_status_value(row))
    near_1m = _safe_float(row.get("近1月(%)"))
    near_3m = _safe_float(row.get("近3月(%)"))

    if holding_status == "已确认<=90天":
        base_score += PRIORITY_POOL_CORE_CONFIRMED_FILL_BONUS
    elif _is_holding_period_name_unparsed_status(holding_status):
        base_score -= PRIORITY_POOL_CORE_HOLDING_PERIOD_UNPARSED_PENALTY
        if formal_group == ENHANCED_BOND_GROUP_LABEL:
            base_score -= PRIORITY_POOL_CORE_ENHANCED_UNPARSED_EXTRA_PENALTY

    if formal_group == STABLE_BOND_GROUP_LABEL:
        if holding_status == "已确认<=90天":
            base_score += PRIORITY_POOL_CORE_STABLE_CONFIRMED_BONUS
        elif _is_holding_period_name_unparsed_status(holding_status) and formal_score < 90:
            base_score -= PRIORITY_POOL_CORE_STABLE_UNPARSED_MID_SCORE_PENALTY
            if formal_score < 85:
                base_score -= PRIORITY_POOL_CORE_STABLE_UNPARSED_LOW_SCORE_EXTRA_PENALTY

    if formal_score < 75:
        base_score -= PRIORITY_POOL_CORE_LOW_FORMAL_SCORE_PENALTY
        if formal_score < 70:
            base_score -= PRIORITY_POOL_CORE_VERY_LOW_FORMAL_SCORE_EXTRA_PENALTY

    if (near_1m is not None and near_1m < 0) or (near_3m is not None and near_3m < 0):
        base_score -= PRIORITY_POOL_CORE_RECENT_WEAK_PENALTY
    if autostock_raw in {"soft_pass_with_warnings", "soft_pass"}:
        base_score -= PRIORITY_POOL_CORE_SOFT_WARNING_PENALTY

    return round(base_score, 4)


def _portfolio_extension_priority_score(row: pd.Series | dict[str, Any]) -> float:
    if isinstance(row, pd.Series):
        row = row.to_dict()

    base_score = float(_safe_float(row.get("投资优先池得分_num")) or 0.0)
    weak_signal_count = _priority_pool_unknown_weak_signal_count(row)
    if weak_signal_count <= 0:
        return round(base_score, 4)

    penalty = weak_signal_count * PRIORITY_POOL_EXTENSION_UNKNOWN_WEAK_SIGNAL_PENALTY
    return round(base_score - penalty, 4)


def _shrink_extension_group_by_confirmed_floor(
    extension_df: pd.DataFrame,
    *,
    group_label: str,
    min_confirmed: int | None = None,
    unknown_ratio_cap: float | None = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """P2-01 / B2: 对单个扩展组应用"最小确认样本下限"。

    规则：
    - ``confirmed`` = 持有期状态为 ``"已确认<=90天"`` 的样本数。
    - ``confirmed >= min_confirmed``：返回原 df，``downgrade.downgraded=False``。
    - ``confirmed == 0``：不再直接返回空 df，而是仅保留满足严格 fallback 条件的未知样本；
      若 fallback 也为空，则 ``downgrade.reason='no_confirmed_holding_sample'``。
    - 其它：只保留全部 confirmed + 最多 ``floor(confirmed / (1 / unknown_ratio_cap - 1))``
      个未知样本，默认 ``unknown_ratio_cap=0.5`` 等效 ``未知 ≤ 确认数``，
      整组规模从 ``len(ext)`` 缩到 ``2 * confirmed``，``reason='insufficient_confirmed_holding_sample'``。

    保持原 df 的排序；返回的子集索引已 reset。
    两个下限参数若传 ``None``，则读模块级 ``EXTENSION_MIN_CONFIRMED_HOLDING`` /
    ``EXTENSION_UNKNOWN_SAMPLE_RATIO_CAP``——这样测试可以用 monkeypatch 动态改变阈值。
    """
    if min_confirmed is None:
        min_confirmed = EXTENSION_MIN_CONFIRMED_HOLDING
    if unknown_ratio_cap is None:
        unknown_ratio_cap = EXTENSION_UNKNOWN_SAMPLE_RATIO_CAP
    if extension_df is None or extension_df.empty:
        return extension_df.iloc[0:0].copy() if extension_df is not None else extension_df, {
            "group_label": group_label,
            "downgraded": False,
            "reason": None,
            "confirmed_count": 0,
            "min_confirmed": int(min_confirmed),
            "original_size": 0,
            "final_size": 0,
        }

    status_series = extension_df.get("持有期状态", pd.Series(dtype=object)).astype(str)
    confirmed_mask = status_series.eq("已确认<=90天")
    confirmed_df = extension_df[confirmed_mask]
    unknown_df = extension_df[~confirmed_mask]
    original_size = int(len(extension_df.index))
    confirmed_count = int(len(confirmed_df.index))

    if confirmed_count >= int(min_confirmed):
        return extension_df.reset_index(drop=True), {
            "group_label": group_label,
            "downgraded": False,
            "reason": None,
            "confirmed_count": confirmed_count,
            "min_confirmed": int(min_confirmed),
            "original_size": original_size,
            "final_size": original_size,
        }

    if confirmed_count == 0:
        strict_unknown_df = unknown_df[
            unknown_df.apply(_is_extension_strict_unknown_fallback_eligible, axis=1)
        ].copy()
        if strict_unknown_df.empty:
            return extension_df.iloc[0:0].copy(), {
                "group_label": group_label,
                "downgraded": True,
                "reason": "no_confirmed_holding_sample",
                "confirmed_count": 0,
                "min_confirmed": int(min_confirmed),
                "original_size": original_size,
                "final_size": 0,
                "strict_unknown_fallback_count": 0,
            }
        strict_unknown_df = strict_unknown_df.sort_values(
            by=["扩展优先排序得分_num", "投资优先池得分_num", "正式投资评分_num", "基金代码"],
            ascending=[False, False, False, True],
            kind="mergesort",
        ).reset_index(drop=True)
        return strict_unknown_df, {
            "group_label": group_label,
            "downgraded": True,
            "reason": "strict_unknown_fallback_only",
            "confirmed_count": 0,
            "min_confirmed": int(min_confirmed),
            "original_size": original_size,
            "final_size": int(len(strict_unknown_df.index)),
            "strict_unknown_fallback_count": int(len(strict_unknown_df.index)),
        }

    # unknown_ratio_cap=0.5 → unknown_slots = confirmed_count
    try:
        cap = float(unknown_ratio_cap)
    except (TypeError, ValueError):
        cap = 0.5
    if cap <= 0 or cap >= 1:
        unknown_slots = 0
    else:
        unknown_slots = int(confirmed_count * cap / (1.0 - cap))
    unknown_slots = max(0, min(unknown_slots, int(len(unknown_df.index))))
    kept_unknown_df = unknown_df.head(unknown_slots)

    shrunk = pd.concat([confirmed_df, kept_unknown_df]).sort_values(
        by=["扩展优先排序得分_num", "投资优先池得分_num", "正式投资评分_num", "基金代码"],
        ascending=[False, False, False, True],
        kind="mergesort",
    ).reset_index(drop=True)

    return shrunk, {
        "group_label": group_label,
        "downgraded": True,
        "reason": "insufficient_confirmed_holding_sample",
        "confirmed_count": confirmed_count,
        "min_confirmed": int(min_confirmed),
        "original_size": original_size,
        "final_size": int(len(shrunk.index)),
        "unknown_slots_allowed": int(unknown_slots),
    }


def _build_portfolio_suggestions(
    df: pd.DataFrame,
    *,
    tracker_features_df: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    base_columns = [
        "组合名称",
        "建议权重",
        "基金代码",
        "基金名称",
        "基金公司",
        FORMAL_GROUP_LABEL,
        "持有期状态",
        "债基风格标签",
        "基金规模(亿)",
        "最新净值日期",
        "费率合计(%)",
        FORMAL_SCORE_LABEL,
        "投资优先池得分",
        "项目质量信心",
        "出现次数",
        "出现频率(%)",
        "1年-年化收益(%)",
        "近1月(%)",
        "近3月(%)",
        "1年-夏普比率",
        "1年-最大回撤(%)",
        "1年-波动率",
        "AutoStock原始校验状态",
        "长期稳定性标签",
        "入选原因",
        "风险提示",
    ]
    if df.empty:
        return pd.DataFrame(columns=base_columns), {"available": False, "groups": []}

    candidates = _portfolio_candidates(df, tracker_features_df=tracker_features_df)
    if not candidates:
        return pd.DataFrame(columns=base_columns), {"available": False, "groups": []}

    quality_gated_candidates, quality_gate_summary = _apply_priority_pool_quality_gate(candidates)
    if not quality_gated_candidates:
        return pd.DataFrame(columns=base_columns), {"available": False, "groups": []}

    candidates = quality_gated_candidates
    unique_candidate_count = len(candidates)
    target_count, target_policy = _compute_priority_pool_target_count(unique_candidate_count)

    selected = _select_diversified_funds(candidates, target_count=target_count, max_per_company=PRIORITY_POOL_COMPANY_CAP)
    if not selected:
        return pd.DataFrame(columns=base_columns), {"available": False, "groups": []}

    selected_df = pd.DataFrame(selected).copy()
    selected_df["投资优先池得分_num"] = pd.to_numeric(selected_df.get("投资优先池得分_num"), errors="coerce").fillna(-999.0)
    selected_df["正式投资评分_num"] = pd.to_numeric(selected_df.get("正式投资评分_num"), errors="coerce").fillna(-999.0)
    selected_df = selected_df.sort_values(
        by=["投资优先池得分_num", "正式投资评分_num", "基金代码"],
        ascending=[False, False, True],
        kind="mergesort",
    ).reset_index(drop=True)
    selected_df["优先级序号"] = range(1, len(selected_df.index) + 1)

    core_count = min(
        len(selected_df.index),
        max(
            PRIORITY_POOL_MIN_CORE_COUNT,
            min(PRIORITY_POOL_MAX_CORE_COUNT, int(round(len(selected_df.index) * 0.30))),
        ),
    )
    if len(selected_df.index) <= PRIORITY_POOL_MIN_CORE_COUNT:
        core_count = len(selected_df.index)

    selected_df["核心优先排序得分_num"] = selected_df.apply(_portfolio_core_priority_score, axis=1)
    selected_df["扩展优先排序得分_num"] = selected_df.apply(_portfolio_extension_priority_score, axis=1)
    core_priority_df = selected_df.sort_values(
        by=["核心优先排序得分_num", "投资优先池得分_num", "正式投资评分_num", "基金代码"],
        ascending=[False, False, False, True],
        kind="mergesort",
    ).reset_index(drop=True)

    confirmed_mask = core_priority_df.get("持有期状态", pd.Series(dtype=object)).astype(str).eq("已确认<=90天")
    confirmed_core_df = core_priority_df[confirmed_mask].copy()
    fallback_core_df = core_priority_df[~confirmed_mask].copy()

    core_segments: list[pd.DataFrame] = []
    remaining_core_slots = int(core_count)
    for candidate_df in (confirmed_core_df, fallback_core_df):
        if remaining_core_slots <= 0 or candidate_df.empty:
            continue
        picked = candidate_df.head(remaining_core_slots).copy()
        if not picked.empty:
            core_segments.append(picked)
            remaining_core_slots -= int(len(picked.index))

    core_df = (
        pd.concat(core_segments, ignore_index=True)
        if core_segments
        else core_priority_df.head(core_count).copy()
    )
    core_codes = set(core_df.get("基金代码", pd.Series(dtype=object)).astype(str).tolist())

    extension_df = selected_df[~selected_df.get("基金代码", pd.Series(dtype=object)).astype(str).isin(core_codes)].copy()
    extension_df = extension_df.sort_values(
        by=["扩展优先排序得分_num", "投资优先池得分_num", "正式投资评分_num", "基金代码"],
        ascending=[False, False, False, True],
        kind="mergesort",
    ).reset_index(drop=True)
    stable_extension_df = extension_df[extension_df[FORMAL_GROUP_LABEL] == STABLE_BOND_GROUP_LABEL].copy()
    enhanced_extension_df = extension_df[extension_df[FORMAL_GROUP_LABEL] == ENHANCED_BOND_GROUP_LABEL].copy()
    # 推荐页目标是"不遗漏潜在优质基金"。对于真实待确认的持有期样本
    # （名称未识别/通用占位），这里仅保留风险提示与排序惩罚，不再做额外
    # 硬过滤；"未见持有期限制信号" 属于中性状态，不应被当成待确认处罚。
    # 正式主榜本身的 holding_days>90 硬门禁仍然保持不变。
    stable_extension_df, stable_extension_downgrade = _shrink_extension_group_by_confirmed_floor(
        stable_extension_df,
        group_label="纯债稳健扩展",
        min_confirmed=0,
    )
    enhanced_extension_df, enhanced_extension_downgrade = _shrink_extension_group_by_confirmed_floor(
        enhanced_extension_df,
        group_label="增强债弹性扩展",
        min_confirmed=0,
    )
    core_df["组合权重基准得分_num"] = core_df.get("投资优先池得分_num")
    stable_extension_df["组合权重基准得分_num"] = stable_extension_df.get("扩展优先排序得分_num")
    enhanced_extension_df["组合权重基准得分_num"] = enhanced_extension_df.get("扩展优先排序得分_num")

    group_frames = [
        (
            "核心优先池",
            core_df,
            "默认优先研究名单，按投资优先池得分排序，优先保留持续入围、质量信心高且同风格相对排名靠前的基金。",
            None,
        ),
        (
            "纯债稳健扩展",
            stable_extension_df,
            "核心优先池之外的纯债稳健补充池，便于从低波动风格继续扩展备选。",
            stable_extension_downgrade,
        ),
        (
            "增强债弹性扩展",
            enhanced_extension_df,
            "核心优先池之外的增强债补充池,保留收益弹性但继续执行公司上限与母基金去重。",
            enhanced_extension_downgrade,
        ),
    ]

    rows: list[dict[str, Any]] = []
    groups: list[dict[str, Any]] = []
    for group_name, group_df, description, downgrade_info in group_frames:
        if group_df.empty:
            # P2-01 / B2: 即使 df 为空（未知样本被全剔除），仍要暴露 downgrade_info 让前端提示"该组已降档"。
            if downgrade_info and bool(downgrade_info.get("downgraded")):
                groups.append(
                    {
                        "name": group_name,
                        "description": description,
                        "count": 0,
                        "funds": [],
                        "downgrade_info": downgrade_info,
                    }
                )
            continue
        group_df = _assign_portfolio_group_weights(group_df)

        group_rows: list[dict[str, Any]] = []
        for row in group_df.to_dict(orient="records"):
            record = {
                "组合名称": group_name,
                "建议权重": row.get("建议权重", "0.00%"),
                "基金代码": row.get("基金代码", ""),
                "基金名称": row.get("基金名称", ""),
                "基金公司": row.get("基金公司", ""),
                FORMAL_GROUP_LABEL: row.get(FORMAL_GROUP_LABEL, ""),
                "持有期状态": row.get("持有期状态", ""),
                "债基风格标签": row.get("债基风格标签", ""),
                "基金规模(亿)": row.get("基金规模(亿)", ""),
                "最新净值日期": row.get("最新净值日期", ""),
                "费率合计(%)": row.get("费率合计(%)", ""),
                FORMAL_SCORE_LABEL: row.get(FORMAL_SCORE_LABEL, ""),
                "投资优先池得分": row.get("投资优先池得分", ""),
                "项目质量信心": row.get("项目质量信心", ""),
                "出现次数": row.get("出现次数", ""),
                "出现频率(%)": row.get("出现频率(%)", ""),
                "1年-年化收益(%)": row.get("1年-年化收益(%)", ""),
                "近1月(%)": row.get("近1月(%)", ""),
                "近3月(%)": row.get("近3月(%)", ""),
                "1年-夏普比率": row.get("1年-夏普比率", ""),
                "1年-最大回撤(%)": row.get("1年-最大回撤(%)", ""),
                "1年-波动率": row.get("1年-波动率", ""),
                "AutoStock原始校验状态": row.get("AutoStock原始校验状态", ""),
                **{column: row.get(column, "") for column in DATA_VALIDATION_COLUMNS if column in row},
                "长期稳定性标签": row.get("长期稳定性标签", ""),
                "入选原因": row.get("入选原因", ""),
                "风险提示": row.get("风险提示", PRIORITY_POOL_DEFAULT_RISK_TEXT),
            }
            rows.append(record)
            group_rows.append({key: value for key, value in record.items() if key != "组合名称"})

        groups.append(
            {
                "name": group_name,
                "description": description,
                "count": len(group_rows),
                "funds": group_rows,
                "downgrade_info": downgrade_info,
            }
        )

    share_deduped_count = int(
        pd.DataFrame(candidates)["share_key"].astype(str).nunique()
        if candidates
        else 0
    )
    compression_audit = _build_priority_pool_compression_audit(
        candidates,
        target_count=target_count,
        selected=selected,
    )
    quality_gate_payload = quality_gate_summary
    return pd.DataFrame(rows, columns=base_columns), {
        "available": bool(groups),
        "fund_count": int(len(rows)),
        "target_count": int(target_count),
        "candidate_count": int(unique_candidate_count),
        "source_candidate_count": int(quality_gate_summary.get("source_candidate_count") or unique_candidate_count),
        "group_count": int(len(groups)),
        "company_cap": int(PRIORITY_POOL_COMPANY_CAP),
        "share_class_unique_count": int(share_deduped_count),
        "target_policy": target_policy,
        "quality_gate": quality_gate_payload,
        "core_quality_gate": quality_gate_payload,
        "compression_audit": compression_audit,
        "groups": groups,
        "selection_policy": "推荐页先执行正式投资评分+优先池得分双重质量门槛，再在通过质量门的候选内做同母基金折叠、单公司上限与分组分散；展示数量收敛到 24-36 只窄带，优先保证买入质量而不是机械凑满；核心优先池仍优先吸纳持有期已确认<=90天样本，不足部分再由未见持有期限制信号的中性样本补足；对持有期未确认样本（名称未识别/通用占位），优质基金推荐页对持有期未确认样本不再额外硬过滤，只保留显式风险提示与排序惩罚，避免遗漏潜在优质基金。",
    }


def _load_candidate_record_df(path: str | Path | None) -> pd.DataFrame:
    payload = _read_json_file(path)
    records = payload.get("candidate_records")
    if not isinstance(records, list):
        return pd.DataFrame()

    rows: list[dict[str, Any]] = []
    for record in records:
        if not isinstance(record, dict):
            continue
        pool_status = str(record.get("selected_pool_status") or "").strip()
        if pool_status not in {"eligible_pool", "final_mainlist"}:
            continue
        fcode = _normalize_code(record.get("fcode"))
        if not fcode:
            continue
        rows.append(
            {
                "fcode": fcode,
                "基金名称": record.get("fund_name"),
                "母基金名称": record.get("base_fund_name"),
                FORMAL_SCORE_LABEL: record.get("formal_score") if record.get("formal_score") is not None else record.get(FORMAL_SCORE_LABEL),
                "综合加权得分": record.get("weighted_score"),
                "优选类别": record.get("fund_category"),
                "近期得分": record.get("short_score"),
                "长期得分": record.get("long_score"),
                "autostock_validation_status": record.get("autostock_validation_status"),
                "AutoStock校验备注": "" if record.get("validation_source") and record.get("validation_source") != "autostock" else record.get("autostock_warning_display") or record.get("autostock_validation_reason") or "",
                "核心候选池状态": pool_status,
                "核心主榜是否入选": bool(record.get("final_mainlist_selected")),
            }
        )

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df = _ensure_formal_score_column(df)
    df["综合加权得分"] = pd.to_numeric(df.get("综合加权得分"), errors="coerce")
    df["近期得分"] = pd.to_numeric(df.get("近期得分"), errors="coerce")
    df["长期得分"] = pd.to_numeric(df.get("长期得分"), errors="coerce")
    df = df.sort_values(
        by=[FORMAL_SCORE_LABEL, "综合加权得分", "近期得分", "长期得分"],
        ascending=[False, False, False, False],
        kind="mergesort",
    )
    df = df.drop_duplicates(subset=["fcode"], keep="first").reset_index(drop=True)
    return df


def _read_return_snapshot(db_path: Path) -> pd.DataFrame:
    if not db_path.exists():
        return pd.DataFrame()
    conn = sqlite3.connect(str(db_path))
    try:
        columns = {row[1] for row in conn.execute("PRAGMA table_info(fund_returns)")}
        provenance_select = ", ".join(
            field if field in columns else f"NULL AS {field}" for field in (*RETURN_PUBLIC_FIELDS, *RETURN_VALIDATION_FIELDS)
        )
        query = f"""
        SELECT
            fcode,
            daily_growth,
            latest_nav_date,
            latest_nav,
            {provenance_select},
            return_1w,
            return_1m,
            return_3m,
            return_6m,
            return_1y,
            return_2y,
            return_3y,
            return_ytd,
            return_since
        FROM fund_returns
        """
        df = pd.read_sql_query(query, conn)
    finally:
        conn.close()
    if not df.empty:
        df["fcode"] = df["fcode"].map(_normalize_code)
    df = attach_period_return_status(df)
    return df


def _read_risk_snapshot(db_path: Path) -> pd.DataFrame:
    if not db_path.exists():
        return pd.DataFrame()
    effective_nav_snapshot = load_effective_nav_snapshot(db_path)
    risk_end_date = str(
        effective_nav_snapshot.risk_1y_effective_end_date
        or effective_nav_snapshot.risk_1y_latest_end_date
        or ""
    ).strip()
    if not risk_end_date:
        return pd.DataFrame()
    conn = sqlite3.connect(str(db_path))
    try:
        columns = {
            str(row[1] or "").strip()
            for row in conn.execute("PRAGMA table_info(fund_risk_metrics)").fetchall()
        }
        raw_sharpe_expr = "raw_sharpe_ratio" if "raw_sharpe_ratio" in columns else "sharpe_ratio"
        raw_sortino_expr = "raw_sortino_ratio" if "raw_sortino_ratio" in columns else "sortino_ratio"
        sharpe_cap_hit_expr = "sharpe_ratio_cap_hit" if "sharpe_ratio_cap_hit" in columns else "NULL"
        sortino_cap_hit_expr = "sortino_ratio_cap_hit" if "sortino_ratio_cap_hit" in columns else "NULL"
        provenance_select = ', '.join(
            f"risk.{field} AS {field}" if field in columns else f"NULL AS {field}"
            for field in ('max_drawdown_duration', 'start_date', 'nav_snapshot_signature', 'calculated_at', 'data_points', 'formula_contract_json')
        )
        risk_ready_predicate = _risk_metric_ready_predicate(conn, "risk")
        latest_risk_rowid_predicate = _latest_risk_rowid_predicate("risk", "latest")
        query = """
        SELECT
            risk.fcode,
            risk.end_date,
            {provenance_select},
            risk.period,
            risk.volatility,
            risk.max_drawdown,
            risk.annual_return,
            risk.sharpe_ratio,
            risk.sortino_ratio,
            {raw_sharpe_expr} AS raw_sharpe_ratio,
            {raw_sortino_expr} AS raw_sortino_ratio,
            {sharpe_cap_hit_expr} AS sharpe_ratio_cap_hit,
            {sortino_cap_hit_expr} AS sortino_ratio_cap_hit,
            risk.calmar_ratio,
            risk.var_95,
            risk.var_99,
            risk.positive_ratio
        FROM fund_risk_metrics risk
        WHERE risk.period = '1y'
          AND risk.end_date = ?
          AND {latest_risk_rowid_predicate}
          AND {risk_ready_predicate}
        """.format(
            raw_sharpe_expr=f"risk.{raw_sharpe_expr}" if raw_sharpe_expr != "sharpe_ratio" else "risk.sharpe_ratio",
            raw_sortino_expr=f"risk.{raw_sortino_expr}" if raw_sortino_expr != "sortino_ratio" else "risk.sortino_ratio",
            sharpe_cap_hit_expr=f"risk.{sharpe_cap_hit_expr}" if sharpe_cap_hit_expr != "NULL" else "NULL",
            sortino_cap_hit_expr=f"risk.{sortino_cap_hit_expr}" if sortino_cap_hit_expr != "NULL" else "NULL",
            provenance_select=provenance_select,
            latest_risk_rowid_predicate=latest_risk_rowid_predicate,
            risk_ready_predicate=risk_ready_predicate,
        )
        df = pd.read_sql_query(query, conn, params=(risk_end_date,))
    finally:
        conn.close()
    if not df.empty:
        df["fcode"] = df["fcode"].map(_normalize_code)
        df["raw_sharpe_ratio"] = pd.to_numeric(df.get("raw_sharpe_ratio"), errors="coerce")
        df["raw_sortino_ratio"] = pd.to_numeric(df.get("raw_sortino_ratio"), errors="coerce")
        df["sharpe_ratio"] = pd.to_numeric(df.get("sharpe_ratio"), errors="coerce")
        df["sortino_ratio"] = pd.to_numeric(df.get("sortino_ratio"), errors="coerce")
        df["sharpe_ratio_cap_hit"] = pd.to_numeric(df.get("sharpe_ratio_cap_hit"), errors="coerce").fillna(0).astype(int)
        df["sortino_ratio_cap_hit"] = pd.to_numeric(df.get("sortino_ratio_cap_hit"), errors="coerce").fillna(0).astype(int)
    return df


def _build_risk_freshness(effective_nav_snapshot: dict[str, Any]) -> dict[str, Any]:
    candidate_date = str(effective_nav_snapshot.get("candidate_date") or "").strip()
    latest_end_date = str(
        effective_nav_snapshot.get("risk_1y_latest_end_date")
        or effective_nav_snapshot.get("risk_1y_max_end_date")
        or ""
    ).strip()
    effective_end_date = str(effective_nav_snapshot.get("risk_1y_effective_end_date") or "").strip()
    candidate_fund_count = int(effective_nav_snapshot.get("candidate_fund_count") or 0)
    risk_candidate_fund_count = int(
        effective_nav_snapshot.get("risk_candidate_fund_count")
        or candidate_fund_count
        or 0
    )
    latest_fund_count = int(
        effective_nav_snapshot.get("risk_1y_latest_fund_count")
        or effective_nav_snapshot.get("risk_1y_fund_count")
        or 0
    )
    effective_fund_count = int(effective_nav_snapshot.get("risk_1y_effective_fund_count") or 0)
    coverage_ratio_vs_candidate = effective_nav_snapshot.get("risk_1y_effective_coverage_ratio_vs_candidate")
    latest_coverage_ratio_vs_candidate = effective_nav_snapshot.get("risk_1y_coverage_ratio_vs_candidate")
    try:
        parsed_effective_coverage = float(coverage_ratio_vs_candidate) if coverage_ratio_vs_candidate is not None else 0.0
    except (TypeError, ValueError):
        parsed_effective_coverage = 0.0
    try:
        parsed_latest_coverage = (
            float(latest_coverage_ratio_vs_candidate) if latest_coverage_ratio_vs_candidate is not None else 0.0
        )
    except (TypeError, ValueError):
        parsed_latest_coverage = 0.0
    if (
        (coverage_ratio_vs_candidate is None or parsed_effective_coverage <= 0)
        and risk_candidate_fund_count > 0
        and effective_fund_count > 0
    ):
        coverage_ratio_vs_candidate = round(effective_fund_count / risk_candidate_fund_count, 6)
    if (
        (latest_coverage_ratio_vs_candidate is None or parsed_latest_coverage <= 0)
        and risk_candidate_fund_count > 0
        and latest_fund_count > 0
    ):
        latest_coverage_ratio_vs_candidate = round(latest_fund_count / risk_candidate_fund_count, 6)
    try:
        coverage_ratio = float(coverage_ratio_vs_candidate) if coverage_ratio_vs_candidate is not None else 0.0
    except (TypeError, ValueError):
        coverage_ratio = 0.0
    try:
        latest_coverage_ratio = (
            float(latest_coverage_ratio_vs_candidate) if latest_coverage_ratio_vs_candidate is not None else 0.0
        )
    except (TypeError, ValueError):
        latest_coverage_ratio = 0.0

    dates_aligned = bool(candidate_date and effective_end_date and candidate_date == effective_end_date)
    threshold_semantics = build_risk_coverage_threshold_semantics(
        {"coverage_ratio_vs_candidate": coverage_ratio}
    )
    coverage_sufficient = bool(threshold_semantics["coverage_sufficient"])
    aligned = bool(
        effective_nav_snapshot.get("ready")
        and dates_aligned
        and coverage_sufficient
    )
    return {
        "candidate_date": candidate_date,
        "risk_1y_latest_end_date": latest_end_date,
        "risk_1y_latest_fund_count": latest_fund_count,
        "risk_1y_latest_coverage_ratio_vs_candidate": round(latest_coverage_ratio, 6) if latest_coverage_ratio > 0 else 0.0,
        "risk_1y_effective_end_date": effective_end_date,
        "risk_1y_effective_fund_count": effective_fund_count,
        "candidate_fund_count": risk_candidate_fund_count,
        "effective_nav_candidate_fund_count": candidate_fund_count,
        **threshold_semantics,
        "dates_aligned": dates_aligned,
        "coverage_sufficient": coverage_sufficient,
        "aligned": aligned,
        "reason": (
            "aligned"
            if aligned
            else (
                "risk_end_date_not_aligned"
                if not dates_aligned and not coverage_sufficient
                else (
                    "risk_effective_snapshot_lagging"
                    if not dates_aligned
                    else "risk_coverage_below_threshold"
                )
            )
        ),
    }


def _select_unique_columns(df: pd.DataFrame, preferred: list[str]) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    selected_positions: list[int] = []
    selected_names: set[str] = set()
    for column in preferred:
        if column in selected_names:
            continue
        matches = [idx for idx, name in enumerate(df.columns) if name == column]
        if not matches:
            continue
        selected_positions.append(matches[0])
        selected_names.add(column)
    if not selected_positions:
        return pd.DataFrame(index=df.index)
    return df.iloc[:, selected_positions].copy()


def _read_latest_accum_nav(db_path: Path) -> pd.DataFrame:
    if not db_path.exists():
        return pd.DataFrame()
    conn = sqlite3.connect(str(db_path))
    try:
        archive_binding = load_latest_nav_history_archive_binding()
        if not nav_history_archive_binding_matches_database(archive_binding, db_path):
            archive_binding = {}
        archive_attach = attach_nav_history_archive_if_available(
            conn,
            archive_path=archive_binding.get("archive_path"),
            archive_table_name=archive_binding.get("archive_table_name") or "fund_nav_history_archive",
            archive_cutoff_date=archive_binding.get("archive_cutoff_date") or "",
        )
        hot_frame = _read_latest_accum_nav_from_sql_source(
            conn,
            source_sql="fund_nav_history",
            source_order=1,
        )
        frames = [hot_frame]
        if archive_attach.get("attached"):
            archive_alias = str(archive_attach.get("alias") or "nav_archive_db").strip()
            archive_table_name = str(
                archive_attach.get("archive_table_name") or "fund_nav_history_archive"
            ).strip()
            if _is_safe_sql_identifier(archive_alias) and _is_safe_sql_identifier(archive_table_name):
                archive_source_sql = f"{archive_alias}.{archive_table_name}"
                archive_cutoff_date = str(
                    archive_attach.get("archive_cutoff_date") or ""
                ).strip()
                cold_candidate_codes: set[str] | None = None
                if archive_cutoff_date:
                    # 冷库严格只含 cutoff 前记录；热库某代码已有 cutoff 及之后记录时，
                    # 冷记录不可能成为该代码的 latest。先读窄索引中的 distinct code，
                    # 再只对真正需要的少量代码做 MAX(nav_date)，避免每次报告都聚合整座冷库。
                    cold_codes = _read_distinct_nav_codes_from_sql_source(
                        conn,
                        source_sql=archive_source_sql,
                        max_nav_date_exclusive=archive_cutoff_date,
                    )
                    authoritative_hot_codes = {
                        _normalize_code(row.get("fcode"))
                        for row in hot_frame.to_dict(orient="records")
                        if _normalize_code(row.get("fcode"))
                        and str(row.get("nav_date") or "") >= archive_cutoff_date
                    }
                    cold_candidate_codes = cold_codes - authoritative_hot_codes
                frames.append(
                    _read_latest_accum_nav_from_sql_source(
                        conn,
                        source_sql=archive_source_sql,
                        source_order=0,
                        max_nav_date_exclusive=archive_cutoff_date,
                        include_fcodes=cold_candidate_codes,
                    )
                )
        df = pd.concat([frame for frame in frames if frame is not None and not frame.empty], ignore_index=True)
    finally:
        if "archive_attach" in locals() and archive_attach.get("attached"):
            detach_nav_history_archive(
                conn,
                alias=str(archive_attach.get("alias") or "nav_archive_db"),
            )
        conn.close()
    if not df.empty:
        df["fcode"] = df["fcode"].map(_normalize_code)
        df = df[df["fcode"].astype(str).str.strip() != ""].copy()
        df = df.sort_values(by=["fcode", "nav_date", "source_order"], kind="mergesort")
        df = df.drop_duplicates(subset=["fcode"], keep="last")
        df = df[["fcode", "accum_nav"]].reset_index(drop=True)
    return df


def _is_safe_sql_identifier(value: object) -> bool:
    text = str(value or "").strip()
    if not text:
        return False
    safe_chars = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_")
    return not any(ch not in safe_chars for ch in text)


def _read_latest_accum_nav_from_sql_source(
    conn: sqlite3.Connection,
    *,
    source_sql: str,
    source_order: int,
    max_nav_date_exclusive: str = "",
    include_fcodes: set[str] | None = None,
) -> pd.DataFrame:
    hot_summary_frame = _read_latest_accum_nav_from_hot_summary(
        conn,
        source_sql=source_sql,
        source_order=source_order,
        max_nav_date_exclusive=max_nav_date_exclusive,
    )
    if hot_summary_frame is not None:
        return hot_summary_frame

    where_predicates: list[str] = []
    params: list[Any] = []
    cutoff = str(max_nav_date_exclusive or "").strip()
    if cutoff:
        where_predicates.append("nav_date < ?")
        params.append(cutoff)
    if include_fcodes is not None:
        normalized_codes = sorted(
            {
                _normalize_code(code)
                for code in include_fcodes
                if _normalize_code(code)
            }
        )
        if not normalized_codes:
            return pd.DataFrame(columns=["fcode", "nav_date", "accum_nav", "source_order"])
        placeholders = ",".join("?" for _ in normalized_codes)
        where_predicates.append(f"fcode IN ({placeholders})")
        params.extend(normalized_codes)
    where_sql = f"WHERE {' AND '.join(where_predicates)}" if where_predicates else ""
    query = f"""
        SELECT n.fcode, latest.nav_date, n.accum_nav, ? AS source_order
        FROM {source_sql} n
        INNER JOIN (
            SELECT fcode, MAX(nav_date) AS nav_date
            FROM {source_sql}
            {where_sql}
            GROUP BY fcode
        ) latest
            ON latest.fcode = n.fcode AND latest.nav_date = n.nav_date
        """
    return pd.read_sql_query(query, conn, params=(int(source_order), *params))


def _read_distinct_nav_codes_from_sql_source(
    conn: sqlite3.Connection,
    *,
    source_sql: str,
    max_nav_date_exclusive: str = "",
) -> set[str]:
    """Read only the narrow code index needed to route latest-NAV cold lookups."""
    cutoff = str(max_nav_date_exclusive or "").strip()
    where_sql = "WHERE nav_date < ?" if cutoff else ""
    params: tuple[Any, ...] = (cutoff,) if cutoff else ()
    rows = conn.execute(
        f"SELECT DISTINCT fcode FROM {source_sql} {where_sql}",
        params,
    ).fetchall()
    return {
        code
        for code in (_normalize_code(row[0]) for row in rows)
        if code
    }


def _read_latest_accum_nav_from_hot_summary(
    conn: sqlite3.Connection,
    *,
    source_sql: str,
    source_order: int,
    max_nav_date_exclusive: str = "",
) -> pd.DataFrame | None:
    if source_sql != "fund_nav_history" or str(max_nav_date_exclusive or "").strip():
        return None
    if not _sqlite_table_exists(conn, "fund_returns"):
        return None
    return_columns = _sqlite_table_columns(conn, "fund_returns")
    if not {"fcode", "latest_nav_date"}.issubset(return_columns):
        return None

    query = """
        SELECT
            r.fcode,
            CASE
                WHEN h_summary.nav_date IS NOT NULL
                  AND (h_latest.nav_date IS NULL OR h_summary.nav_date >= h_latest.nav_date)
                THEN h_summary.nav_date
                ELSE h_latest.nav_date
            END AS nav_date,
            CASE
                WHEN h_summary.nav_date IS NOT NULL
                  AND (h_latest.nav_date IS NULL OR h_summary.nav_date >= h_latest.nav_date)
                THEN h_summary.accum_nav
                ELSE h_latest.accum_nav
            END AS accum_nav,
            ? AS source_order
        FROM fund_returns r
        LEFT JOIN fund_nav_history h_summary
            ON h_summary.fcode = r.fcode
           AND h_summary.nav_date = r.latest_nav_date
        LEFT JOIN fund_nav_history h_latest
            ON h_latest.fcode = r.fcode
           AND h_latest.nav_date = (
                SELECT h2.nav_date
                FROM fund_nav_history h2
                WHERE h2.fcode = r.fcode
                ORDER BY h2.nav_date DESC
                LIMIT 1
           )
        WHERE h_summary.nav_date IS NOT NULL
           OR h_latest.nav_date IS NOT NULL
        """
    try:
        df = pd.read_sql_query(query, conn, params=(int(source_order),))
    except (sqlite3.Error, pd.errors.DatabaseError):
        return None
    if df.empty:
        return df

    try:
        orphan_df = _read_latest_accum_nav_orphan_hot_rows(conn, source_order=source_order)
        if orphan_df is None:
            return None
        if not orphan_df.empty:
            df = pd.concat([df, orphan_df], ignore_index=True)
        returns_with_history_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM fund_returns r
            WHERE EXISTS (
                SELECT 1
                FROM fund_nav_history h
                WHERE h.fcode = r.fcode
                LIMIT 1
            )
            """
        ).fetchone()[0]
    except sqlite3.Error:
        return None
    expected_fund_count = int(returns_with_history_count or 0) + int(orphan_df["fcode"].nunique())
    if expected_fund_count != int(df["fcode"].nunique()):
        return None
    return df


def _read_latest_accum_nav_orphan_hot_rows(
    conn: sqlite3.Connection,
    *,
    source_order: int,
) -> pd.DataFrame | None:
    query = """
        WITH orphan_codes AS (
            SELECT h.fcode
            FROM (
                SELECT DISTINCT fcode
                FROM fund_nav_history
            ) h
            WHERE NOT EXISTS (
                SELECT 1
                FROM fund_returns r
                WHERE r.fcode = h.fcode
            )
        )
        SELECT n.fcode, n.nav_date, n.accum_nav, ? AS source_order
        FROM orphan_codes o
        JOIN fund_nav_history n
            ON n.fcode = o.fcode
           AND n.nav_date = (
                SELECT h2.nav_date
                FROM fund_nav_history h2
                WHERE h2.fcode = o.fcode
                ORDER BY h2.nav_date DESC
                LIMIT 1
           )
        """
    try:
        return pd.read_sql_query(query, conn, params=(int(source_order),))
    except (sqlite3.Error, pd.errors.DatabaseError):
        return None


def _read_pick_snapshot_metrics(db_path: Path) -> pd.DataFrame:
    if not db_path.exists():
        return pd.DataFrame(columns=["fcode", "max_drawdown_duration"])
    conn = sqlite3.connect(str(db_path))
    try:
        df = _read_pick_snapshot_metrics_from_materialized(conn)
        if df is not None:
            return df
        df = _read_pick_snapshot_metrics_from_detail(conn)
    finally:
        conn.close()

    return df


def _sqlite_table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1",
        (table_name,),
    ).fetchone()
    return row is not None


def _sqlite_table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    try:
        return {
            str(row[1] or "").strip()
            for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        }
    except sqlite3.Error:
        return set()


def _pick_metric_snapshot_materialized_available(conn: sqlite3.Connection) -> bool:
    if not (
        _sqlite_table_exists(conn, "screening_picks")
        and _sqlite_table_exists(conn, "screening_pick_metric_snapshots")
    ):
        return False

    columns = _sqlite_table_columns(conn, "screening_pick_metric_snapshots")
    required = {"fcode", "fund_type", "period_years", "pick_id", "max_drawdown_duration"}
    return required.issubset(columns)


def _read_pick_snapshot_metrics_from_materialized(conn: sqlite3.Connection) -> pd.DataFrame | None:
    if not _pick_metric_snapshot_materialized_available(conn):
        return None

    query = """
    WITH latest AS (
        SELECT fcode, MAX(id) AS max_pick_id
        FROM screening_picks
        WHERE fund_type = 'bond'
          AND period_years >= 0.9999
          AND period_years <= 1.0001
        GROUP BY fcode
    )
    SELECT
        latest.fcode,
        latest.max_pick_id,
        m.pick_id AS metric_pick_id,
        m.max_drawdown_duration
    FROM latest
    LEFT JOIN screening_pick_metric_snapshots m
        ON m.pick_id = latest.max_pick_id
    """
    try:
        df = pd.read_sql_query(query, conn)
    except (sqlite3.Error, pd.errors.DatabaseError):
        return None
    if df.empty:
        return pd.DataFrame(columns=["fcode", "max_drawdown_duration"])
    if df.get("metric_pick_id").isna().any():
        return None
    df["fcode"] = df["fcode"].map(_normalize_code)
    df["max_drawdown_duration"] = pd.to_numeric(
        df.get("max_drawdown_duration"),
        errors="coerce",
    )
    return df[["fcode", "max_drawdown_duration"]]


def _read_pick_snapshot_metrics_from_detail(conn: sqlite3.Connection) -> pd.DataFrame:
    if not _sqlite_table_exists(conn, "screening_picks"):
        return pd.DataFrame(columns=["fcode", "max_drawdown_duration"])
    try:
        query = """
        SELECT p.fcode, p.snapshot_json
        FROM screening_picks p
        INNER JOIN (
            SELECT fcode, MAX(id) AS max_id
            FROM screening_picks
            WHERE fund_type = 'bond'
              AND period_years >= 0.9999
              AND period_years <= 1.0001
            GROUP BY fcode
        ) latest
            ON latest.max_id = p.id
        """
        df = pd.read_sql_query(query, conn)
    except (sqlite3.Error, pd.errors.DatabaseError):
        return pd.DataFrame(columns=["fcode", "max_drawdown_duration"])

    rows: list[dict[str, Any]] = []
    for row in df.to_dict(orient="records"):
        fcode = _normalize_code(row.get("fcode"))
        payload = {}
        snapshot_text = decode_snapshot(row.get("snapshot_json"))
        try:
            payload = json.loads(snapshot_text or "{}")
        except (TypeError, ValueError, json.JSONDecodeError):
            payload = {}
        duration = ((payload.get("risk_metrics") or {}).get("max_drawdown_duration"))
        rows.append({"fcode": fcode, "max_drawdown_duration": duration})
    result = pd.DataFrame(rows)
    if result.empty:
        return pd.DataFrame(columns=["fcode", "max_drawdown_duration"])
    result["max_drawdown_duration"] = pd.to_numeric(
        result.get("max_drawdown_duration"),
        errors="coerce",
    )
    return result[["fcode", "max_drawdown_duration"]]


def _build_support_frame(long_df: pd.DataFrame, short_df: pd.DataFrame) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []

    if not long_df.empty:
        current = long_df.copy()
        if "fcode" in current.columns:
            current["fcode"] = current["fcode"].map(_normalize_code)
        frames.append(
            current[
                [
                    column
                    for column in [
                        "fcode",
                        "base_fund_name",
                        "share_class_count",
                        "share_class_names",
                        "share_class_codes",
                    ]
                    if column in current.columns
                ]
            ].drop_duplicates(subset=["fcode"])
        )

    if not short_df.empty:
        current = short_df.copy()
        if "fcode" in current.columns:
            current["fcode"] = current["fcode"].map(_normalize_code)
        frames.append(
            current[
                [
                    column
                    for column in [
                        "fcode",
                        "base_fund_name",
                        "share_class_count",
                        "share_class_names",
                        "share_class_codes",
                    ]
                    if column in current.columns
                ]
            ].drop_duplicates(subset=["fcode"])
        )

    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    combined = combined.sort_values(by=["fcode"], kind="mergesort").drop_duplicates(subset=["fcode"], keep="first")
    return combined


def _build_share_family_support_frame(metadata_df: pd.DataFrame) -> pd.DataFrame:
    """基于基金元数据构造全量母基金家族口径，供候选池与审计统一复用。"""
    if metadata_df.empty or "fcode" not in metadata_df.columns:
        return pd.DataFrame(
            columns=[
                "fcode",
                "base_fund_name",
                "share_class_count",
                "share_class_names",
                "share_class_codes",
                "family_scale_billion",
                "family_scale_report_date",
            ]
        )

    current = metadata_df.copy()
    current["fcode"] = current.get("fcode", pd.Series(dtype=object)).map(_normalize_code)
    current["shortname"] = current.get("shortname", pd.Series("", index=current.index)).fillna("").astype(str).str.strip()
    current["base_fund_name"] = current["shortname"].map(normalize_base_fund_name)
    current["company_family_key"] = current.get("company_id", pd.Series("", index=current.index)).fillna("").astype(str).str.strip()
    empty_company_mask = current["company_family_key"].eq("")
    if "company_name" in current.columns:
        current.loc[empty_company_mask, "company_family_key"] = (
            current.loc[empty_company_mask, "company_name"].fillna("").astype(str).str.strip()
        )
    current["family_key"] = (
        current["company_family_key"].fillna("").astype(str).str.strip()
        + "||"
        + current["base_fund_name"].fillna("").astype(str).str.strip()
    )
    current["scale_billion"] = pd.to_numeric(current.get("scale_billion"), errors="coerce")
    current["scale_report_date_dt"] = pd.to_datetime(current.get("scale_report_date"), errors="coerce")

    def _join_unique_text(series: pd.Series) -> str:
        values: list[str] = []
        seen: set[str] = set()
        for value in series.tolist():
            text = str(value or "").strip()
            if not text or text in seen:
                continue
            values.append(text)
            seen.add(text)
        return " / ".join(values)

    def _sum_complete_family_scale(series: pd.Series) -> float:
        values = pd.to_numeric(series, errors="coerce")
        if values.empty or values.isna().any():
            return math.nan
        return float(values.sum())

    family_df = (
        current.groupby("family_key", dropna=False)
        .agg(
            base_fund_name=("base_fund_name", "first"),
            share_class_count=("fcode", lambda series: int(series.dropna().astype(str).str.strip().replace("", pd.NA).dropna().nunique())),
            share_class_names=("shortname", _join_unique_text),
            share_class_codes=("fcode", _join_unique_text),
            family_scale_billion=("scale_billion", _sum_complete_family_scale),
            family_scale_report_date=("scale_report_date_dt", "max"),
        )
        .reset_index()
    )
    family_df["share_class_count"] = pd.to_numeric(family_df.get("share_class_count"), errors="coerce").fillna(1).astype(int)
    family_df["family_scale_report_date"] = family_df["family_scale_report_date"].dt.strftime("%Y-%m-%d").fillna("")

    return current[["fcode", "family_key"]].merge(
        family_df,
        on="family_key",
        how="left",
    )[
        [
            "fcode",
            "base_fund_name",
            "share_class_count",
            "share_class_names",
            "share_class_codes",
            "family_scale_billion",
            "family_scale_report_date",
        ]
    ].drop_duplicates(subset=["fcode"], keep="first")


def _is_bond_fund_row(row: dict[str, Any]) -> bool:
    for key in ["ftype", "sub_type", "type_category"]:
        value = str(row.get(key) or "").strip()
        if "债" in value:
            return True
        if key == "ftype" and "固收" in value:
            return True
        if key == "type_category" and value.lower() in {"fixed_income", "bond", "bond_fund"}:
            return True
    return False


def _project_style_floor(style: str) -> dict[str, float]:
    return project_style_floor_impl(style)


def _combined_quality_floor(style: str) -> dict[str, float]:
    return combined_quality_floor_impl(style)


def _percentile_rank(series: pd.Series, value: float | None, *, higher_is_better: bool) -> float | None:
    current = pd.to_numeric(series, errors="coerce").dropna()
    if value is None or current.empty:
        return None
    if higher_is_better:
        return float((current <= value).mean() * 100)
    return float((current >= value).mean() * 100)


def _rank_percentile_series(
    series: pd.Series,
    *,
    higher_is_better: bool,
) -> pd.Series:
    current = pd.to_numeric(series, errors="coerce")
    if current.dropna().empty:
        return pd.Series(index=current.index, dtype=float)
    return current.rank(method="max", pct=True, ascending=higher_is_better) * 100.0


def _mean_or_none(values: list[float | None]) -> float | None:
    numeric = [float(value) for value in values if value is not None]
    if not numeric:
        return None
    return float(sum(numeric) / len(numeric))


def _safe_int_text(value: Any) -> str:
    numeric = _safe_float(value)
    if numeric is None:
        return ""
    return str(int(numeric))


def _style_relative_thresholds(group: pd.DataFrame, style: str) -> dict[str, float]:
    return style_relative_thresholds_impl(group, style)


def _format_project_score_mode(style: str) -> str:
    style_text = str(style or "").strip() or "未知"
    return f"项目自有评分/{style_text}"


def _build_project_owned_candidate_universe(
    *,
    metadata_df: pd.DataFrame,
    manager_metrics_df: pd.DataFrame,
    returns_df: pd.DataFrame,
    risk_df: pd.DataFrame,
    long_term_feature_df: pd.DataFrame,
    accum_nav_df: pd.DataFrame,
    pick_metrics_df: pd.DataFrame | None = None,
    support_df: pd.DataFrame,
    decision_map: dict[str, dict[str, Any]],
) -> pd.DataFrame:
    if metadata_df.empty:
        return pd.DataFrame()

    universe = metadata_df.copy()
    universe = universe[universe.apply(lambda row: _is_bond_fund_row(row.to_dict()), axis=1)].copy()
    if universe.empty:
        return universe

    share_family_support_df = _build_share_family_support_frame(universe)

    for extra_df in [
        manager_metrics_df,
        returns_df,
        risk_df,
        long_term_feature_df,
        accum_nav_df,
        share_family_support_df,
        support_df,
    ]:
        if isinstance(extra_df, pd.DataFrame) and not extra_df.empty and "fcode" in extra_df.columns:
            universe = universe.merge(extra_df, on="fcode", how="left", suffixes=("", "_dup"))

    for column in ["base_fund_name", "share_class_names", "share_class_codes"]:
        dup_column = f"{column}_dup"
        if column in universe.columns and dup_column in universe.columns:
            primary = universe[column].fillna("").astype(str)
            fallback = universe[dup_column].fillna("").astype(str)
            blank_mask = primary.str.strip().eq("")
            universe[column] = primary
            universe.loc[blank_mask, column] = fallback.loc[blank_mask]
    if "share_class_count" in universe.columns and "share_class_count_dup" in universe.columns:
        primary_count = pd.to_numeric(universe["share_class_count"], errors="coerce")
        fallback_count = pd.to_numeric(universe["share_class_count_dup"], errors="coerce")
        universe["share_class_count"] = primary_count.where(primary_count.notna(), fallback_count)
    if "family_scale_billion" in universe.columns:
        universe["family_scale_billion"] = pd.to_numeric(universe.get("family_scale_billion"), errors="coerce")
    if "scale_billion" in universe.columns:
        universe["scale_billion"] = pd.to_numeric(universe.get("scale_billion"), errors="coerce")
    universe["effective_scale_billion"] = universe.get("family_scale_billion").combine_first(universe.get("scale_billion"))
    if "family_scale_report_date" in universe.columns:
        universe["family_scale_report_date"] = universe.get("family_scale_report_date").fillna("").astype(str)

    universe["基金代码"] = universe["fcode"]
    universe["基金名称"] = universe.get("shortname").fillna("")
    universe["基金类型"] = universe.get("ftype").fillna("")
    universe["风险等级"] = universe.get("risk_level")
    universe["成立日期"] = universe.get("estab_date")
    universe["基金规模(亿)"] = universe.get("scale_billion")
    universe["合并规模(亿)"] = universe.get("effective_scale_billion")
    universe["基金公司"] = universe.get("company_name")
    universe["基金经理"] = universe.get("manager_names")
    universe["数据来源"] = universe.get("data_source")
    universe = attach_period_return_status(universe, mask_unknown=True)
    for source_column, display_column in RETURN_PUBLIC_FIELDS.items():
        universe[display_column] = universe.get(source_column)
    universe["近1周(%)"] = universe.get("return_1w")
    universe["近1月(%)"] = universe.get("return_1m")
    universe["近3月(%)"] = universe.get("return_3m")
    universe["近6月(%)"] = universe.get("return_6m")
    universe["近1年(%)"] = universe.get("return_1y")
    universe["近2年(%)"] = universe.get("return_2y")
    universe["近3年(%)"] = universe.get("return_3y")
    universe["今年来(%)"] = universe.get("return_ytd")
    universe["成立来(%)"] = universe.get("return_since")
    universe["1年-波动率"] = universe.get("volatility")
    universe["1年-最大回撤(%)"] = universe.get("max_drawdown")
    universe["1年-年化收益(%)"] = universe.get("annual_return")
    universe["1年-夏普比率"] = universe.get("raw_sharpe_ratio").combine_first(universe.get("sharpe_ratio"))
    universe["1年-索提诺比率"] = universe.get("raw_sortino_ratio").combine_first(universe.get("sortino_ratio"))
    universe["1年-夏普比率(展示封顶)"] = universe.get("sharpe_ratio")
    universe["1年-索提诺比率(展示封顶)"] = universe.get("sortino_ratio")
    universe["1年-夏普cap命中"] = universe.get("sharpe_ratio_cap_hit")
    universe["1年-索提诺cap命中"] = universe.get("sortino_ratio_cap_hit")
    universe["1年-卡玛比率"] = universe.get("calmar_ratio")
    universe["1年-VaR95(%)"] = universe.get("var_95")
    universe["1年-VaR99(%)"] = universe.get("var_99")
    universe["1年-正收益比率"] = universe.get("positive_ratio")
    universe["最新日增长率(%)"] = universe.get("daily_growth")
    universe["最新净值日期"] = universe.get("latest_nav_date")
    universe["最新单位净值"] = universe.get("latest_nav")
    universe["最新累计净值"] = universe.get("accum_nav")
    # A legacy screening pick can belong to a different NAV window. This field
    # must come only from the current risk snapshot, including its NULL state.
    duration_by_code = (
        risk_df.set_index('fcode')['max_drawdown_duration']
        if not risk_df.empty and {'fcode', 'max_drawdown_duration'}.issubset(risk_df.columns)
        else pd.Series(dtype=object)
    )
    universe["1年-最长回撤观测数"] = universe['fcode'].map(duration_by_code)
    base_name_series = universe["base_fund_name"] if "base_fund_name" in universe.columns else pd.Series("", index=universe.index)
    universe["母基金名称"] = base_name_series.fillna(universe.get("fullname")).fillna(universe.get("shortname")).fillna("")
    universe["关联份额数"] = universe.get("share_class_count")
    universe["关联份额名称"] = universe.get("share_class_names")
    universe["关联份额代码"] = universe.get("share_class_codes")
    universe["优选类别"] = "项目自有债基候选池"
    universe["核心候选池状态"] = universe["fcode"].map(
        lambda code: str(
            (decision_map.get(_normalize_code(code), {}) or {}).get("核心候选池状态")
            or (decision_map.get(_normalize_code(code), {}) or {}).get("selected_pool_status")
            or ""
        ).strip()
    )
    universe["核心主榜是否入选"] = universe["fcode"].map(
        lambda code: bool(
            (decision_map.get(_normalize_code(code), {}) or {}).get("核心主榜是否入选")
            or (decision_map.get(_normalize_code(code), {}) or {}).get("final_mainlist_selected")
        )
    )
    style_tags: list[str] = []
    benchmark_families: list[str] = []
    exposure_texts: list[str] = []
    fee_totals: list[float | None] = []
    fee_screening_values: list[float | None] = []
    fee_lower_bounds: list[float | None] = []
    fee_statuses: list[str] = []
    fee_ready_flags: list[bool] = []
    fee_floor_flags: list[bool] = []
    autostock_statuses: list[str] = []
    autostock_notes: list[str] = []
    neutral_validation_rows: list[dict[str, Any] | None] = []
    # P1-02 / B6: 久期 / 信用代理桶派生列的中间容器
    duration_bucket_list: list[str] = []
    proxy_duration_years_list: list[str] = []
    credit_bucket_list: list[str] = []
    sales_fee_validation_map = load_sales_fee_validation_classification_map()
    for row in universe.to_dict(orient="records"):
        style_info = classify_bond_style(
            ftype=row.get("ftype"),
            sub_type=row.get("sub_type"),
            benchmark=row.get("benchmark"),
            investment_target=row.get("investment_target"),
            investment_strategy=row.get("investment_strategy"),
        )
        fee_semantics = evaluate_fee_semantics(
            fcode=row.get("fcode"),
            mgr_fee_rate=row.get("mgr_fee_rate"),
            trust_fee_rate=row.get("trust_fee_rate"),
            sales_fee_rate=row.get("sales_fee_rate"),
            validation_map=sales_fee_validation_map,
        )
        decision = decision_map.get(_normalize_code(row.get("fcode")), {}) or {}
        autostock_status = _normalize_autostock_status(decision.get("autostock_validation_status"))
        autostock_warning = str(
            decision.get("autostock_warning_display")
            or decision.get("autostock_validation_reason")
            or ""
        ).strip()
        style_tags.append(style_info["style_tag"])
        benchmark_families.append(style_info["benchmark_family"])
        exposure_texts.append("是" if style_info["has_equity_convertible_exposure"] else "否")
        fee_totals.append(fee_semantics["exact_total"])
        fee_screening_values.append(fee_semantics["screening_value"])
        fee_lower_bounds.append(fee_semantics["lower_bound"])
        fee_statuses.append(str(fee_semantics["status"]))
        fee_ready_flags.append(bool(fee_semantics["screening_ready"]))
        fee_floor_flags.append(bool(fee_semantics["uses_floor"]))
        source = decision.get("validation_source")
        neutral = bool(source and source != "autostock")
        autostock_statuses.append("" if neutral else _format_autostock_display_status(autostock_status))
        autostock_notes.append("" if neutral else autostock_warning)
        neutral_validation_rows.append({
            "数据校验原始状态": autostock_status or "not_checked",
            "数据校验状态": _format_autostock_display_status(autostock_status),
            "数据校验备注": autostock_warning, "数据校验告警原因": autostock_warning,
            "数据校验来源": source, "数据校验契约版本": decision.get("validation_contract") or "",
        } if neutral else None)
        # P1-02 / B6: 同一循环派生久期 / 信用 / 代理基准久期展示列
        # （不参与评分主链路，由 universe 展示字段直接暴露给前端）。
        duration_proxy = classify_duration_proxy(
            benchmark=row.get("benchmark"),
            style_tag=style_info.get("style_tag"),
        )
        credit_proxy = classify_credit_proxy(
            investment_target=row.get("investment_target"),
            investment_strategy=row.get("investment_strategy"),
        )
        duration_bucket_list.append(duration_proxy.bucket)
        proxy_duration_years_list.append(
            "" if duration_proxy.proxy_years is None else f"{duration_proxy.proxy_years:.1f}"
        )
        credit_bucket_list.append(credit_proxy.bucket)
    universe["债基风格标签"] = style_tags
    universe["基准归类"] = benchmark_families
    universe["是否含权益/转债增强"] = exposure_texts
    universe["费率合计(%)"] = fee_totals
    universe["费率筛选值(%)"] = fee_screening_values
    universe["费率下界(%)"] = fee_lower_bounds
    universe["费率口径"] = fee_statuses
    universe["费率筛选就绪"] = fee_ready_flags
    universe["费率筛选使用下界"] = fee_floor_flags
    universe["AutoStock校验状态"] = autostock_statuses
    universe["AutoStock校验备注"] = autostock_notes
    if any(item is not None for item in neutral_validation_rows):
        for column in DATA_VALIDATION_COLUMNS:
            universe[column] = [item.get(column, "") if item is not None else "" for item in neutral_validation_rows]
    # P1-02 派生列；空值以 "—" 展示避免误解为 0
    universe["久期分档"] = [item if item and item != "unknown" else "—" for item in duration_bucket_list]
    universe["代理基准久期(年)"] = [item if item else "—" for item in proxy_duration_years_list]
    universe["信用分档"] = [item if item and item != "unknown" else "—" for item in credit_bucket_list]
    return _drop_duplicate_columns(universe)


def _build_data_source_audit_summary(
    *,
    official_df: pd.DataFrame,
    internal_watch_df: pd.DataFrame,
) -> dict[str, Any]:
    def _normalized_governance_series(df: pd.DataFrame) -> pd.Series:
        if df.empty:
            return pd.Series(dtype=object)
        column = (
            "data_source_governance_flag"
            if "data_source_governance_flag" in df.columns
            else "数据来源治理标签"
            if "数据来源治理标签" in df.columns
            else ""
        )
        if not column:
            return pd.Series(dtype=object)
        return df.get(column, pd.Series(dtype=object)).fillna("").astype(str).str.strip()

    def _normalized_source_series(df: pd.DataFrame) -> pd.Series:
        if df.empty:
            return pd.Series(dtype=object)
        source_column = "data_source" if "data_source" in df.columns else "数据来源" if "数据来源" in df.columns else ""
        if not source_column:
            return pd.Series(dtype=object)
        return df.get(source_column, pd.Series(dtype=object)).fillna("").astype(str).str.strip()

    def _source_counts(df: pd.DataFrame) -> dict[str, int]:
        series = _normalized_source_series(df)
        if series.empty:
            return {}
        counts = series.replace("", "__BLANK__").value_counts(dropna=False)
        return {str(key): int(value) for key, value in counts.to_dict().items()}

    final_source_series = _normalized_source_series(official_df)
    watch_source_series = _normalized_source_series(internal_watch_df)
    final_governance_series = _normalized_governance_series(official_df)
    watch_governance_series = _normalized_governance_series(internal_watch_df)
    final_blank_df = official_df[final_source_series.eq("")].copy() if not final_source_series.empty else pd.DataFrame()
    unresolved_final_blank_count = int(
        (final_source_series.eq("") & final_governance_series.eq("")).sum()
    ) if not final_source_series.empty and not final_governance_series.empty else int(final_source_series.eq("").sum()) if not final_source_series.empty else 0
    governed_final_blank_count = int(
        (final_source_series.eq("") & final_governance_series.ne("")).sum()
    ) if not final_source_series.empty and not final_governance_series.empty else 0
    unresolved_watch_blank_count = int(
        (watch_source_series.eq("") & watch_governance_series.eq("")).sum()
    ) if not watch_source_series.empty and not watch_governance_series.empty else int(watch_source_series.eq("").sum()) if not watch_source_series.empty else 0
    governed_watch_blank_count = int(
        (watch_source_series.eq("") & watch_governance_series.ne("")).sum()
    ) if not watch_source_series.empty and not watch_governance_series.empty else 0

    final_blank_preview: list[dict[str, Any]] = []
    if not final_blank_df.empty:
        for _, row in final_blank_df.head(20).iterrows():
            final_blank_preview.append(
                {
                    "fcode": _normalize_code(row.get("基金代码") or row.get("fcode")),
                    "fund_name": str(row.get("基金名称") or row.get("shortname") or "").strip(),
                    "data_source_governance_flag": str(
                        row.get("数据来源治理标签") or row.get("data_source_governance_flag") or ""
                    ).strip(),
                }
            )

    final_blank_count = int(final_source_series.eq("").sum()) if not final_source_series.empty else 0
    final_fallback_count = int(
        final_source_series.eq("autostock_directory_fallback").sum()
    ) if not final_source_series.empty else 0

    if final_blank_count > 0:
        status = "blocking"
        status_label = "正式榜存在空来源记录"
        recommended_action = "block_publish_and_backfill_data_source"
    elif final_fallback_count > 0:
        status = "warning"
        status_label = "正式榜存在回退来源记录"
        recommended_action = "review_fallback_source_records"
    else:
        status = "ok"
        status_label = "正式榜来源可追溯"
        recommended_action = "none"

    return {
        "available": True,
        "status": status,
        "status_label": status_label,
        "recommended_action": recommended_action,
        "final_blank_count": final_blank_count,
        "final_blank_unresolved_count": unresolved_final_blank_count,
        "final_blank_governed_residual_count": governed_final_blank_count,
        "final_fallback_count": final_fallback_count,
        "final_source_counts": _source_counts(official_df),
        "watch_blank_count": int(watch_source_series.eq("").sum()) if not watch_source_series.empty else 0,
        "watch_blank_unresolved_count": unresolved_watch_blank_count,
        "watch_blank_governed_residual_count": governed_watch_blank_count,
        "watch_source_counts": _source_counts(internal_watch_df),
        "final_blank_preview": final_blank_preview,
    }


def _build_data_source_lookup(metadata_df: pd.DataFrame) -> dict[str, dict[str, str]]:
    if metadata_df.empty or "fcode" not in metadata_df.columns:
        return {}
    if "data_source" not in metadata_df.columns and "data_source_governance_flag" not in metadata_df.columns:
        return {}
    current = metadata_df.copy()
    current["fcode"] = current["fcode"].map(_normalize_code)
    current["data_source"] = current.get("data_source", pd.Series(dtype=object)).fillna("").astype(str).str.strip()
    current["data_source_governance_flag"] = current.get(
        "data_source_governance_flag",
        pd.Series(dtype=object),
    ).fillna("").astype(str).str.strip()
    current = current[current["fcode"] != ""]
    if current.empty:
        return {}
    current = current.drop_duplicates(subset=["fcode"], keep="last")
    return {
        str(row["fcode"]): {
            "data_source": str(row["data_source"] or "").strip(),
            "data_source_governance_flag": str(row["data_source_governance_flag"] or "").strip(),
        }
        for _, row in current.iterrows()
    }


def _apply_data_source_to_records(
    records: list[dict[str, Any]] | None,
    *,
    data_source_by_code: dict[str, dict[str, str]],
) -> list[dict[str, Any]]:
    normalized_records: list[dict[str, Any]] = []
    for item in records or []:
        current = dict(item or {})
        code = _normalize_code(current.get("基金代码") or current.get("fcode"))
        if code:
            current["基金代码"] = code
            source_payload = data_source_by_code.get(code) or {}
            current["数据来源"] = str(
                source_payload.get("data_source") or current.get("数据来源") or ""
            ).strip()
            current["数据来源治理标签"] = str(
                source_payload.get("data_source_governance_flag")
                or current.get("数据来源治理标签")
                or current.get("data_source_governance_flag")
                or ""
            ).strip()
        else:
            current["数据来源"] = str(current.get("数据来源") or "").strip()
            current["数据来源治理标签"] = str(
                current.get("数据来源治理标签") or current.get("data_source_governance_flag") or ""
            ).strip()
        normalized_records.append(current)
    return normalized_records


def _apply_data_source_to_portfolio_summary(
    portfolio_summary: dict[str, Any] | None,
    *,
    data_source_by_code: dict[str, dict[str, str]],
) -> dict[str, Any]:
    summary = dict(portfolio_summary or {})
    groups = summary.get("groups") or []
    normalized_groups: list[dict[str, Any]] = []
    normalized_funds: list[dict[str, Any]] = []
    for group in groups:
        current_group = dict(group or {})
        group_funds = _apply_data_source_to_records(
            current_group.get("funds") or [],
            data_source_by_code=data_source_by_code,
        )
        current_group["funds"] = group_funds
        normalized_groups.append(current_group)
        normalized_funds.extend(group_funds)
    if normalized_groups:
        summary["groups"] = normalized_groups
        summary["funds"] = normalized_funds
        summary["fund_count"] = int(summary.get("fund_count") or len(normalized_funds))
        summary["group_count"] = int(summary.get("group_count") or len(normalized_groups))
    else:
        summary["funds"] = _apply_data_source_to_records(
            summary.get("funds") or [],
            data_source_by_code=data_source_by_code,
        )
    return summary


def _load_run_local_frame_for_data_source_audit(
    *,
    settings: Settings,
    artifact_path: Any,
    data_source_by_code: dict[str, dict[str, str]],
) -> pd.DataFrame:
    resolved_path = _resolve_project_path(settings, artifact_path)
    if resolved_path is None or not artifact_is_file(resolved_path):
        return pd.DataFrame()
    try:
        current = read_csv_artifact(resolved_path, dtype=str).fillna("")
    except CoreCsvStorageError:
        raise
    except Exception:
        return pd.DataFrame()
    if current.empty:
        return current
    code_column = "基金代码" if "基金代码" in current.columns else "fcode" if "fcode" in current.columns else ""
    if not code_column:
        return current
    normalized_codes = current.get(code_column, pd.Series(dtype=object)).map(_normalize_code)
    current[code_column] = normalized_codes
    current["数据来源"] = normalized_codes.map(
        lambda code: str((data_source_by_code.get(code) or {}).get("data_source") or "").strip()
    )
    current["数据来源治理标签"] = normalized_codes.map(
        lambda code: str(
            (data_source_by_code.get(code) or {}).get("data_source_governance_flag") or ""
        ).strip()
    )
    current["data_source"] = current["数据来源"]
    current["data_source_governance_flag"] = current["数据来源治理标签"]
    return current


def _tracker_current_derived_rows_signature(rows: list[dict[str, Any]]) -> str:
    row_basis: list[dict[str, Any]] = []
    for item in list(rows or []):
        row = dict(item or {})
        row_basis.append(
            {
                "code": _normalize_code(row.get("基金代码")),
                "group": str(row.get(FORMAL_GROUP_LABEL) or "").strip(),
                "style": str(row.get("债基风格标签") or "").strip(),
                "data_source": str(row.get("数据来源") or "").strip(),
            }
        )
    return _stable_json_digest(row_basis)


def _build_tracker_current_prepublish_cache_basis(
    *,
    settings: Settings,
    run_id: str,
    generated_at: str,
    rows: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "schema_version": TRACKER_CURRENT_DERIVED_FIELDS_SCHEMA_VERSION,
        "source": "prepublish_report_builder",
        "run_id": str(run_id or "").strip(),
        "generated_at": str(generated_at or "").strip(),
        "database_file": _sqlite_file_cache_token_for_tracker(settings.database_path),
        "tracker_rows_signature": _tracker_current_derived_rows_signature(rows),
    }


def _load_tracker_sidecar_data_source_map(
    codes: list[str],
    data_source_by_code: dict[str, dict[str, str]] | None,
) -> dict[str, str]:
    result: dict[str, str] = {}
    source_lookup = dict(data_source_by_code or {})
    for code in codes:
        normalized = _normalize_code(code)
        if not normalized:
            continue
        source_payload = source_lookup.get(normalized) or {}
        result[normalized] = str(source_payload.get("data_source") or "").strip()
    return result


def _compute_report_accum_nav_ratio_annualized_return_map(
    settings: Settings,
    codes: list[str],
) -> dict[str, str]:
    normalized_codes: list[str] = []
    seen: set[str] = set()
    for code in codes:
        normalized = _normalize_code(code)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        normalized_codes.append(normalized)
    if not normalized_codes or not settings.database_path.exists():
        return {}

    value_rows = ",".join("(?)" for _ in normalized_codes)
    conn = sqlite3.connect(str(settings.database_path))
    archive_attach: dict[str, Any] = {}
    try:
        try:
            archive_binding = load_latest_nav_history_archive_binding(
                project_root=settings.project_root,
            )
            if not nav_history_archive_binding_matches_database(
                archive_binding,
                settings.database_path,
                project_root=settings.project_root,
            ):
                archive_binding = {}
            archive_attach = attach_nav_history_archive_if_available(
                conn,
                archive_path=archive_binding.get("archive_path"),
                archive_table_name=archive_binding.get("archive_table_name") or "fund_nav_history_archive",
                archive_cutoff_date=archive_binding.get("archive_cutoff_date") or "",
            )
        except Exception:
            archive_attach = {}

        query_params: list[Any] = [*normalized_codes]
        cold_point_ctes = ""
        cold_first_union = ""
        cold_last_union = ""
        if archive_attach.get("attached"):
            archive_alias = str(archive_attach.get("alias") or "nav_archive_db").strip()
            archive_table_name = str(archive_attach.get("archive_table_name") or "fund_nav_history_archive").strip()
            archive_cutoff_date = str(archive_attach.get("archive_cutoff_date") or "").strip()
            safe_chars = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_")
            if (
                archive_alias
                and archive_table_name
                and not any(ch not in safe_chars for ch in archive_alias)
                and not any(ch not in safe_chars for ch in archive_table_name)
            ):
                cold_first_cutoff_clause = ""
                cold_last_cutoff_clause = ""
                if archive_cutoff_date:
                    cold_first_cutoff_clause = "AND h2.nav_date < ?"
                    cold_last_cutoff_clause = "AND h2.nav_date < ?"
                    query_params.extend([archive_cutoff_date, archive_cutoff_date])
                cold_table = f"{archive_alias}.{archive_table_name}"
                cold_point_ctes = f"""
        ,
        cold_first AS (
            SELECT
                c.fcode,
                h.nav_date,
                CASE
                    WHEN h.accum_nav IS NOT NULL AND h.accum_nav > 0 THEN h.accum_nav
                    ELSE NULL
                END AS effective_nav
            FROM requested_codes c
            JOIN {cold_table} h
                ON h.rowid = (
                    SELECT h2.rowid
                    FROM {cold_table} h2
                    WHERE h2.fcode = c.fcode
                      AND (
                          (h2.accum_nav IS NOT NULL AND h2.accum_nav > 0)
                      )
                      {cold_first_cutoff_clause}
                    ORDER BY h2.nav_date ASC, h2.rowid ASC
                    LIMIT 1
                )
        ),
        cold_last AS (
            SELECT
                c.fcode,
                h.nav_date,
                CASE
                    WHEN h.accum_nav IS NOT NULL AND h.accum_nav > 0 THEN h.accum_nav
                    ELSE NULL
                END AS effective_nav
            FROM requested_codes c
            JOIN {cold_table} h
                ON h.rowid = (
                    SELECT h2.rowid
                    FROM {cold_table} h2
                    WHERE h2.fcode = c.fcode
                      AND (
                          (h2.accum_nav IS NOT NULL AND h2.accum_nav > 0)
                      )
                      {cold_last_cutoff_clause}
                    ORDER BY h2.nav_date DESC, h2.rowid DESC
                    LIMIT 1
                )
        )
        """
                cold_first_union = """
            UNION ALL
            SELECT fcode, nav_date, effective_nav, 1 AS source_order
            FROM cold_first
        """
                cold_last_union = """
            UNION ALL
            SELECT fcode, nav_date, effective_nav, 1 AS source_order
            FROM cold_last
        """

        query = f"""
        WITH requested_codes(fcode) AS (
            VALUES {value_rows}
        ),
        hot_first AS (
            SELECT
                c.fcode,
                h.nav_date,
                CASE
                    WHEN h.accum_nav IS NOT NULL AND h.accum_nav > 0 THEN h.accum_nav
                    ELSE NULL
                END AS effective_nav
            FROM requested_codes c
            JOIN fund_nav_history h
                ON h.rowid = (
                    SELECT h2.rowid
                    FROM fund_nav_history h2
                    WHERE h2.fcode = c.fcode
                      AND (
                          (h2.accum_nav IS NOT NULL AND h2.accum_nav > 0)
                      )
                    ORDER BY h2.nav_date ASC, h2.rowid ASC
                    LIMIT 1
                )
        ),
        hot_last AS (
            SELECT
                c.fcode,
                h.nav_date,
                CASE
                    WHEN h.accum_nav IS NOT NULL AND h.accum_nav > 0 THEN h.accum_nav
                    ELSE NULL
                END AS effective_nav
            FROM requested_codes c
            JOIN fund_nav_history h
                ON h.rowid = (
                    SELECT h2.rowid
                    FROM fund_nav_history h2
                    WHERE h2.fcode = c.fcode
                      AND (
                          (h2.accum_nav IS NOT NULL AND h2.accum_nav > 0)
                      )
                    ORDER BY h2.nav_date DESC, h2.rowid DESC
                    LIMIT 1
                )
        )
        {cold_point_ctes},
        first_candidates AS (
            SELECT fcode, nav_date, effective_nav, 0 AS source_order
            FROM hot_first
            {cold_first_union}
        ),
        last_candidates AS (
            SELECT fcode, nav_date, effective_nav, 0 AS source_order
            FROM hot_last
            {cold_last_union}
        ),
        first_nav AS (
            SELECT fc.fcode, fc.nav_date, fc.effective_nav
            FROM first_candidates fc
            WHERE fc.nav_date IS NOT NULL
              AND fc.effective_nav IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1
                  FROM first_candidates other
                  WHERE other.fcode = fc.fcode
                    AND other.nav_date IS NOT NULL
                    AND other.effective_nav IS NOT NULL
                    AND (
                        other.nav_date < fc.nav_date
                        OR (other.nav_date = fc.nav_date AND other.source_order < fc.source_order)
                    )
              )
        ),
        last_nav AS (
            SELECT lc.fcode, lc.nav_date, lc.effective_nav
            FROM last_candidates lc
            WHERE lc.nav_date IS NOT NULL
              AND lc.effective_nav IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1
                  FROM last_candidates other
                  WHERE other.fcode = lc.fcode
                    AND other.nav_date IS NOT NULL
                    AND other.effective_nav IS NOT NULL
                    AND (
                        other.nav_date > lc.nav_date
                        OR (other.nav_date = lc.nav_date AND other.source_order < lc.source_order)
                    )
              )
        )
        SELECT
            f.fcode,
            f.estab_date,
            first_nav.nav_date AS first_nav_date,
            first_nav.effective_nav AS first_nav_value,
            last_nav.nav_date AS last_nav_date,
            last_nav.effective_nav AS last_nav_value
        FROM requested_codes c
        JOIN funds AS f ON f.fcode = c.fcode
        LEFT JOIN first_nav ON first_nav.fcode = f.fcode
        LEFT JOIN last_nav ON last_nav.fcode = f.fcode
        """
        rows = conn.execute(query, query_params).fetchall()
    except sqlite3.Error:
        rows = []
    finally:
        if archive_attach.get("attached"):
            try:
                detach_nav_history_archive(
                    conn,
                    alias=str(archive_attach.get("alias") or "nav_archive_db"),
                )
            except Exception:
                pass
        conn.close()

    result: dict[str, str] = {}
    for row in rows:
        code = _normalize_code(row[0])
        if not code:
            continue
        estab_date = str(row[1] or "").strip()
        first_nav_date = str(row[2] or "").strip()
        first_nav_value = _safe_float(row[3])
        last_nav_date = str(row[4] or "").strip()
        last_nav_value = _safe_float(row[5])
        if not estab_date or not first_nav_date or not last_nav_date:
            continue
        if first_nav_value is None or last_nav_value is None:
            continue
        if first_nav_value <= 0 or last_nav_value <= 0:
            continue
        try:
            estab_dt = datetime.fromisoformat(estab_date)
            first_nav_dt = datetime.fromisoformat(first_nav_date)
            last_nav_dt = datetime.fromisoformat(last_nav_date)
        except ValueError:
            continue
        estab_gap_days = (first_nav_dt.date() - estab_dt.date()).days
        if estab_gap_days > SINCE_INCEPTION_ANNUALIZED_MAX_ESTAB_GAP_DAYS:
            continue
        if estab_gap_days < -SINCE_INCEPTION_ANNUALIZED_NEGATIVE_ESTAB_TOLERANCE_DAYS:
            continue
        elapsed_days = (last_nav_dt.date() - first_nav_dt.date()).days
        if elapsed_days < SINCE_INCEPTION_ANNUALIZED_MIN_HISTORY_DAYS:
            continue
        try:
            annualized_return = (
                (last_nav_value / first_nav_value) ** (SINCE_INCEPTION_ANNUALIZED_YEAR_DAYS / elapsed_days) - 1.0
            ) * 100.0
        except (OverflowError, ZeroDivisionError, ValueError):
            continue
        result[code] = _normalize_number_text(annualized_return, digits=2)
    return result



def _compute_report_since_inception_annualized_return_map(settings: Settings, codes: list[str]) -> dict[str, str]:
    from backend.lifetime_returns import platform_annualized_map
    return platform_annualized_map(settings.database_path, [_normalize_code(code) for code in codes])


def _load_report_since_inception_annual_return_breakdown_map(
    settings: Settings,
    codes: list[str],
) -> dict[str, list[dict[str, Any]]]:
    normalized_codes: list[str] = []
    seen: set[str] = set()
    for code in codes:
        normalized = _normalize_code(code)
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
        code = _normalize_code(row[0])
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
        annual_return = _safe_float(row[3])
        rank_in_category = int(_safe_float(row[4]) or 0) if row[4] is not None else None
        category_count = int(_safe_float(row[5]) or 0) if row[5] is not None else None
        percentile = _safe_float(row[6])
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


def _build_tracker_current_prepublish_derived_fields(
    *,
    settings: Settings,
    run_id: str,
    generated_at: str,
    rows: list[dict[str, Any]],
    data_source_by_code: dict[str, dict[str, str]] | None,
) -> dict[str, Any]:
    codes: list[str] = []
    seen: set[str] = set()
    for row in rows:
        code = _normalize_code((row or {}).get("基金代码"))
        if not code or code in seen:
            continue
        seen.add(code)
        codes.append(code)
    if not codes:
        return {
            "schema_version": TRACKER_CURRENT_DERIVED_FIELDS_SCHEMA_VERSION,
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "cache_basis": _build_tracker_current_prepublish_cache_basis(
                settings=settings,
                run_id=run_id,
                generated_at=generated_at,
                rows=rows,
            ),
            "data_source_by_code": {},
            "annualized_return_by_code": {},
            "annual_return_breakdown_by_code": {},
            "style_metric_references": {},
        }
    return {
        "schema_version": TRACKER_CURRENT_DERIVED_FIELDS_SCHEMA_VERSION,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "cache_basis": _build_tracker_current_prepublish_cache_basis(
            settings=settings,
            run_id=run_id,
            generated_at=generated_at,
            rows=rows,
        ),
        "data_source_by_code": _load_tracker_sidecar_data_source_map(codes, data_source_by_code),
        "annualized_return_by_code": _compute_report_since_inception_annualized_return_map(settings, codes),
        "accum_nav_ratio_annualized_by_code": _compute_report_accum_nav_ratio_annualized_return_map(settings, codes),
        "lifetime_return_observations": load_platform_lifetime_returns(settings.database_path, codes),
        "annual_return_breakdown_by_code": _load_report_since_inception_annual_return_breakdown_map(settings, codes),
        "style_metric_references": {},
    }


def _weighted_project_component(percentiles: pd.DataFrame, components: list) -> tuple[pd.Series, pd.Series]:
    """Missing optional history is not zero; required data remains required."""
    observed = pd.Series(0.0, index=percentiles.index)
    numerator = pd.Series(0.0, index=percentiles.index)
    required_ready = pd.Series(True, index=percentiles.index)
    for metric, weight, _direction in components:
        values = pd.to_numeric(percentiles[metric], errors="coerce")
        observed += values.notna().astype(float) * weight
        numerator += values.fillna(0.0) * weight
        if metric != "return_3y":
            required_ready &= values.notna()
    score = numerator.div(observed.where(observed.gt(0))).where(required_ready)
    return score, observed


def _assign_project_scores(df: pd.DataFrame) -> pd.DataFrame:
    current = df.copy()
    if current.empty:
        return current

    current["费率合计(%)"] = pd.to_numeric(current.get("费率合计(%)"), errors="coerce")
    current["基金规模(亿)"] = pd.to_numeric(current.get("基金规模(亿)"), errors="coerce")
    current["manager_min_total_days"] = pd.to_numeric(current.get("manager_min_total_days"), errors="coerce")
    current["manager_avg_score_overall"] = pd.to_numeric(current.get("manager_avg_score_overall"), errors="coerce")
    current["scale_billion"] = pd.to_numeric(current.get("scale_billion"), errors="coerce")
    current["fee_total_numeric"] = current.apply(
        lambda row: resolve_fee_screening_value(row.to_dict()),
        axis=1,
    )
    style_series = current.get("债基风格标签", pd.Series("", index=current.index)).map(
        lambda value: str(value or "").strip() or "未知"
    )

    current["综合加权得分"] = pd.Series(index=current.index, dtype=float)
    current["近期得分"] = pd.Series(index=current.index, dtype=float)
    current["长期得分"] = pd.Series(index=current.index, dtype=float)
    current["综合评分"] = pd.Series(index=current.index, dtype=float)
    current["评分等级"] = "无评分"
    current["评分模式"] = "项目自有评分"
    current["项目评分说明"] = ""

    for style, group in current.groupby(style_series, dropna=False):
        style_df = group.copy()
        short_percentile_df = pd.DataFrame(index=style_df.index)
        long_percentile_df = pd.DataFrame(index=style_df.index)

        for metric, _weight, higher_is_better in PROJECT_SCORE_COMPONENTS["short_term"]:
            if metric not in style_df.columns:
                short_percentile_df[metric] = pd.Series(index=style_df.index, dtype=float)
                continue
            short_percentile_df[metric] = _rank_percentile_series(
                style_df[metric],
                higher_is_better=higher_is_better,
            )

        for metric, _weight, higher_is_better in PROJECT_SCORE_COMPONENTS["long_term"]:
            if metric not in style_df.columns:
                long_percentile_df[metric] = pd.Series(index=style_df.index, dtype=float)
                continue
            long_percentile_df[metric] = _rank_percentile_series(
                style_df[metric],
                higher_is_better=higher_is_better,
            )

        short_raw, short_coverage = _weighted_project_component(short_percentile_df, PROJECT_SCORE_COMPONENTS["short_term"])
        long_raw, long_coverage = _weighted_project_component(long_percentile_df, PROJECT_SCORE_COMPONENTS["long_term"])
        # These components measure different evidence. Missing required inputs
        # cannot be manufactured by copying the other component. Only optional
        # 3Y history is renormalized inside _weighted_project_component.
        short_component = short_raw
        long_component = long_raw
        weighted = (
            short_component * FORMAL_SCORE_SHORT_TERM_WEIGHT
            + long_component * FORMAL_SCORE_LONG_TERM_WEIGHT
        )

        grade_series = pd.Series("无评分", index=style_df.index, dtype=object)
        grade_series = grade_series.mask(weighted.notna() & weighted.ge(75), "A")
        grade_series = grade_series.mask(weighted.notna() & weighted.lt(75) & weighted.ge(68), "B+")
        grade_series = grade_series.mask(weighted.notna() & weighted.lt(68) & weighted.ge(60), "B")
        grade_series = grade_series.mask(weighted.notna() & weighted.lt(60) & weighted.ge(52), "C")
        grade_series = grade_series.mask(weighted.notna() & weighted.lt(52), "D")

        note_series = pd.Series("", index=style_df.index, dtype=object)
        for row_index in style_df.index:
            note_parts = [
                f"{metric}:{short_percentile_df.at[row_index, metric]:.1f}"
                for metric, _weight, _higher_is_better in PROJECT_SCORE_COMPONENTS["short_term"]
                if metric in short_percentile_df.columns and pd.notna(short_percentile_df.at[row_index, metric])
            ]
            note_series.at[row_index] = "；".join(note_parts[:8])

        current.loc[style_df.index, "综合加权得分"] = weighted.round(4)
        current.loc[style_df.index, "近期得分"] = short_raw.round(4)
        current.loc[style_df.index, "长期得分"] = long_raw.round(4)
        current.loc[style_df.index, "近期评分权重覆盖"] = short_coverage.round(4)
        current.loc[style_df.index, "长期评分权重覆盖"] = long_coverage.round(4)
        current.loc[style_df.index, "综合评分"] = weighted.round(4)
        current.loc[style_df.index, "评分等级"] = grade_series
        current.loc[style_df.index, "评分模式"] = _format_project_score_mode(style)
        current.loc[style_df.index, "项目评分说明"] = note_series

    current[FORMAL_SCORE_LABEL] = current["综合加权得分"]
    return current


def _build_risk_metric_cap_audit(
    df: pd.DataFrame,
    *,
    preview_top_n: int,
) -> dict[str, Any]:
    """审计 raw/capped 风险指标对项目评分与排序的影响。"""
    if not isinstance(df, pd.DataFrame) or df.empty:
        return {
            "fund_count": 0,
            "sharpe_cap_hit_count": 0,
            "sortino_cap_hit_count": 0,
            "either_cap_hit_count": 0,
            "sharpe_cap_hit_ratio": 0.0,
            "sortino_cap_hit_ratio": 0.0,
            "either_cap_hit_ratio": 0.0,
            "top_n": int(preview_top_n),
            "top_n_overlap_count": 0,
            "top_n_changed_count": 0,
            "top_n_only_raw_codes": [],
            "top_n_only_capped_codes": [],
            "top_n_comparable_count": 0,
            "top_n_incomparable_count": 0,
            "style_hit_rows": [],
        }

    current = df.copy()
    current["基金代码"] = current.get("基金代码", current.get("fcode", pd.Series(dtype=object))).astype(str)
    current["债基风格标签"] = current.get("债基风格标签", pd.Series("", index=current.index)).fillna("").astype(str)
    current["1年-夏普cap命中"] = pd.to_numeric(current.get("1年-夏普cap命中"), errors="coerce").fillna(0).astype(int)
    current["1年-索提诺cap命中"] = pd.to_numeric(current.get("1年-索提诺cap命中"), errors="coerce").fillna(0).astype(int)
    current["1年-夏普比率(展示封顶)"] = pd.to_numeric(current.get("1年-夏普比率(展示封顶)"), errors="coerce")
    current["1年-索提诺比率(展示封顶)"] = pd.to_numeric(current.get("1年-索提诺比率(展示封顶)"), errors="coerce")
    current["1年-夏普比率"] = pd.to_numeric(current.get("1年-夏普比率"), errors="coerce")
    current["1年-索提诺比率"] = pd.to_numeric(current.get("1年-索提诺比率"), errors="coerce")
    current["综合加权得分"] = pd.to_numeric(current.get("综合加权得分"), errors="coerce")
    current[FORMAL_SCORE_LABEL] = pd.to_numeric(current.get(FORMAL_SCORE_LABEL), errors="coerce")
    current["1年-年化收益(%)"] = pd.to_numeric(current.get("1年-年化收益(%)"), errors="coerce")
    current["1年-卡玛比率"] = pd.to_numeric(current.get("1年-卡玛比率"), errors="coerce")

    fund_count = int(len(current.index))
    sharpe_cap_hit_count = int(current["1年-夏普cap命中"].sum())
    sortino_cap_hit_count = int(current["1年-索提诺cap命中"].sum())
    either_mask = current["1年-夏普cap命中"].eq(1) | current["1年-索提诺cap命中"].eq(1)
    either_cap_hit_count = int(either_mask.sum())

    style_rows: list[dict[str, Any]] = []
    grouped = current.groupby("债基风格标签", dropna=False)
    for style, group in grouped:
        total = int(len(group.index))
        if total <= 0:
            continue
        sharpe_hits = int(group["1年-夏普cap命中"].sum())
        sortino_hits = int(group["1年-索提诺cap命中"].sum())
        either_hits = int((group["1年-夏普cap命中"].eq(1) | group["1年-索提诺cap命中"].eq(1)).sum())
        style_rows.append(
            {
                "债基风格标签": str(style or "").strip() or "未知",
                "样本数": total,
                "Sharpe cap-hit 数": sharpe_hits,
                "Sortino cap-hit 数": sortino_hits,
                "任一 cap-hit 数": either_hits,
                "任一 cap-hit 比例(%)": round((either_hits / total) * 100, 2) if total else 0.0,
            }
        )
    style_rows.sort(key=lambda item: (-item["任一 cap-hit 比例(%)"], -item["任一 cap-hit 数"], item["债基风格标签"]))

    raw_variant = current.copy()
    raw_variant["sharpe_ratio"] = pd.to_numeric(
        raw_variant.get("raw_sharpe_ratio", raw_variant["1年-夏普比率"]),
        errors="coerce",
    ).combine_first(raw_variant["1年-夏普比率"])
    raw_variant["sortino_ratio"] = pd.to_numeric(
        raw_variant.get("raw_sortino_ratio", raw_variant["1年-索提诺比率"]),
        errors="coerce",
    ).combine_first(raw_variant["1年-索提诺比率"])
    raw_variant = _assign_project_scores(raw_variant)

    capped_variant = current.copy()
    capped_variant["sharpe_ratio"] = capped_variant["1年-夏普比率(展示封顶)"].combine_first(
        pd.to_numeric(
            capped_variant.get("sharpe_ratio", pd.Series(index=capped_variant.index, dtype=float)),
            errors="coerce",
        )
    )
    capped_variant["sortino_ratio"] = capped_variant["1年-索提诺比率(展示封顶)"].combine_first(
        pd.to_numeric(
            capped_variant.get("sortino_ratio", pd.Series(index=capped_variant.index, dtype=float)),
            errors="coerce",
        )
    )
    capped_variant["1年-夏普比率"] = capped_variant["1年-夏普比率(展示封顶)"].combine_first(capped_variant["1年-夏普比率"])
    capped_variant["1年-索提诺比率"] = capped_variant["1年-索提诺比率(展示封顶)"].combine_first(capped_variant["1年-索提诺比率"])
    capped_variant = _assign_project_scores(capped_variant)

    # A cap audit compares the same calculable population in both variants.
    # Rows without a complete formal score are not ranked candidates; allowing
    # them into Top-N makes the audit depend on stable sort order and can hide
    # a real cap-induced change behind an unscored row.
    comparable_mask = (
        pd.to_numeric(raw_variant.get(FORMAL_SCORE_LABEL), errors="coerce").notna()
        & pd.to_numeric(capped_variant.get(FORMAL_SCORE_LABEL), errors="coerce").notna()
    )
    raw_variant = raw_variant.loc[comparable_mask].copy()
    capped_variant = capped_variant.loc[comparable_mask].copy()

    raw_sorted = raw_variant.sort_values(
        by=[FORMAL_SCORE_LABEL, "综合加权得分", "1年-年化收益(%)", "1年-夏普比率", "1年-卡玛比率", "基金代码"],
        ascending=[False, False, False, False, False, True],
        kind="mergesort",
    )
    capped_sorted = capped_variant.sort_values(
        by=[FORMAL_SCORE_LABEL, "综合加权得分", "1年-年化收益(%)", "1年-夏普比率", "1年-卡玛比率", "基金代码"],
        ascending=[False, False, False, False, False, True],
        kind="mergesort",
    )
    top_n = max(int(preview_top_n or 0), 0)
    raw_top_codes = [
        str(code).strip()
        for code in raw_sorted.get("基金代码", pd.Series(dtype=object)).head(top_n).tolist()
        if str(code).strip()
    ]
    capped_top_codes = [
        str(code).strip()
        for code in capped_sorted.get("基金代码", pd.Series(dtype=object)).head(top_n).tolist()
        if str(code).strip()
    ]
    raw_top_set = set(raw_top_codes)
    capped_top_set = set(capped_top_codes)

    return {
        "fund_count": fund_count,
        "sharpe_cap_hit_count": sharpe_cap_hit_count,
        "sortino_cap_hit_count": sortino_cap_hit_count,
        "either_cap_hit_count": either_cap_hit_count,
        "sharpe_cap_hit_ratio": round((sharpe_cap_hit_count / fund_count) * 100, 2) if fund_count else 0.0,
        "sortino_cap_hit_ratio": round((sortino_cap_hit_count / fund_count) * 100, 2) if fund_count else 0.0,
        "either_cap_hit_ratio": round((either_cap_hit_count / fund_count) * 100, 2) if fund_count else 0.0,
        "top_n": top_n,
        "top_n_overlap_count": int(len(raw_top_set & capped_top_set)),
        "top_n_changed_count": int(len((raw_top_set - capped_top_set) | (capped_top_set - raw_top_set))),
        "top_n_only_raw_codes": sorted(raw_top_set - capped_top_set),
        "top_n_only_capped_codes": sorted(capped_top_set - raw_top_set),
        "top_n_comparable_count": int(comparable_mask.sum()),
        "top_n_incomparable_count": int((~comparable_mask).sum()),
        "style_hit_rows": style_rows,
    }


def _build_style_pass_rate_audit(
    *,
    candidate_df: pd.DataFrame,
    dynamic_baseline_df: pd.DataFrame,
    official_df: pd.DataFrame,
) -> pd.DataFrame:
    if candidate_df.empty:
        return pd.DataFrame(
            columns=[
                "债基风格标签",
                "治理候选数",
                "动态基准通过数",
                "正式发布数",
                "动态基准通过率(%)",
                "正式发布率(%)",
            ]
        )

    def _style_counter(df: pd.DataFrame) -> Counter:
        if df.empty:
            return Counter()
        return Counter(
            str(value or "").strip() or "未知"
            for value in df.get("债基风格标签", pd.Series(dtype=object)).tolist()
        )

    candidate_counter = _style_counter(candidate_df)
    dynamic_counter = _style_counter(dynamic_baseline_df)
    official_counter = _style_counter(official_df)

    rows: list[dict[str, Any]] = []
    for style, candidate_count in candidate_counter.items():
        dynamic_count = int(dynamic_counter.get(style, 0))
        official_count = int(official_counter.get(style, 0))
        rows.append(
            {
                "债基风格标签": style,
                "治理候选数": int(candidate_count),
                "动态基准通过数": dynamic_count,
                "正式发布数": official_count,
                "动态基准通过率(%)": round(dynamic_count / candidate_count * 100, 2) if candidate_count else 0.0,
                "正式发布率(%)": round(official_count / candidate_count * 100, 2) if candidate_count else 0.0,
            }
        )

    return pd.DataFrame(rows).sort_values(
        by=["正式发布率(%)", "动态基准通过率(%)", "治理候选数", "债基风格标签"],
        ascending=[False, False, False, True],
        kind="mergesort",
    ).reset_index(drop=True)


def _build_style_veto_reason_audit(ledger_df: pd.DataFrame, *, top_n: int = 5) -> pd.DataFrame:
    if ledger_df.empty:
        return pd.DataFrame(columns=["债基风格标签", "主排除原因", "数量", "占该风格未入榜比例(%)"])

    current = ledger_df.copy()
    current["债基风格标签"] = current.get("债基风格标签", pd.Series(dtype=object)).fillna("").astype(str).str.strip().replace("", "未知")
    current["主排除原因"] = current.get("主排除原因", pd.Series(dtype=object)).fillna("").astype(str).str.strip().replace("", "未知原因")
    style_totals = current.groupby("债基风格标签").size().to_dict()

    rows: list[dict[str, Any]] = []
    for style, group in current.groupby("债基风格标签", dropna=False):
        reason_counter = Counter(group["主排除原因"].tolist())
        style_total = int(style_totals.get(style, 0))
        for reason, count in reason_counter.most_common(max(0, int(top_n))):
            rows.append(
                {
                    "债基风格标签": style,
                    "主排除原因": reason,
                    "数量": int(count),
                    "占该风格未入榜比例(%)": round(count / style_total * 100, 2) if style_total else 0.0,
                }
            )

    return pd.DataFrame(rows)


def _build_reason_group_audit(ledger_df: pd.DataFrame) -> pd.DataFrame:
    columns = ["主原因分组", "数量", "占未入榜样本比例(%)"]
    if ledger_df.empty:
        return pd.DataFrame(columns=columns)

    group_series = (
        ledger_df.get("主原因分组", pd.Series(dtype=object))
        .fillna("")
        .astype(str)
        .str.strip()
        .replace("", "未归类")
    )
    counter = Counter(group_series.tolist())
    total = int(len(group_series.index))
    rows = [
        {
            "主原因分组": name,
            "数量": int(count),
            "占未入榜样本比例(%)": round(count / total * 100, 2) if total else 0.0,
        }
        for name, count in counter.most_common()
    ]
    return pd.DataFrame(rows, columns=columns)


def _build_status_split_audit(ledger_df: pd.DataFrame) -> pd.DataFrame:
    columns = ["状态维度", "状态值", "数量", "占未入榜样本比例(%)"]
    if ledger_df.empty:
        return pd.DataFrame(columns=columns)

    mapping = [
        ("发布结论状态", "发布结论状态"),
        ("可投资性状态", "可投资性状态"),
        ("数据就绪状态", "数据就绪状态"),
        ("质量结论状态", "质量结论状态"),
    ]
    total = int(len(ledger_df.index))
    rows: list[dict[str, Any]] = []
    for column, label in mapping:
        series = (
            ledger_df.get(column, pd.Series(dtype=object))
            .fillna("")
            .astype(str)
            .str.strip()
            .replace("", "未标记")
        )
        counter = Counter(series.tolist())
        for name, count in counter.most_common():
            rows.append(
                {
                    "状态维度": label,
                    "状态值": name,
                    "数量": int(count),
                    "占未入榜样本比例(%)": round(count / total * 100, 2) if total else 0.0,
                }
            )
    return pd.DataFrame(rows, columns=columns)


QUALITY_FACTOR_GROUPS = {
    "score_insufficient": "综合评分门槛",
    "below_weighted_floor": "综合评分门槛",
    "below_short_floor": "综合评分门槛",
    "below_long_floor": "综合评分门槛",
    "below_annual_floor": "收益效率",
    "below_positive_ratio_floor": "收益效率",
    "below_sharpe_floor": "风险调整收益",
    "below_sortino_floor": "风险调整收益",
    "below_calmar_floor": "风险调整收益",
    "mdd_too_high": "风险暴露",
    "volatility_too_high": "风险暴露",
    "fee_too_high": "摩擦与承载",
    "scale_too_small": "摩擦与承载",
}


QUALITY_FACTOR_DETAIL_CODES = [
    "below_weighted_floor",
    "below_short_floor",
    "below_long_floor",
    "below_annual_floor",
    "below_positive_ratio_floor",
    "below_sharpe_floor",
    "below_sortino_floor",
    "below_calmar_floor",
    "mdd_too_high",
    "volatility_too_high",
    "fee_too_high",
    "scale_too_small",
]

SCORE_CLUSTER_REASON_CODES = (
    "below_weighted_floor",
    "below_short_floor",
    "below_long_floor",
)

SCORE_CLUSTER_COMBO_SPECS: list[tuple[str, str, tuple[str, ...]]] = [
    ("weighted_only", "仅综合加权得分", ("below_weighted_floor",)),
    ("short_only", "仅近期得分", ("below_short_floor",)),
    ("long_only", "仅长期得分", ("below_long_floor",)),
    ("weighted_short_only", "综合加权得分+近期得分", ("below_weighted_floor", "below_short_floor")),
    ("weighted_long_only", "综合加权得分+长期得分", ("below_weighted_floor", "below_long_floor")),
    ("short_long_only", "近期得分+长期得分", ("below_short_floor", "below_long_floor")),
    (
        "all_three",
        "综合加权得分+近期得分+长期得分",
        ("below_weighted_floor", "below_short_floor", "below_long_floor"),
    ),
]

ANNUAL_RETURN_SCORE_SHARPE_FOCUS_COMBO_SPECS: list[tuple[str, str, tuple[str, ...]]] = [
    ("annual_return_only", "仅年化收益", ("annual_return",)),
    ("annual_return_plus_score", "年化收益+得分", ("annual_return", "score")),
    ("annual_return_plus_sharpe", "年化收益+夏普", ("annual_return", "sharpe")),
    (
        "annual_return_plus_sharpe_plus_score",
        "年化收益+夏普+得分",
        ("annual_return", "score", "sharpe"),
    ),
]


def _quality_detail_codes_from_row(row: dict[str, Any]) -> list[str]:
    original_codes = _split_reason_codes(row.get("原始原因代码"))
    if not original_codes:
        original_codes = []
        primary_code = str(row.get("主排除原因代码") or "").strip()
        if primary_code:
            original_codes.append(primary_code)
        original_codes.extend(_split_reason_codes(row.get("次排除原因代码")))

    unique_codes: list[str] = []
    for code in original_codes:
        if code not in QUALITY_FACTOR_DETAIL_CODES or code in unique_codes:
            continue
        unique_codes.append(code)
    return unique_codes


def _build_score_cluster_explainability_audit(ledger_df: pd.DataFrame) -> dict[str, Any]:
    empty_combo_rows = [
        {
            "combo_key": combo_key,
            "combo_label": combo_label,
            "count": 0,
            "share_pct_of_score_cluster": 0.0,
        }
        for combo_key, combo_label, _ in SCORE_CLUSTER_COMBO_SPECS
    ]
    empty_factor_rows = [
        {
            "factor_key": "weighted_score",
            "factor_label": "综合加权得分",
            "hit_count": 0,
            "share_pct_of_score_cluster": 0.0,
            "independent_hit_count": 0,
            "without_weighted_hit_count": 0,
            "role": "not_triggered",
        },
        {
            "factor_key": "short_term_score",
            "factor_label": "近期得分",
            "hit_count": 0,
            "share_pct_of_score_cluster": 0.0,
            "independent_hit_count": 0,
            "without_weighted_hit_count": 0,
            "role": "not_triggered",
        },
        {
            "factor_key": "long_term_score",
            "factor_label": "长期得分",
            "hit_count": 0,
            "share_pct_of_score_cluster": 0.0,
            "independent_hit_count": 0,
            "without_weighted_hit_count": 0,
            "role": "not_triggered",
        },
    ]
    empty_payload = {
        "available": False,
        "quality_intercept_total": 0,
        "score_cluster_hit_total": 0,
        "score_cluster_hit_share_pct": 0.0,
        "combo_breakdown": empty_combo_rows,
        "factor_role_summary": empty_factor_rows,
        "sample_preview": {},
        "judgement": {
            "suspected_redundancy_present": False,
            "dominant_anchor_factor": None,
            "recommended_action": "observe_only",
            "direct_threshold_relaxation_recommended_now": False,
            "reason": "当前没有可用于拆解得分簇解释职责的质量拦截样本。",
        },
    }
    if ledger_df.empty:
        return empty_payload

    current = ledger_df.copy()
    current = current[
        current.get("发布结论状态", pd.Series(dtype=object)).fillna("").astype(str).eq("质量筛选拦截")
    ].copy()
    if current.empty:
        return empty_payload

    combo_counter: Counter[str] = Counter()
    sample_rows_by_combo: dict[str, list[dict[str, Any]]] = {combo_key: [] for combo_key, _, _ in SCORE_CLUSTER_COMBO_SPECS}
    quality_intercept_total = int(len(current.index))

    factor_hit_counts = {
        "weighted_score": 0,
        "short_term_score": 0,
        "long_term_score": 0,
    }
    factor_display = {
        "weighted_score": "综合加权得分",
        "short_term_score": "近期得分",
        "long_term_score": "长期得分",
    }
    factor_to_code = {
        "weighted_score": "below_weighted_floor",
        "short_term_score": "below_short_floor",
        "long_term_score": "below_long_floor",
    }

    for row in current.to_dict(orient="records"):
        codes = set(_quality_detail_codes_from_row(row))
        combo_codes = tuple(code for code in SCORE_CLUSTER_REASON_CODES if code in codes)
        if not combo_codes:
            continue
        combo_key = next(
            (
                key
                for key, _label, spec_codes in SCORE_CLUSTER_COMBO_SPECS
                if tuple(spec_codes) == combo_codes
            ),
            "",
        )
        if not combo_key:
            continue
        combo_counter[combo_key] += 1
        for factor_key, reason_code in factor_to_code.items():
            if reason_code in combo_codes:
                factor_hit_counts[factor_key] += 1
        sample_rows_by_combo[combo_key].append(
            {
                "基金代码": row.get("基金代码"),
                "基金名称": row.get("基金名称"),
                "债基风格标签": row.get("债基风格标签"),
                "综合加权得分": row.get("综合加权得分"),
                "近期得分": row.get("近期得分"),
                "长期得分": row.get("长期得分"),
                "主排除原因": row.get("主排除原因"),
                "次排除原因": row.get("次排除原因"),
                "原始原因代码": row.get("原始原因代码"),
            }
        )

    score_cluster_hit_total = int(sum(combo_counter.values()))
    if score_cluster_hit_total <= 0:
        return empty_payload

    combo_breakdown: list[dict[str, Any]] = []
    for combo_key, combo_label, _ in SCORE_CLUSTER_COMBO_SPECS:
        count = int(combo_counter.get(combo_key, 0))
        combo_breakdown.append(
            {
                "combo_key": combo_key,
                "combo_label": combo_label,
                "count": count,
                "share_pct_of_score_cluster": round(count / score_cluster_hit_total * 100, 2),
            }
        )

    combo_count_map = {item["combo_key"]: int(item["count"]) for item in combo_breakdown}
    weighted_without_components_total = combo_count_map["weighted_only"]
    short_without_weighted_total = combo_count_map["short_only"] + combo_count_map["short_long_only"]
    long_without_weighted_total = combo_count_map["long_only"] + combo_count_map["short_long_only"]
    weighted_anchor_coverage_ratio = (
        factor_hit_counts["weighted_score"] / score_cluster_hit_total if score_cluster_hit_total else 0.0
    )
    suspected_redundancy = (
        weighted_anchor_coverage_ratio >= 0.995
        and (short_without_weighted_total + long_without_weighted_total) <= 10
    )

    factor_role_summary: list[dict[str, Any]] = []
    for factor_key, display_label in factor_display.items():
        hit_count = int(factor_hit_counts.get(factor_key, 0))
        independent_hit_count = 0
        without_weighted_hit_count = 0
        role = "parallel_gate"
        if factor_key == "weighted_score":
            independent_hit_count = weighted_without_components_total
            without_weighted_hit_count = 0
            role = (
                "anchor_primary_gate"
                if weighted_anchor_coverage_ratio >= 0.8
                and short_without_weighted_total <= 5
                and long_without_weighted_total <= 5
                else "parallel_gate"
            )
        elif factor_key == "short_term_score":
            independent_hit_count = combo_count_map["short_only"]
            without_weighted_hit_count = short_without_weighted_total
            if without_weighted_hit_count <= 5:
                role = "confirmatory_recent_component"
        elif factor_key == "long_term_score":
            independent_hit_count = combo_count_map["long_only"]
            without_weighted_hit_count = long_without_weighted_total
            if without_weighted_hit_count <= 5:
                role = "confirmatory_long_component"
        factor_role_summary.append(
            {
                "factor_key": factor_key,
                "factor_label": display_label,
                "hit_count": hit_count,
                "share_pct_of_score_cluster": round(hit_count / score_cluster_hit_total * 100, 2),
                "independent_hit_count": int(independent_hit_count),
                "without_weighted_hit_count": int(without_weighted_hit_count),
                "role": role,
            }
        )

    sample_preview: dict[str, list[dict[str, Any]]] = {}
    preview_columns = [
        "基金代码",
        "基金名称",
        "债基风格标签",
        "综合加权得分",
        "近期得分",
        "长期得分",
        "主排除原因",
        "次排除原因",
        "原始原因代码",
    ]
    for combo_key in ("weighted_only", "weighted_short_only", "weighted_long_only", "all_three"):
        sample_df = pd.DataFrame(sample_rows_by_combo.get(combo_key) or [])
        if sample_df.empty:
            sample_preview[combo_key] = []
            continue
        for column in ("综合加权得分", "近期得分", "长期得分"):
            sample_df[column] = pd.to_numeric(sample_df.get(column), errors="coerce")
        sample_df = sample_df.sort_values(
            by=["综合加权得分", "长期得分", "近期得分", "基金代码"],
            ascending=[False, False, False, True],
            kind="mergesort",
        )
        sample_preview[combo_key] = (
            sample_df[preview_columns].fillna("").head(5).to_dict(orient="records")
        )

    return {
        "available": True,
        "quality_intercept_total": quality_intercept_total,
        "score_cluster_hit_total": score_cluster_hit_total,
        "score_cluster_hit_share_pct": round(score_cluster_hit_total / quality_intercept_total * 100, 2),
        "combo_breakdown": combo_breakdown,
        "factor_role_summary": factor_role_summary,
        "sample_preview": sample_preview,
        "judgement": {
            "suspected_redundancy_present": suspected_redundancy,
            "dominant_anchor_factor": "weighted_score",
            "recommended_action": "review_short_long_as_confirmatory_components_before_threshold_change",
            "direct_threshold_relaxation_recommended_now": False,
            "reason": (
                "近期/长期得分脱离综合加权得分的独立命中极少，当前更像确认性组件而不是与综合加权得分并列的独立主门槛。"
                if suspected_redundancy
                else "得分簇仍存在一定独立解释贡献，建议先继续做职责拆分观察。"
            ),
            "weighted_anchor_coverage_ratio": round(weighted_anchor_coverage_ratio, 4),
            "short_without_weighted_total": int(short_without_weighted_total),
            "long_without_weighted_total": int(long_without_weighted_total),
            "all_three_count": int(combo_count_map["all_three"]),
        },
    }


def _build_annual_return_score_sharpe_coupling_audit(
    ledger_df: pd.DataFrame,
    *,
    style_threshold_rows: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    empty_combo_rows = [
        {
            "combo_key": combo_key,
            "combo_label": combo_label,
            "count": 0,
            "share_pct_of_focus_scope": 0.0,
        }
        for combo_key, combo_label, _ in ANNUAL_RETURN_SCORE_SHARPE_FOCUS_COMBO_SPECS
    ]
    empty_payload = {
        "available": False,
        "quality_intercept_total": 0,
        "focus_scope_total": 0,
        "focus_scope_share_pct": 0.0,
        "combo_breakdown": empty_combo_rows,
        "style_responsibility_preview": [],
        "dominant_local_boundary_review": {"available": False},
        "judgement": {
            "annual_return_independent_hit_total": 0,
            "annual_return_with_score_total": 0,
            "annual_return_with_sharpe_total": 0,
            "annual_return_with_score_and_sharpe_total": 0,
            "annual_return_coupled_share_pct": 0.0,
            "score_requires_annual_return_share_pct": 0.0,
            "sharpe_requires_annual_return_share_pct": 0.0,
            "dominant_coupling_pattern": None,
            "recommended_action": "observe_only",
            "direct_threshold_relaxation_recommended_now": False,
            "reason": "当前没有足够样本用于拆解年化收益与得分/夏普的耦合职责。",
        },
    }
    if ledger_df.empty:
        return empty_payload

    current = ledger_df.copy()
    current = current[
        current.get("发布结论状态", pd.Series(dtype=object)).fillna("").astype(str).eq("质量筛选拦截")
    ].copy()
    if current.empty:
        return empty_payload

    metric_order = ("annual_return", "score", "sharpe")
    combo_counter: Counter[tuple[str, ...]] = Counter()
    style_counter: dict[str, Counter[tuple[str, ...]]] = {}
    annual_return_only_style_counter: Counter[str] = Counter()
    annual_return_only_score_band_counter: Counter[str] = Counter()
    annual_return_only_style_band_counter: Counter[tuple[str, str]] = Counter()
    annual_return_only_rows_by_style_band: dict[tuple[str, str], list[dict[str, Any]]] = {}
    annual_return_hit_total = 0
    score_hit_total = 0
    sharpe_hit_total = 0
    quality_intercept_total = int(len(current.index))

    annual_return_only_score_band_display_order = {
        "80+": 0,
        "70-79.99": 1,
        "60-69.99": 2,
        "<60": 3,
        "unknown": 4,
    }
    annual_return_only_score_band_review_order = {
        "70-79.99": 0,
        "60-69.99": 1,
        "80+": 2,
        "<60": 3,
        "unknown": 4,
    }

    for row in current.to_dict(orient="records"):
        codes = set(_quality_detail_codes_from_row(row))
        metric_flags = {
            "annual_return": "below_annual_floor" in codes,
            "score": any(
                code in codes
                for code in ("below_weighted_floor", "below_short_floor", "below_long_floor")
            ),
            "sharpe": "below_sharpe_floor" in codes,
        }
        if metric_flags["annual_return"]:
            annual_return_hit_total += 1
        if metric_flags["score"]:
            score_hit_total += 1
        if metric_flags["sharpe"]:
            sharpe_hit_total += 1
        combo_metrics = tuple(metric for metric in metric_order if metric_flags[metric])
        if not combo_metrics:
            continue
        has_other_quality_gate = any(
            code in codes
            for code in (
                "below_sortino_floor",
                "below_calmar_floor",
                "below_positive_ratio_floor",
                "mdd_too_high",
                "volatility_too_high",
                "fee_too_high",
                "scale_too_small",
            )
        )
        if has_other_quality_gate:
            continue
        combo_counter[combo_metrics] += 1
        style_key = str(row.get("债基风格标签") or "").strip() or "未知"
        style_counter.setdefault(style_key, Counter())[combo_metrics] += 1
        if combo_metrics == ("annual_return",):
            annual_return_only_style_counter[style_key] += 1
            score_value = pd.to_numeric(pd.Series([row.get("综合加权得分")]), errors="coerce").iloc[0]
            if pd.isna(score_value):
                score_band = "unknown"
            elif float(score_value) >= 80:
                score_band = "80+"
            elif float(score_value) >= 70:
                score_band = "70-79.99"
            elif float(score_value) >= 60:
                score_band = "60-69.99"
            else:
                score_band = "<60"
            annual_return_only_score_band_counter[score_band] += 1
            annual_return_only_style_band_counter[(style_key, score_band)] += 1
            annual_return_only_rows_by_style_band.setdefault((style_key, score_band), []).append(
                dict(row or {})
            )

    focus_scope_total = int(sum(combo_counter.values()))
    if focus_scope_total <= 0:
        return empty_payload

    combo_label_map = {
        tuple(spec_metrics): (combo_key, combo_label)
        for combo_key, combo_label, spec_metrics in ANNUAL_RETURN_SCORE_SHARPE_FOCUS_COMBO_SPECS
    }
    combo_count_lookup = {
        combo_key: int(combo_counter.get(tuple(spec_metrics), 0))
        for combo_key, _combo_label, spec_metrics in ANNUAL_RETURN_SCORE_SHARPE_FOCUS_COMBO_SPECS
    }
    combo_breakdown = []
    for combo_key, combo_label, spec_metrics in ANNUAL_RETURN_SCORE_SHARPE_FOCUS_COMBO_SPECS:
        count = int(combo_counter.get(tuple(spec_metrics), 0))
        combo_breakdown.append(
            {
                "combo_key": combo_key,
                "combo_label": combo_label,
                "count": count,
                "share_pct_of_focus_scope": round(count / focus_scope_total * 100, 2),
            }
        )

    annual_return_only_count = combo_count_lookup["annual_return_only"]
    annual_return_plus_score_count = combo_count_lookup["annual_return_plus_score"]
    annual_return_plus_sharpe_count = combo_count_lookup["annual_return_plus_sharpe"]
    annual_return_plus_score_and_sharpe_count = combo_count_lookup[
        "annual_return_plus_sharpe_plus_score"
    ]
    annual_return_coupled_total = (
        annual_return_plus_score_count
        + annual_return_plus_sharpe_count
        + annual_return_plus_score_and_sharpe_count
    )
    annual_return_focus_total = annual_return_only_count + annual_return_coupled_total
    annual_return_coupled_share_pct = (
        annual_return_coupled_total / annual_return_focus_total * 100.0
        if annual_return_focus_total
        else 0.0
    )
    annual_return_with_score_total = (
        annual_return_plus_score_count + annual_return_plus_score_and_sharpe_count
    )
    annual_return_with_sharpe_total = (
        annual_return_plus_sharpe_count + annual_return_plus_score_and_sharpe_count
    )
    score_requires_annual_return_share_pct = (
        annual_return_with_score_total / score_hit_total * 100.0 if score_hit_total else 0.0
    )
    sharpe_requires_annual_return_share_pct = (
        annual_return_with_sharpe_total / sharpe_hit_total * 100.0 if sharpe_hit_total else 0.0
    )
    dominant_combo = max(
        combo_breakdown,
        key=lambda item: (int(item.get("count") or 0), str(item.get("combo_key") or "")),
    )

    style_responsibility_rows: list[dict[str, Any]] = []
    for style_key, counter in sorted(
        style_counter.items(),
        key=lambda item: (
            -sum(item[1].values()),
            item[0],
        ),
    )[:8]:
        style_total = int(sum(counter.values()))
        annual_only = int(counter.get(("annual_return",), 0))
        annual_plus_score = int(counter.get(("annual_return", "score"), 0))
        annual_plus_sharpe = int(counter.get(("annual_return", "sharpe"), 0))
        annual_plus_score_and_sharpe = int(counter.get(("annual_return", "score", "sharpe"), 0))
        style_responsibility_rows.append(
            {
                "债基风格标签": style_key,
                "focus_scope_count": style_total,
                "annual_return_only_count": annual_only,
                "annual_return_plus_score_count": annual_plus_score,
                "annual_return_plus_sharpe_count": annual_plus_sharpe,
                "annual_return_plus_sharpe_plus_score_count": annual_plus_score_and_sharpe,
                "annual_return_coupled_share_pct": round(
                    (
                        (annual_plus_score + annual_plus_sharpe + annual_plus_score_and_sharpe)
                        / style_total
                        * 100.0
                    )
                    if style_total
                    else 0.0,
                    2,
                ),
            }
        )

    annual_return_only_style_preview = [
        {
            "债基风格标签": style_key,
            "annual_return_only_count": int(count),
            "share_pct_of_annual_return_only": round(
                count / annual_return_only_count * 100.0 if annual_return_only_count else 0.0,
                2,
            ),
        }
        for style_key, count in annual_return_only_style_counter.most_common(8)
    ]
    annual_return_only_score_band_preview = [
        {
            "score_band": score_band,
            "annual_return_only_count": int(count),
            "share_pct_of_annual_return_only": round(
                count / annual_return_only_count * 100.0 if annual_return_only_count else 0.0,
                2,
            ),
        }
        for score_band, count in sorted(
            annual_return_only_score_band_counter.items(),
            key=lambda item: (
                annual_return_only_score_band_display_order.get(str(item[0]), 99),
                -int(item[1]),
                str(item[0]),
            ),
        )
    ]
    annual_return_only_style_band_preview = [
        {
            "债基风格标签": style_key,
            "score_band": score_band,
            "annual_return_only_count": int(count),
            "share_pct_of_annual_return_only": round(
                count / annual_return_only_count * 100.0 if annual_return_only_count else 0.0,
                2,
            ),
        }
        for (style_key, score_band), count in sorted(
            annual_return_only_style_band_counter.items(),
            key=lambda item: (
                -int(item[1]),
                str(item[0][0]),
                annual_return_only_score_band_review_order.get(str(item[0][1]), 99),
                str(item[0][1]),
            ),
        )[:10]
    ]
    annual_return_only_local_review_priority_preview: list[dict[str, Any]] = []
    for style_key, count in sorted(
        annual_return_only_style_counter.items(),
        key=lambda item: (-int(item[1]), str(item[0])),
    )[:8]:
        style_band_rows = [
            (band_key, int(band_count))
            for (row_style, band_key), band_count in annual_return_only_style_band_counter.items()
            if row_style == style_key
        ]
        if not style_band_rows:
            continue
        dominant_score_band, dominant_score_band_count = sorted(
            style_band_rows,
            key=lambda item: (
                -int(item[1]),
                annual_return_only_score_band_review_order.get(str(item[0]), 99),
                str(item[0]),
            ),
        )[0]
        if int(count) >= 20:
            review_priority = "high"
        elif int(count) >= 10:
            review_priority = "medium"
        else:
            review_priority = "low"
        if dominant_score_band in {"70-79.99", "60-69.99"}:
            review_action = "review_style_band_boundary_before_global_relaxation"
        elif dominant_score_band == "80+":
            review_action = "check_high_score_outliers_before_threshold_change"
        else:
            review_action = "observe_only"
        annual_return_only_local_review_priority_preview.append(
            {
                "债基风格标签": style_key,
                "annual_return_only_count": int(count),
                "share_pct_of_annual_return_only": round(
                    count / annual_return_only_count * 100.0 if annual_return_only_count else 0.0,
                    2,
                ),
                "dominant_score_band": dominant_score_band,
                "dominant_score_band_count": int(dominant_score_band_count),
                "dominant_score_band_share_within_style_pct": round(
                    dominant_score_band_count / count * 100.0 if count else 0.0,
                    2,
                ),
                "review_priority": review_priority,
                "review_action": review_action,
            }
        )
    high_priority_local_review_styles = [
        str(item.get("债基风格标签") or "")
        for item in annual_return_only_local_review_priority_preview
        if str(item.get("review_priority") or "") == "high"
    ]
    recommended_review_score_bands = [
        score_band
        for score_band in ("70-79.99", "60-69.99")
        if int(annual_return_only_score_band_counter.get(score_band, 0)) > 0
    ]
    dominant_style_band = annual_return_only_style_band_preview[0] if annual_return_only_style_band_preview else {}
    dominant_style_band_label = (
        f"{dominant_style_band.get('债基风格标签')}/{dominant_style_band.get('score_band')}"
        if dominant_style_band
        else None
    )
    style_threshold_lookup = {
        str((row or {}).get("债基风格标签") or "").strip(): dict(row or {})
        for row in list(style_threshold_rows or [])
        if str((row or {}).get("债基风格标签") or "").strip()
    }
    dominant_local_boundary_review: dict[str, Any] = {"available": False}
    dominant_style_key = str(dominant_style_band.get("债基风格标签") or "").strip()
    dominant_score_band_key = str(dominant_style_band.get("score_band") or "").strip()
    dominant_local_rows = list(
        annual_return_only_rows_by_style_band.get((dominant_style_key, dominant_score_band_key)) or []
    )
    dominant_style_threshold_row = style_threshold_lookup.get(dominant_style_key) or {}
    if dominant_local_rows and dominant_style_threshold_row:
        local_df = pd.DataFrame(dominant_local_rows)
        annual_series = pd.to_numeric(
            local_df.get("1年-年化收益(%)", pd.Series(index=local_df.index, dtype=float)),
            errors="coerce",
        )
        weighted_score_series = pd.to_numeric(
            local_df.get("综合加权得分", pd.Series(index=local_df.index, dtype=float)),
            errors="coerce",
        )
        short_score_series = pd.to_numeric(
            local_df.get("近期得分", pd.Series(index=local_df.index, dtype=float)),
            errors="coerce",
        )
        long_score_series = pd.to_numeric(
            local_df.get("长期得分", pd.Series(index=local_df.index, dtype=float)),
            errors="coerce",
        )
        annual_floor = float(dominant_style_threshold_row.get("1年年化收益下限") or 0.0)
        weighted_floor = float(dominant_style_threshold_row.get("综合加权得分下限") or 0.0)
        short_floor = float(dominant_style_threshold_row.get("近期得分下限") or 0.0)
        long_floor = float(dominant_style_threshold_row.get("长期得分下限") or 0.0)
        annual_gap_series = annual_series - annual_floor
        weighted_margin_series = weighted_score_series - weighted_floor
        short_margin_series = short_score_series - short_floor
        long_margin_series = long_score_series - long_floor
        annual_gap_valid = annual_gap_series.dropna()
        weighted_margin_valid = weighted_margin_series.dropna()
        short_margin_valid = short_margin_series.dropna()
        long_margin_valid = long_margin_series.dropna()
        sample_count = int(len(local_df.index))
        near_threshold_10_total = int((annual_gap_series >= -0.10).fillna(False).sum())
        near_threshold_05_total = int((annual_gap_series >= -0.05).fillna(False).sum())
        near_threshold_03_total = int((annual_gap_series >= -0.03).fillna(False).sum())
        weighted_margin_ge_5_total = int((weighted_margin_series >= 5.0).fillna(False).sum())
        weighted_margin_ge_8_total = int((weighted_margin_series >= 8.0).fillna(False).sum())
        short_long_margin_ge_10_total = int(
            ((short_margin_series >= 10.0) & (long_margin_series >= 10.0)).fillna(False).sum()
        )
        buyable_total = int(local_df.get("是否可申购", pd.Series(dtype=object)).fillna("").astype(str).eq("可申购").sum())
        strict_match_total = int(
            validation_status_series(local_df)
            .fillna("")
            .astype(str)
            .eq("strict_match")
            .sum()
        )
        shadow_review_recommended_now = bool(
            sample_count > 0
            and near_threshold_10_total / sample_count >= 0.3
            and (
                (not weighted_margin_valid.empty)
                and float(weighted_margin_valid.median()) >= 5.0
            )
            and short_long_margin_ge_10_total / sample_count >= 0.5
        )
        preview_columns = [
            "基金代码",
            "基金名称",
            "综合加权得分",
            "近期得分",
            "长期得分",
            "1年-年化收益(%)",
            "是否可申购",
            "AutoStock原始校验状态",
        ]
        local_preview_df = local_df.copy()
        local_preview_df["annual_gap_pct"] = annual_gap_series
        local_preview_df["weighted_margin"] = weighted_margin_series
        local_preview_df["short_margin"] = short_margin_series
        local_preview_df["long_margin"] = long_margin_series
        local_preview_df = local_preview_df.sort_values(
            by=["annual_gap_pct", "weighted_margin", "综合加权得分", "基金代码"],
            ascending=[False, False, False, True],
            kind="mergesort",
        )
        dominant_local_boundary_review = {
            "available": True,
            "style_tag": dominant_style_key,
            "score_band": dominant_score_band_key,
            "sample_count": sample_count,
            "style_threshold_source": dominant_style_threshold_row.get("阈值来源"),
            "annual_return_floor_pct": annual_floor,
            "weighted_score_floor": weighted_floor,
            "short_term_score_floor": short_floor,
            "long_term_score_floor": long_floor,
            "annual_gap_stats": {
                "median_gap_pct": (
                    round(float(annual_gap_valid.median()), 4) if not annual_gap_valid.empty else None
                ),
                "p75_gap_pct": (
                    round(float(annual_gap_valid.quantile(0.75)), 4)
                    if not annual_gap_valid.empty
                    else None
                ),
                "p90_gap_pct": (
                    round(float(annual_gap_valid.quantile(0.90)), 4)
                    if not annual_gap_valid.empty
                    else None
                ),
                "max_gap_pct": (
                    round(float(annual_gap_valid.max()), 4) if not annual_gap_valid.empty else None
                ),
                "min_gap_pct": (
                    round(float(annual_gap_valid.min()), 4) if not annual_gap_valid.empty else None
                ),
            },
            "near_threshold_counts": {
                "gap_ge_neg_0_10_total": near_threshold_10_total,
                "gap_ge_neg_0_05_total": near_threshold_05_total,
                "gap_ge_neg_0_03_total": near_threshold_03_total,
            },
            "score_margin_stats": {
                "weighted_margin_median": (
                    round(float(weighted_margin_valid.median()), 4)
                    if not weighted_margin_valid.empty
                    else None
                ),
                "short_margin_median": (
                    round(float(short_margin_valid.median()), 4)
                    if not short_margin_valid.empty
                    else None
                ),
                "long_margin_median": (
                    round(float(long_margin_valid.median()), 4)
                    if not long_margin_valid.empty
                    else None
                ),
                "weighted_margin_ge_5_total": weighted_margin_ge_5_total,
                "weighted_margin_ge_8_total": weighted_margin_ge_8_total,
                "short_long_margin_ge_10_total": short_long_margin_ge_10_total,
            },
            "buyable_total": buyable_total,
            "strict_match_total": strict_match_total,
            "shadow_review_recommended_now": shadow_review_recommended_now,
            "recommended_action": (
                "run_style_band_local_annual_floor_shadow_review"
                if shadow_review_recommended_now
                else "observe_only"
            ),
            "reason": (
                "该局部样本离年化门槛多数已很近，同时综合/近期/长期得分缓冲充足；"
                "下一步应先做风格/得分带局部影子复核，而不是全局放宽收益门槛。"
                if shadow_review_recommended_now
                else "该局部样本暂未形成足够强的近门槛证据，先继续观察。"
            ),
            "sample_preview": (
                local_preview_df[
                    [column for column in preview_columns if column in local_preview_df.columns]
                    + ["annual_gap_pct", "weighted_margin", "short_margin", "long_margin"]
                ]
                .fillna("")
                .head(8)
                .to_dict(orient="records")
            ),
        }

    if annual_return_only_count >= annual_return_plus_score_and_sharpe_count:
        dominant_coupling_pattern = "annual_return_independent_gate_still_material"
        recommended_action = "review_annual_return_as_independent_floor_before_any_relaxation"
        reason = (
            "年化收益单独命中样本仍然明显存在，说明其仍承担独立质量下限职责；"
            "当前更适合先拆解与得分/夏普的边界，而不是直接放松收益门槛。"
        )
    else:
        dominant_coupling_pattern = "annual_return_score_sharpe_joint_gate"
        recommended_action = "review_joint_return_score_sharpe_gate_before_any_relaxation"
        reason = (
            "年化收益与得分/夏普的联合命中样本占比更高，说明当前存在明显的复合拦截簇；"
            "下一步应先拆解三者职责边界，再决定是否调整任何单一门槛。"
        )

    return {
        "available": True,
        "quality_intercept_total": quality_intercept_total,
        "focus_scope_total": focus_scope_total,
        "focus_scope_share_pct": round(focus_scope_total / quality_intercept_total * 100, 2),
        "combo_breakdown": combo_breakdown,
        "style_responsibility_preview": style_responsibility_rows,
        "annual_return_only_style_preview": annual_return_only_style_preview,
        "annual_return_only_score_band_preview": annual_return_only_score_band_preview,
        "annual_return_only_style_band_preview": annual_return_only_style_band_preview,
        "annual_return_only_local_review_priority_preview": (
            annual_return_only_local_review_priority_preview
        ),
        "dominant_local_boundary_review": dominant_local_boundary_review,
        "judgement": {
            "annual_return_independent_hit_total": annual_return_only_count,
            "annual_return_with_score_total": annual_return_with_score_total,
            "annual_return_with_sharpe_total": annual_return_with_sharpe_total,
            "annual_return_with_score_and_sharpe_total": annual_return_plus_score_and_sharpe_count,
            "annual_return_coupled_share_pct": round(annual_return_coupled_share_pct, 2),
            "score_requires_annual_return_share_pct": round(
                score_requires_annual_return_share_pct, 2
            ),
            "sharpe_requires_annual_return_share_pct": round(
                sharpe_requires_annual_return_share_pct, 2
            ),
            "dominant_coupling_pattern": dominant_coupling_pattern,
            "recommended_action": recommended_action,
            "direct_threshold_relaxation_recommended_now": False,
            "reason": reason,
            "dominant_combo_key": dominant_combo.get("combo_key"),
            "dominant_combo_label": dominant_combo.get("combo_label"),
            "dominant_combo_count": int(dominant_combo.get("count") or 0),
            "local_boundary_review_recommended_now": bool(annual_return_only_count > 0),
            "local_boundary_review_scope": "style_and_score_band",
            "local_boundary_review_reason": (
                "年化收益单独命中样本主要集中在少数风格，且集中在 60-79.99 得分带；"
                "下一步应先做局部边界复核，而不是全局放宽收益门槛。"
            ),
            "dominant_style_band_label": dominant_style_band_label,
            "dominant_style_band_count": int(
                dominant_style_band.get("annual_return_only_count") or 0
            ),
            "dominant_style_band_share_pct": dominant_style_band.get(
                "share_pct_of_annual_return_only"
            ),
            "high_priority_local_review_style_total": int(len(high_priority_local_review_styles)),
            "recommended_review_styles": high_priority_local_review_styles[:6],
            "recommended_review_score_bands": recommended_review_score_bands,
            "dominant_local_boundary_shadow_review_recommended_now": bool(
                dominant_local_boundary_review.get("shadow_review_recommended_now")
            ),
            "dominant_local_boundary_sample_count": int(
                dominant_local_boundary_review.get("sample_count") or 0
            ),
            "dominant_local_boundary_weighted_margin_median": (
                (dominant_local_boundary_review.get("score_margin_stats") or {}).get(
                    "weighted_margin_median"
                )
            ),
            "dominant_local_boundary_annual_gap_median_pct": (
                (dominant_local_boundary_review.get("annual_gap_stats") or {}).get(
                    "median_gap_pct"
                )
            ),
        },
    }


def _build_quality_factor_group_audit(ledger_df: pd.DataFrame) -> pd.DataFrame:
    columns = ["质量因子组", "数量", "占质量拦截样本比例(%)"]
    if ledger_df.empty:
        return pd.DataFrame(columns=columns)

    current = ledger_df.copy()
    current = current[current.get("发布结论状态", pd.Series(dtype=object)).fillna("").astype(str).eq("质量筛选拦截")]
    if current.empty:
        return pd.DataFrame(columns=columns)

    factor_counter: Counter[str] = Counter()
    total = int(len(current.index))
    for row in current.to_dict(orient="records"):
        groups: list[str] = []
        for code in _quality_detail_codes_from_row(row):
            group = QUALITY_FACTOR_GROUPS.get(code)
            if group and group not in groups:
                groups.append(group)
        primary_group = groups[0] if groups else "未归类"
        factor_counter[primary_group] += 1

    rows = [
        {
            "质量因子组": name,
            "数量": int(count),
            "占质量拦截样本比例(%)": round(count / total * 100, 2) if total else 0.0,
        }
        for name, count in factor_counter.most_common()
    ]
    return pd.DataFrame(rows, columns=columns)


def _build_quality_factor_detail_audit(ledger_df: pd.DataFrame) -> pd.DataFrame:
    columns = ["原因代码", "质量细分因子", "所属因子组", "命中样本数", "占质量拦截样本比例(%)"]
    if ledger_df.empty:
        return pd.DataFrame(columns=columns)

    current = ledger_df.copy()
    current = current[current.get("发布结论状态", pd.Series(dtype=object)).fillna("").astype(str).eq("质量筛选拦截")]
    if current.empty:
        return pd.DataFrame(columns=columns)

    detail_counter: Counter[str] = Counter()
    total = int(len(current.index))
    for row in current.to_dict(orient="records"):
        for code in _quality_detail_codes_from_row(row):
            detail_counter[code] += 1

    rows = [
        {
            "原因代码": code,
            "质量细分因子": PROJECT_RECOMMENDATION_REASONS.get(code, code),
            "所属因子组": QUALITY_FACTOR_GROUPS.get(code, "未归类"),
            "命中样本数": int(count),
            "占质量拦截样本比例(%)": round(count / total * 100, 2) if total else 0.0,
        }
        for code, count in detail_counter.most_common()
    ]
    return pd.DataFrame(rows, columns=columns)


def _build_quality_overlap_audit(ledger_df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    pair_columns = ["因子A", "因子B", "共同命中样本数", "占质量拦截样本比例(%)"]
    combo_columns = ["重叠组合", "因子数", "样本数", "占质量拦截样本比例(%)"]
    sample_columns = [
        "基金代码",
        "基金名称",
        "债基风格标签",
        "基金公司",
        "综合加权得分",
        "近期得分",
        "长期得分",
        "主排除原因",
        "原始原因代码",
        "重叠组合",
    ]
    empty_pair = pd.DataFrame(columns=pair_columns)
    empty_combo = pd.DataFrame(columns=combo_columns)
    empty_sample = pd.DataFrame(columns=sample_columns)
    if ledger_df.empty:
        return {
            "pair_df": empty_pair,
            "combo_df": empty_combo,
            "sample_df": empty_sample,
        }

    current = ledger_df.copy()
    current = current[current.get("发布结论状态", pd.Series(dtype=object)).fillna("").astype(str).eq("质量筛选拦截")].copy()
    if current.empty:
        return {
            "pair_df": empty_pair,
            "combo_df": empty_combo,
            "sample_df": empty_sample,
        }

    pair_counter: Counter[tuple[str, str]] = Counter()
    combo_counter: Counter[tuple[str, ...]] = Counter()
    combo_codes_by_row: list[tuple[str, ...]] = []
    total = int(len(current.index))

    for row in current.to_dict(orient="records"):
        codes = tuple(_quality_detail_codes_from_row(row))
        combo_codes_by_row.append(codes)
        if not codes:
            continue
        combo_counter[codes] += 1
        for pair in combinations(codes, 2):
            pair_counter[pair] += 1

    pair_rows = [
        {
            "因子A": PROJECT_RECOMMENDATION_REASONS.get(code_a, code_a),
            "因子B": PROJECT_RECOMMENDATION_REASONS.get(code_b, code_b),
            "共同命中样本数": int(count),
            "占质量拦截样本比例(%)": round(count / total * 100, 2) if total else 0.0,
        }
        for (code_a, code_b), count in pair_counter.most_common(20)
    ]
    combo_rows = [
        {
            "重叠组合": " | ".join(PROJECT_RECOMMENDATION_REASONS.get(code, code) for code in codes),
            "因子数": int(len(codes)),
            "样本数": int(count),
            "占质量拦截样本比例(%)": round(count / total * 100, 2) if total else 0.0,
        }
        for codes, count in combo_counter.most_common(20)
    ]

    sample_df = empty_sample
    if combo_counter:
        dominant_codes, _ = combo_counter.most_common(1)[0]
        dominant_signature = " | ".join(PROJECT_RECOMMENDATION_REASONS.get(code, code) for code in dominant_codes)
        current = current.reset_index(drop=True)
        current["重叠组合"] = [
            " | ".join(PROJECT_RECOMMENDATION_REASONS.get(code, code) for code in codes)
            for codes in combo_codes_by_row
        ]
        sample_df = current[current["重叠组合"] == dominant_signature].copy()
        sample_df["综合加权得分"] = pd.to_numeric(sample_df.get("综合加权得分"), errors="coerce")
        sample_df["近期得分"] = pd.to_numeric(sample_df.get("近期得分"), errors="coerce")
        sample_df["长期得分"] = pd.to_numeric(sample_df.get("长期得分"), errors="coerce")
        sample_df = sample_df.sort_values(
            by=["综合加权得分", "长期得分", "近期得分", "基金代码"],
            ascending=[False, False, False, True],
            kind="mergesort",
        ).head(30)
        sample_df = _select_unique_columns(sample_df, sample_columns)

    return {
        "pair_df": pd.DataFrame(pair_rows, columns=pair_columns),
        "combo_df": pd.DataFrame(combo_rows, columns=combo_columns),
        "sample_df": sample_df,
    }


def _build_high_score_rejection_audit(
    ledger_df: pd.DataFrame,
    *,
    score_threshold: float = HIGH_SCORE_AUDIT_THRESHOLD,
) -> dict[str, pd.DataFrame]:
    base_columns = [
        "基金代码",
        "基金名称",
        "债基风格标签",
        "基金公司",
        "未入榜阶段",
        "主排除原因",
        "次排除原因",
        "综合加权得分",
        "近期得分",
        "长期得分",
        "1年-年化收益(%)",
        "是否可申购",
        "AutoStock原始校验状态",
        "推荐理由",
    ]
    if ledger_df.empty:
        empty = pd.DataFrame(columns=base_columns)
        return {
            "high_score_rejected_df": empty.copy(),
            "high_score_not_buyable_df": empty.copy(),
        }

    current = ledger_df.copy()
    current["综合加权得分"] = pd.to_numeric(current.get("综合加权得分"), errors="coerce")
    current["近期得分"] = pd.to_numeric(current.get("近期得分"), errors="coerce")
    current["长期得分"] = pd.to_numeric(current.get("长期得分"), errors="coerce")
    current["1年-年化收益(%)"] = pd.to_numeric(current.get("1年-年化收益(%)"), errors="coerce")
    current["是否可申购"] = current.get("是否可申购", pd.Series(dtype=object)).fillna("").astype(str).str.strip()
    current["主排除原因"] = current.get("主排除原因", pd.Series(dtype=object)).fillna("").astype(str).str.strip()
    current = current.sort_values(
        by=["综合加权得分", "长期得分", "近期得分", "1年-年化收益(%)", "基金代码"],
        ascending=[False, False, False, False, True],
        kind="mergesort",
    )

    high_score_rejected_df = current[current["综合加权得分"].ge(float(score_threshold))].copy()
    high_score_not_buyable_df = high_score_rejected_df[
        high_score_rejected_df["主排除原因"].eq("当前不可申购")
        | high_score_rejected_df["是否可申购"].eq("不可申购")
    ].copy()

    return {
        "high_score_rejected_df": _select_unique_columns(high_score_rejected_df, base_columns),
        "high_score_not_buyable_df": _select_unique_columns(high_score_not_buyable_df, base_columns),
    }


def _build_high_score_friction_audit(
    ledger_df: pd.DataFrame,
    *,
    score_threshold: float = HIGH_SCORE_AUDIT_THRESHOLD,
) -> dict[str, pd.DataFrame]:
    sample_columns = [
        "基金代码",
        "基金名称",
        "债基风格标签",
        "基金公司",
        "综合加权得分",
        "近期得分",
        "长期得分",
        "主排除原因",
        "次排除原因",
        "原始原因代码",
    ]
    reason_columns = ["主排除原因", "样本数", "占高分摩擦样本比例(%)"]
    style_columns = ["债基风格标签", "样本数", "占高分摩擦样本比例(%)"]
    empty_sample = pd.DataFrame(columns=sample_columns)
    empty_reason = pd.DataFrame(columns=reason_columns)
    empty_style = pd.DataFrame(columns=style_columns)
    if ledger_df.empty:
        return {
            "sample_df": empty_sample,
            "reason_df": empty_reason,
            "style_df": empty_style,
        }

    current = ledger_df.copy()
    current["综合加权得分"] = pd.to_numeric(current.get("综合加权得分"), errors="coerce")
    current["近期得分"] = pd.to_numeric(current.get("近期得分"), errors="coerce")
    current["长期得分"] = pd.to_numeric(current.get("长期得分"), errors="coerce")
    current["主原因分组"] = current.get("主原因分组", pd.Series(dtype=object)).fillna("").astype(str).str.strip()
    current["主排除原因"] = current.get("主排除原因", pd.Series(dtype=object)).fillna("").astype(str).str.strip()
    current["债基风格标签"] = current.get("债基风格标签", pd.Series(dtype=object)).fillna("").astype(str).str.strip()
    current = current[
        current["综合加权得分"].ge(float(score_threshold))
        & current["主原因分组"].eq("摩擦与承载")
    ].copy()
    if current.empty:
        return {
            "sample_df": empty_sample,
            "reason_df": empty_reason,
            "style_df": empty_style,
        }

    current = current.sort_values(
        by=["综合加权得分", "长期得分", "近期得分", "基金代码"],
        ascending=[False, False, False, True],
        kind="mergesort",
    )
    total = int(len(current.index))
    reason_df = (
        current.groupby("主排除原因", dropna=False)
        .size()
        .reset_index(name="样本数")
        .sort_values(by=["样本数", "主排除原因"], ascending=[False, True], kind="mergesort")
    )
    reason_df["占高分摩擦样本比例(%)"] = reason_df["样本数"].apply(
        lambda value: round(float(value) / total * 100, 2) if total else 0.0
    )
    style_df = (
        current.groupby("债基风格标签", dropna=False)
        .size()
        .reset_index(name="样本数")
        .sort_values(by=["样本数", "债基风格标签"], ascending=[False, True], kind="mergesort")
    )
    style_df["占高分摩擦样本比例(%)"] = style_df["样本数"].apply(
        lambda value: round(float(value) / total * 100, 2) if total else 0.0
    )

    return {
        "sample_df": _select_unique_columns(current, sample_columns),
        "reason_df": pd.DataFrame(reason_df, columns=reason_columns),
        "style_df": pd.DataFrame(style_df, columns=style_columns),
    }


def _build_high_score_reason_focus_audit(
    ledger_df: pd.DataFrame,
    detail_df: pd.DataFrame,
    *,
    reason_code: str,
    score_threshold: float = HIGH_SCORE_AUDIT_THRESHOLD,
) -> dict[str, pd.DataFrame]:
    sample_columns = [
        "基金代码",
        "基金名称",
        "债基风格标签",
        "基金公司",
        "成立日期",
        "基金规模(亿)",
        "费率合计(%)",
        "规模数据状态",
        "规模数据账龄(天)",
        "母基金名称",
        "关联份额数",
        "关联份额名称",
        "关联份额代码",
        "综合加权得分",
        "近期得分",
        "长期得分",
        "主排除原因",
        "次排除原因",
        "原始原因代码",
    ]
    style_columns = ["债基风格标签", "样本数", "占专题样本比例(%)"]
    empty_sample = pd.DataFrame(columns=sample_columns)
    empty_style = pd.DataFrame(columns=style_columns)
    if ledger_df.empty:
        return {
            "sample_df": empty_sample,
            "style_df": empty_style,
        }

    current = ledger_df.copy()
    current["基金代码"] = current.get("基金代码", pd.Series(dtype=object)).map(_normalize_code)
    current["综合加权得分"] = pd.to_numeric(current.get("综合加权得分"), errors="coerce")
    current["近期得分"] = pd.to_numeric(current.get("近期得分"), errors="coerce")
    current["长期得分"] = pd.to_numeric(current.get("长期得分"), errors="coerce")
    current["主排除原因代码"] = current.get("主排除原因代码", pd.Series(dtype=object)).fillna("").astype(str).str.strip()
    current["债基风格标签"] = current.get("债基风格标签", pd.Series(dtype=object)).fillna("").astype(str).str.strip()
    current = current[
        current["综合加权得分"].ge(float(score_threshold))
        & current["主排除原因代码"].eq(reason_code)
    ].copy()
    if current.empty:
        return {
            "sample_df": empty_sample,
            "style_df": empty_style,
        }

    detail_columns = [
        "基金代码",
        "成立日期",
        "基金规模(亿)",
        "费率合计(%)",
        "规模数据状态",
        "规模数据账龄(天)",
        "母基金名称",
        "关联份额数",
        "关联份额名称",
        "关联份额代码",
    ]
    detail_current = detail_df.copy()
    if not detail_current.empty:
        detail_current = _drop_duplicate_columns(detail_current)
        detail_current["基金代码"] = detail_current.get(
            "基金代码",
            detail_current.get("fcode", pd.Series(dtype=object)),
        ).map(_normalize_code)
        detail_current["综合加权得分"] = pd.to_numeric(detail_current.get("综合加权得分"), errors="coerce")
        detail_current = detail_current.sort_values(
            by=["综合加权得分", "基金代码"],
            ascending=[False, True],
            kind="mergesort",
        )
        detail_current = detail_current.drop_duplicates(subset=["基金代码"], keep="first")
        current = current.merge(
            _select_unique_columns(detail_current, detail_columns),
            on="基金代码",
            how="left",
        )

    current["基金规模(亿)"] = pd.to_numeric(current.get("基金规模(亿)"), errors="coerce")
    current["费率合计(%)"] = pd.to_numeric(current.get("费率合计(%)"), errors="coerce")
    current["规模数据账龄(天)"] = pd.to_numeric(current.get("规模数据账龄(天)"), errors="coerce")
    current["关联份额数"] = pd.to_numeric(current.get("关联份额数"), errors="coerce")
    current = current.sort_values(
        by=["综合加权得分", "长期得分", "近期得分", "基金代码"],
        ascending=[False, False, False, True],
        kind="mergesort",
    )
    total = int(len(current.index))
    style_df = (
        current.groupby("债基风格标签", dropna=False)
        .size()
        .reset_index(name="样本数")
        .sort_values(by=["样本数", "债基风格标签"], ascending=[False, True], kind="mergesort")
    )
    style_df["占专题样本比例(%)"] = style_df["样本数"].apply(
        lambda value: round(float(value) / total * 100, 2) if total else 0.0
    )

    return {
        "sample_df": _select_unique_columns(current, sample_columns),
        "style_df": pd.DataFrame(style_df, columns=style_columns),
    }


def _build_high_score_scale_review_audit(scale_gate_sample_df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    band_columns = ["规模分层", "样本数", "占规模门槛专题比例(%)"]
    age_columns = ["成立年限分层", "样本数", "占规模门槛专题比例(%)"]
    near_columns = [
        "基金代码",
        "基金名称",
        "债基风格标签",
        "基金公司",
        "成立日期",
        "基金规模(亿)",
        "合并规模(亿)",
        "距2亿门槛缺口(亿)",
        "费率合计(%)",
        "规模数据状态",
        "规模数据账龄(天)",
        "关联份额数",
        "综合加权得分",
        "近期得分",
        "长期得分",
        "主排除原因",
        "次排除原因",
        "原始原因代码",
    ]
    empty_band = pd.DataFrame(columns=band_columns)
    empty_age = pd.DataFrame(columns=age_columns)
    empty_near = pd.DataFrame(columns=near_columns)
    if scale_gate_sample_df.empty:
        return {
            "band_df": empty_band,
            "age_df": empty_age,
            "near_df": empty_near,
        }

    current = scale_gate_sample_df.copy()
    current["基金规模(亿)"] = pd.to_numeric(current.get("基金规模(亿)"), errors="coerce")
    current["合并规模(亿)"] = pd.to_numeric(current.get("合并规模(亿)"), errors="coerce")
    current["有效规模(亿)"] = current["合并规模(亿)"].combine_first(current["基金规模(亿)"])
    current["费率合计(%)"] = pd.to_numeric(current.get("费率合计(%)"), errors="coerce")
    current["综合加权得分"] = pd.to_numeric(current.get("综合加权得分"), errors="coerce")
    current["近期得分"] = pd.to_numeric(current.get("近期得分"), errors="coerce")
    current["长期得分"] = pd.to_numeric(current.get("长期得分"), errors="coerce")
    current["成立日期"] = pd.to_datetime(current.get("成立日期"), errors="coerce")
    reference_date = pd.Timestamp("2026-04-15")
    current["成立年限"] = ((reference_date - current["成立日期"]).dt.days / 365.25).clip(lower=0)
    current["距2亿门槛缺口(亿)"] = (float(PROJECT_ABSOLUTE_QUALITY_THRESHOLDS["scale_billion_min"]) - current["有效规模(亿)"]).round(4)
    total = int(len(current.index))

    scale_band = pd.cut(
        current["有效规模(亿)"],
        bins=[-float("inf"), 0.5, 1.0, 1.5, 1.8, 1.95, 2.0],
        labels=["<=0.5亿", "0.5-1.0亿", "1.0-1.5亿", "1.5-1.8亿", "1.8-1.95亿", "1.95-2.0亿"],
        include_lowest=True,
    )
    age_band = pd.cut(
        current["成立年限"],
        bins=[-float("inf"), 1.0, 2.0, 3.0, 5.0, float("inf")],
        labels=["<=1年", "1-2年", "2-3年", "3-5年", ">5年"],
        include_lowest=True,
    )
    band_df = (
        scale_band.value_counts(dropna=False, sort=False)
        .rename_axis("规模分层")
        .reset_index(name="样本数")
    )
    band_df["规模分层"] = band_df["规模分层"].astype(str).replace("nan", "未知")
    band_df["占规模门槛专题比例(%)"] = band_df["样本数"].apply(
        lambda value: round(float(value) / total * 100, 2) if total else 0.0
    )
    age_df = (
        age_band.value_counts(dropna=False, sort=False)
        .rename_axis("成立年限分层")
        .reset_index(name="样本数")
    )
    age_df["成立年限分层"] = age_df["成立年限分层"].astype(str).replace("nan", "未知")
    age_df["占规模门槛专题比例(%)"] = age_df["样本数"].apply(
        lambda value: round(float(value) / total * 100, 2) if total else 0.0
    )

    near_df = current[current["有效规模(亿)"].ge(1.5)].copy()
    near_df["成立日期"] = near_df["成立日期"].dt.strftime("%Y-%m-%d").fillna("")
    near_df = near_df.sort_values(
        by=["有效规模(亿)", "综合加权得分", "基金代码"],
        ascending=[False, False, True],
        kind="mergesort",
    )

    return {
        "band_df": pd.DataFrame(band_df, columns=band_columns),
        "age_df": pd.DataFrame(age_df, columns=age_columns),
        "near_df": _select_unique_columns(near_df, near_columns),
    }


def _build_high_score_scale_near_review_audit(near_df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    review_columns = ["复审建议", "样本数", "占近门槛样本比例(%)"]
    sample_columns = [
        "基金代码",
        "基金名称",
        "债基风格标签",
        "基金公司",
        "成立日期",
        "基金规模(亿)",
        "合并规模(亿)",
        "距2亿门槛缺口(亿)",
        "费率合计(%)",
        "综合加权得分",
        "近期得分",
        "长期得分",
        "主排除原因",
        "次排除原因",
        "原始原因代码",
        "复审建议",
        "复审依据",
    ]
    empty_review = pd.DataFrame(columns=review_columns)
    empty_sample = pd.DataFrame(columns=sample_columns)
    if near_df.empty:
        return {
            "review_df": empty_review,
            "sample_df": empty_sample,
        }

    current = near_df.copy()
    current["基金规模(亿)"] = pd.to_numeric(current.get("基金规模(亿)"), errors="coerce")
    current["合并规模(亿)"] = pd.to_numeric(current.get("合并规模(亿)"), errors="coerce")
    current["有效规模(亿)"] = current["合并规模(亿)"].combine_first(current["基金规模(亿)"])
    current["距2亿门槛缺口(亿)"] = pd.to_numeric(current.get("距2亿门槛缺口(亿)"), errors="coerce")
    current["费率合计(%)"] = pd.to_numeric(current.get("费率合计(%)"), errors="coerce")
    current["综合加权得分"] = pd.to_numeric(current.get("综合加权得分"), errors="coerce")
    current["近期得分"] = pd.to_numeric(current.get("近期得分"), errors="coerce")
    current["长期得分"] = pd.to_numeric(current.get("长期得分"), errors="coerce")
    current["规模数据账龄(天)"] = pd.to_numeric(current.get("规模数据账龄(天)"), errors="coerce")
    current["关联份额数"] = pd.to_numeric(current.get("关联份额数"), errors="coerce")
    current["规模数据状态"] = current.get("规模数据状态", pd.Series(dtype=object)).fillna("").astype(str).str.strip()
    current["原始原因代码"] = current.get("原始原因代码", pd.Series(dtype=object)).fillna("").astype(str).str.strip()

    max_scale_age_days = int(SCALE_DATA_THRESHOLDS.get("max_report_age_days") or 200)

    def _review_row(row: pd.Series) -> tuple[str, str]:
        share_count = float(row.get("关联份额数")) if pd.notna(row.get("关联份额数")) else 1.0
        raw_scale_status = row.get("规模数据状态")
        scale_status = str(raw_scale_status).strip() if pd.notna(raw_scale_status) else ""
        scale_age = float(row.get("规模数据账龄(天)")) if pd.notna(row.get("规模数据账龄(天)")) else None
        reason_codes = str(row.get("原始原因代码") or "").strip()
        scale_value = float(row.get("有效规模(亿)")) if pd.notna(row.get("有效规模(亿)")) else None
        share_scale_value = float(row.get("基金规模(亿)")) if pd.notna(row.get("基金规模(亿)")) else None
        weighted_score = float(row.get("综合加权得分")) if pd.notna(row.get("综合加权得分")) else None

        needs_verify_reasons: list[str] = []
        if scale_status and scale_status.lower() != "nan" and scale_status != "新鲜":
            needs_verify_reasons.append(f"规模数据状态={scale_status}")
        if scale_age is not None and scale_age > max_scale_age_days:
            needs_verify_reasons.append(f"规模数据账龄>{max_scale_age_days}天")
        if share_count > 1 and scale_value is None:
            needs_verify_reasons.append(f"关联份额数={int(share_count)}但缺少合并规模")
        if needs_verify_reasons:
            return "待核实数据/份额结构", "；".join(needs_verify_reasons)

        if (
            scale_value is not None
            and scale_value >= 1.8
            and weighted_score is not None
            and weighted_score >= 80.0
            and reason_codes == "scale_too_small"
        ):
            if share_count > 1:
                if share_scale_value is None or share_scale_value < 2.0:
                    return "待核实数据/份额结构", f"关联份额数={int(share_count)}"
                return "候选惩罚项复审", "同母合并规模>=1.8亿；综合加权得分>=80；当前仅规模单因子触发"
            return "候选惩罚项复审", "规模>=1.8亿；综合加权得分>=80；当前仅规模单因子触发"

        hard_reasons: list[str] = []
        if scale_value is not None and scale_value < 1.8:
            hard_reasons.append("规模距离2亿门槛仍超过0.2亿")
        if weighted_score is not None and weighted_score < 80.0:
            hard_reasons.append("综合加权得分<80")
        if "|" in reason_codes:
            hard_reasons.append("除规模外仍有其他质量因子")
        if not hard_reasons:
            hard_reasons.append("近门槛但暂不满足惩罚项候选条件")
        return "继续硬拦截", "；".join(hard_reasons)

    review_pairs = current.apply(_review_row, axis=1).tolist()
    current["复审建议"] = [item[0] for item in review_pairs]
    current["复审依据"] = [item[1] for item in review_pairs]
    current["成立日期"] = pd.to_datetime(current.get("成立日期"), errors="coerce").dt.strftime("%Y-%m-%d").fillna("")
    current = current.sort_values(
        by=["复审建议", "有效规模(亿)", "综合加权得分", "基金代码"],
        ascending=[True, False, False, True],
        kind="mergesort",
    )

    total = int(len(current.index))
    review_df = (
        current.groupby("复审建议", dropna=False)
        .size()
        .reset_index(name="样本数")
    )
    review_df["占近门槛样本比例(%)"] = review_df["样本数"].apply(
        lambda value: round(float(value) / total * 100, 2) if total else 0.0
    )
    review_order = {
        "候选惩罚项复审": 0,
        "待核实数据/份额结构": 1,
        "继续硬拦截": 2,
    }
    review_df["_order"] = review_df["复审建议"].map(review_order).fillna(99)
    review_df = review_df.sort_values(by=["_order", "复审建议"], ascending=[True, True], kind="mergesort").drop(columns=["_order"])

    return {
        "review_df": pd.DataFrame(review_df, columns=review_columns),
        "sample_df": _select_unique_columns(current, sample_columns),
    }


def _build_standard_screening_audit_summary(
    *,
    style_pass_rate_df: pd.DataFrame,
    style_veto_reason_df: pd.DataFrame,
    reason_group_audit_df: pd.DataFrame,
    quality_factor_group_audit_df: pd.DataFrame,
    quality_factor_detail_audit_df: pd.DataFrame,
    quality_overlap_pair_df: pd.DataFrame,
    quality_overlap_combo_df: pd.DataFrame,
    annual_return_score_sharpe_coupling_audit: dict[str, Any] | None = None,
    score_cluster_explainability_audit: dict[str, Any] | None = None,
    risk_adjusted_metric_stack_review_audit: dict[str, Any] | None = None,
    status_split_audit_df: pd.DataFrame,
    high_score_rejected_df: pd.DataFrame,
    high_score_not_buyable_df: pd.DataFrame,
    high_score_friction_sample_df: pd.DataFrame,
    high_score_friction_reason_df: pd.DataFrame,
    high_score_friction_style_df: pd.DataFrame,
    high_score_scale_gate_sample_df: pd.DataFrame,
    high_score_scale_gate_style_df: pd.DataFrame,
    high_score_scale_gate_band_df: pd.DataFrame,
    high_score_scale_gate_age_df: pd.DataFrame,
    high_score_scale_gate_near_df: pd.DataFrame,
    high_score_scale_gate_near_review_df: pd.DataFrame,
    high_score_scale_gate_near_review_sample_df: pd.DataFrame,
    high_score_fee_gate_sample_df: pd.DataFrame,
    high_score_fee_gate_style_df: pd.DataFrame,
    high_score_friction_gate_review_audit: dict[str, Any] | None = None,
    risk_metric_cap_audit: dict[str, Any] | None = None,
) -> dict[str, Any]:
    annual_return_score_sharpe_coupling_audit = dict(annual_return_score_sharpe_coupling_audit or {})
    score_cluster_explainability_audit = dict(score_cluster_explainability_audit or {})
    risk_adjusted_metric_stack_review_audit = dict(risk_adjusted_metric_stack_review_audit or {})
    high_score_friction_gate_review_audit = dict(high_score_friction_gate_review_audit or {})
    risk_metric_cap_audit = dict(risk_metric_cap_audit or {})
    top_pass_styles = style_pass_rate_df.head(8).fillna("").to_dict(orient="records") if not style_pass_rate_df.empty else []
    top_veto_styles = style_veto_reason_df.head(20).fillna("").to_dict(orient="records") if not style_veto_reason_df.empty else []
    top_reason_groups = reason_group_audit_df.head(8).fillna("").to_dict(orient="records") if not reason_group_audit_df.empty else []
    top_quality_factor_groups = quality_factor_group_audit_df.head(8).fillna("").to_dict(orient="records") if not quality_factor_group_audit_df.empty else []
    top_quality_factor_details = quality_factor_detail_audit_df.head(12).fillna("").to_dict(orient="records") if not quality_factor_detail_audit_df.empty else []
    top_quality_overlap_pairs = quality_overlap_pair_df.head(12).fillna("").to_dict(orient="records") if not quality_overlap_pair_df.empty else []
    top_quality_overlap_combos = quality_overlap_combo_df.head(12).fillna("").to_dict(orient="records") if not quality_overlap_combo_df.empty else []
    top_status_splits = status_split_audit_df.head(16).fillna("").to_dict(orient="records") if not status_split_audit_df.empty else []
    top_high_score_friction_reasons = high_score_friction_reason_df.head(8).fillna("").to_dict(orient="records") if not high_score_friction_reason_df.empty else []
    top_high_score_friction_styles = high_score_friction_style_df.head(8).fillna("").to_dict(orient="records") if not high_score_friction_style_df.empty else []
    top_high_score_friction_samples = high_score_friction_sample_df.head(12).fillna("").to_dict(orient="records") if not high_score_friction_sample_df.empty else []
    top_high_score_scale_gate_styles = high_score_scale_gate_style_df.head(8).fillna("").to_dict(orient="records") if not high_score_scale_gate_style_df.empty else []
    top_high_score_scale_gate_samples = high_score_scale_gate_sample_df.head(12).fillna("").to_dict(orient="records") if not high_score_scale_gate_sample_df.empty else []
    top_high_score_scale_gate_bands = high_score_scale_gate_band_df.head(8).fillna("").to_dict(orient="records") if not high_score_scale_gate_band_df.empty else []
    top_high_score_scale_gate_ages = high_score_scale_gate_age_df.head(8).fillna("").to_dict(orient="records") if not high_score_scale_gate_age_df.empty else []
    top_high_score_scale_gate_near = high_score_scale_gate_near_df.head(12).fillna("").to_dict(orient="records") if not high_score_scale_gate_near_df.empty else []
    top_high_score_scale_gate_near_reviews = high_score_scale_gate_near_review_df.head(8).fillna("").to_dict(orient="records") if not high_score_scale_gate_near_review_df.empty else []
    top_high_score_scale_gate_near_review_samples = high_score_scale_gate_near_review_sample_df.head(12).fillna("").to_dict(orient="records") if not high_score_scale_gate_near_review_sample_df.empty else []
    top_high_score_fee_gate_styles = high_score_fee_gate_style_df.head(8).fillna("").to_dict(orient="records") if not high_score_fee_gate_style_df.empty else []
    top_high_score_fee_gate_samples = high_score_fee_gate_sample_df.head(12).fillna("").to_dict(orient="records") if not high_score_fee_gate_sample_df.empty else []
    return {
        "available": True,
        "high_score_threshold": HIGH_SCORE_AUDIT_THRESHOLD,
        "style_pass_rate_preview": top_pass_styles,
        "style_veto_reason_preview": top_veto_styles,
        "reason_group_preview": top_reason_groups,
        "quality_factor_group_preview": top_quality_factor_groups,
        "quality_factor_detail_preview": top_quality_factor_details,
        "quality_overlap_pair_preview": top_quality_overlap_pairs,
        "quality_overlap_combo_preview": top_quality_overlap_combos,
        "annual_return_score_sharpe_coupling": annual_return_score_sharpe_coupling_audit,
        "score_cluster_explainability": score_cluster_explainability_audit,
        "risk_adjusted_metric_stack_review": risk_adjusted_metric_stack_review_audit,
        "status_split_preview": top_status_splits,
        "high_score_rejected_count": int(len(high_score_rejected_df.index)),
        "high_score_not_buyable_count": int(len(high_score_not_buyable_df.index)),
        "high_score_friction_count": int(len(high_score_friction_sample_df.index)),
        "high_score_friction_reason_preview": top_high_score_friction_reasons,
        "high_score_friction_style_preview": top_high_score_friction_styles,
        "high_score_friction_sample_preview": top_high_score_friction_samples,
        "high_score_scale_gate_count": int(len(high_score_scale_gate_sample_df.index)),
        "high_score_scale_gate_style_preview": top_high_score_scale_gate_styles,
        "high_score_scale_gate_sample_preview": top_high_score_scale_gate_samples,
        "high_score_scale_gate_near_count": int(len(high_score_scale_gate_near_df.index)),
        "high_score_scale_gate_candidate_penalty_count": int(
            high_score_scale_gate_near_review_sample_df.get("复审建议", pd.Series(dtype=object)).eq("候选惩罚项复审").sum()
        ),
        "high_score_scale_gate_band_preview": top_high_score_scale_gate_bands,
        "high_score_scale_gate_age_preview": top_high_score_scale_gate_ages,
        "high_score_scale_gate_near_preview": top_high_score_scale_gate_near,
        "high_score_scale_gate_near_review_preview": top_high_score_scale_gate_near_reviews,
        "high_score_scale_gate_near_review_sample_preview": top_high_score_scale_gate_near_review_samples,
        "high_score_fee_gate_count": int(len(high_score_fee_gate_sample_df.index)),
        "high_score_fee_gate_style_preview": top_high_score_fee_gate_styles,
        "high_score_fee_gate_sample_preview": top_high_score_fee_gate_samples,
        "high_score_friction_gate_review": high_score_friction_gate_review_audit,
        "cap_hit_overview": {
            "sharpe_cap_hit_count": int(risk_metric_cap_audit.get("sharpe_cap_hit_count") or 0),
            "sortino_cap_hit_count": int(risk_metric_cap_audit.get("sortino_cap_hit_count") or 0),
            "either_cap_hit_count": int(risk_metric_cap_audit.get("either_cap_hit_count") or 0),
            "either_cap_hit_ratio": float(risk_metric_cap_audit.get("either_cap_hit_ratio") or 0.0),
            "top_n_changed_count": int(risk_metric_cap_audit.get("top_n_changed_count") or 0),
        },
    }


def _build_project_candidate_universe(
    *,
    candidate_records_df: pd.DataFrame,
    metadata_df: pd.DataFrame,
    manager_metrics_df: pd.DataFrame,
    returns_df: pd.DataFrame,
    risk_df: pd.DataFrame,
    long_term_feature_df: pd.DataFrame,
    accum_nav_df: pd.DataFrame,
    pick_metrics_df: pd.DataFrame | None = None,
    support_df: pd.DataFrame,
) -> pd.DataFrame:
    decision_map = {
        _normalize_code(row.get("fcode")): row
        for row in candidate_records_df.to_dict(orient="records")
        if _normalize_code(row.get("fcode"))
    } if not candidate_records_df.empty else {}

    df = _build_project_owned_candidate_universe(
        metadata_df=metadata_df,
        manager_metrics_df=manager_metrics_df,
        returns_df=returns_df,
        risk_df=risk_df,
        long_term_feature_df=long_term_feature_df,
        accum_nav_df=accum_nav_df,
        pick_metrics_df=pick_metrics_df,
        support_df=support_df,
        decision_map=decision_map,
    )
    if df.empty:
        return df
    return _assign_project_scores(df)


def _apply_publish_post_filters(
    df: pd.DataFrame,
    *,
    threshold_mode: str,
    style_thresholds: dict[str, dict[str, float]],
    global_thresholds: dict[str, float],
    evaluated_at: datetime | None = None,
) -> pd.DataFrame:
    return apply_publish_post_filters_impl(
        df,
        threshold_mode=threshold_mode,
        style_thresholds=style_thresholds,
        global_thresholds=global_thresholds,
        evaluated_at=evaluated_at,
    )


def _apply_final_official_selection(
    df: pd.DataFrame,
    *,
    settings: Settings | None = None,
    tracker_features_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    current = _ensure_formal_score_column(df)
    if current.empty:
        return current
    current = current[
        current.apply(lambda row: fee_semantics_ready(row.to_dict()), axis=1)
    ].copy()
    if current.empty:
        return current
    if FORMAL_GROUP_LABEL not in current.columns:
        current[FORMAL_GROUP_LABEL] = current.apply(lambda row: _formal_group_from_row(row.to_dict()), axis=1)
    current[FORMAL_SCORE_LABEL] = pd.to_numeric(current.get(FORMAL_SCORE_LABEL), errors="coerce")
    current["综合加权得分"] = pd.to_numeric(current.get("综合加权得分"), errors="coerce")
    current["1年-年化收益(%)"] = pd.to_numeric(current.get("1年-年化收益(%)"), errors="coerce")
    current["1年-夏普比率"] = pd.to_numeric(current.get("1年-夏普比率"), errors="coerce")
    current["1年-卡玛比率"] = pd.to_numeric(current.get("1年-卡玛比率"), errors="coerce")
    current["费率合计(%)"] = pd.to_numeric(current.get("费率合计(%)"), errors="coerce")
    current["基金规模(亿)"] = pd.to_numeric(current.get("基金规模(亿)"), errors="coerce")

    current["_confidence_sort"] = current["项目质量信心"].map({"高信心": 3, "中信心": 2, "边界观察": 1}).fillna(0)
    current["_admission_sort"] = current["项目准入方式"].map({"增强债基补充准入": 2, "标准动态阈值": 1}).fillna(0)
    current = current.sort_values(
        by=["_confidence_sort", FORMAL_SCORE_LABEL, "综合加权得分", "1年-年化收益(%)", "1年-夏普比率", "1年-卡玛比率", "基金代码"],
        ascending=[False, False, False, False, False, False, True],
        kind="mergesort",
    ).reset_index(drop=True)

    if (
        settings is not None
        and isinstance(tracker_features_df, pd.DataFrame)
        and not tracker_features_df.empty
        and int(getattr(settings, "final_selection_target_max", 0) or 0) > 0
        and len(current.index) > int(getattr(settings, "final_selection_target_max", 0) or 0)
        and "基金代码" in current.columns
    ):
        tracker_current = tracker_features_df.copy()
        tracker_current["基金代码"] = tracker_current.get("基金代码", pd.Series(dtype=object)).map(_normalize_code)
        tracker_current = tracker_current.drop_duplicates(subset=["基金代码"], keep="first")
        merge_columns = [column for column in tracker_current.columns if column != "基金代码"]
        current["基金代码"] = current.get("基金代码", pd.Series(dtype=object)).map(_normalize_code)
        current = current.merge(
            tracker_current[["基金代码", *merge_columns]],
            on="基金代码",
            how="left",
        )
        for column_name in ["出现次数", "出现频率(%)", "连续缺席期数", "当前是否在榜", "跟踪总期数"]:
            current = _coalesce_merge_conflict_columns(current, column_name)
        current["出现次数"] = pd.to_numeric(current.get("出现次数"), errors="coerce").fillna(1.0)
        current["出现频率(%)"] = pd.to_numeric(current.get("出现频率(%)"), errors="coerce").fillna(100.0)
        current["跟踪总期数"] = pd.to_numeric(current.get("跟踪总期数"), errors="coerce").fillna(1.0)
        current["_final_confidence_bonus"] = current["项目质量信心"].map(
            {"高信心": FINAL_SELECTION_HIGH_CONFIDENCE_BONUS, "中信心": FINAL_SELECTION_MEDIUM_CONFIDENCE_BONUS, "边界观察": -FINAL_SELECTION_BORDERLINE_CONFIDENCE_PENALTY}
        ).fillna(0.0)
        current["_final_new_high_bonus"] = (
            (
                current["出现次数"].le(2)
                & current[FORMAL_SCORE_LABEL].ge(float(getattr(settings, "final_selection_high_score_new_fund_threshold", 85.0) or 85.0))
            )
            .astype(float)
            * FINAL_SELECTION_HIGH_SCORE_NEW_FUND_BONUS
        )
        current["_final_new_low_penalty"] = (
            (
                current["出现次数"].le(2)
                & current[FORMAL_SCORE_LABEL].lt(float(getattr(settings, "final_selection_low_score_new_fund_threshold", 75.0) or 75.0))
            )
            .astype(float)
            * FINAL_SELECTION_LOW_SCORE_NEW_FUND_PENALTY
        )
        current["_final_stability_score"] = (
            current[FORMAL_SCORE_LABEL].fillna(0.0)
            + current["_final_confidence_bonus"]
            + current["出现次数"].clip(lower=0).fillna(0.0) * FINAL_SELECTION_HISTORY_OCCURRENCE_WEIGHT
            + current["出现频率(%)"].clip(lower=0).fillna(0.0) * FINAL_SELECTION_HISTORY_FREQUENCY_WEIGHT
            + current["_final_new_high_bonus"]
            - current["_final_new_low_penalty"]
        )

        target_total = int(getattr(settings, "final_selection_target_max", 0) or 0)
        group_min_ratio = float(getattr(settings, "final_selection_group_min_ratio", 0.25) or 0.25)
        group_label_series = current.get(FORMAL_GROUP_LABEL, pd.Series("", index=current.index, dtype=object)).fillna("").astype(str)
        min_group_quota = max(1, int(round(target_total * max(0.0, min(group_min_ratio, 0.49)))))
        selected_parts: list[pd.DataFrame] = []
        selected_codes: set[str] = set()
        if FORMAL_GROUP_LABEL in current.columns:
            for group_label in (STABLE_BOND_GROUP_LABEL, ENHANCED_BOND_GROUP_LABEL):
                group_df = current[group_label_series.eq(group_label)].copy()
                if group_df.empty:
                    continue
                keep_count = min(len(group_df.index), min_group_quota)
                picked = group_df.sort_values(
                    by=["_final_stability_score", FORMAL_SCORE_LABEL, "出现次数", "出现频率(%)", "基金代码"],
                    ascending=[False, False, False, False, True],
                    kind="mergesort",
                ).head(keep_count).copy()
                if picked.empty:
                    continue
                selected_parts.append(picked)
                selected_codes.update(
                    code for code in picked.get("基金代码", pd.Series(dtype=object)).astype(str).tolist() if code
                )
        remaining_slots = max(0, target_total - len(selected_codes))
        remaining_df = current[
            ~current.get("基金代码", pd.Series(dtype=object)).astype(str).isin(selected_codes)
        ].copy()
        if remaining_slots > 0 and not remaining_df.empty:
            picked_remaining = remaining_df.sort_values(
                by=["_final_stability_score", FORMAL_SCORE_LABEL, "出现次数", "出现频率(%)", "基金代码"],
                ascending=[False, False, False, False, True],
                kind="mergesort",
            ).head(remaining_slots).copy()
            if not picked_remaining.empty:
                selected_parts.append(picked_remaining)
        if selected_parts:
            current = pd.concat(selected_parts, ignore_index=True)
            current = current.sort_values(
                by=["_final_stability_score", FORMAL_SCORE_LABEL, "出现次数", "出现频率(%)", "基金代码"],
                ascending=[False, False, False, False, True],
                kind="mergesort",
            ).reset_index(drop=True)

    current = current.drop(
        columns=[
            "_confidence_sort",
            "_admission_sort",
            "_final_confidence_bonus",
            "_final_new_high_bonus",
            "_final_new_low_penalty",
            "_final_stability_score",
        ],
        errors="ignore",
    )
    return current.reset_index(drop=True)


def _build_final_selection_contraction_explainability(
    candidate_df: pd.DataFrame,
    *,
    official_df: pd.DataFrame,
    removed_after_final_selection_df: pd.DataFrame,
    tracker_features_df: pd.DataFrame,
    settings: Settings | None = None,
) -> pd.DataFrame:
    columns = [
        "基金代码",
        "基金名称",
        FORMAL_GROUP_LABEL,
        "债基风格标签",
        "正式精选状态",
        "最终精选原因代码",
        "最终精选解释",
        "最终精选稳定性排名",
        "分组稳定性排名",
        FORMAL_SCORE_LABEL,
        "综合加权得分",
        "1年-年化收益(%)",
        "项目质量信心",
        "出现次数",
        "出现频率(%)",
        "跟踪总期数",
        "_final_stability_score",
        "_final_confidence_bonus",
        "_final_history_occurrence_bonus",
        "_final_history_frequency_bonus",
        "_final_new_high_bonus",
        "_final_new_low_penalty",
        "距整体入围最低分差",
        "距同分组入围最低分差",
    ]
    if not isinstance(candidate_df, pd.DataFrame) or candidate_df.empty:
        return pd.DataFrame(columns=columns)

    current = _ensure_export_formal_score_column(candidate_df.copy())
    if "基金代码" not in current.columns:
        return pd.DataFrame(columns=columns)
    current["基金代码"] = current["基金代码"].map(_normalize_code)
    current = current[current["基金代码"].astype(str).str.strip() != ""].copy()
    current = current.drop_duplicates(subset=["基金代码"], keep="first")
    if current.empty:
        return pd.DataFrame(columns=columns)

    if FORMAL_GROUP_LABEL not in current.columns:
        current[FORMAL_GROUP_LABEL] = current.apply(lambda row: _formal_group_from_row(row.to_dict()), axis=1)

    official_codes = {
        _normalize_code(code)
        for code in official_df.get("基金代码", pd.Series(dtype=object)).astype(str).tolist()
        if _normalize_code(code)
    } if isinstance(official_df, pd.DataFrame) and not official_df.empty else set()
    removed_codes = {
        _normalize_code(code)
        for code in removed_after_final_selection_df.get("基金代码", pd.Series(dtype=object)).astype(str).tolist()
        if _normalize_code(code)
    } if isinstance(removed_after_final_selection_df, pd.DataFrame) and not removed_after_final_selection_df.empty else set()
    focus_codes = official_codes | removed_codes
    if focus_codes:
        current = current[current["基金代码"].isin(focus_codes)].copy()
    if current.empty:
        return pd.DataFrame(columns=columns)

    if isinstance(tracker_features_df, pd.DataFrame) and not tracker_features_df.empty:
        tracker_current = tracker_features_df.copy()
        tracker_current["基金代码"] = tracker_current.get("基金代码", pd.Series(dtype=object)).map(_normalize_code)
        tracker_current = tracker_current.drop_duplicates(subset=["基金代码"], keep="first")
        merge_columns = [column for column in tracker_current.columns if column != "基金代码"]
        if merge_columns:
            current = current.merge(
                tracker_current[["基金代码", *merge_columns]],
                on="基金代码",
                how="left",
            )
            for column_name in ["出现次数", "出现频率(%)", "连续缺席期数", "当前是否在榜", "跟踪总期数"]:
                current = _coalesce_merge_conflict_columns(current, column_name)

    for column_name, default_value in {
        "出现次数": 1.0,
        "出现频率(%)": 100.0,
        "跟踪总期数": 1.0,
    }.items():
        if column_name not in current.columns:
            current[column_name] = default_value
        current[column_name] = pd.to_numeric(current[column_name], errors="coerce").fillna(default_value)

    current[FORMAL_SCORE_LABEL] = pd.to_numeric(current.get(FORMAL_SCORE_LABEL), errors="coerce")
    current["综合加权得分"] = pd.to_numeric(current.get("综合加权得分"), errors="coerce")
    current["1年-年化收益(%)"] = pd.to_numeric(current.get("1年-年化收益(%)"), errors="coerce")
    current["项目质量信心"] = current.get("项目质量信心", pd.Series("", index=current.index, dtype=object)).fillna("").astype(str)

    high_score_new_threshold = float(
        getattr(settings, "final_selection_high_score_new_fund_threshold", 85.0) if settings is not None else 85.0
    )
    low_score_new_threshold = float(
        getattr(settings, "final_selection_low_score_new_fund_threshold", 75.0) if settings is not None else 75.0
    )
    current["_final_confidence_bonus"] = current["项目质量信心"].map(
        {
            "高信心": FINAL_SELECTION_HIGH_CONFIDENCE_BONUS,
            "中信心": FINAL_SELECTION_MEDIUM_CONFIDENCE_BONUS,
            "边界观察": -FINAL_SELECTION_BORDERLINE_CONFIDENCE_PENALTY,
        }
    ).fillna(0.0)
    current["_final_new_high_bonus"] = (
        current["出现次数"].le(2)
        & current[FORMAL_SCORE_LABEL].ge(high_score_new_threshold)
    ).astype(float) * FINAL_SELECTION_HIGH_SCORE_NEW_FUND_BONUS
    current["_final_new_low_penalty"] = (
        current["出现次数"].le(2)
        & current[FORMAL_SCORE_LABEL].lt(low_score_new_threshold)
    ).astype(float) * FINAL_SELECTION_LOW_SCORE_NEW_FUND_PENALTY
    current["_final_history_occurrence_bonus"] = (
        current["出现次数"].clip(lower=0).fillna(0.0) * FINAL_SELECTION_HISTORY_OCCURRENCE_WEIGHT
    )
    current["_final_history_frequency_bonus"] = (
        current["出现频率(%)"].clip(lower=0).fillna(0.0) * FINAL_SELECTION_HISTORY_FREQUENCY_WEIGHT
    )
    current["_final_stability_score"] = (
        current[FORMAL_SCORE_LABEL].fillna(0.0)
        + current["_final_confidence_bonus"]
        + current["_final_history_occurrence_bonus"]
        + current["_final_history_frequency_bonus"]
        + current["_final_new_high_bonus"]
        - current["_final_new_low_penalty"]
    )

    removed_reason_map: dict[str, str] = {}
    if isinstance(removed_after_final_selection_df, pd.DataFrame) and not removed_after_final_selection_df.empty:
        reason_frame = removed_after_final_selection_df.copy()
        reason_frame["基金代码"] = reason_frame.get("基金代码", pd.Series(dtype=object)).map(_normalize_code)
        for row in reason_frame.to_dict(orient="records"):
            code = _normalize_code(row.get("基金代码"))
            if code and code not in removed_reason_map:
                removed_reason_map[code] = str(row.get("项目推荐原因代码") or "").strip()

    current["正式精选状态"] = current["基金代码"].map(
        lambda code: "正式入围" if code in official_codes else ("最终精选未入围" if code in removed_codes else "未入本轮解释范围")
    )
    current["最终精选原因代码"] = current["基金代码"].map(lambda code: removed_reason_map.get(code, ""))
    current["最终精选解释"] = current.apply(
        lambda row: (
            "稳定性收敛后保留"
            if row["正式精选状态"] == "正式入围"
            else (
                "稳定性 cap 收敛，转入自动诊断"
                if "final_selection_stability_cap" in str(row.get("最终精选原因代码") or "")
                else "发布后复核未入围"
            )
        ),
        axis=1,
    )
    current = current.sort_values(
        by=["_final_stability_score", FORMAL_SCORE_LABEL, "出现次数", "出现频率(%)", "基金代码"],
        ascending=[False, False, False, False, True],
        kind="mergesort",
    ).reset_index(drop=True)
    current["最终精选稳定性排名"] = range(1, len(current.index) + 1)
    current["分组稳定性排名"] = (
        current.groupby(FORMAL_GROUP_LABEL, dropna=False).cumcount() + 1
        if FORMAL_GROUP_LABEL in current.columns
        else current["最终精选稳定性排名"]
    )
    selected_score = pd.to_numeric(
        current.loc[current["基金代码"].isin(official_codes), "_final_stability_score"],
        errors="coerce",
    )
    selected_min_score = float(selected_score.min()) if not selected_score.dropna().empty else math.nan
    if math.isfinite(selected_min_score):
        current["距整体入围最低分差"] = (current["_final_stability_score"] - selected_min_score).round(4)
    else:
        current["距整体入围最低分差"] = pd.NA
    group_selected_min = (
        current[current["基金代码"].isin(official_codes)]
        .groupby(FORMAL_GROUP_LABEL)["_final_stability_score"]
        .min()
        .to_dict()
        if FORMAL_GROUP_LABEL in current.columns
        else {}
    )
    current["距同分组入围最低分差"] = current.apply(
        lambda row: (
            round(float(row["_final_stability_score"]) - float(group_selected_min.get(row.get(FORMAL_GROUP_LABEL))), 4)
            if group_selected_min.get(row.get(FORMAL_GROUP_LABEL)) is not None
            else pd.NA
        ),
        axis=1,
    )
    current["_status_sort"] = current["正式精选状态"].map({"最终精选未入围": 0, "正式入围": 1}).fillna(2)
    current = current.sort_values(
        by=["_status_sort", "最终精选稳定性排名", "基金代码"],
        ascending=[True, True, True],
        kind="mergesort",
    )
    current = _drop_duplicate_columns(current)
    return _select_unique_columns(current, columns).reset_index(drop=True)


def _build_final_selection_contraction_summary(
    contraction_df: pd.DataFrame,
    *,
    preview_top_n: int,
) -> dict[str, Any]:
    if not isinstance(contraction_df, pd.DataFrame) or contraction_df.empty:
        return {
            "available": False,
            "input_count": 0,
            "official_count": 0,
            "removed_count": 0,
            "stability_cap_removed_count": 0,
            "style_summary": [],
            "preview": [],
        }

    current = contraction_df.copy()
    status = current.get("正式精选状态", pd.Series(dtype=object)).fillna("").astype(str)
    reason_codes = current.get("最终精选原因代码", pd.Series(dtype=object)).fillna("").astype(str)
    official_count = int(status.eq("正式入围").sum())
    removed_count = int(status.eq("最终精选未入围").sum())
    stability_cap_removed_count = int(
        (status.eq("最终精选未入围") & reason_codes.str.contains("final_selection_stability_cap", na=False)).sum()
    )
    removed_df = current[status.eq("最终精选未入围")].copy()
    official_df = current[status.eq("正式入围")].copy()
    style_summary: list[dict[str, Any]] = []
    if FORMAL_GROUP_LABEL in current.columns:
        grouped = current.groupby(FORMAL_GROUP_LABEL, dropna=False)
        for group_label, group_df in grouped:
            group_status = group_df.get("正式精选状态", pd.Series(dtype=object)).fillna("").astype(str)
            style_summary.append(
                {
                    FORMAL_GROUP_LABEL: str(group_label or "未分组"),
                    "输入样本数": int(len(group_df.index)),
                    "正式入围数": int(group_status.eq("正式入围").sum()),
                    "最终精选未入围数": int(group_status.eq("最终精选未入围").sum()),
                }
            )
    preview_df = pd.concat(
        [
            removed_df.sort_values("最终精选稳定性排名", kind="mergesort").head(preview_top_n),
            official_df.sort_values("最终精选稳定性排名", kind="mergesort").head(preview_top_n),
        ],
        ignore_index=True,
    )
    preview_columns = [
        "基金代码",
        "基金名称",
        FORMAL_GROUP_LABEL,
        "正式精选状态",
        "最终精选稳定性排名",
        FORMAL_SCORE_LABEL,
        "_final_stability_score",
        "出现次数",
        "出现频率(%)",
        "最终精选解释",
    ]
    selected_min = pd.to_numeric(official_df.get("_final_stability_score"), errors="coerce").min() if not official_df.empty else None
    removed_max = pd.to_numeric(removed_df.get("_final_stability_score"), errors="coerce").max() if not removed_df.empty else None
    selected_min_value = float(selected_min) if selected_min is not None and math.isfinite(float(selected_min)) else None
    removed_max_value = float(removed_max) if removed_max is not None and math.isfinite(float(removed_max)) else None
    return {
        "available": True,
        "input_count": int(len(current.index)),
        "official_count": official_count,
        "removed_count": removed_count,
        "stability_cap_removed_count": stability_cap_removed_count,
        "style_summary": style_summary,
        "selected_min_stability_score": selected_min_value,
        "removed_max_stability_score": removed_max_value,
        "preview": _select_unique_columns(preview_df, preview_columns).fillna("").astype(str).to_dict(orient="records"),
    }


def _dataframe_from_payload_records(payload: dict[str, Any], key: str) -> pd.DataFrame:
    records = payload.get(key)
    return pd.DataFrame(records if isinstance(records, list) else [])


def _build_final_selection_contraction_from_publish_chain_snapshot(
    snapshot_payload: dict[str, Any],
    *,
    settings: Settings | None,
) -> pd.DataFrame:
    if not isinstance(snapshot_payload, dict) or not snapshot_payload:
        return pd.DataFrame()

    existing_df = _dataframe_from_payload_records(
        snapshot_payload,
        "final_selection_contraction_records",
    )
    if not existing_df.empty:
        return existing_df

    official_df = _dataframe_from_payload_records(snapshot_payload, "final_official_records")
    removed_df = _dataframe_from_payload_records(snapshot_payload, "final_selection_removed_records")
    candidate_df = _dataframe_from_payload_records(snapshot_payload, "dynamic_post_review_records")
    if candidate_df.empty:
        candidate_df = pd.concat([official_df, removed_df], ignore_index=True)
    if candidate_df.empty:
        return pd.DataFrame()

    return _build_final_selection_contraction_explainability(
        candidate_df,
        official_df=official_df,
        removed_after_final_selection_df=removed_df,
        tracker_features_df=pd.DataFrame(),
        settings=settings,
    )


def _load_lightweight_tracker_features_for_contraction(
    *,
    settings: Settings,
    localized_artifacts: dict[str, Any],
) -> pd.DataFrame:
    tracker_excel = _resolve_project_path(settings, (localized_artifacts or {}).get("project_tracker_excel"))
    if tracker_excel is None or not tracker_excel.exists() or not tracker_excel.is_file():
        return pd.DataFrame()

    try:
        tracker_df = pd.read_excel(tracker_excel, sheet_name="完整数据", dtype=str, header=2)
    except Exception:
        return pd.DataFrame()
    if tracker_df.empty:
        return pd.DataFrame()

    tracker_df = tracker_df.fillna("")
    code_column = _pick_column(tracker_df, ["基金代码", "fcode", "代码", "fund_code"])
    if not code_column:
        return pd.DataFrame()

    current = pd.DataFrame(
        {
            "基金代码": tracker_df[code_column].map(_normalize_code),
            "出现次数": pd.to_numeric(tracker_df.get("出现次数"), errors="coerce"),
            "出现频率(%)": pd.to_numeric(tracker_df.get("出现频率(%)"), errors="coerce"),
            "连续缺席期数": pd.to_numeric(tracker_df.get("连续缺席期数"), errors="coerce"),
            "当前是否在榜": tracker_df.get("当前是否在榜", pd.Series(dtype=object)).fillna("").astype(str),
            "跟踪总期数": pd.to_numeric(tracker_df.get("累计跟踪期数"), errors="coerce"),
        }
    )
    current = current[current["基金代码"].astype(str).str.strip() != ""].copy()
    if current.empty:
        return pd.DataFrame()

    current["出现次数"] = current["出现次数"].fillna(1.0)
    current["出现频率(%)"] = current["出现频率(%)"].fillna(100.0)
    current["连续缺席期数"] = current["连续缺席期数"].fillna(0.0)
    current["当前是否在榜"] = current["当前是否在榜"].replace({"": "是"})
    current["跟踪总期数"] = current["跟踪总期数"].fillna(1.0)
    return current.drop_duplicates(subset=["基金代码"], keep="first").reset_index(drop=True)


def _annotate_lightweight_publish_chain_snapshot(
    snapshot_payload: dict[str, Any],
    *,
    run_id: str,
    generated_at: str,
) -> dict[str, Any]:
    if not isinstance(snapshot_payload, dict) or not snapshot_payload:
        return {}
    current = dict(snapshot_payload)
    source_run_id = str(current.get("content_source_run_id") or current.get("run_id") or "").strip()
    source_generated_at = str(
        current.get("content_source_generated_at") or current.get("generated_at") or ""
    ).strip()
    current["run_id"] = str(run_id or "").strip()
    current["generated_at"] = str(generated_at or current.get("generated_at") or "").strip()
    current["report_runtime_run_id"] = str(run_id or "").strip()
    current["lightweight_refresh_reused_snapshot"] = True
    current["snapshot_scope"] = "lightweight_reuse_report_runtime"
    current["records_source"] = "publish_chain_snapshot_reuse"
    if source_run_id:
        current["content_source_run_id"] = source_run_id
    if source_generated_at:
        current["content_source_generated_at"] = source_generated_at
    return current


def _refresh_lightweight_final_selection_contraction(
    *,
    settings: Settings,
    run_id: str,
    generated_at: str,
    current_nav_snapshot_date: str,
    localized_artifacts: dict[str, Any],
    publish_summary: dict[str, Any],
    preview_top_n: int,
) -> dict[str, Any]:
    current_summary = dict(publish_summary or {})
    existing_summary = current_summary.get("final_selection_contraction")
    existing_summary_available = (
        isinstance(existing_summary, dict) and existing_summary.get("available") is True
    )

    snapshot_path = _resolve_project_path(
        settings,
        (localized_artifacts or {}).get("publish_chain_snapshot_json"),
    )
    raw_snapshot_payload = _read_json_file(snapshot_path)
    snapshot_payload = _annotate_lightweight_publish_chain_snapshot(
        raw_snapshot_payload,
        run_id=run_id,
        generated_at=generated_at,
    )
    official_df = _dataframe_from_payload_records(snapshot_payload, "final_official_records")
    removed_df = _dataframe_from_payload_records(snapshot_payload, "final_selection_removed_records")
    candidate_df = _dataframe_from_payload_records(snapshot_payload, "dynamic_post_review_records")
    if candidate_df.empty:
        candidate_df = pd.concat([official_df, removed_df], ignore_index=True)

    tracker_features_df = pd.DataFrame()
    tracker_feature_source = ""
    if not candidate_df.empty:
        tracker_features_df = _build_final_selection_tracker_features(
            settings,
            current_run_id=run_id,
            current_generated_at=generated_at,
            current_nav_snapshot_date=current_nav_snapshot_date,
            current_df=candidate_df,
        )
        if not tracker_features_df.empty:
            tracker_feature_source = "canonical_history_rebuild"
    if tracker_features_df.empty:
        tracker_features_df = _load_lightweight_tracker_features_for_contraction(
            settings=settings,
            localized_artifacts=localized_artifacts,
        )
        if not tracker_features_df.empty:
            tracker_feature_source = "lightweight_tracker_workbook"
    contraction_df = (
        _build_final_selection_contraction_explainability(
            candidate_df,
            official_df=official_df,
            removed_after_final_selection_df=removed_df,
            tracker_features_df=tracker_features_df,
            settings=settings,
        )
        if not candidate_df.empty
        else _build_final_selection_contraction_from_publish_chain_snapshot(
            snapshot_payload,
            settings=settings,
        )
    )
    if not tracker_features_df.empty:
        snapshot_payload["final_selection_contraction_records"] = (
            contraction_df.fillna("").astype(str).to_dict(orient="records")
        )
        snapshot_payload["tracker_features_available"] = True
        snapshot_payload["tracker_feature_source"] = tracker_feature_source
    snapshot_payload_changed = bool(snapshot_payload) and snapshot_payload != raw_snapshot_payload
    snapshot_records = snapshot_payload.get("final_selection_contraction_records") if snapshot_payload else []
    should_write_snapshot = bool(snapshot_payload_changed)
    if snapshot_payload and (not isinstance(snapshot_records, list) or not snapshot_records) and not contraction_df.empty:
        snapshot_payload["final_selection_contraction_records"] = (
            contraction_df.fillna("").astype(str).to_dict(orient="records")
        )
        should_write_snapshot = True
    if snapshot_path is not None and snapshot_payload and should_write_snapshot:
        try:
            snapshot_path.write_text(
                json.dumps(snapshot_payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except OSError:
            pass
    if existing_summary_available:
        return current_summary
    if contraction_df.empty:
        return current_summary

    rebuilt_summary = _build_final_selection_contraction_summary(
        contraction_df,
        preview_top_n=preview_top_n,
    )
    rebuilt_summary["source"] = "publish_chain_snapshot_lightweight_rebuild"
    rebuilt_summary["recovered_from_publish_chain_snapshot"] = True
    rebuilt_summary["tracker_features_available"] = not tracker_features_df.empty
    if not tracker_features_df.empty:
        rebuilt_summary["tracker_feature_source"] = tracker_feature_source
    rebuilt_summary["calculation_note"] = (
        "轻量复用报告未重跑完整筛选链；该解释从发布链快照中的动态复核后样本、"
        "最终正式样本和最终精选未入围样本重建。"
    )
    current_summary["final_selection_contraction"] = rebuilt_summary

    return current_summary


def _annotate_final_selection_removed_rows(removed_df: pd.DataFrame) -> pd.DataFrame:
    current = removed_df.copy()
    if current.empty:
        return current
    current["项目推荐结论"] = AUTO_DIAGNOSIS_VERDICT
    current[AUTO_DIAGNOSIS_CATEGORY_COLUMN] = FINAL_SELECTION_NOT_SELECTED_DIAGNOSIS
    current["发布结论状态"] = "最终精选未入围"
    current["推荐理由"] = current.get("推荐理由", "").astype(str).map(
        lambda text: "；".join(
            [
                item
                for item in [
                    str(text or "").strip(),
                    PROJECT_RECOMMENDATION_REASONS["final_selection_stability_cap"],
                ]
                if item
            ]
        )
    )
    current["项目推荐原因代码"] = current.get("项目推荐原因代码", pd.Series(dtype=object)).astype(str).map(
        lambda text: "final_selection_stability_cap"
        if not str(text or "").strip()
        else f"{str(text).strip()}|final_selection_stability_cap"
    )
    current["主原因分组"] = "自动发布门禁"
    current["原因分组汇总"] = current.get("原因分组汇总", pd.Series(dtype=object)).astype(str).map(
        lambda text: "自动发布门禁" if not str(text or "").strip() else f"{str(text).strip()}；自动发布门禁"
    )
    return current


def _build_generic_holding_period_placeholder_watch(
    excluded_df: pd.DataFrame,
    *,
    preview_top_n: int,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    columns = [
        "基金代码",
        "基金名称",
        "持有期状态",
        "holding_period_source",
        "债基风格标签",
        "是否可申购",
        FORMAL_SCORE_LABEL,
        "项目质量信心",
        "1年-年化收益(%)",
        "1年-夏普比率",
        "1年-最大回撤(%)",
        "1年-波动率",
        "推荐备注",
    ]
    empty_df = pd.DataFrame(columns=columns)
    empty_summary = {
        "available": False,
        "count": 0,
        "buyable_count": 0,
        "high_score_count": 0,
        "preview": [],
        "style_counts": [],
        "source_counts": [],
    }
    if excluded_df is None or excluded_df.empty:
        return empty_df, empty_summary

    current = _ensure_formal_score_column(excluded_df.copy())
    current["holding_days_num"] = pd.to_numeric(current.get("holding_days"), errors="coerce").fillna(0).astype(int)
    current = current[current["holding_days_num"] == 999].copy()
    if current.empty:
        return empty_df, empty_summary

    current["holding_period_source"] = current.get("holding_period_source", pd.Series("", index=current.index, dtype=object)).fillna("").astype(str).str.strip()
    current["持有期状态"] = current.get("持有期状态", pd.Series("", index=current.index, dtype=object)).fillna("").astype(str).str.strip()
    current.loc[current["持有期状态"] == "", "持有期状态"] = HOLDING_PERIOD_GENERIC_PLACEHOLDER_STATUS
    current["债基风格标签"] = current.get("债基风格标签", pd.Series("", index=current.index, dtype=object)).fillna("").astype(str).str.strip()
    current["是否可申购"] = current.get("是否可申购", pd.Series("", index=current.index, dtype=object)).fillna("").astype(str).str.strip()
    if "is_buyable" in current.columns:
        buyable_fallback = current["is_buyable"].map(buyability_label)
        current.loc[current["是否可申购"] == "", "是否可申购"] = buyable_fallback.loc[current["是否可申购"] == ""]
    current["项目质量信心"] = current.get("项目质量信心", pd.Series("", index=current.index, dtype=object)).fillna("").astype(str).str.strip()
    current[FORMAL_SCORE_LABEL] = pd.to_numeric(current.get(FORMAL_SCORE_LABEL), errors="coerce")
    current["1年-年化收益(%)"] = pd.to_numeric(current.get("1年-年化收益(%)"), errors="coerce")
    current["1年-夏普比率"] = pd.to_numeric(current.get("1年-夏普比率"), errors="coerce")
    current["1年-最大回撤(%)"] = pd.to_numeric(current.get("1年-最大回撤(%)"), errors="coerce")
    current["1年-波动率"] = pd.to_numeric(current.get("1年-波动率"), errors="coerce")
    current["推荐备注"] = current.get("推荐备注", pd.Series("", index=current.index, dtype=object)).fillna("").astype(str).str.strip()

    preview_df = current.sort_values(
        by=[FORMAL_SCORE_LABEL, "基金代码"],
        ascending=[False, True],
        kind="mergesort",
    ).copy()
    preview_df = _select_unique_columns(preview_df, columns)
    style_counts = (
        current.get("债基风格标签", pd.Series(dtype=object))
        .replace("", "未知")
        .value_counts()
        .head(10)
        .to_dict()
    )
    source_counts = (
        current.get("holding_period_source", pd.Series(dtype=object))
        .replace("", "empty")
        .value_counts()
        .to_dict()
    )
    summary = {
        "available": True,
        "count": int(len(current.index)),
        "buyable_count": int(current.get("是否可申购", pd.Series(dtype=object)).eq("可申购").sum()),
        "high_score_count": int((pd.to_numeric(current.get(FORMAL_SCORE_LABEL), errors="coerce") >= 80).sum()),
        "preview": _preview_records(preview_df.head(preview_top_n)),
        "style_counts": [{"债基风格标签": str(key), "数量": int(value)} for key, value in style_counts.items()],
        "source_counts": [{"holding_period_source": str(key), "数量": int(value)} for key, value in source_counts.items()],
    }
    return preview_df, summary


def _compute_low_vol_monthly_shadow_metrics(daily_returns: pd.Series) -> dict[str, Any]:
    series = pd.to_numeric(daily_returns, errors="coerce")
    series = pd.Series(series.to_numpy(copy=False), index=daily_returns.index).dropna()
    if len(series.index) < 2:
        return {
            "calendar_month_count": 0,
            "positive_month_ratio_calendar_pct": None,
        }

    # `fund_nav_history.daily_growth` 当前存的是百分比数值，先转成收益率小数。
    series = series / 100.0
    # 与当前 1y 风险指标窗口口径保持一致：去掉窗口首日涨跌幅。
    series = series.iloc[1:]
    if len(series.index) < 20:
        return {
            "calendar_month_count": 0,
            "positive_month_ratio_calendar_pct": None,
        }

    grouped = series.groupby(series.index.to_period("M"))
    calendar_returns = [float((1 + chunk).prod() - 1) for _period, chunk in grouped if len(chunk.index) > 0]
    if len(calendar_returns) < 2:
        return {
            "calendar_month_count": int(len(calendar_returns)),
            "positive_month_ratio_calendar_pct": None,
        }

    positive_count = sum(1 for value in calendar_returns if value > 0)
    return {
        "calendar_month_count": int(len(calendar_returns)),
        "positive_month_ratio_calendar_pct": round(float(positive_count / len(calendar_returns) * 100), 6),
    }


def _build_positive_ratio_semantics_summary(
    low_vol_positive_ratio_shadow_watch_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return build_positive_ratio_semantics_summary_api(
        {},
        low_vol_positive_ratio_shadow_watch_summary or {},
    )


def _build_low_vol_positive_ratio_shadow_watch(
    candidate_df: pd.DataFrame,
    *,
    settings: Settings,
    preview_top_n: int,
    replay_inputs: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if replay_inputs is not None:
        replay_inputs.clear()
    empty_summary = {
        "available": False,
        "style_scope": list(LOW_VOL_STYLE_TAGS),
        "focus_sample_count": 0,
        "monthly_shadow_ready_count": 0,
        "positive_ratio_gate_hit_count": 0,
        "positive_ratio_gap_alert_count": 0,
        "style_summary": [],
        "gap_reason_summary": [],
        "gap_companion_reason_summary": [],
        "style_gap_reason_summary": [],
        "low_vol_overlap_summary": {},
        "semantics": _build_positive_ratio_semantics_summary({}),
        "headline": "",
        "explanation": "",
        "preview": [],
    }
    if candidate_df is None or candidate_df.empty or not settings.database_path.exists():
        return empty_summary

    current = candidate_df.copy()
    current["债基风格标签"] = current.get("债基风格标签", pd.Series("", index=current.index, dtype=object)).fillna("").astype(str).str.strip()
    current = current[current["债基风格标签"].isin(LOW_VOL_STYLE_TAGS)].copy()
    if current.empty:
        return empty_summary

    current["基金代码"] = current.get("基金代码", pd.Series("", index=current.index, dtype=object)).astype(str).map(_normalize_code)
    current = current[current["基金代码"] != ""].copy()
    if current.empty:
        return empty_summary

    current["dynamic_threshold_reason_codes"] = current.get(
        "dynamic_threshold_reason_codes",
        pd.Series("", index=current.index, dtype=object),
    ).fillna("").astype(str)
    current["reason_code_list"] = current["dynamic_threshold_reason_codes"].map(_unique_reason_codes)
    current["reason_has_positive_ratio"] = current["dynamic_threshold_reason_codes"].map(
        lambda text: "below_positive_ratio_floor" in split_reason_codes_impl(text)
    )

    effective_nav_snapshot = load_effective_nav_snapshot(settings.database_path)
    risk_end_date = str(
        effective_nav_snapshot.risk_1y_effective_end_date
        or effective_nav_snapshot.risk_1y_latest_end_date
        or ""
    ).strip()
    if not risk_end_date:
        return empty_summary

    code_list = current["基金代码"].drop_duplicates().tolist()
    chunk_size = 400
    conn = sqlite3.connect(str(settings.database_path))
    try:
        risk_frames: list[pd.DataFrame] = []
        for start in range(0, len(code_list), chunk_size):
            chunk = code_list[start : start + chunk_size]
            if not chunk:
                continue
            placeholders = ",".join(["?"] * len(chunk))
            risk_query = f"""
            SELECT
                risk.fcode,
                risk.start_date,
                risk.end_date,
                risk.data_points,
                risk.rowid AS risk_metric_rowid
            FROM fund_risk_metrics risk
            WHERE risk.period = '1y'
              AND risk.end_date = ?
              AND risk.fcode IN ({placeholders})
              AND risk.rowid = (
                  SELECT latest.rowid
                  FROM fund_risk_metrics latest
                  WHERE latest.fcode = risk.fcode
                    AND latest.period = risk.period
                    AND COALESCE(TRIM(latest.end_date), '') = COALESCE(TRIM(risk.end_date), '')
                  ORDER BY latest.rowid DESC
                  LIMIT 1
              )
            """
            risk_frames.append(pd.read_sql_query(risk_query, conn, params=[risk_end_date, *chunk]))
        risk_window_df = pd.concat(risk_frames, ignore_index=True) if risk_frames else pd.DataFrame()
        if risk_window_df.empty:
            return empty_summary
        risk_window_df["fcode"] = risk_window_df["fcode"].map(_normalize_code)
        risk_window_df["start_date"] = pd.to_datetime(risk_window_df["start_date"], errors="coerce")
        risk_window_df["end_date"] = pd.to_datetime(risk_window_df["end_date"], errors="coerce")
        risk_window_df = risk_window_df.dropna(subset=["start_date", "end_date"]).drop_duplicates(subset=["fcode"]).reset_index(drop=True)
        if risk_window_df.empty:
            return empty_summary

        min_start = risk_window_df["start_date"].min()
        max_end = risk_window_df["end_date"].max()
        if pd.isna(min_start) or pd.isna(max_end):
            return empty_summary

        nav_frames: list[pd.DataFrame] = []
        start_date = pd.Timestamp(min_start).date().isoformat()
        end_date = pd.Timestamp(max_end).date().isoformat()
        for start in range(0, len(code_list), chunk_size):
            chunk = code_list[start : start + chunk_size]
            if not chunk:
                continue
            placeholders = ",".join(["?"] * len(chunk))
            nav_query = f"""
            SELECT
                fcode,
                nav_date,
                daily_growth
            FROM fund_nav_history
            WHERE nav_date >= ?
              AND nav_date <= ?
              AND fcode IN ({placeholders})
            ORDER BY fcode, nav_date
            """
            nav_frames.append(pd.read_sql_query(nav_query, conn, params=[start_date, end_date, *chunk]))
    finally:
        conn.close()

    nav_history_df = pd.concat(nav_frames, ignore_index=True) if nav_frames else pd.DataFrame()
    if nav_history_df.empty:
        return empty_summary
    nav_history_df["fcode"] = nav_history_df["fcode"].map(_normalize_code)
    nav_history_df["nav_date"] = pd.to_datetime(nav_history_df["nav_date"], errors="coerce")
    nav_history_df["daily_growth"] = pd.to_numeric(nav_history_df["daily_growth"], errors="coerce")
    nav_history_df = nav_history_df.dropna(subset=["nav_date"]).reset_index(drop=True)
    if nav_history_df.empty:
        return empty_summary

    if replay_inputs is not None:
        # Preserve these observed frames once; the offline audit must not query
        # a newer database to reconstruct the report's monthly window.
        replay_inputs.update(
            risk_windows=risk_window_df.copy(), nav_history=nav_history_df.copy(),
            contract={"version": "low-vol-monthly-inputs-2026-09-11.1", "daily_growth_unit": "percent",
                      "risk_period": "1y", "risk_end_date": risk_end_date, "scope_codes": sorted(code_list),
                      "style_tags": list(LOW_VOL_STYLE_TAGS), "observed_at": datetime.now().astimezone().isoformat(),
                      "source": "actual_in_memory_report_query_results",
                      "missing_growth_policy": "drop_before_removing_first_observed_return",
                      "first_return_policy": "exclude_window_first_observed_return"},
        )

    # 用向量化聚合替代逐基金 Python 循环，保持口径不变但显著降低低波观察构建耗时。
    monthly_source_df = nav_history_df.merge(
        risk_window_df[["fcode", "start_date", "end_date"]],
        on="fcode",
        how="inner",
    )
    if monthly_source_df.empty:
        return empty_summary
    monthly_source_df = monthly_source_df[
        monthly_source_df["nav_date"].ge(monthly_source_df["start_date"])
        & monthly_source_df["nav_date"].le(monthly_source_df["end_date"])
    ].copy()
    if monthly_source_df.empty:
        return empty_summary
    monthly_source_df["daily_growth"] = pd.to_numeric(monthly_source_df["daily_growth"], errors="coerce")
    monthly_source_df = monthly_source_df.dropna(subset=["daily_growth"]).sort_values(
        ["fcode", "nav_date"],
        kind="stable",
    )
    if monthly_source_df.empty:
        return empty_summary
    monthly_source_df["window_return_order"] = monthly_source_df.groupby("fcode").cumcount()
    monthly_source_df = monthly_source_df[monthly_source_df["window_return_order"] > 0].copy()
    if monthly_source_df.empty:
        return empty_summary
    daily_count_by_code = monthly_source_df.groupby("fcode").size()
    eligible_codes = daily_count_by_code[daily_count_by_code >= 20].index
    if len(eligible_codes) <= 0:
        return empty_summary
    monthly_source_df = monthly_source_df[monthly_source_df["fcode"].isin(eligible_codes)].copy()
    if monthly_source_df.empty:
        return empty_summary
    monthly_source_df["calendar_month"] = monthly_source_df["nav_date"].dt.to_period("M")
    monthly_source_df["growth_factor"] = 1.0 + (monthly_source_df["daily_growth"] / 100.0)
    monthly_shadow_df = (
        monthly_source_df.groupby(["fcode", "calendar_month"], sort=False)["growth_factor"]
        .prod()
        .sub(1.0)
        .reset_index(name="calendar_month_return")
    )
    if monthly_shadow_df.empty:
        return empty_summary
    monthly_shadow_df = (
        monthly_shadow_df.groupby("fcode", sort=False)["calendar_month_return"]
        .agg(
            calendar_month_count="size",
            positive_month_count=lambda series: int(series.gt(0).sum()),
        )
        .reset_index()
        .rename(columns={"fcode": "基金代码"})
    )
    monthly_shadow_df["positive_month_ratio_calendar_pct"] = None
    ready_ratio_mask = monthly_shadow_df["calendar_month_count"].ge(2)
    monthly_shadow_df.loc[ready_ratio_mask, "positive_month_ratio_calendar_pct"] = (
        monthly_shadow_df.loc[ready_ratio_mask, "positive_month_count"]
        .div(monthly_shadow_df.loc[ready_ratio_mask, "calendar_month_count"])
        .mul(100.0)
        .round(6)
    )
    monthly_shadow_df = monthly_shadow_df[["基金代码", "calendar_month_count", "positive_month_ratio_calendar_pct"]]
    if monthly_shadow_df.empty:
        return empty_summary

    current["1年-正收益比率"] = pd.to_numeric(current.get("1年-正收益比率"), errors="coerce")
    current[FORMAL_SCORE_LABEL] = pd.to_numeric(current.get(FORMAL_SCORE_LABEL), errors="coerce")
    current = current.merge(monthly_shadow_df, on="基金代码", how="left")
    current["positive_ratio_daily_vs_calendar_gap_pct"] = (
        pd.to_numeric(current.get("positive_month_ratio_calendar_pct"), errors="coerce")
        - pd.to_numeric(current.get("1年-正收益比率"), errors="coerce")
    ).round(6)
    current["abs_positive_ratio_gap_pct"] = pd.to_numeric(
        current.get("positive_ratio_daily_vs_calendar_gap_pct"),
        errors="coerce",
    ).abs()
    current["positive_ratio_gap_alert"] = current["abs_positive_ratio_gap_pct"].ge(LOW_VOL_POSITIVE_RATIO_GAP_ALERT_PCT)
    gap_focus_df = current[current["positive_ratio_gap_alert"]].copy()

    def _build_reason_summary_rows(
        frame: pd.DataFrame,
        *,
        exclude_codes: set[str] | None = None,
    ) -> list[dict[str, Any]]:
        if frame.empty:
            return []
        excluded = set(exclude_codes or set())
        reason_counts: dict[str, int] = {}
        for codes in frame.get("reason_code_list", pd.Series(dtype=object)).tolist():
            for code in codes or []:
                normalized = str(code or "").strip()
                if not normalized or normalized in excluded:
                    continue
                reason_counts[normalized] = int(reason_counts.get(normalized) or 0) + 1
        total = int(len(frame.index))
        rows = [
            {
                "原因代码": code,
                "原因标签": PROJECT_RECOMMENDATION_REASONS.get(code, code),
                "样本数": int(count),
                "占高差样本比例(%)": round(count / total * 100.0, 2) if total > 0 else 0.0,
            }
            for code, count in reason_counts.items()
        ]
        rows.sort(
            key=lambda item: (
                -int(item.get("样本数") or 0),
                str(item.get("原因标签") or ""),
            )
        )
        return rows

    gap_reason_rows = _build_reason_summary_rows(gap_focus_df)
    gap_companion_reason_rows = _build_reason_summary_rows(
        gap_focus_df,
        exclude_codes={"below_positive_ratio_floor"},
    )

    style_gap_reason_rows: list[dict[str, Any]] = []
    for style, group in gap_focus_df.groupby("债基风格标签", dropna=False):
        style_name = str(style or "").strip() or "未知"
        companion_rows = _build_reason_summary_rows(
            group,
            exclude_codes={"below_positive_ratio_floor"},
        )
        top_companion = companion_rows[0] if companion_rows else {
            "原因代码": "below_positive_ratio_floor",
            "原因标签": PROJECT_RECOMMENDATION_REASONS.get("below_positive_ratio_floor", "below_positive_ratio_floor"),
            "样本数": int(len(group.index)),
            "占高差样本比例(%)": 100.0 if len(group.index) > 0 else 0.0,
        }
        style_gap_reason_rows.append(
            {
                "债基风格标签": style_name,
                "高差样本数": int(len(group.index)),
                "命中正收益比率门槛数": _safe_count(group["reason_has_positive_ratio"]),
                "主伴随原因代码": str(top_companion.get("原因代码") or ""),
                "主伴随原因": str(top_companion.get("原因标签") or ""),
                "主伴随原因样本数": int(top_companion.get("样本数") or 0),
                "主伴随原因占高差样本比例(%)": float(top_companion.get("占高差样本比例(%)") or 0.0),
            }
        )
    style_gap_reason_rows.sort(
        key=lambda item: (
            -int(item.get("高差样本数") or 0),
            str(item.get("债基风格标签") or ""),
        )
    )

    style_rows: list[dict[str, Any]] = []
    for style, group in current.groupby("债基风格标签", dropna=False):
        style_name = str(style or "").strip() or "未知"
        style_rows.append(
            {
                "债基风格标签": style_name,
                "样本数": int(len(group.index)),
                "命中正收益比率门槛数": _safe_count(group["reason_has_positive_ratio"]),
                "月度shadow可复算数": _safe_count(pd.to_numeric(group.get("calendar_month_count"), errors="coerce").fillna(0).gt(0)),
                "自然月vs日频正收益率差中位数(%)": _safe_median(group.get("positive_ratio_daily_vs_calendar_gap_pct", pd.Series(dtype=float))),
                "自然月正收益差>=15pct样本数": _safe_count(group.get("positive_ratio_gap_alert", pd.Series(dtype=bool))),
            }
        )
    style_rows.sort(
        key=lambda item: (
            -int(item.get("自然月正收益差>=15pct样本数") or 0),
            -int(item.get("命中正收益比率门槛数") or 0),
            str(item.get("债基风格标签") or ""),
        )
    )

    preview_columns = [
        "基金代码",
        "基金名称",
        "债基风格标签",
        FORMAL_SCORE_LABEL,
        "1年-正收益比率",
        "positive_month_ratio_calendar_pct",
        "positive_ratio_daily_vs_calendar_gap_pct",
        "dynamic_threshold_reason_codes",
    ]
    preview_df = current[current["positive_ratio_gap_alert"]].copy()
    preview_df = preview_df.sort_values(
        by=["abs_positive_ratio_gap_pct", FORMAL_SCORE_LABEL, "基金代码"],
        ascending=[False, False, True],
        kind="mergesort",
    )
    preview_df = _select_unique_columns(preview_df, preview_columns)

    # An audit of an earlier report cannot establish this report's strategy
    # impact. Keep offline experiments in their own run/revision-bound outputs.
    low_vol_overlap_summary = {"available": False, "status": "no_same_revision_audit"}

    summary = {
        "available": True,
        "style_scope": list(LOW_VOL_STYLE_TAGS),
        "focus_sample_count": int(len(current.index)),
        "monthly_shadow_ready_count": _safe_count(pd.to_numeric(current.get("calendar_month_count"), errors="coerce").fillna(0).gt(0)),
        "positive_ratio_gate_hit_count": _safe_count(current["reason_has_positive_ratio"]),
        "positive_ratio_gap_alert_count": _safe_count(current["positive_ratio_gap_alert"]),
        "style_summary": style_rows,
        "gap_reason_summary": gap_reason_rows,
        "gap_companion_reason_summary": gap_companion_reason_rows,
        "style_gap_reason_summary": style_gap_reason_rows,
        "low_vol_overlap_summary": low_vol_overlap_summary,
        "preview": _preview_records(preview_df.head(preview_top_n)),
    }
    semantics = _build_positive_ratio_semantics_summary(summary)
    summary["semantics"] = semantics
    summary["headline"] = semantics.get("headline") or ""
    summary["explanation"] = semantics.get("explanation") or ""
    return summary


def _build_artifact_consistency_audit(
    *,
    settings: Settings,
    summary: dict[str, Any],
) -> dict[str, Any]:
    artifacts = summary.get("artifacts") or {}
    counts = summary.get("counts") or {}
    project_publish_summary = summary.get("project_publish_summary") or {}
    tracker_summary = project_publish_summary.get("project_tracker_summary") or {}
    forward_validation_summary = project_publish_summary.get("forward_validation_summary") or {}
    latest_business_date = str(tracker_summary.get("latest_business_date") or "").strip()
    generated_business_date = str(summary.get("generated_at") or "")[:10]
    checks: list[dict[str, Any]] = []

    def add_check(name: str, aligned: bool, detail: str, **extra: Any) -> None:
        payload: dict[str, Any] = {
            "name": name,
            "aligned": bool(aligned),
            "detail": detail,
        }
        payload.update(extra)
        checks.append(payload)

    required_artifact_keys = [
        "report_markdown",
        "report_json",
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
        "stage_b_reason_excel",
        "stage_b_reason_csv",
        "stage_b_reason_excel_meta",
        "project_tracker_excel",
        "project_tracker_excel_meta",
        "forward_validation_excel",
        "forward_validation_json",
        "forward_validation_markdown",
    ]
    missing_artifacts: list[str] = []
    for key in required_artifact_keys:
        resolved = _resolve_project_path(settings, artifacts.get(key))
        if key in {"report_markdown", "report_json"}:
            if not resolved:
                missing_artifacts.append(key)
            continue
        if not resolved or not resolved.exists() or not resolved.is_file():
            missing_artifacts.append(key)
    add_check(
        "required_artifacts_present",
        not missing_artifacts,
        "全部关键产物存在" if not missing_artifacts else f"缺失关键产物: {', '.join(missing_artifacts)}",
        missing_artifact_keys=missing_artifacts,
    )

    supplement_audit = summary.get("autostock_validation_audit") or {}
    if supplement_audit.get("supplement_evidence_required"):
        try:
            descriptor = supplement_audit["supplement_snapshot"]
            if artifacts.get("autostock_supplement_snapshot_json_gz") != descriptor.get("path"):
                raise ValueError("autostock_supplement_artifact_path_mismatch")
            supplement_snapshot = load_supplement_snapshot(
                settings.project_root, descriptor, reports_dir=settings.reports_dir,
                run_id=str(summary["run_id"]), revision=str(summary["publication_control"]["revision"]),
            )
            frozen_statuses = {
                _normalize_code(row.get("基金代码")): _normalize_autostock_status(validation_status_value(row))
                for record in supplement_snapshot["records"] for row in record["output_statuses"]
            }
            for key in ("filtered_csv", "dynamic_filtered_csv"):
                published_rows = pd.read_csv(_resolve_project_path(settings, artifacts[key]), dtype={"基金代码": str})
                for row in published_rows.to_dict(orient="records"):
                    code = _normalize_code(row.get("基金代码"))
                    status = _normalize_autostock_status(row.get("AutoStock原始校验状态") or row.get("AutoStock校验状态"))
                    if code not in frozen_statuses or frozen_statuses[code] != status:
                        raise ValueError(f"autostock_supplement_export_status_mismatch:{key}:{code}")
            add_check("autostock_supplement_snapshot_bound", True,
                      "AutoStock补验输入、响应及结论已绑定当前run/revision并通过哈希核验")
        except (OSError, ValueError, KeyError, TypeError, EOFError) as exc:
            add_check("autostock_supplement_snapshot_bound", False, f"{type(exc).__name__}: {exc}")

    data_audit = summary.get("data_validation_audit") or {}
    if data_audit.get("required"):
        try:
            descriptor = data_audit["snapshot"]
            if artifacts.get("data_validation_snapshot_json_gz") != descriptor.get("path"):
                raise ValueError("data_validation_artifact_path_mismatch")
            snapshot = load_data_validation_snapshot(
                settings.project_root, descriptor, reports_dir=settings.reports_dir,
                run_id=str(summary["run_id"]), revision=str(summary["publication_control"]["revision"]),
            )
            records = snapshot["records"]
            published_members = pd.read_csv(_resolve_project_path(settings, artifacts["filtered_csv"]), dtype={"基金代码": str})
            if not records and not published_members.empty:
                raise ValueError("data_validation_records_missing")
            if data_audit.get("unavailable") or any(
                not record["upstream_metadata"].get("source_available")
                or not record["upstream_metadata"].get("missing_gate_passed", record["upstream_metadata"].get("source_complete")) for record in records
            ):
                raise ValueError("cross_endpoint_validation_incomplete")
            statuses = {_normalize_code(row["基金代码"]): row
                        for record in records for row in record["output_statuses"]}
            for key in ("filtered_csv", "dynamic_filtered_csv"):
                rows = pd.read_csv(_resolve_project_path(settings, artifacts[key]), dtype={"基金代码": str}).fillna("")
                for row in rows.to_dict(orient="records"):
                    code = _normalize_code(row["基金代码"])
                    frozen = statuses.get(code)
                    if (not frozen or any(str(row.get(column, "")) != str(frozen.get(column, ""))
                                          for column in DATA_VALIDATION_COLUMNS)):
                        raise ValueError(f"data_validation_export_status_mismatch:{key}:{code}")
                    if (_normalize_autostock_status(validation_status_value(row)) not in {"strict_match", "soft_pass_with_warnings"}
                            or row.get("数据校验来源") != "eastmoney_cross_endpoint"
                            or not row.get("数据校验契约版本")):
                        raise ValueError(f"data_validation_member_unverified:{key}:{code}")
            add_check("data_validation_snapshot_bound", True,
                      "同供应商跨接口核验输入、响应、来源和结论已绑定当前run/revision并通过哈希核验；不代表独立供应商核验")
        except (OSError, ValueError, KeyError, TypeError, EOFError) as exc:
            add_check("data_validation_snapshot_bound", False, f"{type(exc).__name__}: {exc}")

    core_artifacts = artifacts.get("core") or {}
    replay_artifact_keys = [
        "long_csv",
        "short_csv",
        "final_screening_decision_ledger_file",
        "bond_mainlist_quality_audit_json_file",
    ]
    missing_replay_artifacts: list[str] = []
    non_run_local_replay_artifacts: list[str] = []
    expected_run_dir_fragment = f"/reports/runs/{summary.get('run_id')}/"
    for key in replay_artifact_keys:
        artifact_value = core_artifacts.get(key)
        resolved = _resolve_project_path(settings, artifact_value)
        if not artifact_value or not resolved or not resolved.exists() or not resolved.is_file():
            missing_replay_artifacts.append(key)
            continue
        normalized = str(artifact_value).replace("\\", "/")
        if expected_run_dir_fragment not in f"/{normalized.lstrip('/')}":
            non_run_local_replay_artifacts.append(key)
    add_check(
        "run_local_replay_artifacts_present",
        not missing_replay_artifacts and not non_run_local_replay_artifacts,
        (
            "关键重放输入已固化到当前 run 目录"
            if not missing_replay_artifacts and not non_run_local_replay_artifacts
            else (
                f"缺失重放输入: {', '.join(missing_replay_artifacts)}; "
                f"未固化到 run 目录: {', '.join(non_run_local_replay_artifacts)}"
            ).strip("; ")
        ),
        missing_replay_artifact_keys=missing_replay_artifacts,
        non_run_local_replay_artifact_keys=non_run_local_replay_artifacts,
    )

    filtered_meta = _read_json_file(_resolve_project_path(settings, artifacts.get("filtered_excel_meta")))
    filtered_meta_count = int(((filtered_meta.get("counts") or {}).get("official_recommend_count")) or 0)
    final_count = int(counts.get("final_count") or 0)
    add_check(
        "filtered_meta_matches_final_count",
        filtered_meta_count == final_count,
        f"正式推荐数量 report={final_count}, meta={filtered_meta_count}",
        report_final_count=final_count,
        meta_final_count=filtered_meta_count,
    )

    dynamic_filtered_meta = _read_json_file(_resolve_project_path(settings, artifacts.get("dynamic_filtered_excel_meta")))
    dynamic_meta_count = int(((dynamic_filtered_meta.get("counts") or {}).get("official_recommend_count")) or 0)
    dynamic_final_count = int(counts.get("dynamic_final_count") or 0)
    add_check(
        "dynamic_filtered_meta_matches_dynamic_final_count",
        dynamic_meta_count == dynamic_final_count,
        f"动态正式推荐数量 report={dynamic_final_count}, meta={dynamic_meta_count}",
        report_dynamic_final_count=dynamic_final_count,
        meta_dynamic_final_count=dynamic_meta_count,
    )

    watch_meta = _read_json_file(_resolve_project_path(settings, artifacts.get("watch_excel_meta")))
    watch_meta_count = int(((watch_meta.get("counts") or {}).get("internal_watch_count")) or 0)
    internal_watch_count = int(counts.get("internal_watch_count") or 0)
    add_check(
        "watch_meta_matches_internal_watch_count",
        watch_meta_count == internal_watch_count,
        f"自动诊断池数量 report={internal_watch_count}, meta={watch_meta_count}",
        report_internal_watch_count=internal_watch_count,
        meta_internal_watch_count=watch_meta_count,
    )

    excluded_meta = _read_json_file(_resolve_project_path(settings, artifacts.get("excluded_excel_meta")))
    excluded_meta_count = int(((excluded_meta.get("counts") or {}).get("excluded_count")) or 0)
    excluded_count = int(counts.get("excluded_count") or 0)
    add_check(
        "excluded_meta_matches_excluded_count",
        excluded_meta_count == excluded_count,
        f"持有期排除数量 report={excluded_count}, meta={excluded_meta_count}",
        report_excluded_count=excluded_count,
        meta_excluded_count=excluded_meta_count,
    )

    tracker_meta = _read_json_file(_resolve_project_path(settings, artifacts.get("project_tracker_excel_meta")))
    tracker_meta_summary = tracker_meta.get("accumulation_summary") or {}
    tracker_active_count = int(tracker_summary.get("current_active_count") or 0)
    tracker_meta_active_count = int(tracker_meta_summary.get("current_active_count") or 0)
    add_check(
        "project_tracker_summary_matches_counts",
        tracker_active_count == final_count == tracker_meta_active_count,
        (
            "累计跟踪当前在榜数量 "
            f"report={final_count}, tracker_summary={tracker_active_count}, tracker_meta={tracker_meta_active_count}"
        ),
        report_final_count=final_count,
        tracker_summary_current_active_count=tracker_active_count,
        tracker_meta_current_active_count=tracker_meta_active_count,
    )

    tracker_latest_run_id = str(tracker_summary.get("latest_run_id") or "").strip()
    summary_run_id = str(summary.get("run_id") or "").strip()
    add_check(
        "project_tracker_latest_run_id_matches_report",
        tracker_latest_run_id == summary_run_id,
        f"累计跟踪 latest_run_id={tracker_latest_run_id or '-'}, report_run_id={summary_run_id or '-'}",
        tracker_latest_run_id=tracker_latest_run_id,
        report_run_id=summary_run_id,
    )

    add_check(
        "project_tracker_business_date_matches_report_date",
        not latest_business_date or latest_business_date == generated_business_date,
        f"累计跟踪 latest_business_date={latest_business_date or '-'}, report_business_date={generated_business_date or '-'}",
        tracker_latest_business_date=latest_business_date,
        report_business_date=generated_business_date,
    )

    forward_validation_json = _read_json_file(_resolve_project_path(settings, artifacts.get("forward_validation_json")))
    forward_validation_json_summary = forward_validation_json.get("summary") or {}
    forward_snapshot_count = int(forward_validation_summary.get("snapshot_total") or 0)
    forward_json_snapshot_count = int(forward_validation_json_summary.get("snapshot_total") or 0)
    add_check(
        "forward_validation_summary_matches_json",
        forward_snapshot_count == forward_json_snapshot_count,
        f"前瞻验证快照数 report={forward_snapshot_count}, json={forward_json_snapshot_count}",
        report_forward_validation_snapshot_count=forward_snapshot_count,
        json_forward_validation_snapshot_count=forward_json_snapshot_count,
    )

    if (summary.get("publication_control") or {}).get("artifact_checks_required"):
        from backend.publication_checks import build_publication_member_checks

        checks.extend(build_publication_member_checks(settings, summary))

    aligned = all(bool(item.get("aligned")) for item in checks)
    blockers = [item for item in checks if not bool(item.get("aligned"))]
    return {
        "aligned": aligned,
        "check_count": len(checks),
        "blocker_count": len(blockers),
        "checks": checks,
        "blockers": blockers,
    }


def _extract_timestamp_token(run_id: str) -> str:
    matched = re.search(r"(\d{8}_\d{6})", str(run_id or ""))
    if matched:
        return matched.group(1)
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _to_project_relative_path(
    settings: Settings,
    base_dir: Path,
    raw_path: Any,
) -> str | None:
    if not raw_path:
        return None
    raw_text = str(raw_path).strip()
    if not raw_text:
        return None
    candidate = Path(raw_text)
    if not candidate.is_absolute():
        candidate = (base_dir / candidate).resolve()
    return _project_relative(settings, candidate)



def _ordered_columns(df: pd.DataFrame, preferred: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for column in preferred:
        if column in df.columns and column not in seen:
            ordered.append(column)
            seen.add(column)
    for column in df.columns:
        if column not in seen:
            ordered.append(column)
            seen.add(column)
    return ordered


PROJECT_TRACKER_STATE_PREVIEW_COLUMNS = project_tracker.PROJECT_TRACKER_STATE_PREVIEW_COLUMNS
PROJECT_TRACKER_COLUMNS = project_tracker.PROJECT_TRACKER_COLUMNS
PROJECT_TRACKER_CURRENT_COLUMNS = project_tracker.PROJECT_TRACKER_CURRENT_COLUMNS
PROJECT_TRACKER_EXPORT_COLUMNS = project_tracker.PROJECT_TRACKER_EXPORT_COLUMNS
PROJECT_TRACKER_EXPORT_COLUMN_WIDTHS = project_tracker.PROJECT_TRACKER_EXPORT_COLUMN_WIDTHS
PROJECT_TRACKER_TOP_DECILE_RULES = project_tracker.PROJECT_TRACKER_TOP_DECILE_RULES
PROJECT_TRACKER_GROUP_HEADER_SCHEMES = project_tracker.PROJECT_TRACKER_GROUP_HEADER_SCHEMES
PROJECT_TRACKER_SUMMARY_ITEMS = project_tracker.PROJECT_TRACKER_SUMMARY_ITEMS
PROJECT_TRACKER_HEADER_REQUIRED_COLUMNS = project_tracker.PROJECT_TRACKER_HEADER_REQUIRED_COLUMNS
PROJECT_TRACKER_DEFAULT_SORT_COLUMNS = project_tracker.PROJECT_TRACKER_DEFAULT_SORT_COLUMNS
PROJECT_TRACKER_PROCESS_COLUMNS = project_tracker.PROJECT_TRACKER_PROCESS_COLUMNS
PROJECT_TRACKER_EXPORT_TO_INTERNAL_RENAME = project_tracker.PROJECT_TRACKER_EXPORT_TO_INTERNAL_RENAME
# Tracker governance stays anchored on formal score after the project_tracker split:
# "正式投资评分": "" if _resolve_formal_score_value(latest_row) is None else _resolve_formal_score_value(latest_row)
# by=["_current_active_sort", "_formal_score_sort", "出现次数", "_confidence_sort", "_annual_return_sort", "基金代码"]
_restore_project_tracker_internal_view = project_tracker._restore_project_tracker_internal_view
_read_project_tracker_export_sheet = project_tracker._read_project_tracker_export_sheet
_read_project_tracker_process_sheet = project_tracker._read_project_tracker_process_sheet
_load_tracker_source_frame = project_tracker._load_tracker_source_frame


def _try_reuse_project_final_tracker(
    settings: Settings,
    *,
    current_run_id: str,
    current_generated_at: str,
    current_filtered_excel: Path,
    current_final_count: int,
    current_nav_snapshot_date: str = "",
    **_: Any,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]] | None:
    return project_tracker._try_reuse_project_final_tracker(
        settings,
        current_run_id=current_run_id,
        current_generated_at=current_generated_at,
        current_filtered_excel=current_filtered_excel,
        current_final_count=current_final_count,
        current_nav_snapshot_date=current_nav_snapshot_date,
        read_project_tracker_export_sheet=_read_project_tracker_export_sheet,
        read_project_tracker_process_sheet=_read_project_tracker_process_sheet,
        load_tracker_source_frame=_load_tracker_source_frame,
        restore_project_tracker_internal_view=_restore_project_tracker_internal_view,
        project_relative=_project_relative,
    )


def _load_project_report_history(
    settings: Settings,
    *,
    current_run_id: str,
    current_generated_at: str,
    current_filtered_excel: Path,
    current_final_count: int,
    current_nav_snapshot_date: str = "",
    history_capture: dict[str, Any] | None = None,
    **_: Any,
) -> list[dict[str, Any]]:
    return project_tracker._load_project_report_history(
        settings,
        current_run_id=current_run_id,
        current_generated_at=current_generated_at,
        current_filtered_excel=current_filtered_excel,
        current_final_count=current_final_count,
        current_nav_snapshot_date=current_nav_snapshot_date,
        publication_registry_builder=build_publication_registry,
        history_capture=history_capture,
    )


def _build_project_final_tracker(
    settings: Settings,
    *,
    current_run_id: str,
    current_generated_at: str,
    current_filtered_excel: Path,
    current_final_count: int,
    current_nav_snapshot_date: str = "",
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    return project_tracker._build_project_final_tracker(
        settings,
        current_run_id=current_run_id,
        current_generated_at=current_generated_at,
        current_filtered_excel=current_filtered_excel,
        current_final_count=current_final_count,
        current_nav_snapshot_date=current_nav_snapshot_date,
        try_reuse_project_final_tracker=_try_reuse_project_final_tracker,
        load_project_report_history=_load_project_report_history,
        load_tracker_source_frame=_load_tracker_source_frame,
        project_relative=_project_relative,
    )


_build_project_tracker_export_view = project_tracker._build_project_tracker_export_view
_build_project_tracker_summary_items = project_tracker._build_project_tracker_summary_items
_find_project_tracker_header_row = project_tracker._find_project_tracker_header_row
_refresh_project_tracker_summary_row = project_tracker._refresh_project_tracker_summary_row
_apply_project_tracker_workbook_presentation = project_tracker._apply_project_tracker_workbook_presentation
_update_project_tracker_workbook_for_duplicate_snapshot = project_tracker._update_project_tracker_workbook_for_duplicate_snapshot


def _markdown_table(df: pd.DataFrame, columns: list[str]) -> str:
    available = [column for column in columns if column in df.columns]
    if not available or df.empty:
        return "_无数据_"
    rows = df[available].fillna("").astype(str).values.tolist()
    header = "| " + " | ".join(available) + " |"
    separator = "| " + " | ".join(["---"] * len(available)) + " |"
    body = ["| " + " | ".join(row) + " |" for row in rows]
    return "\n".join([header, separator, *body])



def _coalesce_display_columns(df: pd.DataFrame) -> pd.DataFrame:
    renamed = df.copy()
    code_col = _pick_column(renamed, CODE_CANDIDATES)
    name_col = _pick_column(renamed, NAME_CANDIDATES)
    type_col = _pick_column(renamed, TYPE_CANDIDATES)
    score_col = _pick_column(renamed, SCORE_CANDIDATES)

    if code_col is None:
        renamed["基金代码"] = ""
        code_col = "基金代码"
    renamed[code_col] = renamed[code_col].map(_normalize_code)
    renamed = renamed.rename(columns={code_col: "fcode"})

    if name_col and name_col != "基金名称":
        renamed = renamed.rename(columns={name_col: "基金名称"})
    if type_col and type_col != "基金类型":
        renamed = renamed.rename(columns={type_col: "基金类型"})
    if score_col and score_col != "综合加权得分":
        renamed = renamed.rename(columns={score_col: "综合加权得分"})

    return renamed



def _drop_duplicate_columns(df: pd.DataFrame) -> pd.DataFrame:
    # Export renames can collide even when all funds were filtered out.  An
    # empty frame still needs the same unique schema as a populated frame.
    return df.loc[:, ~df.columns.duplicated()].copy()


def _attach_incremental_review_flags(
    df: pd.DataFrame,
    incremental_review_df: pd.DataFrame,
) -> pd.DataFrame:
    current = df.copy()
    if current.empty:
        current["新增样本复核状态"] = pd.Series(dtype=object)
        current["新增样本体检备注"] = pd.Series(dtype=object)
        return current
    current["基金代码"] = current.get("基金代码", pd.Series(dtype=object)).map(_normalize_code)
    if incremental_review_df.empty or "基金代码" not in incremental_review_df.columns:
        current["新增样本复核状态"] = ""
        current["新增样本体检备注"] = ""
        return current

    review_df = incremental_review_df.copy()
    review_df["基金代码"] = review_df.get("基金代码", pd.Series(dtype=object)).map(_normalize_code)
    review_df = review_df.drop_duplicates(subset=["基金代码"], keep="first")
    review_df = review_df[
        [column for column in ["基金代码", "即时体检结论", "体检备注"] if column in review_df.columns]
    ]
    merged = current.merge(review_df, on="基金代码", how="left")
    merged["新增样本复核状态"] = merged.get("即时体检结论", pd.Series(dtype=object)).fillna("")
    merged["新增样本体检备注"] = merged.get("体检备注", pd.Series(dtype=object)).fillna("")
    return merged.drop(columns=["即时体检结论", "体检备注"], errors="ignore")


def _build_incremental_safety_watch_seed_df(
    official_df: pd.DataFrame,
    incremental_review_df: pd.DataFrame,
) -> pd.DataFrame:
    if official_df.empty:
        return official_df.copy()
    current = official_df.copy()
    current["基金代码"] = current.get("基金代码", pd.Series(dtype=object)).map(_normalize_code)
    current = _attach_incremental_review_flags(current, incremental_review_df)
    return current


def _build_incremental_review_summary_frame(
    incremental_review_df: pd.DataFrame,
    *,
    official_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if incremental_review_df.empty:
        return pd.DataFrame(columns=["指标", "数值"])

    summary_df = incremental_review_df.copy()
    if isinstance(official_df, pd.DataFrame) and not official_df.empty and "基金代码" in official_df.columns:
        official_codes = {
            _normalize_code(code)
            for code in official_df.get("基金代码", pd.Series(dtype=object)).astype(str).tolist()
            if _normalize_code(code)
        }
        if official_codes and "基金代码" in summary_df.columns:
            summary_df = summary_df[
                summary_df.get("基金代码", pd.Series(dtype=object)).map(_normalize_code).isin(official_codes)
            ].copy()

    review_status_series = summary_df.get("即时体检结论", pd.Series(dtype=object)).astype(str)
    track_df = summary_df[review_status_series.eq("需持续跟踪")].copy()
    formal_group_counts = (
        track_df.groupby(FORMAL_GROUP_LABEL, dropna=False).size().reset_index(name="数量")
        if not track_df.empty and FORMAL_GROUP_LABEL in track_df.columns
        else pd.DataFrame(columns=[FORMAL_GROUP_LABEL, "数量"])
    )
    note_counter: Counter[str] = Counter()
    if not track_df.empty and "体检备注" in track_df.columns:
        for value in track_df["体检备注"].astype(str).tolist():
            for note in [item.strip() for item in str(value).split("；") if item.strip()]:
                note_counter[note] += 1

    summary_rows = [
        {"指标": "相对上一快照新增正式样本数", "数值": int(len(summary_df.index))},
        {"指标": "新增样本中当前未见显著拖累数", "数值": int(review_status_series.eq("当前未见显著拖累").sum())},
        {"指标": "新增样本中需持续跟踪数", "数值": int(review_status_series.eq("需持续跟踪").sum())},
        {
            "指标": "需持续跟踪样本主要集中分组",
            "数值": "；".join(
                f"{str(row.get(FORMAL_GROUP_LABEL) or '未分组')} {int(row.get('数量') or 0)}"
                for row in formal_group_counts.to_dict(orient="records")[:3]
            ),
        },
        {
            "指标": "需持续跟踪样本主要偏弱信号",
            "数值": "；".join(f"{label}（{count}）" for label, count in note_counter.most_common(5)),
        },
    ]
    return pd.DataFrame(summary_rows)


def _is_secondary_share_row(row: dict[str, Any]) -> bool:
    share_count = _safe_float(row.get("关联份额数"))
    fund_name = str(row.get("基金名称") or "").strip()
    suffix = fund_name[-1] if fund_name else ""
    if suffix in {"B", "C", "D", "E", "F", "H", "I", "R", "Y"}:
        return True
    if suffix != "A":
        return False
    return share_count is not None and share_count > 1 and suffix != "A"


def _is_incremental_safety_watch_candidate(row: dict[str, Any]) -> bool:
    admission_mode = str(row.get("项目准入方式") or "").strip()
    confidence_label = str(row.get("项目质量信心") or "").strip()
    incremental_status = str(row.get("新增样本复核状态") or "").strip()
    incremental_note = str(row.get("新增样本体检备注") or "").strip()
    fee_total = _safe_float(row.get("费率合计(%)"))
    note_items = {item.strip() for item in incremental_note.split("；") if item.strip()}

    if admission_mode != "增强债基补充准入":
        return False
    if confidence_label != "中信心":
        return False
    if incremental_status != "需持续跟踪":
        return False
    if not _is_secondary_share_row(row):
        return False
    if {"评分分位偏后", "年化分位偏后"}.issubset(note_items):
        return True
    if "年化分位偏后" in note_items and fee_total is not None and fee_total >= 1.0:
        return True
    return False


def _apply_incremental_safety_watch(
    official_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if official_df.empty:
        return official_df.copy(), pd.DataFrame(columns=official_df.columns)

    current = official_df.copy()
    demoted_mask = current.apply(
        lambda row: _is_incremental_safety_watch_candidate(row.to_dict()),
        axis=1,
    )
    demoted_df = current[demoted_mask].copy()
    kept_df = current[~demoted_mask].copy()

    if demoted_df.empty:
        return kept_df.reset_index(drop=True), demoted_df

    demoted_df["项目推荐结论"] = AUTO_DIAGNOSIS_VERDICT
    demoted_df[AUTO_DIAGNOSIS_CATEGORY_COLUMN] = AUTO_PUBLISH_GATE_DIAGNOSIS
    demoted_df["发布结论状态"] = "自动发布门禁拦截"
    demoted_df["推荐理由"] = demoted_df.get("推荐理由", pd.Series(dtype=object)).astype(str).map(
        lambda text: "；".join(
            [
                item
                for item in [
                    str(text or "").strip(),
                    PROJECT_PUBLISH_RECOMMENDATION_REASONS["incremental_safety_watch"],
                ]
                if item
            ]
        )
    )
    demoted_df["项目推荐原因代码"] = demoted_df.get("项目推荐原因代码", pd.Series(dtype=object)).astype(str).map(
        lambda text: "incremental_safety_watch"
        if not str(text or "").strip()
        else f"{str(text).strip()}|incremental_safety_watch"
    )
    demoted_df["主原因分组"] = "自动发布门禁"
    demoted_df["原因分组汇总"] = demoted_df.get("原因分组汇总", pd.Series(dtype=object)).astype(str).map(
        lambda text: "自动发布门禁" if not str(text or "").strip() else f"{str(text).strip()}；自动发布门禁"
    )
    return kept_df.reset_index(drop=True), demoted_df.reset_index(drop=True)


def _demote_incremental_safety_watch_codes(
    df: pd.DataFrame,
    *,
    target_codes: set[str],
) -> pd.DataFrame:
    current = df.copy()
    if current.empty or not target_codes:
        return current
    current_codes = current.get("基金代码", pd.Series(dtype=object)).map(_normalize_code)
    hit_mask = current_codes.isin(target_codes) & current.get("项目推荐结论", pd.Series(dtype=object)).astype(str).eq("正式推荐")
    if not hit_mask.any():
        return current

    current.loc[hit_mask, "项目推荐结论"] = AUTO_DIAGNOSIS_VERDICT
    current.loc[hit_mask, AUTO_DIAGNOSIS_CATEGORY_COLUMN] = AUTO_PUBLISH_GATE_DIAGNOSIS
    current.loc[hit_mask, "发布结论状态"] = "自动发布门禁拦截"
    current.loc[hit_mask, "推荐理由"] = current.loc[hit_mask, "推荐理由"].astype(str).map(
        lambda text: "；".join(
            [
                item
                for item in [
                    str(text or "").strip(),
                    PROJECT_PUBLISH_RECOMMENDATION_REASONS["incremental_safety_watch"],
                ]
                if item
            ]
        )
    )
    current.loc[hit_mask, "项目推荐原因代码"] = current.loc[hit_mask, "项目推荐原因代码"].astype(str).map(
        lambda text: "incremental_safety_watch"
        if not str(text or "").strip()
        else f"{str(text).strip()}|incremental_safety_watch"
    )
    current.loc[hit_mask, "主原因分组"] = "自动发布门禁"
    current.loc[hit_mask, "原因分组汇总"] = current.loc[hit_mask, "原因分组汇总"].astype(str).map(
        lambda text: "自动发布门禁" if not str(text or "").strip() else f"{str(text).strip()}；自动发布门禁"
    )
    return current


def _coalesce_merge_conflict_columns(df: pd.DataFrame, column_name: str) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    left_name = f"{column_name}_x"
    right_name = f"{column_name}_y"
    if left_name not in df.columns and right_name not in df.columns:
        return df

    current = df.copy()
    base_values = current[column_name] if column_name in current.columns else pd.Series([""] * len(current.index), index=current.index)
    left_values = current[left_name] if left_name in current.columns else pd.Series([""] * len(current.index), index=current.index)
    right_values = current[right_name] if right_name in current.columns else pd.Series([""] * len(current.index), index=current.index)

    def _clean(series: pd.Series) -> pd.Series:
        return series.map(lambda value: "" if pd.isna(value) else str(value).strip())

    merged = _clean(base_values)
    for candidate in (_clean(left_values), _clean(right_values)):
        merged = merged.where(merged.ne(""), candidate)

    current[column_name] = merged
    drop_columns = [name for name in [left_name, right_name] if name in current.columns]
    if drop_columns:
        current = current.drop(columns=drop_columns)
    return current


def _stringify_frame(df: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(df, pd.DataFrame) or df.empty:
        return df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame()
    safe_df = df.copy().astype(object)
    return safe_df.where(safe_df.notna(), "").astype(str)


_project_relative = report_workbook._project_relative
_resolve_project_path = report_workbook._resolve_project_path
_write_excel_meta = report_workbook._write_excel_meta
_frame_content_signature = report_workbook._frame_content_signature
_build_multisheet_workbook_signature = report_workbook._build_multisheet_workbook_signature
_try_reuse_previous_workbook_artifact = report_workbook._try_reuse_previous_workbook_artifact
_materialize_multisheet_workbook = report_workbook._materialize_multisheet_workbook
_add_sheet_with_style = report_workbook._add_sheet_with_style
_write_multisheet_workbook = report_workbook._write_multisheet_workbook
_replace_sheet_with_style = report_workbook._replace_sheet_with_style
_style_workbook = report_workbook._style_workbook
_stringify_frame = report_workbook._stringify_frame


def _preview_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    if not isinstance(df, pd.DataFrame) or df.empty:
        return []
    return _stringify_frame(df).to_dict(orient="records")


def _run_report_filters_and_audits(
    *,
    settings: Settings,
    run_id: str,
    current_generated_at: str,
    candidate_universe_df: pd.DataFrame,
    decision_map: dict[str, Any],
    soft_warning_map: dict[str, Any],
    risk_metric_cap_audit: dict[str, Any],
    finish_report_sub_stage,
) -> dict[str, Any]:
    filter_sub_stage_started_at = perf_counter()
    merged = candidate_universe_df.copy()
    effective_holding_days = merged.apply(
        lambda row: resolve_holding_period_days(
            holding_days=row.get("holding_days"),
            holding_period_source=row.get("holding_period_source"),
            holding_keywords=row.get("holding_keywords"),
            fund_name=combine_holding_period_names(
                row.get("基金名称"), row.get("shortname"), row.get("fund_name"), row.get("fullname"),
            ),
        ),
        axis=1,
    ) if not merged.empty else pd.Series(index=merged.index, dtype=object)
    merged["需排除长持有期产品"] = pd.to_numeric(effective_holding_days, errors="coerce").gt(settings.exclude_holding_days_gt)

    excluded_df = merged[merged["需排除长持有期产品"]].copy()
    if not excluded_df.empty:
        excluded_df["排除原因"] = effective_holding_days.loc[excluded_df.index].apply(
            lambda days: f"已识别持有期 {int(days)} 天，超过 {settings.exclude_holding_days_gt} 天阈值"
        )
    generic_holding_placeholder_watch_df, generic_holding_placeholder_watch_summary = _build_generic_holding_period_placeholder_watch(
        excluded_df,
        preview_top_n=settings.preview_top_n,
    )

    holding_gate_stage_started_at = filter_sub_stage_started_at

    candidate_df = merged[~merged["需排除长持有期产品"]].copy()
    decision_evaluated_at = datetime.now()
    candidate_df = _append_project_decision_columns(
        candidate_df,
        decision_map=decision_map,
        soft_warning_map=soft_warning_map,
        evaluated_at=decision_evaluated_at,
    )
    filter_sub_stage_started_at = finish_report_sub_stage(
        "build_filters_and_audits",
        "holding_gate_candidate_preparation",
        filter_sub_stage_started_at,
    )

    filter_sub_stage_started_at = perf_counter()
    candidate_df = _apply_fixed_quality_thresholds(candidate_df)
    fixed_threshold_track_df = _materialize_track_frame(
        candidate_df,
        verdict_column="fixed_threshold_verdict",
        watch_bucket_column="fixed_threshold_watch_bucket",
        reason_column="fixed_threshold_reason",
    )
    filter_sub_stage_started_at = finish_report_sub_stage(
        "build_filters_and_audits",
        "fixed_threshold_application",
        filter_sub_stage_started_at,
    )

    filter_sub_stage_started_at = perf_counter()
    dynamic_base_df = candidate_df[candidate_df["基础门禁通过"]].copy()
    style_thresholds, global_thresholds, style_meta = _compute_dynamic_style_thresholds(dynamic_base_df)
    candidate_df = _apply_dynamic_quality_thresholds(
        candidate_df,
        style_thresholds=style_thresholds,
        global_thresholds=global_thresholds,
        style_meta=style_meta,
    )
    candidate_df = _apply_enhanced_supplemental_gate(candidate_df)
    dynamic_threshold_track_df = _materialize_track_frame(
        candidate_df,
        verdict_column="dynamic_threshold_verdict",
        watch_bucket_column="dynamic_threshold_watch_bucket",
        reason_column="dynamic_threshold_reason",
    )
    filter_sub_stage_started_at = finish_report_sub_stage(
        "build_filters_and_audits",
        "dynamic_threshold_application",
        filter_sub_stage_started_at,
    )

    filter_sub_stage_started_at = perf_counter()
    low_vol_monthly_replay_inputs: dict[str, Any] = {}
    low_vol_positive_ratio_shadow_watch_summary = _build_low_vol_positive_ratio_shadow_watch(
        candidate_df,
        settings=settings,
        preview_top_n=settings.preview_top_n,
        replay_inputs=low_vol_monthly_replay_inputs,
    )
    filter_sub_stage_started_at = finish_report_sub_stage(
        "build_filters_and_audits",
        "low_vol_positive_ratio_shadow_watch",
        filter_sub_stage_started_at,
    )
    filter_sub_stage_started_at = finish_report_sub_stage(
        "build_filters_and_audits",
        "holding_gate_and_threshold_tracks",
        holding_gate_stage_started_at,
    )

    filter_sub_stage_started_at = perf_counter()
    fixed_post_review_evaluated_at = datetime.now()
    fixed_publish_track_df = _apply_publish_post_filters(
        fixed_threshold_track_df,
        threshold_mode="fixed",
        style_thresholds=style_thresholds,
        global_thresholds=global_thresholds,
        evaluated_at=fixed_post_review_evaluated_at,
    )
    dynamic_post_review_evaluated_at = datetime.now()
    dynamic_publish_track_df = _apply_publish_post_filters(
        dynamic_threshold_track_df,
        threshold_mode="dynamic",
        style_thresholds=style_thresholds,
        global_thresholds=global_thresholds,
        evaluated_at=dynamic_post_review_evaluated_at,
    )

    dynamic_publish_official_df = dynamic_publish_track_df[dynamic_publish_track_df["项目推荐结论"] == "正式推荐"].copy()
    fixed_official_audit_df = fixed_publish_track_df[fixed_publish_track_df["项目推荐结论"] == "正式推荐"].copy()
    fixed_internal_watch_audit_df = fixed_publish_track_df[fixed_publish_track_df["项目推荐结论"] != "正式推荐"].copy()
    dynamic_official_df = dynamic_threshold_track_df[dynamic_threshold_track_df["项目推荐结论"] == "正式推荐"].copy()
    effective_nav_snapshot = load_effective_nav_snapshot(
        settings.database_path,
        coverage_ratio=settings.effective_nav_coverage_ratio,
        weekday_only=settings.effective_nav_weekday_only,
    ).to_dict()
    effective_nav_candidate_date = str(effective_nav_snapshot.get("candidate_date") or "").strip()
    selection_tracker_history_capture: dict[str, Any] = {}
    final_selection_tracker_features_df = _build_final_selection_tracker_features(
        settings,
        current_run_id=run_id,
        current_generated_at=current_generated_at,
        current_nav_snapshot_date=effective_nav_candidate_date,
        current_df=dynamic_publish_official_df,
        history_capture=selection_tracker_history_capture,
    )
    official_df = _apply_final_official_selection(
        dynamic_publish_official_df,
        settings=settings,
        tracker_features_df=final_selection_tracker_features_df,
    )
    removed_after_final_selection_df = dynamic_publish_official_df[
        ~dynamic_publish_official_df.get("基金代码", pd.Series(dtype=str)).astype(str).isin(
            official_df.get("基金代码", pd.Series(dtype=str)).astype(str).tolist()
        )
    ].copy()
    removed_after_final_selection_df = _annotate_final_selection_removed_rows(removed_after_final_selection_df)
    filter_sub_stage_started_at = finish_report_sub_stage(
        "build_filters_and_audits",
        "publish_filters_and_final_selection",
        filter_sub_stage_started_at,
    )

    # P0-04.4 / V5: 汇总 AutoStock 补充校验阶段的降级信号，
    # 通过 dict 引用传递，规避依赖 pandas DataFrame.attrs 的脆弱性。
    autostock_status_context: dict[str, Any] = {}

    filter_sub_stage_started_at = perf_counter()
    official_df, dynamic_official_df = _supplement_report_autostock_tracks(
        official_df, dynamic_official_df,
        settings=settings,
        status_context=autostock_status_context,
    )
    dynamic_official_df, dynamic_autostock_blocked_df = _demote_autostock_hard_block_rows(dynamic_official_df)
    official_df, official_autostock_blocked_df = _demote_autostock_hard_block_rows(official_df)
    if not official_autostock_blocked_df.empty:
        if removed_after_final_selection_df.empty:
            removed_after_final_selection_df = official_autostock_blocked_df.copy()
        else:
            removed_after_final_selection_df = pd.concat(
                [removed_after_final_selection_df, official_autostock_blocked_df],
                ignore_index=True,
            )
    internal_watch_df = pd.concat(
        [
            dynamic_publish_track_df[dynamic_publish_track_df["项目推荐结论"] != "正式推荐"].copy(),
            removed_after_final_selection_df,
        ],
        ignore_index=True,
    )
    dynamic_internal_watch_df = pd.concat(
        [
            dynamic_threshold_track_df[dynamic_threshold_track_df["项目推荐结论"] != "正式推荐"].copy(),
            dynamic_autostock_blocked_df,
        ],
        ignore_index=True,
    )
    official_df = _refresh_recommendation_notes(official_df)
    dynamic_official_df = _refresh_recommendation_notes(dynamic_official_df)
    final_selection_contraction_df = _build_final_selection_contraction_explainability(
        dynamic_publish_official_df,
        official_df=official_df,
        removed_after_final_selection_df=removed_after_final_selection_df,
        tracker_features_df=final_selection_tracker_features_df,
        settings=settings,
    )
    filter_sub_stage_started_at = finish_report_sub_stage(
        "build_filters_and_audits",
        "autostock_overlay_and_watch_materialization",
        filter_sub_stage_started_at,
    )

    filter_sub_stage_started_at = perf_counter()
    official_codes = {
        _normalize_code(code)
        for code in official_df.get("基金代码", pd.Series(dtype=object)).astype(str).tolist()
        if _normalize_code(code)
    }
    stage_b_reason_ledger_df, stage_b_reason_summary = _build_stage_b_reason_ledger(
        candidate_df=internal_watch_df,
        excluded_df=excluded_df,
        official_codes=official_codes,
    )
    official_export_full = _ensure_export_formal_score_column(official_df.copy()).rename(columns=PROJECT_EXPORT_RENAME_MAPPING)
    official_export_full = with_manager_display_columns(_drop_duplicate_columns(official_export_full))
    official_export_full = _append_formal_group_column(official_export_full)
    official_style_df = _distribution_frame(official_df, "债基风格标签", "风格标签", "占正式推荐比例(%)")
    official_company_df = _distribution_frame(official_df, "company_name", "基金公司", "占正式推荐比例(%)")
    fee_band_df = _build_fee_band_frame(official_df)
    formal_group_distribution_df = _build_formal_group_distribution(official_export_full)
    stable_official_export_full = official_export_full[official_export_full[FORMAL_GROUP_LABEL] == STABLE_BOND_GROUP_LABEL].copy()
    enhanced_official_export_full = official_export_full[official_export_full[FORMAL_GROUP_LABEL] == ENHANCED_BOND_GROUP_LABEL].copy()
    stable_official_export = _select_unique_columns(stable_official_export_full, REFERENCE_FINAL_COLUMNS)
    enhanced_official_export = _select_unique_columns(enhanced_official_export_full, REFERENCE_FINAL_COLUMNS)
    stable_official_style_df = _distribution_frame(stable_official_export_full, "债基风格标签", "风格标签", "占纯债稳健榜比例(%)")
    enhanced_official_style_df = _distribution_frame(enhanced_official_export_full, "债基风格标签", "风格标签", "占增强债弹性榜比例(%)")
    dynamic_official_style_df = _distribution_frame(dynamic_official_df, "债基风格标签", "风格标签", "占正式推荐比例(%)")
    dynamic_official_company_df = _distribution_frame(dynamic_official_df, "company_name", "基金公司", "占正式推荐比例(%)")
    dynamic_fee_band_df = _build_fee_band_frame(dynamic_official_df)

    internal_watch_export_full = _ensure_export_formal_score_column(internal_watch_df.copy()).rename(columns=PROJECT_EXPORT_RENAME_MAPPING)
    internal_watch_export_full = with_manager_display_columns(_drop_duplicate_columns(internal_watch_export_full))
    internal_watch_summary_df = _build_watch_summary_frame(internal_watch_df)
    dynamic_internal_watch_export_full = _ensure_export_formal_score_column(dynamic_internal_watch_df.copy()).rename(columns=PROJECT_EXPORT_RENAME_MAPPING)
    dynamic_internal_watch_export_full = with_manager_display_columns(
        _drop_duplicate_columns(dynamic_internal_watch_export_full), default_status="未执行经理门禁",
    )
    dynamic_internal_watch_summary_df = _build_watch_summary_frame(dynamic_internal_watch_df)
    dynamic_threshold_detail_df = _build_dynamic_threshold_detail_frame(
        style_meta=style_meta,
        global_thresholds=global_thresholds,
    )
    publish_stage_audit_summary = _build_publish_stage_audit_summary(
        candidate_df=candidate_df,
        style_meta=style_meta,
        global_thresholds=global_thresholds,
        fixed_threshold_track_df=fixed_threshold_track_df,
        fixed_publish_track_df=fixed_publish_track_df,
        dynamic_threshold_track_df=dynamic_threshold_track_df,
        dynamic_publish_track_df=dynamic_publish_track_df,
        official_df=official_df,
        removed_after_final_selection_df=removed_after_final_selection_df,
    )
    publish_stage_audit_df = _build_publish_stage_audit_frame(publish_stage_audit_summary)
    filter_sub_stage_started_at = finish_report_sub_stage(
        "build_filters_and_audits",
        "export_views_distributions_and_publish_audit",
        filter_sub_stage_started_at,
    )

    filter_sub_stage_started_at = perf_counter()
    fixed_baseline_codes = set(
        fixed_threshold_track_df[fixed_threshold_track_df["项目推荐结论"] == "正式推荐"]
        .get("基金代码", fixed_threshold_track_df.get("fcode", pd.Series(dtype=str)))
        .astype(str)
    )
    dynamic_baseline_codes = set(
        dynamic_threshold_track_df[dynamic_threshold_track_df["项目推荐结论"] == "正式推荐"]
        .get("基金代码", dynamic_threshold_track_df.get("fcode", pd.Series(dtype=str)))
        .astype(str)
    )
    dynamic_reference_export_codes = set(
        dynamic_official_df.get("基金代码", dynamic_official_df.get("fcode", pd.Series(dtype=str))).astype(str)
    )
    fixed_only_codes = sorted(code for code in fixed_baseline_codes - dynamic_baseline_codes if code)
    dynamic_only_codes = sorted(code for code in dynamic_baseline_codes - fixed_baseline_codes if code)
    overlap_codes = sorted(code for code in fixed_baseline_codes & dynamic_baseline_codes if code)
    style_pass_rate_df = _build_style_pass_rate_audit(
        candidate_df=candidate_df,
        dynamic_baseline_df=dynamic_threshold_track_df[dynamic_threshold_track_df["项目推荐结论"] == "正式推荐"].copy(),
        official_df=official_df,
    )
    style_veto_reason_df = _build_style_veto_reason_audit(stage_b_reason_ledger_df, top_n=5)
    reason_group_audit_df = _build_reason_group_audit(stage_b_reason_ledger_df)
    quality_factor_group_audit_df = _build_quality_factor_group_audit(stage_b_reason_ledger_df)
    quality_factor_detail_audit_df = _build_quality_factor_detail_audit(stage_b_reason_ledger_df)
    quality_overlap_audit = _build_quality_overlap_audit(stage_b_reason_ledger_df)
    annual_return_score_sharpe_coupling_audit = _build_annual_return_score_sharpe_coupling_audit(
        stage_b_reason_ledger_df,
        style_threshold_rows=dynamic_threshold_detail_df.fillna("").to_dict(orient="records"),
    )
    score_cluster_explainability_audit = _build_score_cluster_explainability_audit(stage_b_reason_ledger_df)
    quality_overlap_pair_df = quality_overlap_audit["pair_df"]
    quality_overlap_combo_df = quality_overlap_audit["combo_df"]
    quality_overlap_sample_df = quality_overlap_audit["sample_df"]
    risk_adjusted_metric_stack_review_audit = build_risk_adjusted_metric_stack_review(
        quality_factor_detail_df=quality_factor_detail_audit_df,
        quality_overlap_pair_df=quality_overlap_pair_df,
        quality_overlap_combo_df=quality_overlap_combo_df,
        positive_ratio_semantics=_build_positive_ratio_semantics_summary(
            low_vol_positive_ratio_shadow_watch_summary
        ),
        low_vol_positive_ratio_shadow_watch=low_vol_positive_ratio_shadow_watch_summary,
    )
    status_split_audit_df = _build_status_split_audit(stage_b_reason_ledger_df)
    high_score_audit = _build_high_score_rejection_audit(
        stage_b_reason_ledger_df,
        score_threshold=HIGH_SCORE_AUDIT_THRESHOLD,
    )
    high_score_rejected_df = high_score_audit["high_score_rejected_df"]
    high_score_not_buyable_df = high_score_audit["high_score_not_buyable_df"]
    high_score_friction_audit = _build_high_score_friction_audit(
        stage_b_reason_ledger_df,
        score_threshold=HIGH_SCORE_AUDIT_THRESHOLD,
    )
    high_score_friction_sample_df = high_score_friction_audit["sample_df"]
    high_score_friction_reason_df = high_score_friction_audit["reason_df"]
    high_score_friction_style_df = high_score_friction_audit["style_df"]
    high_score_scale_gate_audit = _build_high_score_reason_focus_audit(
        stage_b_reason_ledger_df,
        internal_watch_df,
        reason_code="scale_too_small",
        score_threshold=HIGH_SCORE_AUDIT_THRESHOLD,
    )
    high_score_scale_gate_sample_df = high_score_scale_gate_audit["sample_df"]
    high_score_scale_gate_style_df = high_score_scale_gate_audit["style_df"]
    high_score_scale_review_audit = _build_high_score_scale_review_audit(high_score_scale_gate_sample_df)
    high_score_scale_gate_band_df = high_score_scale_review_audit["band_df"]
    high_score_scale_gate_age_df = high_score_scale_review_audit["age_df"]
    high_score_scale_gate_near_df = high_score_scale_review_audit["near_df"]
    high_score_scale_near_review_audit = _build_high_score_scale_near_review_audit(high_score_scale_gate_near_df)
    high_score_scale_gate_near_review_df = high_score_scale_near_review_audit["review_df"]
    high_score_scale_gate_near_review_sample_df = high_score_scale_near_review_audit["sample_df"]
    high_score_fee_gate_audit = _build_high_score_reason_focus_audit(
        stage_b_reason_ledger_df,
        internal_watch_df,
        reason_code="fee_too_high",
        score_threshold=HIGH_SCORE_AUDIT_THRESHOLD,
    )
    high_score_fee_gate_sample_df = high_score_fee_gate_audit["sample_df"]
    high_score_fee_gate_style_df = high_score_fee_gate_audit["style_df"]
    high_score_friction_gate_review_audit = build_high_score_friction_gate_review(
        high_score_threshold=HIGH_SCORE_AUDIT_THRESHOLD,
        high_score_friction_reason_df=high_score_friction_reason_df,
        high_score_friction_style_df=high_score_friction_style_df,
        high_score_friction_sample_df=high_score_friction_sample_df,
        high_score_scale_gate_sample_df=high_score_scale_gate_sample_df,
        high_score_scale_gate_near_df=high_score_scale_gate_near_df,
        high_score_scale_gate_near_review_df=high_score_scale_gate_near_review_df,
        high_score_scale_gate_near_review_sample_df=high_score_scale_gate_near_review_sample_df,
        high_score_fee_gate_style_df=high_score_fee_gate_style_df,
        high_score_fee_gate_sample_df=high_score_fee_gate_sample_df,
    )
    standard_screening_audit_summary = _build_standard_screening_audit_summary(
        style_pass_rate_df=style_pass_rate_df,
        style_veto_reason_df=style_veto_reason_df,
        reason_group_audit_df=reason_group_audit_df,
        quality_factor_group_audit_df=quality_factor_group_audit_df,
        quality_factor_detail_audit_df=quality_factor_detail_audit_df,
        quality_overlap_pair_df=quality_overlap_pair_df,
        quality_overlap_combo_df=quality_overlap_combo_df,
        annual_return_score_sharpe_coupling_audit=annual_return_score_sharpe_coupling_audit,
        score_cluster_explainability_audit=score_cluster_explainability_audit,
        risk_adjusted_metric_stack_review_audit=risk_adjusted_metric_stack_review_audit,
        status_split_audit_df=status_split_audit_df,
        high_score_rejected_df=high_score_rejected_df,
        high_score_not_buyable_df=high_score_not_buyable_df,
        high_score_friction_sample_df=high_score_friction_sample_df,
        high_score_friction_reason_df=high_score_friction_reason_df,
        high_score_friction_style_df=high_score_friction_style_df,
        high_score_scale_gate_sample_df=high_score_scale_gate_sample_df,
        high_score_scale_gate_style_df=high_score_scale_gate_style_df,
        high_score_scale_gate_band_df=high_score_scale_gate_band_df,
        high_score_scale_gate_age_df=high_score_scale_gate_age_df,
        high_score_scale_gate_near_df=high_score_scale_gate_near_df,
        high_score_scale_gate_near_review_df=high_score_scale_gate_near_review_df,
        high_score_scale_gate_near_review_sample_df=high_score_scale_gate_near_review_sample_df,
        high_score_fee_gate_sample_df=high_score_fee_gate_sample_df,
        high_score_fee_gate_style_df=high_score_fee_gate_style_df,
        high_score_friction_gate_review_audit=high_score_friction_gate_review_audit,
        risk_metric_cap_audit=risk_metric_cap_audit,
    )
    finish_report_sub_stage(
        "build_filters_and_audits",
        "threshold_overlap_and_screening_audits",
        filter_sub_stage_started_at,
    )
    return {
        "merged": merged,
        "evaluation_times": {
            "decision": decision_evaluated_at.astimezone().isoformat(),
            "fixed_post_review": fixed_post_review_evaluated_at.astimezone().isoformat(),
            "dynamic_post_review": dynamic_post_review_evaluated_at.astimezone().isoformat(),
        },
        "excluded_df": excluded_df,
        "generic_holding_placeholder_watch_df": generic_holding_placeholder_watch_df,
        "generic_holding_placeholder_watch_summary": generic_holding_placeholder_watch_summary,
        "candidate_df": candidate_df,
        "fixed_threshold_track_df": fixed_threshold_track_df,
        "style_thresholds": style_thresholds,
        "global_thresholds": global_thresholds,
        "style_meta": style_meta,
        "dynamic_threshold_track_df": dynamic_threshold_track_df,
        "low_vol_positive_ratio_shadow_watch_summary": low_vol_positive_ratio_shadow_watch_summary,
        "low_vol_monthly_replay_inputs": low_vol_monthly_replay_inputs,
        "fixed_publish_track_df": fixed_publish_track_df,
        "dynamic_publish_track_df": dynamic_publish_track_df,
        "dynamic_publish_official_df": dynamic_publish_official_df,
        "fixed_official_audit_df": fixed_official_audit_df,
        "fixed_internal_watch_audit_df": fixed_internal_watch_audit_df,
        "dynamic_official_df": dynamic_official_df,
        "autostock_status_context": autostock_status_context,
        "effective_nav_snapshot": effective_nav_snapshot,
        "effective_nav_candidate_date": effective_nav_candidate_date,
        "official_df": official_df,
        "removed_after_final_selection_df": removed_after_final_selection_df,
        "final_selection_tracker_features_df": final_selection_tracker_features_df,
        "selection_tracker_history": selection_tracker_history_capture,
        "final_selection_contraction_df": final_selection_contraction_df,
        "internal_watch_df": internal_watch_df,
        "dynamic_internal_watch_df": dynamic_internal_watch_df,
        "stage_b_reason_ledger_df": stage_b_reason_ledger_df,
        "stage_b_reason_summary": stage_b_reason_summary,
        "official_export_full": official_export_full,
        "official_style_df": official_style_df,
        "official_company_df": official_company_df,
        "fee_band_df": fee_band_df,
        "formal_group_distribution_df": formal_group_distribution_df,
        "stable_official_export_full": stable_official_export_full,
        "enhanced_official_export_full": enhanced_official_export_full,
        "stable_official_export": stable_official_export,
        "enhanced_official_export": enhanced_official_export,
        "stable_official_style_df": stable_official_style_df,
        "enhanced_official_style_df": enhanced_official_style_df,
        "dynamic_official_style_df": dynamic_official_style_df,
        "dynamic_official_company_df": dynamic_official_company_df,
        "dynamic_fee_band_df": dynamic_fee_band_df,
        "internal_watch_export_full": internal_watch_export_full,
        "internal_watch_summary_df": internal_watch_summary_df,
        "dynamic_internal_watch_export_full": dynamic_internal_watch_export_full,
        "dynamic_internal_watch_summary_df": dynamic_internal_watch_summary_df,
        "dynamic_threshold_detail_df": dynamic_threshold_detail_df,
        "publish_stage_audit_summary": publish_stage_audit_summary,
        "publish_stage_audit_df": publish_stage_audit_df,
        "fixed_baseline_codes": fixed_baseline_codes,
        "dynamic_baseline_codes": dynamic_baseline_codes,
        "dynamic_reference_export_codes": dynamic_reference_export_codes,
        "fixed_only_codes": fixed_only_codes,
        "dynamic_only_codes": dynamic_only_codes,
        "overlap_codes": overlap_codes,
        "style_pass_rate_df": style_pass_rate_df,
        "style_veto_reason_df": style_veto_reason_df,
        "reason_group_audit_df": reason_group_audit_df,
        "quality_factor_group_audit_df": quality_factor_group_audit_df,
        "quality_factor_detail_audit_df": quality_factor_detail_audit_df,
        "quality_overlap_pair_df": quality_overlap_pair_df,
        "quality_overlap_combo_df": quality_overlap_combo_df,
        "quality_overlap_sample_df": quality_overlap_sample_df,
        "status_split_audit_df": status_split_audit_df,
        "high_score_rejected_df": high_score_rejected_df,
        "high_score_not_buyable_df": high_score_not_buyable_df,
        "high_score_friction_sample_df": high_score_friction_sample_df,
        "high_score_friction_reason_df": high_score_friction_reason_df,
        "high_score_friction_style_df": high_score_friction_style_df,
        "high_score_scale_gate_sample_df": high_score_scale_gate_sample_df,
        "high_score_scale_gate_style_df": high_score_scale_gate_style_df,
        "high_score_scale_gate_band_df": high_score_scale_gate_band_df,
        "high_score_scale_gate_age_df": high_score_scale_gate_age_df,
        "high_score_scale_gate_near_df": high_score_scale_gate_near_df,
        "high_score_scale_gate_near_review_df": high_score_scale_gate_near_review_df,
        "high_score_scale_gate_near_review_sample_df": high_score_scale_gate_near_review_sample_df,
        "high_score_fee_gate_sample_df": high_score_fee_gate_sample_df,
        "high_score_fee_gate_style_df": high_score_fee_gate_style_df,
        "standard_screening_audit_summary": standard_screening_audit_summary,
    }


def _prepare_materialized_export_context(
    *,
    settings: Settings,
    run_id: str,
    current_generated_at: str,
    effective_nav_candidate_date: str,
    filtered_excel: Path,
    watch_excel: Path,
    candidate_df: pd.DataFrame,
    excluded_df: pd.DataFrame,
    official_df: pd.DataFrame,
    dynamic_publish_track_df: pd.DataFrame,
    dynamic_publish_official_df: pd.DataFrame,
    dynamic_official_df: pd.DataFrame,
    dynamic_internal_watch_df: pd.DataFrame,
    dynamic_internal_watch_export_full: pd.DataFrame,
    stable_official_export_full: pd.DataFrame,
    enhanced_official_export_full: pd.DataFrame,
    dynamic_threshold_track_df: pd.DataFrame,
    fixed_threshold_track_df: pd.DataFrame,
    fixed_publish_track_df: pd.DataFrame,
    style_meta: dict[str, Any],
    global_thresholds: dict[str, Any],
    removed_after_final_selection_df: pd.DataFrame,
    final_selection_contraction_df: pd.DataFrame,
    internal_watch_df: pd.DataFrame,
) -> dict[str, Any]:
    temp_official_export_full = _ensure_export_formal_score_column(official_df.copy()).rename(columns=PROJECT_EXPORT_RENAME_MAPPING)
    temp_official_export_full = _drop_duplicate_columns(temp_official_export_full)
    temp_official_export_full = _append_formal_group_column(temp_official_export_full)
    incremental_review_capture: dict[str, Any] = {}
    incremental_review_full_df = build_incremental_review_frame(
        settings,
        run_id=run_id,
        generated_at=current_generated_at,
        snapshot_date=effective_nav_candidate_date,
        filtered_excel=filtered_excel,
        watch_excel=watch_excel,
        final_count=int(len(official_df.index)),
        stable_final_count=int(len(stable_official_export_full.index)),
        enhanced_final_count=int(len(enhanced_official_export_full.index)),
        current_formal_df=temp_official_export_full,
        input_capture=incremental_review_capture,
    )
    incremental_review_seed_df = _build_incremental_safety_watch_seed_df(official_df, incremental_review_full_df)
    incremental_safety_watch_codes = {
        _normalize_code(row.get("基金代码"))
        for row in incremental_review_seed_df.to_dict(orient="records")
        if _is_incremental_safety_watch_candidate(row)
    }
    incremental_review_capture["demoted_codes"] = sorted(incremental_safety_watch_codes)
    if incremental_safety_watch_codes:
        dynamic_publish_track_df = _demote_incremental_safety_watch_codes(
            dynamic_publish_track_df,
            target_codes=incremental_safety_watch_codes,
        )
        dynamic_publish_official_df = dynamic_publish_track_df[dynamic_publish_track_df["项目推荐结论"] == "正式推荐"].copy()
        official_df = official_df[
            ~official_df.get("基金代码", pd.Series(dtype=object)).map(_normalize_code).isin(incremental_safety_watch_codes)
        ].copy()

    internal_watch_df = pd.concat(
        [
            dynamic_publish_track_df[dynamic_publish_track_df["项目推荐结论"] != "正式推荐"].copy(),
            removed_after_final_selection_df,
        ],
        ignore_index=True,
    )
    official_codes = {
        _normalize_code(code)
        for code in official_df.get("基金代码", pd.Series(dtype=object)).astype(str).tolist()
        if _normalize_code(code)
    }
    stage_b_reason_ledger_df, stage_b_reason_summary = _build_stage_b_reason_ledger(
        candidate_df=internal_watch_df,
        excluded_df=excluded_df,
        official_codes=official_codes,
    )
    official_style_df = _distribution_frame(official_df, "债基风格标签", "风格标签", "占正式推荐比例(%)")
    official_company_df = _distribution_frame(official_df, "company_name", "基金公司", "占正式推荐比例(%)")
    fee_band_df = _build_fee_band_frame(official_df)
    official_export_full = _ensure_export_formal_score_column(official_df.copy()).rename(columns=PROJECT_EXPORT_RENAME_MAPPING)
    official_export_full = _drop_duplicate_columns(official_export_full)
    official_export_full = _append_formal_group_column(official_export_full)
    formal_group_distribution_df = _build_formal_group_distribution(official_export_full)
    stable_official_export_full = official_export_full[official_export_full[FORMAL_GROUP_LABEL] == STABLE_BOND_GROUP_LABEL].copy()
    enhanced_official_export_full = official_export_full[official_export_full[FORMAL_GROUP_LABEL] == ENHANCED_BOND_GROUP_LABEL].copy()
    stable_official_export = _select_unique_columns(stable_official_export_full, REFERENCE_FINAL_COLUMNS)
    enhanced_official_export = _select_unique_columns(enhanced_official_export_full, REFERENCE_FINAL_COLUMNS)
    stable_official_style_df = _distribution_frame(stable_official_export_full, "债基风格标签", "风格标签", "占纯债稳健榜比例(%)")
    enhanced_official_style_df = _distribution_frame(enhanced_official_export_full, "债基风格标签", "风格标签", "占增强债弹性榜比例(%)")
    internal_watch_export_full = _ensure_export_formal_score_column(internal_watch_df.copy()).rename(columns=PROJECT_EXPORT_RENAME_MAPPING)
    internal_watch_export_full = _drop_duplicate_columns(internal_watch_export_full)
    internal_watch_summary_df = _build_watch_summary_frame(internal_watch_df)
    publish_stage_audit_summary = _build_publish_stage_audit_summary(
        candidate_df=candidate_df,
        style_meta=style_meta,
        global_thresholds=global_thresholds,
        fixed_threshold_track_df=fixed_threshold_track_df,
        fixed_publish_track_df=fixed_publish_track_df,
        dynamic_threshold_track_df=dynamic_threshold_track_df,
        dynamic_publish_track_df=dynamic_publish_track_df,
        official_df=official_df,
        removed_after_final_selection_df=removed_after_final_selection_df,
    )
    publish_stage_audit_df = _build_publish_stage_audit_frame(publish_stage_audit_summary)
    incremental_review_summary_df = _build_incremental_review_summary_frame(
        incremental_review_full_df,
        official_df=official_df,
    )
    official_export_full = _attach_incremental_review_flags(official_export_full, incremental_review_full_df)
    official_export = _select_unique_columns(official_export_full, REFERENCE_FINAL_COLUMNS)
    recommendation_detail = _select_unique_columns(
        official_export_full,
        RECOMMENDATION_DETAIL_COLUMNS
        + [FORMAL_GROUP_LABEL, "推荐备注", "基金经理", "综合加权得分", "近期得分", "长期得分", "dynamic_threshold_label", "dynamic_threshold_source", "enhanced_supplemental_label", "新增样本复核状态", "新增样本体检备注"],
    )
    internal_watch_export_full = _attach_incremental_review_flags(internal_watch_export_full, incremental_review_full_df)
    internal_watch_export = _select_unique_columns(internal_watch_export_full, WATCH_EXPORT_COLUMNS + ["新增样本复核状态", "新增样本体检备注"])
    dynamic_official_export_full = _ensure_export_formal_score_column(dynamic_official_df.copy()).rename(columns=PROJECT_EXPORT_RENAME_MAPPING)
    dynamic_official_export_full = with_manager_display_columns(
        _drop_duplicate_columns(dynamic_official_export_full), default_status="未执行经理门禁",
    )
    dynamic_official_export_full = _append_formal_group_column(dynamic_official_export_full)
    dynamic_official_export_full = _attach_incremental_review_flags(dynamic_official_export_full, incremental_review_full_df)
    dynamic_official_export = _select_unique_columns(dynamic_official_export_full, REFERENCE_FINAL_COLUMNS)
    dynamic_recommendation_detail = _select_unique_columns(
        dynamic_official_export_full,
        RECOMMENDATION_DETAIL_COLUMNS
        + [FORMAL_GROUP_LABEL, "推荐备注", "基金经理", "综合加权得分", "近期得分", "长期得分", "dynamic_threshold_label", "dynamic_threshold_source", "enhanced_supplemental_label", "新增样本复核状态", "新增样本体检备注"],
    )
    dynamic_internal_watch_export_full = _attach_incremental_review_flags(dynamic_internal_watch_export_full, incremental_review_full_df)
    dynamic_internal_watch_export = _select_unique_columns(dynamic_internal_watch_export_full, WATCH_EXPORT_COLUMNS + ["新增样本复核状态", "新增样本体检备注"])
    priority_pool_tracker_features_df = _build_priority_pool_tracker_features(
        settings,
        current_run_id=run_id,
        current_generated_at=current_generated_at,
        current_nav_snapshot_date=effective_nav_candidate_date,
        current_df=official_export_full,
    )
    portfolio_df, portfolio_summary = _build_portfolio_suggestions(
        official_export_full,
        tracker_features_df=priority_pool_tracker_features_df,
    )
    return {
        "incremental_review_full_df": incremental_review_full_df,
        "incremental_review_capture": incremental_review_capture,
        "dynamic_publish_track_df": dynamic_publish_track_df,
        "dynamic_publish_official_df": dynamic_publish_official_df,
        "official_df": official_df,
        "internal_watch_df": internal_watch_df,
        "stage_b_reason_ledger_df": stage_b_reason_ledger_df,
        "stage_b_reason_summary": stage_b_reason_summary,
        "official_style_df": official_style_df,
        "official_company_df": official_company_df,
        "fee_band_df": fee_band_df,
        "official_export_full": official_export_full,
        "formal_group_distribution_df": formal_group_distribution_df,
        "stable_official_export_full": stable_official_export_full,
        "enhanced_official_export_full": enhanced_official_export_full,
        "stable_official_export": stable_official_export,
        "enhanced_official_export": enhanced_official_export,
        "stable_official_style_df": stable_official_style_df,
        "enhanced_official_style_df": enhanced_official_style_df,
        "internal_watch_export_full": internal_watch_export_full,
        "internal_watch_export": internal_watch_export,
        "internal_watch_summary_df": internal_watch_summary_df,
        "publish_stage_audit_summary": publish_stage_audit_summary,
        "publish_stage_audit_df": publish_stage_audit_df,
        "final_selection_contraction_df": final_selection_contraction_df,
        "incremental_review_summary_df": incremental_review_summary_df,
        "official_export": official_export,
        "recommendation_detail": recommendation_detail,
        "dynamic_official_export_full": dynamic_official_export_full,
        "dynamic_official_export": dynamic_official_export,
        "dynamic_recommendation_detail": dynamic_recommendation_detail,
        "dynamic_internal_watch_export_full": dynamic_internal_watch_export_full,
        "dynamic_internal_watch_export": dynamic_internal_watch_export,
        "portfolio_df": portfolio_df,
        "portfolio_summary": portfolio_summary,
    }


def _materialize_primary_report_workbooks(
    *,
    settings: Settings,
    run_id: str,
    current_generated_at: str,
    effective_nav_candidate_date: str,
    universe_snapshot: dict[str, Any],
    merged: pd.DataFrame,
    excluded_df: pd.DataFrame,
    candidate_df: pd.DataFrame,
    official_df: pd.DataFrame,
    dynamic_official_df: pd.DataFrame,
    dynamic_internal_watch_df: pd.DataFrame,
    publish_summary_df: pd.DataFrame,
    dynamic_threshold_detail_df: pd.DataFrame,
    dynamic_official_style_df: pd.DataFrame,
    dynamic_official_company_df: pd.DataFrame,
    dynamic_fee_band_df: pd.DataFrame,
    dynamic_internal_watch_summary_df: pd.DataFrame,
    export_context: dict[str, Any],
    report_paths: dict[str, Path],
) -> dict[str, Any]:
    filtered_workbook_result = _materialize_multisheet_workbook(
        settings=settings,
        output_file=report_paths["filtered_excel"],
        artifact_key="filtered_excel",
        artifact_meta_key="filtered_excel_meta",
        main_sheet_name="Sheet1",
        main_df=export_context["official_export"],
        extra_sheets=[
            {
                "title": "推荐理由与风格",
                "df": export_context["recommendation_detail"],
                "left_align_column": "基金名称",
                "column_widths": {
                    "基金代码": 12,
                    "基金名称": 24,
                    "项目推荐结论": 14,
                    "推荐理由": 24,
                    "债基风格标签": 14,
                    "基准归类": 12,
                    "AutoStock告警原因": 20,
                    "新增样本复核状态": 16,
                    "新增样本体检备注": 28,
                },
            },
            {
                "title": "新增样本复核摘要",
                "df": export_context["incremental_review_summary_df"],
                "left_align_column": "指标",
                "column_widths": {"指标": 26, "数值": 40},
            },
            {
                "title": "发布摘要",
                "df": publish_summary_df,
                "left_align_column": "指标",
                "column_widths": {"指标": 22, "数值": 16},
            },
            {
                "title": "阶段审计摘要",
                "df": export_context["publish_stage_audit_df"],
                "left_align_column": "阶段",
                "column_widths": {
                    "阶段": 18,
                    "范围": 16,
                    "输入样本数": 12,
                    "通过数": 10,
                    "淘汰数": 10,
                    "淘汰率(%)": 12,
                    "主原因1": 28,
                    "主原因2": 28,
                    "主原因3": 28,
                    "说明": 40,
                },
            },
            {
                "title": "最终精选收敛解释",
                "df": export_context["final_selection_contraction_df"],
                "left_align_column": "基金名称",
                "column_widths": {
                    "基金代码": 12,
                    "基金名称": 24,
                    FORMAL_GROUP_LABEL: 16,
                    "债基风格标签": 14,
                    "正式精选状态": 14,
                    "最终精选原因代码": 28,
                    "最终精选解释": 24,
                    "最终精选稳定性排名": 16,
                    "分组稳定性排名": 16,
                    FORMAL_SCORE_LABEL: 14,
                    "_final_stability_score": 18,
                    "出现次数": 10,
                    "出现频率(%)": 12,
                    "距整体入围最低分差": 18,
                    "距同分组入围最低分差": 18,
                },
            },
            {
                "title": "正式推荐风格分布",
                "df": export_context["official_style_df"],
                "left_align_column": "风格标签",
                "column_widths": {"风格标签": 16, "数量": 10, "占正式推荐比例(%)": 18},
            },
            {
                "title": "正式推荐公司分布",
                "df": export_context["official_company_df"],
                "left_align_column": "基金公司",
                "column_widths": {"基金公司": 18, "数量": 10, "占正式推荐比例(%)": 18},
            },
            {
                "title": "正式推荐费率分布",
                "df": export_context["fee_band_df"],
                "left_align_column": "费率区间",
                "column_widths": {"费率区间": 16, "数量": 10},
            },
            {
                "title": "正式榜分组摘要",
                "df": export_context["formal_group_distribution_df"],
                "left_align_column": FORMAL_GROUP_LABEL,
                "column_widths": {FORMAL_GROUP_LABEL: 16, "数量": 10, "占正式推荐比例(%)": 18},
            },
            {
                "title": STABLE_BOND_GROUP_LABEL,
                "df": export_context["stable_official_export"],
                "left_align_column": "基金名称",
                "column_widths": FINAL_MAIN_COLUMN_WIDTHS,
            },
            {
                "title": ENHANCED_BOND_GROUP_LABEL,
                "df": export_context["enhanced_official_export"],
                "left_align_column": "基金名称",
                "column_widths": FINAL_MAIN_COLUMN_WIDTHS,
            },
            {
                "title": "组合建议",
                "df": export_context["portfolio_df"],
                "left_align_column": "基金名称",
                "column_widths": {
                    "组合名称": 16,
                    "建议权重": 12,
                    "基金代码": 12,
                    "基金名称": 24,
                    "基金公司": 18,
                    FORMAL_GROUP_LABEL: 14,
                    "债基风格标签": 14,
                    "基金规模(亿)": 12,
                    "最新净值日期": 12,
                    "费率合计(%)": 12,
                    "正式投资评分": 14,
                    "投资优先池得分": 14,
                    "项目质量信心": 12,
                    "出现次数": 10,
                    "出现频率(%)": 12,
                    "1年-年化收益(%)": 14,
                    "近1月(%)": 12,
                    "近3月(%)": 12,
                    "1年-夏普比率": 12,
                    "1年-最大回撤(%)": 14,
                    "1年-波动率": 12,
                    "AutoStock原始校验状态": 18,
                    "长期稳定性标签": 16,
                    "入选原因": 32,
                    "风险提示": 34,
                },
            },
        ],
        left_align_column="基金名称",
        column_widths=FINAL_MAIN_COLUMN_WIDTHS,
        current_snapshot_date=effective_nav_candidate_date,
        csv_output_file=report_paths["filtered_csv"],
        csv_artifact_key="filtered_csv",
        csv_df=_select_unique_columns(export_context["official_export_full"], PROJECT_REFERENCE_CSV_COLUMNS),
    )

    project_tracker_df, project_tracker_process_df, project_tracker_summary = _build_project_final_tracker(
        settings,
        current_run_id=run_id,
        current_generated_at=current_generated_at,
        current_filtered_excel=report_paths["filtered_excel"],
        current_final_count=int(len(official_df.index)),
        current_nav_snapshot_date=effective_nav_candidate_date,
    )
    dynamic_publish_summary_df = _build_summary_sheet_rows(
        [
            ("债基存储全集数量", int(universe_snapshot.get("stored_universe_count") or 0)),
            ("债基活跃全集数量", int(universe_snapshot.get("active_universe_count") or 0)),
            ("具备最新净值数量", int(universe_snapshot.get("has_latest_nav_count") or 0)),
            ("具备1年风险数量", int(universe_snapshot.get("has_1y_risk_metrics_count") or 0)),
            ("正式可筛选宇宙数量", int(universe_snapshot.get("screenable_universe_count") or 0)),
            ("本地全量债基候选数量", int(len(merged.index))),
            ("持有期排除数量", int(len(excluded_df.index))),
            ("项目治理候选池数量", int(len(candidate_df.index))),
            ("动态阈值参考榜数量", int(len(dynamic_official_df.index))),
            ("动态阈值基准自动诊断数量", int(len(dynamic_internal_watch_df.index))),
        ]
    )
    dynamic_filtered_workbook_result = _materialize_multisheet_workbook(
        settings=settings,
        output_file=report_paths["dynamic_filtered_excel"],
        artifact_key="dynamic_filtered_excel",
        artifact_meta_key="dynamic_filtered_excel_meta",
        main_sheet_name="Sheet1",
        main_df=export_context["dynamic_official_export"],
        extra_sheets=[
            {
                "title": "推荐理由与风格",
                "df": export_context["dynamic_recommendation_detail"],
                "left_align_column": "基金名称",
                "column_widths": {
                    "基金代码": 12,
                    "基金名称": 24,
                    "项目推荐结论": 14,
                    "推荐理由": 24,
                    "债基风格标签": 14,
                    "基准归类": 12,
                    "dynamic_threshold_label": 46,
                    "dynamic_threshold_source": 16,
                    "enhanced_supplemental_label": 46,
                },
            },
            {
                "title": "发布摘要",
                "df": dynamic_publish_summary_df,
                "left_align_column": "指标",
                "column_widths": {"指标": 24, "数值": 16},
            },
            {
                "title": "正式推荐风格分布",
                "df": dynamic_official_style_df,
                "left_align_column": "风格标签",
                "column_widths": {"风格标签": 16, "数量": 10, "占正式推荐比例(%)": 18},
            },
            {
                "title": "正式推荐公司分布",
                "df": dynamic_official_company_df,
                "left_align_column": "基金公司",
                "column_widths": {"基金公司": 18, "数量": 10, "占正式推荐比例(%)": 18},
            },
            {
                "title": "正式推荐费率分布",
                "df": dynamic_fee_band_df,
                "left_align_column": "费率区间",
                "column_widths": {"费率区间": 16, "数量": 10},
            },
            {
                "title": "正式榜分组摘要",
                "df": _build_formal_group_distribution(export_context["dynamic_official_export_full"]),
                "left_align_column": FORMAL_GROUP_LABEL,
                "column_widths": {FORMAL_GROUP_LABEL: 16, "数量": 10, "占正式推荐比例(%)": 18},
            },
            {
                "title": "动态阈值明细",
                "df": dynamic_threshold_detail_df,
                "left_align_column": "债基风格标签",
                "column_widths": {
                    "债基风格标签": 16,
                    "样本数": 10,
                    "阈值来源": 12,
                    "综合加权得分下限": 16,
                    "近期得分下限": 14,
                    "长期得分下限": 14,
                    "费率上限(%)": 12,
                    "1年最大回撤上限": 16,
                    "规模下限(亿)": 12,
                },
            },
        ],
        left_align_column="基金名称",
        column_widths=FINAL_MAIN_COLUMN_WIDTHS,
        current_snapshot_date=effective_nav_candidate_date,
        csv_output_file=report_paths["dynamic_filtered_csv"],
        csv_artifact_key="dynamic_filtered_csv",
        csv_df=_select_unique_columns(export_context["dynamic_official_export_full"], PROJECT_REFERENCE_CSV_COLUMNS),
    )
    watch_workbook_result = _materialize_multisheet_workbook(
        settings=settings,
        output_file=report_paths["watch_excel"],
        artifact_key="watch_excel",
        artifact_meta_key="watch_excel_meta",
        main_sheet_name="Sheet1",
        main_df=export_context["internal_watch_export"],
        extra_sheets=[
            {
                "title": "分流统计",
                "df": export_context["internal_watch_summary_df"],
                "left_align_column": AUTO_DIAGNOSIS_CATEGORY_COLUMN,
                "column_widths": {AUTO_DIAGNOSIS_CATEGORY_COLUMN: 18, "数量": 10, "占自动诊断池比例(%)": 20},
            },
            {
                "title": "新增样本复核摘要",
                "df": export_context["incremental_review_summary_df"],
                "left_align_column": "指标",
                "column_widths": {"指标": 26, "数值": 40},
            },
        ],
        left_align_column="基金名称",
        current_snapshot_date=effective_nav_candidate_date,
        csv_output_file=report_paths["watch_csv"],
        csv_artifact_key="watch_csv",
        csv_df=export_context["internal_watch_export"],
    )
    dynamic_watch_workbook_result = _materialize_multisheet_workbook(
        settings=settings,
        output_file=report_paths["dynamic_watch_excel"],
        artifact_key="dynamic_watch_excel",
        artifact_meta_key="dynamic_watch_excel_meta",
        main_sheet_name="Sheet1",
        main_df=export_context["dynamic_internal_watch_export"],
        extra_sheets=[
            {
                "title": "分流统计",
                "df": dynamic_internal_watch_summary_df,
                "left_align_column": AUTO_DIAGNOSIS_CATEGORY_COLUMN,
                "column_widths": {AUTO_DIAGNOSIS_CATEGORY_COLUMN: 18, "数量": 10, "占自动诊断池比例(%)": 20},
            },
        ],
        left_align_column="基金名称",
        current_snapshot_date=effective_nav_candidate_date,
        csv_output_file=report_paths["dynamic_watch_csv"],
        csv_artifact_key="dynamic_watch_csv",
        csv_df=export_context["dynamic_internal_watch_export"],
    )
    return {
        "filtered_workbook_result": filtered_workbook_result,
        "project_tracker_df": project_tracker_df,
        "project_tracker_process_df": project_tracker_process_df,
        "project_tracker_summary": project_tracker_summary,
        "dynamic_filtered_workbook_result": dynamic_filtered_workbook_result,
        "watch_workbook_result": watch_workbook_result,
        "dynamic_watch_workbook_result": dynamic_watch_workbook_result,
    }


def _prepare_report_materialization_assets(
    *,
    settings: Settings,
    run_id: str,
    run_dir: Path,
    universe_snapshot: dict[str, Any],
    long_term_history_snapshot: dict[str, Any],
    publish_stage_audit_summary: dict[str, Any],
    standard_screening_audit_summary: dict[str, Any],
    merged: pd.DataFrame,
    excluded_df: pd.DataFrame,
    candidate_df: pd.DataFrame,
    official_df: pd.DataFrame,
    internal_watch_df: pd.DataFrame,
    generic_holding_placeholder_watch_df: pd.DataFrame,
    generic_holding_placeholder_watch_summary: dict[str, Any],
    fixed_baseline_codes: set[str],
    dynamic_baseline_codes: set[str],
    dynamic_reference_export_codes: set[str],
    fixed_official_audit_df: pd.DataFrame,
    dynamic_publish_official_df: pd.DataFrame,
    fixed_internal_watch_audit_df: pd.DataFrame,
    dynamic_internal_watch_df: pd.DataFrame,
    removed_after_final_selection_df: pd.DataFrame,
    overlap_codes: list[str],
    fixed_only_codes: list[str],
    dynamic_only_codes: list[str],
    stable_official_export_full: pd.DataFrame,
    enhanced_official_export_full: pd.DataFrame,
    official_export_full: pd.DataFrame,
) -> dict[str, Any]:
    long_term_governance_payload = _build_long_term_governance_markdown_payload(
        settings=settings,
        long_term_snapshot=long_term_history_snapshot,
    )
    positive_ratio_semantics = _build_positive_ratio_semantics_summary({})
    publish_reason_payload = _build_publish_reason_markdown_payload(
        {
            "project_publish_summary": {
                "publish_reason_summary": _build_publish_reason_summary(publish_stage_audit_summary),
                "standard_screening_audit": standard_screening_audit_summary,
            }
        }
    )
    comparison_df = pd.DataFrame(
        [
            {"指标": "固定阈值基准通过数", "数值": int(len(fixed_baseline_codes))},
            {"指标": "固定阈值自动发布门禁通过数", "数值": int(len(fixed_official_audit_df.index))},
            {"指标": "动态阈值基准通过数", "数值": int(len(dynamic_baseline_codes))},
            {"指标": "动态阈值导出参考榜数量", "数值": int(len(dynamic_reference_export_codes))},
            {"指标": "动态阈值自动发布门禁通过数", "数值": int(len(dynamic_publish_official_df.index))},
            {"指标": "固定阈值自动诊断数", "数值": int(len(fixed_internal_watch_audit_df.index))},
            {"指标": "动态阈值自动诊断数", "数值": int(len(dynamic_internal_watch_df.index))},
            {"指标": "双轨基准重合基金数", "数值": int(len(overlap_codes))},
            {"指标": "仅固定阈值基准入选数", "数值": int(len(fixed_only_codes))},
            {"指标": "仅动态阈值基准入选数", "数值": int(len(dynamic_only_codes))},
            {"指标": "最终精选二次收敛剔除数", "数值": int(len(removed_after_final_selection_df.index))},
        ]
    )
    publish_summary_df = _build_summary_sheet_rows(
        [
            ("债基存储全集数量", int(universe_snapshot.get("stored_universe_count") or 0)),
            ("债基活跃全集数量", int(universe_snapshot.get("active_universe_count") or 0)),
            ("具备最新净值数量", int(universe_snapshot.get("has_latest_nav_count") or 0)),
            ("具备1年风险数量", int(universe_snapshot.get("has_1y_risk_metrics_count") or 0)),
            ("正式可筛选宇宙数量", int(universe_snapshot.get("screenable_universe_count") or 0)),
            ("年度涨幅历史覆盖基金数", int(long_term_history_snapshot.get("annual_return_history_fund_count") or 0)),
            ("同类排名历史覆盖基金数", int(long_term_history_snapshot.get("rank_history_fund_count") or 0)),
            ("年度稳定性特征可用基金数", int(long_term_history_snapshot.get("annual_feature_ready_fund_count") or 0)),
            ("排名持续性特征可用基金数", int(long_term_history_snapshot.get("rank_feature_ready_fund_count") or 0)),
            ("长期特征完整可用基金数", int(long_term_history_snapshot.get("full_feature_ready_fund_count") or 0)),
            ("长期特征按中性处理基金数", int(long_term_history_snapshot.get("neutralized_fund_count") or 0)),
            ("年度历史缺口", int(long_term_governance_payload.get("annual_history_missing_total") or 0)),
            ("排名历史缺口", int(long_term_governance_payload.get("rank_history_missing_total") or 0)),
            ("等待历史成熟", int(long_term_governance_payload.get("await_history_maturity_count") or 0)),
            ("本地重试池", int(long_term_governance_payload.get("local_retry_pool_count") or 0)),
            ("上游受限观察池", int(long_term_governance_payload.get("upstream_limited_watchlist_count") or 0)),
            ("上游空载荷观察", int(long_term_governance_payload.get("upstream_limited_empty_payload_total") or 0)),
            (
                "长期增强特征是否具备充分覆盖",
                "是"
                if bool(
                    long_term_history_snapshot.get(
                        "ready_for_long_term_enhancement",
                        long_term_history_snapshot.get("ready_for_scoring"),
                    )
                )
                else "否",
            ),
            ("本地全量债基候选数量", int(len(merged.index))),
            ("持有期排除数量", int(len(excluded_df.index))),
            ("通用占位持有期排除数量", int(generic_holding_placeholder_watch_summary.get("count") or 0)),
            ("通用占位持有期中可申购数量", int(generic_holding_placeholder_watch_summary.get("buyable_count") or 0)),
            ("通用占位持有期中正式投资评分>=80数量", int(generic_holding_placeholder_watch_summary.get("high_score_count") or 0)),
            ("正式推荐中名称未识别持有期限制数量", int(official_df.get("持有期状态", pd.Series(dtype=object)).eq("名称未识别持有期限制").sum()) if not official_df.empty else 0),
            ("自动诊断池中名称未识别持有期限制数量", int(internal_watch_df.get("持有期状态", pd.Series(dtype=object)).eq("名称未识别持有期限制").sum()) if not internal_watch_df.empty else 0),
            ("正式推荐中结构化持有期未知数量", int(official_df.get("持有期状态", pd.Series(dtype=object)).eq(PROJECT_HOLDING_PERIOD_DATA_UNKNOWN_STATUS).sum()) if not official_df.empty else 0),
            ("自动诊断池中结构化持有期未知数量", int(internal_watch_df.get("持有期状态", pd.Series(dtype=object)).eq(PROJECT_HOLDING_PERIOD_DATA_UNKNOWN_STATUS).sum()) if not internal_watch_df.empty else 0),
            ("项目治理候选池数量", int(len(candidate_df.index))),
            ("项目治理候选池说明", "项目内部治理/对账口径，保留治理、观察和差异审计样本，不等同于正式发布主榜。"),
            ("主发布阈值模式", "动态阈值 + 增强补充准入 + 自动发布门禁"),
            ("固定阈值基准通过数", int(len(fixed_baseline_codes))),
            ("固定阈值自动发布门禁通过数", int(len(fixed_official_audit_df.index))),
            ("动态阈值基准通过数", int(len(dynamic_baseline_codes))),
            ("动态阈值导出参考榜数量", int(len(dynamic_reference_export_codes))),
            ("动态阈值自动发布门禁通过数", int(len(dynamic_publish_official_df.index))),
            ("双轨基准重合基金数", int(len(overlap_codes))),
            ("仅固定阈值基准入选数", int(len(fixed_only_codes))),
            ("仅动态阈值基准入选数", int(len(dynamic_only_codes))),
            ("正式发布主榜数量", int(len(official_df.index))),
            ("纯债稳健榜数量", int(len(stable_official_export_full.index))),
            ("增强债弹性榜数量", int(len(enhanced_official_export_full.index))),
            ("最终精选二次收敛剔除数", int(len(removed_after_final_selection_df.index))),
            ("最终精选二次收敛后数量", int(len(official_df.index))),
            ("正式推荐数量", int(len(official_df.index))),
            ("自动诊断池数量", int(len(internal_watch_df.index))),
            ("增强补充准入通过数量", int(official_export_full.get("项目准入方式", pd.Series(dtype=object)).eq("增强债基补充准入").sum()) if not official_export_full.empty else 0),
            ("positive_ratio项目口径", positive_ratio_semantics.get("definition") or "1年日频正收益天数占比"),
            ("positive_ratio说明", "项目内部稳定性代理指标；低波自然月 shadow 仅用于解释增强，不替换生产阈值"),
            ("发布责任链摘要", publish_reason_payload.get("responsibility_chain_headline") or "-"),
            ("基础门禁淘汰数", next((int(item.get("removed_count") or 0) for item in (publish_reason_payload.get("stage_family_summary") or []) if item.get("family_key") == "base_gate"), 0)),
            ("动态阈值淘汰数", next((int(item.get("removed_count") or 0) for item in (publish_reason_payload.get("stage_family_summary") or []) if item.get("family_key") == "dynamic_threshold"), 0)),
            ("发布后复核淘汰数", next((int(item.get("removed_count") or 0) for item in (publish_reason_payload.get("stage_family_summary") or []) if item.get("family_key") == "publish_post_review"), 0)),
            ("最终精选淘汰数", next((int(item.get("removed_count") or 0) for item in (publish_reason_payload.get("stage_family_summary") or []) if item.get("family_key") == "final_selection"), 0)),
            ("年化收益主责拦截数", next((int(item.get("intercepted_count") or 0) for item in (publish_reason_payload.get("focus_metric_summary") or []) if item.get("metric_key") == "annual_return"), 0)),
            ("综合/长短期评分主责拦截数", next((int(item.get("intercepted_count") or 0) for item in (publish_reason_payload.get("focus_metric_summary") or []) if item.get("metric_key") == "score"), 0)),
            ("夏普主责拦截数", next((int(item.get("intercepted_count") or 0) for item in (publish_reason_payload.get("focus_metric_summary") or []) if item.get("metric_key") == "sharpe"), 0)),
            ("正收益稳定性主责拦截数", next((int(item.get("intercepted_count") or 0) for item in (publish_reason_payload.get("focus_metric_summary") or []) if item.get("metric_key") == "positive_ratio"), 0)),
            ("经理从业与评分主责拦截数", next((int(item.get("intercepted_count") or 0) for item in (publish_reason_payload.get("focus_metric_summary") or []) if item.get("metric_key") == "manager_stability"), 0)),
        ]
    )
    comparison_diff_rows: list[dict[str, Any]] = []
    compare_source = candidate_df.copy().rename(columns=PROJECT_EXPORT_RENAME_MAPPING)
    compare_source = _drop_duplicate_columns(compare_source)
    for code in fixed_only_codes:
        row_df = compare_source[compare_source["基金代码"] == code]
        if row_df.empty:
            continue
        row = row_df.iloc[0]
        comparison_diff_rows.append(
            {
                "基金代码": code,
                "基金名称": row.get("基金名称"),
                "差异类型": "仅固定阈值入选",
                "债基风格标签": row.get("债基风格标签"),
                "综合加权得分": row.get("综合加权得分"),
                "近期得分": row.get("近期得分"),
                "长期得分": row.get("长期得分"),
                "费率合计(%)": row.get("费率合计(%)"),
                "1年-最大回撤(%)": row.get("1年-最大回撤(%)"),
                "固定阈值理由": row.get("fixed_threshold_reason"),
                "动态阈值理由": row.get("dynamic_threshold_reason"),
                "动态阈值说明": row.get("dynamic_threshold_label"),
            }
        )
    for code in dynamic_only_codes:
        row_df = compare_source[compare_source["基金代码"] == code]
        if row_df.empty:
            continue
        row = row_df.iloc[0]
        comparison_diff_rows.append(
            {
                "基金代码": code,
                "基金名称": row.get("基金名称"),
                "差异类型": "仅动态阈值入选",
                "债基风格标签": row.get("债基风格标签"),
                "综合加权得分": row.get("综合加权得分"),
                "近期得分": row.get("近期得分"),
                "长期得分": row.get("长期得分"),
                "费率合计(%)": row.get("费率合计(%)"),
                "1年-最大回撤(%)": row.get("1年-最大回撤(%)"),
                "固定阈值理由": row.get("fixed_threshold_reason"),
                "动态阈值理由": row.get("dynamic_threshold_reason"),
                "动态阈值说明": row.get("dynamic_threshold_label"),
            }
        )
    threshold_comparison_full_df = pd.DataFrame(comparison_diff_rows)
    threshold_comparison_preview_df = threshold_comparison_full_df.head(settings.preview_top_n).copy()

    excluded_export = excluded_df.copy().rename(
        columns={
            "fcode": "基金代码",
            "company_name": "基金公司",
            "manager_names": "基金经理",
            "holding_days": "持有期天数",
            "holding_keywords": "持有期关键词",
            "estab_date": "成立日期",
            "scale_billion": "基金规模(亿)",
        }
    )
    excluded_export = _drop_duplicate_columns(excluded_export)
    if excluded_export.empty:
        excluded_export = pd.DataFrame(columns=["基金代码", "基金名称", "持有期天数", "持有期关键词", "排除原因"])
    else:
        excluded_export = excluded_export[_ordered_columns(excluded_export, EXCLUDED_COLUMNS)].copy()
    generic_holding_placeholder_watch_export = generic_holding_placeholder_watch_df.copy()

    timestamp_token = _extract_timestamp_token(run_id)
    report_paths = {
        "filtered_excel": run_dir / f"正式推荐债券基金_{timestamp_token}.xlsx",
        "filtered_csv": run_dir / f"正式推荐债券基金_{timestamp_token}.csv",
        "dynamic_filtered_excel": run_dir / f"正式推荐债券基金_动态阈值_{timestamp_token}.xlsx",
        "dynamic_filtered_csv": run_dir / f"正式推荐债券基金_动态阈值_{timestamp_token}.csv",
        "threshold_compare_excel": run_dir / f"债券基金双轨阈值对比_{timestamp_token}.xlsx",
        "watch_excel": run_dir / f"自动诊断池_债券基金_{timestamp_token}.xlsx",
        "watch_csv": run_dir / f"自动诊断池_债券基金_{timestamp_token}.csv",
        "dynamic_watch_excel": run_dir / f"自动诊断池_债券基金_动态阈值_{timestamp_token}.xlsx",
        "dynamic_watch_csv": run_dir / f"自动诊断池_债券基金_动态阈值_{timestamp_token}.csv",
        "excluded_excel": run_dir / f"当日最终优质基金_持有期排除清单_{timestamp_token}.xlsx",
        "excluded_csv": run_dir / f"当日最终优质基金_持有期排除清单_{timestamp_token}.csv",
        "stage_b_reason_excel": run_dir / f"债券基金_高收益未入榜原因总账_{timestamp_token}.xlsx",
        "stage_b_reason_csv": run_dir / f"债券基金_高收益未入榜原因总账_{timestamp_token}.csv",
        "screening_audit_excel": run_dir / f"债券基金_标准筛选审计_{timestamp_token}.xlsx",
        "project_tracker_excel": run_dir / "最终优质基金累积跟踪.xlsx",
        "project_tracker_excel_meta": run_dir / "最终优质基金累积跟踪_report_meta.json",
        "run_local_project_tracker_excel": run_dir / "最终优质基金累积跟踪.xlsx",
        "run_local_project_tracker_excel_meta": run_dir / "最终优质基金累积跟踪_report_meta.json",
        "run_local_project_tracker_current_detail_json": run_dir / "project_tracker_current_detail.json",
        "recommendations_detail_json": run_dir / "recommendations_detail.json",
        "report_md": run_dir / f"bond_analysis_report_{run_id}.md",
        "report_json": run_dir / f"bond_analysis_report_{run_id}.json",
    }
    report_paths["filtered_excel_meta"] = run_dir / f"{report_paths['filtered_excel'].stem}_report_meta.json"
    report_paths["dynamic_filtered_excel_meta"] = run_dir / f"{report_paths['dynamic_filtered_excel'].stem}_report_meta.json"
    report_paths["threshold_compare_excel_meta"] = run_dir / f"{report_paths['threshold_compare_excel'].stem}_report_meta.json"
    report_paths["watch_excel_meta"] = run_dir / f"{report_paths['watch_excel'].stem}_report_meta.json"
    report_paths["dynamic_watch_excel_meta"] = run_dir / f"{report_paths['dynamic_watch_excel'].stem}_report_meta.json"
    report_paths["excluded_excel_meta"] = run_dir / f"{report_paths['excluded_excel'].stem}_report_meta.json"
    report_paths["stage_b_reason_excel_meta"] = run_dir / f"{report_paths['stage_b_reason_excel'].stem}_report_meta.json"
    report_paths["screening_audit_excel_meta"] = run_dir / f"{report_paths['screening_audit_excel'].stem}_report_meta.json"
    return {
        "comparison_df": comparison_df,
        "publish_summary_df": publish_summary_df,
        "threshold_comparison_full_df": threshold_comparison_full_df,
        "threshold_comparison_preview_df": threshold_comparison_preview_df,
        "excluded_export": excluded_export,
        "generic_holding_placeholder_watch_export": generic_holding_placeholder_watch_export,
        "report_paths": report_paths,
    }


def _write_project_tracker_current_detail_sidecar(
    *,
    settings: Settings,
    output_path: Path | None,
    run_id: str,
    generated_at: str,
    current_export_df: pd.DataFrame,
    source_tracker_excel: Path,
    data_source_by_code: dict[str, dict[str, str]] | None = None,
) -> Path | None:
    if output_path is None:
        return None

    output_path.parent.mkdir(parents=True, exist_ok=True)
    safe_df = _stringify_frame(current_export_df) if isinstance(current_export_df, pd.DataFrame) else pd.DataFrame()
    rows = safe_df.to_dict(orient="records") if not safe_df.empty else []
    payload = {
        "artifact_type": "project_tracker_current_detail_sidecar",
        "schema_version": 1,
        "run_id": run_id,
        "generated_at": generated_at,
        "row_count": int(len(rows)),
        "source": {
            "dataframe": "project_tracker_current_export_df",
            "project_tracker_excel": _project_relative(settings, source_tracker_excel),
        },
        "columns": [str(column) for column in safe_df.columns],
        "rows": rows,
        "derived_fields": _build_tracker_current_prepublish_derived_fields(
            settings=settings,
            run_id=run_id,
            generated_at=generated_at,
            rows=rows,
            data_source_by_code=data_source_by_code,
        ),
    }
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return output_path


def _materialize_project_tracker_assets(
    *,
    settings: Settings,
    run_id: str,
    current_generated_at: str,
    project_tracker_excel: Path,
    project_tracker_excel_meta: Path,
    run_local_project_tracker_excel: Path | None,
    run_local_project_tracker_excel_meta: Path | None,
    run_local_project_tracker_current_detail_json: Path | None,
    project_tracker_df: pd.DataFrame,
    project_tracker_process_df: pd.DataFrame,
    project_tracker_summary: dict[str, Any],
    data_source_by_code: dict[str, dict[str, str]] | None,
    finish_report_sub_stage: Any,
    stage_name: str,
) -> dict[str, Any]:
    project_tracker_current_df = (
        project_tracker_df[project_tracker_df["当前是否在榜"] == "是"].copy()
        if not project_tracker_df.empty and "当前是否在榜" in project_tracker_df.columns
        else pd.DataFrame(columns=project_tracker_df.columns if not project_tracker_df.empty else PROJECT_TRACKER_COLUMNS)
    )
    project_tracker_current_view_df = (
        _select_unique_columns(project_tracker_current_df, PROJECT_TRACKER_CURRENT_COLUMNS)
        if not project_tracker_current_df.empty
        else pd.DataFrame(columns=PROJECT_TRACKER_CURRENT_COLUMNS)
    )
    project_tracker_current_export_df = _build_project_tracker_export_view(project_tracker_current_df)

    tracker_incremental_updated = False
    tracker_sub_stage_started_at = perf_counter()
    if (
        not bool(project_tracker_summary.get("latest_run_is_new_snapshot", True))
        and not bool(project_tracker_summary.get("current_metrics_refreshed"))
    ):
        tracker_incremental_updated = _update_project_tracker_workbook_for_duplicate_snapshot(
            project_tracker_excel,
            process_df=project_tracker_process_df,
            summary=project_tracker_summary,
        )
    finish_report_sub_stage(
        stage_name,
        "project_tracker_incremental_update",
        tracker_sub_stage_started_at,
    )

    if not tracker_incremental_updated:
        tracker_sub_stage_started_at = perf_counter()
        project_tracker_export_df = _build_project_tracker_export_view(project_tracker_df)
        stable_tracker_current_view_df = (
            project_tracker_current_view_df[project_tracker_current_view_df[FORMAL_GROUP_LABEL] == STABLE_BOND_GROUP_LABEL].copy()
            if not project_tracker_current_view_df.empty and FORMAL_GROUP_LABEL in project_tracker_current_view_df.columns
            else pd.DataFrame(columns=PROJECT_TRACKER_CURRENT_COLUMNS)
        )
        stable_tracker_current_export_df = (
            project_tracker_current_export_df[project_tracker_current_export_df[FORMAL_GROUP_LABEL] == STABLE_BOND_GROUP_LABEL].copy()
            if not project_tracker_current_export_df.empty and FORMAL_GROUP_LABEL in project_tracker_current_export_df.columns
            else pd.DataFrame(columns=PROJECT_TRACKER_EXPORT_COLUMNS)
        )
        enhanced_tracker_current_view_df = (
            project_tracker_current_view_df[project_tracker_current_view_df[FORMAL_GROUP_LABEL] == ENHANCED_BOND_GROUP_LABEL].copy()
            if not project_tracker_current_view_df.empty and FORMAL_GROUP_LABEL in project_tracker_current_view_df.columns
            else pd.DataFrame(columns=PROJECT_TRACKER_CURRENT_COLUMNS)
        )
        enhanced_tracker_current_export_df = (
            project_tracker_current_export_df[project_tracker_current_export_df[FORMAL_GROUP_LABEL] == ENHANCED_BOND_GROUP_LABEL].copy()
            if not project_tracker_current_export_df.empty and FORMAL_GROUP_LABEL in project_tracker_current_export_df.columns
            else pd.DataFrame(columns=PROJECT_TRACKER_EXPORT_COLUMNS)
        )
        _write_multisheet_workbook(
            project_tracker_excel,
            main_sheet_name="完整数据",
            main_df=project_tracker_export_df,
            extra_sheets=[
                {
                    "title": "当前在榜",
                    "df": project_tracker_current_export_df,
                    "left_align_column": "基金名称",
                    "column_widths": PROJECT_TRACKER_EXPORT_COLUMN_WIDTHS,
                    "top_decile_rules": PROJECT_TRACKER_TOP_DECILE_RULES,
                },
                {
                    "title": STABLE_BOND_GROUP_LABEL,
                    "df": stable_tracker_current_export_df,
                    "left_align_column": "基金名称",
                    "column_widths": PROJECT_TRACKER_EXPORT_COLUMN_WIDTHS,
                    "top_decile_rules": PROJECT_TRACKER_TOP_DECILE_RULES,
                },
                {
                    "title": ENHANCED_BOND_GROUP_LABEL,
                    "df": enhanced_tracker_current_export_df,
                    "left_align_column": "基金名称",
                    "column_widths": PROJECT_TRACKER_EXPORT_COLUMN_WIDTHS,
                    "top_decile_rules": PROJECT_TRACKER_TOP_DECILE_RULES,
                },
                {
                    "title": "处理记录",
                    "df": project_tracker_process_df,
                    "left_align_column": "来源文件",
                    "column_widths": {
                        "业务日期": 12,
                        "项目批次": 20,
                        "生成时间": 20,
                        "正式推荐数量": 12,
                        "来源文件": 40,
                    },
                },
            ],
            left_align_column="基金名称",
            column_widths=PROJECT_TRACKER_EXPORT_COLUMN_WIDTHS,
            top_decile_rules=PROJECT_TRACKER_TOP_DECILE_RULES,
        )
        _apply_project_tracker_workbook_presentation(
            project_tracker_excel,
            summary=project_tracker_summary,
        )
        finish_report_sub_stage(
            stage_name,
            "project_tracker_full_rebuild_and_presentation",
            tracker_sub_stage_started_at,
        )

    project_tracker_meta_payload = {
        "artifact_type": "project_final_accumulation_tracker",
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "report_file": project_tracker_excel.name,
        "report_filename": project_tracker_excel.name,
        "fund_type": "bond",
        "fund_type_name": "债券型",
        "source_run_ids": project_tracker_process_df.get("项目批次", pd.Series(dtype=object)).tolist(),
        "processed_result_count": int(len(project_tracker_process_df.index)),
        "processed_source_dates": project_tracker_process_df.get("业务日期", pd.Series(dtype=object)).tolist(),
        "accumulation_summary": project_tracker_summary,
    }
    project_tracker_excel_meta.write_text(
        json.dumps(project_tracker_meta_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    if run_local_project_tracker_excel is not None and run_local_project_tracker_excel.resolve() != project_tracker_excel.resolve():
        run_local_project_tracker_excel.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(project_tracker_excel, run_local_project_tracker_excel)
    if run_local_project_tracker_excel_meta is not None and run_local_project_tracker_excel_meta.resolve() != project_tracker_excel_meta.resolve():
        run_local_project_tracker_excel_meta.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(project_tracker_excel_meta, run_local_project_tracker_excel_meta)
    run_local_project_tracker_current_detail_json = _write_project_tracker_current_detail_sidecar(
        settings=settings,
        output_path=run_local_project_tracker_current_detail_json,
        run_id=run_id,
        generated_at=current_generated_at,
        current_export_df=project_tracker_current_export_df,
        source_tracker_excel=run_local_project_tracker_excel or project_tracker_excel,
        data_source_by_code=data_source_by_code,
    )

    return {
        "project_tracker_current_view_df": project_tracker_current_view_df,
        "run_local_project_tracker_excel": run_local_project_tracker_excel,
        "run_local_project_tracker_excel_meta": run_local_project_tracker_excel_meta,
        "run_local_project_tracker_current_detail_json": run_local_project_tracker_current_detail_json,
    }


def _materialize_secondary_report_workbooks(
    *,
    settings: Settings,
    effective_nav_candidate_date: str,
    report_paths: dict[str, Path],
    excluded_export: pd.DataFrame,
    stage_b_reason_ledger_df: pd.DataFrame,
    stage_b_reason_summary: dict[str, Any],
    quality_overlap_pair_df: pd.DataFrame,
    quality_overlap_combo_df: pd.DataFrame,
    quality_overlap_sample_df: pd.DataFrame,
    high_score_friction_sample_df: pd.DataFrame,
    high_score_friction_reason_df: pd.DataFrame,
    high_score_friction_style_df: pd.DataFrame,
    high_score_scale_gate_sample_df: pd.DataFrame,
    high_score_scale_gate_style_df: pd.DataFrame,
    high_score_scale_gate_band_df: pd.DataFrame,
    high_score_scale_gate_age_df: pd.DataFrame,
    high_score_scale_gate_near_df: pd.DataFrame,
    high_score_scale_gate_near_review_df: pd.DataFrame,
    high_score_scale_gate_near_review_sample_df: pd.DataFrame,
    high_score_fee_gate_sample_df: pd.DataFrame,
    high_score_fee_gate_style_df: pd.DataFrame,
    style_pass_rate_df: pd.DataFrame,
    style_veto_reason_df: pd.DataFrame,
    reason_group_audit_df: pd.DataFrame,
    quality_factor_group_audit_df: pd.DataFrame,
    quality_factor_detail_audit_df: pd.DataFrame,
    status_split_audit_df: pd.DataFrame,
    high_score_rejected_df: pd.DataFrame,
    high_score_not_buyable_df: pd.DataFrame,
    risk_metric_cap_audit: dict[str, Any],
    comparison_df: pd.DataFrame,
    threshold_comparison_full_df: pd.DataFrame,
    dynamic_threshold_detail_df: pd.DataFrame,
    finish_report_sub_stage: Any,
    stage_name: str,
) -> dict[str, Any]:
    excluded_workbook_result = _materialize_multisheet_workbook(
        settings=settings,
        output_file=report_paths["excluded_excel"],
        artifact_key="excluded_excel",
        artifact_meta_key="excluded_excel_meta",
        main_sheet_name="Sheet1",
        main_df=excluded_export,
        extra_sheets=[],
        left_align_column="基金名称",
        highlight_fill_color="FFF2CC",
        column_widths=EXCLUDED_COLUMN_WIDTHS,
        current_snapshot_date=effective_nav_candidate_date,
        csv_output_file=report_paths["excluded_csv"],
        csv_artifact_key="excluded_csv",
        csv_df=excluded_export,
    )
    stage_b_threshold_summary_df = _build_summary_sheet_rows(
        [
            ("未入榜总数", int(stage_b_reason_summary.get("excluded_total") or 0)),
            ("年化>=5%未入榜数", _stage_b_threshold_count(stage_b_reason_summary, 5.0)),
            ("年化>=8%未入榜数", _stage_b_threshold_count(stage_b_reason_summary, 8.0)),
            ("年化>=10%未入榜数", _stage_b_threshold_count(stage_b_reason_summary, 10.0)),
        ]
    )
    stage_b_top_reason_df = pd.DataFrame(stage_b_reason_summary.get("top_primary_reasons") or [])
    stage_b_reason_group_df = _build_reason_group_audit(stage_b_reason_ledger_df)
    stage_b_quality_factor_group_df = _build_quality_factor_group_audit(stage_b_reason_ledger_df)
    stage_b_quality_factor_detail_df = _build_quality_factor_detail_audit(stage_b_reason_ledger_df)
    stage_b_status_split_df = _build_status_split_audit(stage_b_reason_ledger_df)

    workbook_sub_stage_started_at = perf_counter()
    stage_b_reason_workbook_result = _materialize_multisheet_workbook(
        settings=settings,
        output_file=report_paths["stage_b_reason_excel"],
        artifact_key="stage_b_reason_excel",
        artifact_meta_key="stage_b_reason_excel_meta",
        main_sheet_name="未入榜原因总账",
        main_df=stage_b_reason_ledger_df,
        extra_sheets=[
            {
                "title": "高收益阈值摘要",
                "df": stage_b_threshold_summary_df,
                "left_align_column": "指标",
                "column_widths": {"指标": 18, "数值": 14},
            },
            {
                "title": "主原因分布",
                "df": stage_b_top_reason_df,
                "left_align_column": "reason",
                "column_widths": {"reason": 22, "count": 10},
            },
            {
                "title": "原因分组",
                "df": stage_b_reason_group_df,
                "left_align_column": "主原因分组",
                "column_widths": {"主原因分组": 16, "数量": 10, "占未入榜样本比例(%)": 20},
            },
            {
                "title": "质量因子组",
                "df": stage_b_quality_factor_group_df,
                "left_align_column": "质量因子组",
                "column_widths": {"质量因子组": 18, "数量": 10, "占质量拦截样本比例(%)": 22},
            },
            {
                "title": "质量细分因子",
                "df": stage_b_quality_factor_detail_df,
                "left_align_column": "质量细分因子",
                "column_widths": {"原因代码": 20, "质量细分因子": 26, "所属因子组": 16, "命中样本数": 12, "占质量拦截样本比例(%)": 22},
            },
            {
                "title": "质量重叠Pair",
                "df": quality_overlap_pair_df,
                "left_align_column": "因子A",
                "column_widths": {"因子A": 26, "因子B": 26, "共同命中样本数": 12, "占质量拦截样本比例(%)": 22},
            },
            {
                "title": "质量重叠Combo",
                "df": quality_overlap_combo_df,
                "left_align_column": "重叠组合",
                "column_widths": {"重叠组合": 96, "因子数": 10, "样本数": 10, "占质量拦截样本比例(%)": 22},
            },
            {
                "title": "质量重叠样本",
                "df": quality_overlap_sample_df,
                "left_align_column": "基金名称",
                "column_widths": {"基金代码": 12, "基金名称": 24, "债基风格标签": 14, "基金公司": 18, "综合加权得分": 12, "近期得分": 12, "长期得分": 12, "主排除原因": 18, "原始原因代码": 72, "重叠组合": 72},
            },
            {
                "title": "高分摩擦主因",
                "df": high_score_friction_reason_df,
                "left_align_column": "主排除原因",
                "column_widths": {"主排除原因": 22, "样本数": 10, "占高分摩擦样本比例(%)": 22},
            },
            {
                "title": "高分摩擦风格",
                "df": high_score_friction_style_df,
                "left_align_column": "债基风格标签",
                "column_widths": {"债基风格标签": 16, "样本数": 10, "占高分摩擦样本比例(%)": 22},
            },
            {
                "title": "高分摩擦样本",
                "df": high_score_friction_sample_df,
                "left_align_column": "基金名称",
                "column_widths": {"基金代码": 12, "基金名称": 24, "债基风格标签": 14, "基金公司": 18, "综合加权得分": 12, "近期得分": 12, "长期得分": 12, "主排除原因": 18, "次排除原因": 32, "原始原因代码": 72},
            },
            {
                "title": "高分规模门槛风格",
                "df": high_score_scale_gate_style_df,
                "left_align_column": "债基风格标签",
                "column_widths": {"债基风格标签": 16, "样本数": 10, "占专题样本比例(%)": 18},
            },
            {
                "title": "高分规模分层",
                "df": high_score_scale_gate_band_df,
                "left_align_column": "规模分层",
                "column_widths": {"规模分层": 14, "样本数": 10, "占规模门槛专题比例(%)": 22},
            },
            {
                "title": "高分规模年龄",
                "df": high_score_scale_gate_age_df,
                "left_align_column": "成立年限分层",
                "column_widths": {"成立年限分层": 14, "样本数": 10, "占规模门槛专题比例(%)": 22},
            },
            {
                "title": "高分规模门槛样本",
                "df": high_score_scale_gate_sample_df,
                "left_align_column": "基金名称",
                "column_widths": {"基金代码": 12, "基金名称": 24, "债基风格标签": 14, "基金公司": 18, "成立日期": 12, "基金规模(亿)": 12, "费率合计(%)": 12, "规模数据状态": 12, "规模数据账龄(天)": 14, "母基金名称": 20, "关联份额数": 10, "关联份额名称": 28, "关联份额代码": 22, "综合加权得分": 12, "近期得分": 12, "长期得分": 12, "主排除原因": 18, "次排除原因": 32, "原始原因代码": 72},
            },
            {
                "title": "高分近门槛样本",
                "df": high_score_scale_gate_near_df,
                "left_align_column": "基金名称",
                "column_widths": {"基金代码": 12, "基金名称": 24, "债基风格标签": 14, "基金公司": 18, "成立日期": 12, "基金规模(亿)": 12, "距2亿门槛缺口(亿)": 16, "费率合计(%)": 12, "综合加权得分": 12, "近期得分": 12, "长期得分": 12, "主排除原因": 18, "次排除原因": 32, "原始原因代码": 72},
            },
            {
                "title": "近门槛复审结论",
                "df": high_score_scale_gate_near_review_df,
                "left_align_column": "复审建议",
                "column_widths": {"复审建议": 18, "样本数": 10, "占近门槛样本比例(%)": 22},
            },
            {
                "title": "近门槛复审样本",
                "df": high_score_scale_gate_near_review_sample_df,
                "left_align_column": "基金名称",
                "column_widths": {"基金代码": 12, "基金名称": 24, "债基风格标签": 14, "基金公司": 18, "成立日期": 12, "基金规模(亿)": 12, "距2亿门槛缺口(亿)": 16, "费率合计(%)": 12, "综合加权得分": 12, "近期得分": 12, "长期得分": 12, "主排除原因": 18, "次排除原因": 32, "原始原因代码": 72, "复审建议": 18, "复审依据": 48},
            },
            {
                "title": "高分费率门槛风格",
                "df": high_score_fee_gate_style_df,
                "left_align_column": "债基风格标签",
                "column_widths": {"债基风格标签": 16, "样本数": 10, "占专题样本比例(%)": 18},
            },
            {
                "title": "高分费率门槛样本",
                "df": high_score_fee_gate_sample_df,
                "left_align_column": "基金名称",
                "column_widths": {"基金代码": 12, "基金名称": 24, "债基风格标签": 14, "基金公司": 18, "成立日期": 12, "基金规模(亿)": 12, "费率合计(%)": 12, "规模数据状态": 12, "规模数据账龄(天)": 14, "母基金名称": 20, "关联份额数": 10, "关联份额名称": 28, "关联份额代码": 22, "综合加权得分": 12, "近期得分": 12, "长期得分": 12, "主排除原因": 18, "次排除原因": 32, "原始原因代码": 72},
            },
            {
                "title": "状态拆分",
                "df": stage_b_status_split_df,
                "left_align_column": "状态维度",
                "column_widths": {"状态维度": 14, "状态值": 16, "数量": 10, "占未入榜样本比例(%)": 20},
            },
        ],
        left_align_column="基金名称",
        column_widths={
            "基金代码": 12,
            "基金名称": 24,
            "基金类型": 16,
            "债基风格标签": 14,
            "基金公司": 18,
            "项目推荐结论": 12,
            "未入榜阶段": 16,
            "主排除原因": 18,
            "次排除原因": 30,
            "1年-年化收益(%)": 14,
            "综合加权得分": 14,
            "近期得分": 12,
            "长期得分": 12,
            "是否可申购": 10,
            "AutoStock原始校验状态": 16,
            "推荐理由": 36,
        },
        current_snapshot_date=effective_nav_candidate_date,
        csv_output_file=report_paths["stage_b_reason_csv"],
        csv_artifact_key="stage_b_reason_csv",
        csv_df=stage_b_reason_ledger_df,
    )
    finish_report_sub_stage(
        stage_name,
        "write_stage_b_reason_workbook",
        workbook_sub_stage_started_at,
    )

    screening_audit_summary_df = _build_summary_sheet_rows(
        [
            ("高分被拒阈值", HIGH_SCORE_AUDIT_THRESHOLD),
            ("高分被拒样本数", int(len(high_score_rejected_df.index))),
            ("高分不可买样本数", int(len(high_score_not_buyable_df.index))),
            ("高分摩擦样本数", int(len(high_score_friction_sample_df.index))),
            ("高分规模门槛样本数", int(len(high_score_scale_gate_sample_df.index))),
            ("高分近门槛样本数", int(len(high_score_scale_gate_near_df.index))),
            ("候选惩罚项复审样本数", int(high_score_scale_gate_near_review_sample_df.get("复审建议", pd.Series(dtype=object)).eq("候选惩罚项复审").sum())),
            ("高分费率门槛样本数", int(len(high_score_fee_gate_sample_df.index))),
            ("Sharpe cap-hit 数", int(risk_metric_cap_audit.get("sharpe_cap_hit_count") or 0)),
            ("Sortino cap-hit 数", int(risk_metric_cap_audit.get("sortino_cap_hit_count") or 0)),
            ("任一 cap-hit 数", int(risk_metric_cap_audit.get("either_cap_hit_count") or 0)),
            ("任一 cap-hit 比例(%)", float(risk_metric_cap_audit.get("either_cap_hit_ratio") or 0.0)),
            (f"Top{risk_metric_cap_audit.get('top_n', settings.preview_top_n)} 重排变化数", int(risk_metric_cap_audit.get("top_n_changed_count") or 0)),
        ]
    )
    workbook_sub_stage_started_at = perf_counter()
    screening_audit_workbook_result = _materialize_multisheet_workbook(
        settings=settings,
        output_file=report_paths["screening_audit_excel"],
        artifact_key="screening_audit_excel",
        artifact_meta_key="screening_audit_excel_meta",
        main_sheet_name="风格通过率",
        main_df=style_pass_rate_df,
        extra_sheets=[
            {
                "title": "风格 veto Top5",
                "df": style_veto_reason_df,
                "left_align_column": "债基风格标签",
                "column_widths": {"债基风格标签": 16, "主排除原因": 22, "数量": 10, "占该风格未入榜比例(%)": 18},
            },
            {
                "title": "原因分组",
                "df": reason_group_audit_df,
                "left_align_column": "主原因分组",
                "column_widths": {"主原因分组": 16, "数量": 10, "占未入榜样本比例(%)": 20},
            },
            {
                "title": "质量因子组",
                "df": quality_factor_group_audit_df,
                "left_align_column": "质量因子组",
                "column_widths": {"质量因子组": 18, "数量": 10, "占质量拦截样本比例(%)": 22},
            },
            {
                "title": "质量细分因子",
                "df": quality_factor_detail_audit_df,
                "left_align_column": "质量细分因子",
                "column_widths": {"原因代码": 20, "质量细分因子": 26, "所属因子组": 16, "命中样本数": 12, "占质量拦截样本比例(%)": 22},
            },
            {
                "title": "质量重叠Pair",
                "df": quality_overlap_pair_df,
                "left_align_column": "因子A",
                "column_widths": {"因子A": 26, "因子B": 26, "共同命中样本数": 12, "占质量拦截样本比例(%)": 22},
            },
            {
                "title": "质量重叠Combo",
                "df": quality_overlap_combo_df,
                "left_align_column": "重叠组合",
                "column_widths": {"重叠组合": 96, "因子数": 10, "样本数": 10, "占质量拦截样本比例(%)": 22},
            },
            {
                "title": "质量重叠样本",
                "df": quality_overlap_sample_df,
                "left_align_column": "基金名称",
                "column_widths": {"基金代码": 12, "基金名称": 24, "债基风格标签": 14, "基金公司": 18, "综合加权得分": 12, "近期得分": 12, "长期得分": 12, "主排除原因": 18, "原始原因代码": 72, "重叠组合": 72},
            },
            {
                "title": "状态拆分",
                "df": status_split_audit_df,
                "left_align_column": "状态维度",
                "column_widths": {"状态维度": 14, "状态值": 16, "数量": 10, "占未入榜样本比例(%)": 20},
            },
            {
                "title": "高分被拒",
                "df": high_score_rejected_df,
                "left_align_column": "基金名称",
            },
            {
                "title": "高分不可买",
                "df": high_score_not_buyable_df,
                "left_align_column": "基金名称",
            },
            {
                "title": "高分摩擦主因",
                "df": high_score_friction_reason_df,
                "left_align_column": "主排除原因",
                "column_widths": {"主排除原因": 22, "样本数": 10, "占高分摩擦样本比例(%)": 22},
            },
            {
                "title": "高分摩擦风格",
                "df": high_score_friction_style_df,
                "left_align_column": "债基风格标签",
                "column_widths": {"债基风格标签": 16, "样本数": 10, "占高分摩擦样本比例(%)": 22},
            },
            {
                "title": "高分摩擦样本",
                "df": high_score_friction_sample_df,
                "left_align_column": "基金名称",
                "column_widths": {"基金代码": 12, "基金名称": 24, "债基风格标签": 14, "基金公司": 18, "综合加权得分": 12, "近期得分": 12, "长期得分": 12, "主排除原因": 18, "次排除原因": 32, "原始原因代码": 72},
            },
            {
                "title": "高分规模门槛风格",
                "df": high_score_scale_gate_style_df,
                "left_align_column": "债基风格标签",
                "column_widths": {"债基风格标签": 16, "样本数": 10, "占专题样本比例(%)": 18},
            },
            {
                "title": "高分规模分层",
                "df": high_score_scale_gate_band_df,
                "left_align_column": "规模分层",
                "column_widths": {"规模分层": 14, "样本数": 10, "占规模门槛专题比例(%)": 22},
            },
            {
                "title": "高分规模年龄",
                "df": high_score_scale_gate_age_df,
                "left_align_column": "成立年限分层",
                "column_widths": {"成立年限分层": 14, "样本数": 10, "占规模门槛专题比例(%)": 22},
            },
            {
                "title": "高分规模门槛样本",
                "df": high_score_scale_gate_sample_df,
                "left_align_column": "基金名称",
                "column_widths": {"基金代码": 12, "基金名称": 24, "债基风格标签": 14, "基金公司": 18, "成立日期": 12, "基金规模(亿)": 12, "费率合计(%)": 12, "规模数据状态": 12, "规模数据账龄(天)": 14, "母基金名称": 20, "关联份额数": 10, "关联份额名称": 28, "关联份额代码": 22, "综合加权得分": 12, "近期得分": 12, "长期得分": 12, "主排除原因": 18, "次排除原因": 32, "原始原因代码": 72},
            },
            {
                "title": "高分近门槛样本",
                "df": high_score_scale_gate_near_df,
                "left_align_column": "基金名称",
                "column_widths": {"基金代码": 12, "基金名称": 24, "债基风格标签": 14, "基金公司": 18, "成立日期": 12, "基金规模(亿)": 12, "距2亿门槛缺口(亿)": 16, "费率合计(%)": 12, "综合加权得分": 12, "近期得分": 12, "长期得分": 12, "主排除原因": 18, "次排除原因": 32, "原始原因代码": 72},
            },
            {
                "title": "近门槛复审结论",
                "df": high_score_scale_gate_near_review_df,
                "left_align_column": "复审建议",
                "column_widths": {"复审建议": 18, "样本数": 10, "占近门槛样本比例(%)": 22},
            },
            {
                "title": "近门槛复审样本",
                "df": high_score_scale_gate_near_review_sample_df,
                "left_align_column": "基金名称",
                "column_widths": {"基金代码": 12, "基金名称": 24, "债基风格标签": 14, "基金公司": 18, "成立日期": 12, "基金规模(亿)": 12, "距2亿门槛缺口(亿)": 16, "费率合计(%)": 12, "综合加权得分": 12, "近期得分": 12, "长期得分": 12, "主排除原因": 18, "次排除原因": 32, "原始原因代码": 72, "复审建议": 18, "复审依据": 48},
            },
            {
                "title": "高分费率门槛风格",
                "df": high_score_fee_gate_style_df,
                "left_align_column": "债基风格标签",
                "column_widths": {"债基风格标签": 16, "样本数": 10, "占专题样本比例(%)": 18},
            },
            {
                "title": "高分费率门槛样本",
                "df": high_score_fee_gate_sample_df,
                "left_align_column": "基金名称",
                "column_widths": {"基金代码": 12, "基金名称": 24, "债基风格标签": 14, "基金公司": 18, "成立日期": 12, "基金规模(亿)": 12, "费率合计(%)": 12, "规模数据状态": 12, "规模数据账龄(天)": 14, "母基金名称": 20, "关联份额数": 10, "关联份额名称": 28, "关联份额代码": 22, "综合加权得分": 12, "近期得分": 12, "长期得分": 12, "主排除原因": 18, "次排除原因": 32, "原始原因代码": 72},
            },
            {
                "title": "风险指标 cap-hit",
                "df": pd.DataFrame(risk_metric_cap_audit.get("style_hit_rows") or []),
                "left_align_column": "债基风格标签",
            },
            {
                "title": "审计摘要",
                "df": screening_audit_summary_df,
                "left_align_column": "指标",
                "column_widths": {"指标": 24, "数值": 16},
            },
        ],
        left_align_column="债基风格标签",
        column_widths={
            "债基风格标签": 16,
            "治理候选数": 12,
            "动态基准通过数": 14,
            "正式发布数": 12,
            "动态基准通过率(%)": 18,
            "正式发布率(%)": 16,
        },
        current_snapshot_date=effective_nav_candidate_date,
    )
    finish_report_sub_stage(
        stage_name,
        "write_screening_audit_workbook",
        workbook_sub_stage_started_at,
    )

    workbook_sub_stage_started_at = perf_counter()
    threshold_compare_workbook_result = _materialize_multisheet_workbook(
        settings=settings,
        output_file=report_paths["threshold_compare_excel"],
        artifact_key="threshold_compare_excel",
        artifact_meta_key="threshold_compare_excel_meta",
        main_sheet_name="对比摘要",
        main_df=comparison_df,
        extra_sheets=[
            {
                "title": "差异样本预览",
                "df": threshold_comparison_full_df,
                "left_align_column": "基金名称",
                "column_widths": {
                    "基金代码": 12,
                    "基金名称": 24,
                    "差异类型": 16,
                    "债基风格标签": 14,
                    "费率合计(%)": 12,
                    "固定阈值理由": 22,
                    "动态阈值理由": 22,
                    "动态阈值说明": 42,
                },
            },
            {
                "title": "动态阈值明细",
                "df": dynamic_threshold_detail_df,
                "left_align_column": "债基风格标签",
            },
        ],
        left_align_column="指标",
        column_widths={"指标": 22, "数值": 16},
        current_snapshot_date=effective_nav_candidate_date,
    )
    finish_report_sub_stage(
        stage_name,
        "write_threshold_compare_workbook",
        workbook_sub_stage_started_at,
    )

    return {
        "excluded_workbook_result": excluded_workbook_result,
        "stage_b_reason_workbook_result": stage_b_reason_workbook_result,
        "screening_audit_workbook_result": screening_audit_workbook_result,
        "threshold_compare_workbook_result": threshold_compare_workbook_result,
    }


def _write_report_artifact_meta_files(
    *,
    settings: Settings,
    run_id: str,
    artifacts: dict[str, Any],
    report_paths: dict[str, Path],
    workbook_results: dict[str, dict[str, Any]],
    universe_snapshot: dict[str, Any],
    merged: pd.DataFrame,
    excluded_df: pd.DataFrame,
    candidate_df: pd.DataFrame,
    official_df: pd.DataFrame,
    dynamic_official_df: pd.DataFrame,
    fixed_baseline_codes: set[str],
    fixed_official_audit_df: pd.DataFrame,
    dynamic_publish_official_df: pd.DataFrame,
    overlap_codes: list[str],
    fixed_only_codes: list[str],
    dynamic_only_codes: list[str],
    removed_after_final_selection_df: pd.DataFrame,
    internal_watch_df: pd.DataFrame,
    dynamic_internal_watch_df: pd.DataFrame,
    generic_holding_placeholder_watch_summary: dict[str, Any],
    stage_b_reason_summary: dict[str, Any],
    style_pass_rate_df: pd.DataFrame,
    high_score_rejected_df: pd.DataFrame,
    high_score_not_buyable_df: pd.DataFrame,
    high_score_friction_sample_df: pd.DataFrame,
    high_score_scale_gate_sample_df: pd.DataFrame,
    high_score_scale_gate_near_df: pd.DataFrame,
    high_score_scale_gate_near_review_sample_df: pd.DataFrame,
    high_score_fee_gate_sample_df: pd.DataFrame,
    risk_metric_cap_audit: dict[str, Any],
) -> None:
    _write_excel_meta(
        settings=settings,
        meta_path=report_paths["filtered_excel_meta"],
        output_file=report_paths["filtered_excel"],
        report_type="filtered_final_main",
        report_label="正式推荐债券基金",
        run_id=run_id,
        source_files=[artifacts.get("final_excel")],
        counts={
            "stored_universe_count": int(universe_snapshot.get("stored_universe_count") or 0),
            "active_universe_count": int(universe_snapshot.get("active_universe_count") or 0),
            "has_latest_nav_count": int(universe_snapshot.get("has_latest_nav_count") or 0),
            "has_1y_risk_metrics_count": int(universe_snapshot.get("has_1y_risk_metrics_count") or 0),
            "screenable_universe_count": int(universe_snapshot.get("screenable_universe_count") or 0),
            "project_universe_count": int(len(merged.index)),
            "excluded_count": int(len(excluded_df.index)),
            "candidate_count_after_holding_gate": int(len(candidate_df.index)),
            "official_recommend_count": int(len(official_df.index)),
        },
        content_signature=workbook_results["filtered_workbook_result"].get("content_signature"),
        reused_from=workbook_results["filtered_workbook_result"].get("reused_from"),
    )
    _write_excel_meta(
        settings=settings,
        meta_path=report_paths["dynamic_filtered_excel_meta"],
        output_file=report_paths["dynamic_filtered_excel"],
        report_type="dynamic_filtered_final_main",
        report_label="债券基金_动态阈值基准榜",
        run_id=run_id,
        source_files=[artifacts.get("final_excel")],
        counts={
            "stored_universe_count": int(universe_snapshot.get("stored_universe_count") or 0),
            "active_universe_count": int(universe_snapshot.get("active_universe_count") or 0),
            "has_latest_nav_count": int(universe_snapshot.get("has_latest_nav_count") or 0),
            "has_1y_risk_metrics_count": int(universe_snapshot.get("has_1y_risk_metrics_count") or 0),
            "screenable_universe_count": int(universe_snapshot.get("screenable_universe_count") or 0),
            "project_universe_count": int(len(merged.index)),
            "excluded_count": int(len(excluded_df.index)),
            "candidate_count_after_holding_gate": int(len(candidate_df.index)),
            "official_recommend_count": int(len(dynamic_official_df.index)),
        },
        content_signature=workbook_results["dynamic_filtered_workbook_result"].get("content_signature"),
        reused_from=workbook_results["dynamic_filtered_workbook_result"].get("reused_from"),
    )
    _write_excel_meta(
        settings=settings,
        meta_path=report_paths["threshold_compare_excel_meta"],
        output_file=report_paths["threshold_compare_excel"],
        report_type="threshold_comparison",
        report_label="债券基金双轨阈值对比",
        run_id=run_id,
        source_files=[artifacts.get("final_excel")],
        counts={
            "fixed_baseline_count": int(len(fixed_baseline_codes)),
            "fixed_post_review_count": int(len(fixed_official_audit_df.index)),
            "dynamic_official_count": int(len(dynamic_official_df.index)),
            "dynamic_baseline_count": int(len(dynamic_official_df.index)),
            "dynamic_post_review_count": int(len(dynamic_publish_official_df.index)),
            "overlap_count": int(len(overlap_codes)),
            "fixed_only_count": int(len(fixed_only_codes)),
            "dynamic_only_count": int(len(dynamic_only_codes)),
            "final_selection_removed_count": int(len(removed_after_final_selection_df.index)),
        },
        content_signature=workbook_results["threshold_compare_workbook_result"].get("content_signature"),
        reused_from=workbook_results["threshold_compare_workbook_result"].get("reused_from"),
    )
    _write_excel_meta(
        settings=settings,
        meta_path=report_paths["watch_excel_meta"],
        output_file=report_paths["watch_excel"],
        report_type="internal_watchlist",
        report_label="自动诊断池债券基金",
        run_id=run_id,
        source_files=[artifacts.get("final_excel")],
        counts={
            "internal_watch_count": int(len(internal_watch_df.index)),
            "not_buyable_watch_count": int(sum(1 for item in internal_watch_df.get(AUTO_DIAGNOSIS_CATEGORY_COLUMN, []) if item == NOT_BUYABLE_DIAGNOSIS)),
            "enhanced_watch_count": int(sum(1 for item in internal_watch_df.get(AUTO_DIAGNOSIS_CATEGORY_COLUMN, []) if item == "增强风格诊断")),
            "quality_warning_watch_count": int(sum(1 for item in internal_watch_df.get(AUTO_DIAGNOSIS_CATEGORY_COLUMN, []) if item == QUALITY_WARNING_DIAGNOSIS)),
            "absolute_quality_watch_count": int(sum(1 for item in internal_watch_df.get(AUTO_DIAGNOSIS_CATEGORY_COLUMN, []) if item == ABSOLUTE_QUALITY_DIAGNOSIS)),
            "manager_stability_watch_count": int(sum(1 for item in internal_watch_df.get(AUTO_DIAGNOSIS_CATEGORY_COLUMN, []) if item == MANAGER_STABILITY_DIAGNOSIS)),
            "publish_post_review_watch_count": int(sum(1 for item in internal_watch_df.get(AUTO_DIAGNOSIS_CATEGORY_COLUMN, []) if item == AUTO_PUBLISH_GATE_DIAGNOSIS)),
        },
        content_signature=workbook_results["watch_workbook_result"].get("content_signature"),
        reused_from=workbook_results["watch_workbook_result"].get("reused_from"),
    )
    _write_excel_meta(
        settings=settings,
        meta_path=report_paths["dynamic_watch_excel_meta"],
        output_file=report_paths["dynamic_watch_excel"],
        report_type="internal_watchlist_dynamic",
        report_label="自动诊断池债券基金_动态阈值",
        run_id=run_id,
        source_files=[artifacts.get("final_excel")],
        counts={
            "internal_watch_count": int(len(dynamic_internal_watch_df.index)),
            "not_buyable_watch_count": int(sum(1 for item in dynamic_internal_watch_df.get(AUTO_DIAGNOSIS_CATEGORY_COLUMN, []) if item == NOT_BUYABLE_DIAGNOSIS)),
            "enhanced_watch_count": int(sum(1 for item in dynamic_internal_watch_df.get(AUTO_DIAGNOSIS_CATEGORY_COLUMN, []) if item == "增强风格诊断")),
            "quality_warning_watch_count": int(sum(1 for item in dynamic_internal_watch_df.get(AUTO_DIAGNOSIS_CATEGORY_COLUMN, []) if item == QUALITY_WARNING_DIAGNOSIS)),
            "dynamic_quality_watch_count": int(sum(1 for item in dynamic_internal_watch_df.get(AUTO_DIAGNOSIS_CATEGORY_COLUMN, []) if item == DYNAMIC_QUALITY_DIAGNOSIS)),
        },
        content_signature=workbook_results["dynamic_watch_workbook_result"].get("content_signature"),
        reused_from=workbook_results["dynamic_watch_workbook_result"].get("reused_from"),
    )
    _write_excel_meta(
        settings=settings,
        meta_path=report_paths["excluded_excel_meta"],
        output_file=report_paths["excluded_excel"],
        report_type="holding_period_excluded_list",
        report_label="当日最终优质基金持有期排除清单",
        run_id=run_id,
        source_files=[artifacts.get("final_excel")],
        counts={
            "excluded_count": int(len(excluded_df.index)),
            "generic_holding_placeholder_excluded_count": int(generic_holding_placeholder_watch_summary.get("count") or 0),
            "generic_holding_placeholder_buyable_count": int(generic_holding_placeholder_watch_summary.get("buyable_count") or 0),
            "generic_holding_placeholder_high_score_count": int(generic_holding_placeholder_watch_summary.get("high_score_count") or 0),
            "holding_period_threshold_days": int(settings.exclude_holding_days_gt),
        },
        content_signature=workbook_results["excluded_workbook_result"].get("content_signature"),
        reused_from=workbook_results["excluded_workbook_result"].get("reused_from"),
    )
    _write_excel_meta(
        settings=settings,
        meta_path=report_paths["stage_b_reason_excel_meta"],
        output_file=report_paths["stage_b_reason_excel"],
        report_type="high_yield_exclusion_ledger",
        report_label="债券基金高收益未入榜原因总账",
        run_id=run_id,
        source_files=[artifacts.get("final_excel"), report_paths["excluded_excel"]],
        counts={
            "excluded_total": int(stage_b_reason_summary.get("excluded_total") or 0),
            "annual_ge_5_excluded_count": _stage_b_threshold_count(stage_b_reason_summary, 5.0),
            "annual_ge_8_excluded_count": _stage_b_threshold_count(stage_b_reason_summary, 8.0),
            "annual_ge_10_excluded_count": _stage_b_threshold_count(stage_b_reason_summary, 10.0),
        },
        content_signature=workbook_results["stage_b_reason_workbook_result"].get("content_signature"),
        reused_from=workbook_results["stage_b_reason_workbook_result"].get("reused_from"),
    )
    _write_excel_meta(
        settings=settings,
        meta_path=report_paths["screening_audit_excel_meta"],
        output_file=report_paths["screening_audit_excel"],
        report_type="standard_screening_audit",
        report_label="债券基金标准筛选审计",
        run_id=run_id,
        source_files=[report_paths["stage_b_reason_excel"], report_paths["threshold_compare_excel"], report_paths["filtered_excel"]],
        counts={
            "style_count": int(len(style_pass_rate_df.index)),
            "high_score_rejected_count": int(len(high_score_rejected_df.index)),
            "high_score_not_buyable_count": int(len(high_score_not_buyable_df.index)),
            "high_score_friction_count": int(len(high_score_friction_sample_df.index)),
            "high_score_scale_gate_count": int(len(high_score_scale_gate_sample_df.index)),
            "high_score_scale_gate_near_count": int(len(high_score_scale_gate_near_df.index)),
            "high_score_scale_gate_candidate_penalty_count": int(high_score_scale_gate_near_review_sample_df.get("复审建议", pd.Series(dtype=object)).eq("候选惩罚项复审").sum()),
            "high_score_fee_gate_count": int(len(high_score_fee_gate_sample_df.index)),
            "cap_hit_count": int(risk_metric_cap_audit.get("either_cap_hit_count") or 0),
        },
        content_signature=workbook_results["screening_audit_workbook_result"].get("content_signature"),
        reused_from=workbook_results["screening_audit_workbook_result"].get("reused_from"),
    )


def _build_project_tracker_current_preview(
    current_view_df: pd.DataFrame,
    *,
    preview_top_n: int,
) -> pd.DataFrame:
    if current_view_df.empty:
        return current_view_df.copy()

    preview_limit = max(1, int(preview_top_n or 1))
    if FORMAL_GROUP_LABEL not in current_view_df.columns:
        return current_view_df.head(preview_limit).copy()

    frames = [
        current_view_df[current_view_df[FORMAL_GROUP_LABEL] == group_label].head(preview_limit).copy()
        for group_label in (STABLE_BOND_GROUP_LABEL, ENHANCED_BOND_GROUP_LABEL)
    ]
    non_empty_frames = [frame for frame in frames if not frame.empty]
    if not non_empty_frames:
        return current_view_df.head(preview_limit).copy()
    return pd.concat(non_empty_frames, ignore_index=True)


def _prepare_report_preview_and_summary_context(
    *,
    settings: Settings,
    official_export_full: pd.DataFrame,
    internal_watch_export: pd.DataFrame,
    excluded_export: pd.DataFrame,
    generic_holding_placeholder_watch_export: pd.DataFrame,
    stage_b_reason_ledger_df: pd.DataFrame,
    project_tracker_df: pd.DataFrame,
    project_tracker_current_view_df: pd.DataFrame,
    forward_validation_package: dict[str, Any],
    stable_official_export_full: pd.DataFrame,
    enhanced_official_export_full: pd.DataFrame,
    official_style_df: pd.DataFrame,
    formal_group_distribution_df: pd.DataFrame,
    stable_official_style_df: pd.DataFrame,
    enhanced_official_style_df: pd.DataFrame,
    internal_watch_summary_df: pd.DataFrame,
    official_company_df: pd.DataFrame,
    fee_band_df: pd.DataFrame,
    dynamic_official_style_df: pd.DataFrame,
    dynamic_internal_watch_summary_df: pd.DataFrame,
    dynamic_official_company_df: pd.DataFrame,
    dynamic_fee_band_df: pd.DataFrame,
    final_screening_summary: dict[str, Any],
    effective_nav_snapshot: dict[str, Any],
    universe_snapshot: dict[str, Any],
    long_term_history_snapshot: dict[str, Any],
    candidate_df: pd.DataFrame,
) -> dict[str, Any]:
    final_preview = _select_unique_columns(
        official_export_full,
        [
            "基金代码",
            "基金名称",
            FORMAL_GROUP_LABEL,
            "基金类型",
            "基金公司",
            "基金规模(亿)",
            "正式投资评分",
            "持有期状态",
            "债基风格标签",
            "基准归类",
            "是否可申购",
            "项目质量信心",
            "推荐备注",
        ],
    ).head(settings.preview_top_n).copy()
    internal_watch_preview = internal_watch_export.head(settings.preview_top_n).copy()
    excluded_preview = (
        _select_unique_columns(excluded_export, EXCLUDED_STATE_PREVIEW_COLUMNS)
        if not excluded_export.empty
        else pd.DataFrame(columns=EXCLUDED_STATE_PREVIEW_COLUMNS)
    ).head(settings.preview_top_n).copy()
    generic_holding_placeholder_preview = (
        _select_unique_columns(
            generic_holding_placeholder_watch_export,
            [
                "基金代码",
                "基金名称",
                "持有期状态",
                "holding_period_source",
                "债基风格标签",
                "是否可申购",
                FORMAL_SCORE_LABEL,
                "项目质量信心",
                "推荐备注",
            ],
        )
        if not generic_holding_placeholder_watch_export.empty
        else pd.DataFrame(
            columns=[
                "基金代码",
                "基金名称",
                "持有期状态",
                "holding_period_source",
                "债基风格标签",
                "是否可申购",
                FORMAL_SCORE_LABEL,
                "项目质量信心",
                "推荐备注",
            ]
        )
    ).head(settings.preview_top_n).copy()
    stage_b_reason_preview = stage_b_reason_ledger_df.head(settings.preview_top_n).copy()
    project_tracker_preview = (
        _select_unique_columns(project_tracker_df, PROJECT_TRACKER_STATE_PREVIEW_COLUMNS)
        if not project_tracker_df.empty
        else pd.DataFrame(columns=PROJECT_TRACKER_STATE_PREVIEW_COLUMNS)
    ).head(settings.preview_top_n).copy()
    project_tracker_current_preview = _build_project_tracker_current_preview(
        project_tracker_current_view_df,
        preview_top_n=settings.preview_top_n,
    )
    forward_validation_summary = forward_validation_package.get("summary") or {}
    forward_validation_artifacts = forward_validation_package.get("artifacts") or {}
    forward_validation_preview = pd.DataFrame(forward_validation_summary.get("comparison_preview") or [])
    stable_final_preview = _select_unique_columns(
        stable_official_export_full,
        [
            "基金代码",
            "基金名称",
            FORMAL_GROUP_LABEL,
            "基金类型",
            "基金公司",
            "基金规模(亿)",
            "正式投资评分",
            "持有期状态",
            "债基风格标签",
            "基准归类",
            "是否可申购",
            "项目质量信心",
            "推荐备注",
        ],
    ).head(settings.preview_top_n).copy()
    enhanced_final_preview = _select_unique_columns(
        enhanced_official_export_full,
        [
            "基金代码",
            "基金名称",
            FORMAL_GROUP_LABEL,
            "基金类型",
            "基金公司",
            "基金规模(亿)",
            "正式投资评分",
            "持有期状态",
            "债基风格标签",
            "基准归类",
            "是否可申购",
            "项目质量信心",
            "推荐备注",
        ],
    ).head(settings.preview_top_n).copy()

    return {
        "final_preview": final_preview,
        "internal_watch_preview": internal_watch_preview,
        "excluded_preview": excluded_preview,
        "generic_holding_placeholder_preview": generic_holding_placeholder_preview,
        "stage_b_reason_preview": stage_b_reason_preview,
        "project_tracker_preview": project_tracker_preview,
        "project_tracker_current_preview": project_tracker_current_preview,
        "forward_validation_summary": forward_validation_summary,
        "forward_validation_artifacts": forward_validation_artifacts,
        "forward_validation_preview": forward_validation_preview,
        "stable_final_preview": stable_final_preview,
        "enhanced_final_preview": enhanced_final_preview,
        "official_style_distribution": official_style_df.fillna("").to_dict(orient="records"),
        "formal_group_distribution": formal_group_distribution_df.fillna("").to_dict(orient="records"),
        "stable_official_style_distribution": stable_official_style_df.fillna("").to_dict(orient="records"),
        "enhanced_official_style_distribution": enhanced_official_style_df.fillna("").to_dict(orient="records"),
        "internal_watch_distribution": internal_watch_summary_df.fillna("").to_dict(orient="records"),
        "official_company_distribution": official_company_df.fillna("").to_dict(orient="records"),
        "fee_band_distribution": fee_band_df.fillna("").to_dict(orient="records"),
        "dynamic_official_style_distribution": dynamic_official_style_df.fillna("").to_dict(orient="records"),
        "dynamic_internal_watch_distribution": dynamic_internal_watch_summary_df.fillna("").to_dict(orient="records"),
        "dynamic_official_company_distribution": dynamic_official_company_df.fillna("").to_dict(orient="records"),
        "dynamic_fee_band_distribution": dynamic_fee_band_df.fillna("").to_dict(orient="records"),
        "quality_gate_summary": _build_quality_gate_summary(final_screening_summary),
        "risk_freshness": _build_risk_freshness(effective_nav_snapshot),
        "coverage_layers": _build_coverage_layers(
            universe_snapshot=universe_snapshot,
            long_term_history_snapshot=long_term_history_snapshot,
            stage_b_candidate_pool_count=int(len(candidate_df.index)),
        ),
    }


def _build_report_summary_payload(
    *,
    settings: Settings,
    run_id: str,
    current_generated_at: str,
    artifacts: dict[str, Any],
    report_paths: dict[str, Path],
    report_input_context: dict[str, Any],
    report_snapshot_context: dict[str, Any],
    report_candidate_universe_context: dict[str, Any],
    report_filters_and_audits_context: dict[str, Any],
    materialized_export_context: dict[str, Any],
    primary_workbook_context: dict[str, Any],
    preview_summary_context: dict[str, Any],
    threshold_comparison_preview_df: pd.DataFrame,
    publication_control: dict[str, Any] | None = None,
) -> dict[str, Any]:
    long_df = report_input_context["long_df"]
    short_df = report_input_context["short_df"]
    long_term_feature_summary = report_input_context["long_term_feature_summary"]
    final_meta = report_input_context["final_meta"]
    accumulation_preview = report_input_context["accumulation_preview"]

    universe_snapshot = report_snapshot_context["universe_snapshot"]
    long_term_history_snapshot = report_snapshot_context["long_term_history_snapshot"]
    final_screening_summary = report_snapshot_context["final_screening_summary"]
    accumulation_summary = report_snapshot_context["accumulation_summary"]
    core_base_dir = report_snapshot_context["core_base_dir"]

    risk_metric_cap_audit = report_candidate_universe_context["risk_metric_cap_audit"]
    # The audit is produced after this report's bytes and revision are fixed.
    # API readers bind its separate artifact to that exact report; a previous
    # revision's audit must not enter a new report while it is being built.
    annual_return_gate_shadow_summary: dict[str, Any] = {}

    merged = report_filters_and_audits_context["merged"]
    excluded_df = report_filters_and_audits_context["excluded_df"]
    generic_holding_placeholder_watch_summary = report_filters_and_audits_context["generic_holding_placeholder_watch_summary"]
    candidate_df = report_filters_and_audits_context["candidate_df"]
    fixed_official_audit_df = report_filters_and_audits_context["fixed_official_audit_df"]
    dynamic_official_df = report_filters_and_audits_context["dynamic_official_df"]
    autostock_status_context = report_filters_and_audits_context["autostock_status_context"]
    effective_nav_snapshot = report_filters_and_audits_context["effective_nav_snapshot"]
    removed_after_final_selection_df = report_filters_and_audits_context["removed_after_final_selection_df"]
    dynamic_internal_watch_df = report_filters_and_audits_context["dynamic_internal_watch_df"]
    low_vol_positive_ratio_shadow_watch_summary = report_filters_and_audits_context["low_vol_positive_ratio_shadow_watch_summary"]
    final_selection_contraction_df = report_filters_and_audits_context["final_selection_contraction_df"]
    fixed_baseline_codes = report_filters_and_audits_context["fixed_baseline_codes"]
    dynamic_baseline_codes = report_filters_and_audits_context["dynamic_baseline_codes"]
    dynamic_reference_export_codes = report_filters_and_audits_context["dynamic_reference_export_codes"]
    fixed_only_codes = report_filters_and_audits_context["fixed_only_codes"]
    dynamic_only_codes = report_filters_and_audits_context["dynamic_only_codes"]
    overlap_codes = report_filters_and_audits_context["overlap_codes"]
    high_score_rejected_df = report_filters_and_audits_context["high_score_rejected_df"]
    high_score_not_buyable_df = report_filters_and_audits_context["high_score_not_buyable_df"]
    high_score_friction_sample_df = report_filters_and_audits_context["high_score_friction_sample_df"]
    high_score_scale_gate_sample_df = report_filters_and_audits_context["high_score_scale_gate_sample_df"]
    high_score_scale_gate_near_df = report_filters_and_audits_context["high_score_scale_gate_near_df"]
    high_score_scale_gate_near_review_sample_df = report_filters_and_audits_context["high_score_scale_gate_near_review_sample_df"]
    high_score_fee_gate_sample_df = report_filters_and_audits_context["high_score_fee_gate_sample_df"]
    standard_screening_audit_summary = report_filters_and_audits_context["standard_screening_audit_summary"]

    dynamic_publish_official_df = materialized_export_context["dynamic_publish_official_df"]
    official_df = materialized_export_context["official_df"]
    internal_watch_df = materialized_export_context["internal_watch_df"]
    stage_b_reason_summary = materialized_export_context["stage_b_reason_summary"]
    stable_official_export_full = materialized_export_context["stable_official_export_full"]
    enhanced_official_export_full = materialized_export_context["enhanced_official_export_full"]
    publish_stage_audit_summary = materialized_export_context["publish_stage_audit_summary"]
    portfolio_summary = materialized_export_context["portfolio_summary"]

    project_tracker_summary = primary_workbook_context["project_tracker_summary"]

    final_preview = preview_summary_context["final_preview"]
    stable_final_preview = preview_summary_context["stable_final_preview"]
    enhanced_final_preview = preview_summary_context["enhanced_final_preview"]
    internal_watch_preview = preview_summary_context["internal_watch_preview"]
    excluded_preview = preview_summary_context["excluded_preview"]
    generic_holding_placeholder_preview = preview_summary_context["generic_holding_placeholder_preview"]
    stage_b_reason_preview = preview_summary_context["stage_b_reason_preview"]
    project_tracker_preview = preview_summary_context["project_tracker_preview"]
    project_tracker_current_preview = preview_summary_context["project_tracker_current_preview"]
    forward_validation_summary = preview_summary_context["forward_validation_summary"]
    forward_validation_artifacts = preview_summary_context["forward_validation_artifacts"]
    forward_validation_preview = preview_summary_context["forward_validation_preview"]
    official_style_distribution = preview_summary_context["official_style_distribution"]
    formal_group_distribution = preview_summary_context["formal_group_distribution"]
    stable_official_style_distribution = preview_summary_context["stable_official_style_distribution"]
    enhanced_official_style_distribution = preview_summary_context["enhanced_official_style_distribution"]
    internal_watch_distribution = preview_summary_context["internal_watch_distribution"]
    official_company_distribution = preview_summary_context["official_company_distribution"]
    fee_band_distribution = preview_summary_context["fee_band_distribution"]
    dynamic_official_style_distribution = preview_summary_context["dynamic_official_style_distribution"]
    dynamic_internal_watch_distribution = preview_summary_context["dynamic_internal_watch_distribution"]
    dynamic_official_company_distribution = preview_summary_context["dynamic_official_company_distribution"]
    dynamic_fee_band_distribution = preview_summary_context["dynamic_fee_band_distribution"]
    quality_gate_summary = preview_summary_context["quality_gate_summary"]
    risk_freshness = preview_summary_context["risk_freshness"]
    coverage_layers = preview_summary_context["coverage_layers"]

    counts = _build_report_counts_summary(
        long_df=long_df,
        short_df=short_df,
        universe_snapshot=universe_snapshot,
        long_term_history_snapshot=long_term_history_snapshot,
        merged=merged,
        excluded_df=excluded_df,
        generic_holding_placeholder_watch_summary=generic_holding_placeholder_watch_summary,
        low_vol_positive_ratio_shadow_watch_summary=low_vol_positive_ratio_shadow_watch_summary,
        candidate_df=candidate_df,
        official_df=official_df,
        stable_official_export_full=stable_official_export_full,
        enhanced_official_export_full=enhanced_official_export_full,
        internal_watch_df=internal_watch_df,
        stage_b_reason_summary=stage_b_reason_summary,
        high_score_rejected_df=high_score_rejected_df,
        high_score_not_buyable_df=high_score_not_buyable_df,
        high_score_friction_sample_df=high_score_friction_sample_df,
        high_score_scale_gate_sample_df=high_score_scale_gate_sample_df,
        high_score_scale_gate_near_df=high_score_scale_gate_near_df,
        high_score_scale_gate_near_review_sample_df=high_score_scale_gate_near_review_sample_df,
        high_score_fee_gate_sample_df=high_score_fee_gate_sample_df,
        dynamic_official_df=dynamic_official_df,
        dynamic_internal_watch_df=dynamic_internal_watch_df,
        overlap_codes=overlap_codes,
        fixed_only_codes=fixed_only_codes,
        dynamic_only_codes=dynamic_only_codes,
        final_screening_summary=final_screening_summary,
        project_tracker_summary=project_tracker_summary,
        forward_validation_summary=forward_validation_summary,
    )
    artifacts_summary = _build_report_artifacts_summary(
        settings=settings,
        artifacts=artifacts,
        report_paths=report_paths,
        forward_validation_artifacts=forward_validation_artifacts,
    )
    final_selection_contraction_summary = _build_final_selection_contraction_summary(
        final_selection_contraction_df,
        preview_top_n=settings.preview_top_n,
    )
    project_publish_summary = _build_project_publish_summary(
        settings=settings,
        current_generated_at=current_generated_at,
        universe_snapshot=universe_snapshot,
        candidate_df=candidate_df,
        fixed_official_audit_df=fixed_official_audit_df,
        dynamic_publish_official_df=dynamic_publish_official_df,
        removed_after_final_selection_df=removed_after_final_selection_df,
        dynamic_baseline_codes=dynamic_baseline_codes,
        dynamic_reference_export_codes=dynamic_reference_export_codes,
        fixed_baseline_codes=fixed_baseline_codes,
        fixed_only_codes=fixed_only_codes,
        dynamic_only_codes=dynamic_only_codes,
        overlap_codes=overlap_codes,
        formal_group_distribution=formal_group_distribution,
        stable_official_style_distribution=stable_official_style_distribution,
        enhanced_official_style_distribution=enhanced_official_style_distribution,
        official_style_distribution=official_style_distribution,
        official_company_distribution=official_company_distribution,
        fee_band_distribution=fee_band_distribution,
        internal_watch_distribution=internal_watch_distribution,
        portfolio_summary=portfolio_summary,
        generic_holding_placeholder_watch_summary=generic_holding_placeholder_watch_summary,
        low_vol_positive_ratio_shadow_watch_summary=low_vol_positive_ratio_shadow_watch_summary,
        project_tracker_summary=project_tracker_summary,
        forward_validation_summary=forward_validation_summary,
        stage_b_reason_summary=stage_b_reason_summary,
        standard_screening_audit_summary=standard_screening_audit_summary,
        publish_stage_audit_summary=publish_stage_audit_summary,
        final_selection_contraction_summary=final_selection_contraction_summary,
        dynamic_threshold_detail_df=report_filters_and_audits_context["dynamic_threshold_detail_df"],
        dynamic_official_style_distribution=dynamic_official_style_distribution,
        dynamic_official_company_distribution=dynamic_official_company_distribution,
        dynamic_fee_band_distribution=dynamic_fee_band_distribution,
        dynamic_internal_watch_distribution=dynamic_internal_watch_distribution,
        annual_return_gate_shadow_summary=annual_return_gate_shadow_summary,
        risk_metric_cap_audit=risk_metric_cap_audit,
        threshold_comparison_preview_df=threshold_comparison_preview_df,
        data_source_audit_summary=_build_data_source_audit_summary(
            official_df=official_df,
            internal_watch_df=internal_watch_df,
        ),
    )
    internal_audit = _build_internal_audit_summary(
        settings=settings,
        run_id=run_id,
        artifacts=artifacts,
        publication_control=publication_control,
        core_base_dir=core_base_dir,
        final_meta=final_meta,
        final_screening_summary=final_screening_summary,
        accumulation_summary=accumulation_summary,
        quality_gate_summary=quality_gate_summary,
    )
    preview_summary = _build_report_preview_summary(
        final_preview=final_preview,
        stable_final_preview=stable_final_preview,
        enhanced_final_preview=enhanced_final_preview,
        internal_watch_preview=internal_watch_preview,
        excluded_preview=excluded_preview,
        generic_holding_placeholder_preview=generic_holding_placeholder_preview,
        stage_b_reason_preview=stage_b_reason_preview,
        accumulation_preview=accumulation_preview,
        project_tracker_preview=project_tracker_preview,
        project_tracker_current_preview=project_tracker_current_preview,
        forward_validation_preview=forward_validation_preview,
    )
    audit_files = _build_report_audit_files_summary(
        settings=settings,
        artifacts=artifacts,
        report_paths=report_paths,
    )
    optimizations = _build_report_optimizations_summary()

    summary = {
        "run_id": run_id,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "data_sources": {
            "tiantian": settings.tiantian_base_url,
            "autostock": settings.autostock_base_url,
            "publication_validation_provider": getattr(settings, "publication_validation_provider", "autostock"),
            "validation_independent_source": getattr(settings, "publication_validation_provider", "autostock") == "autostock",
            "validation_description": ("同供应商跨接口核验，不代表独立供应商核验"
                                       if getattr(settings, "publication_validation_provider", "autostock") == "eastmoney_cross_endpoint"
                                       else "AutoStock交叉核验"),
            "database": str(settings.database_path),
        },
        "autostock_validation_audit": {
            "supplement_evidence_required": getattr(settings, "publication_validation_provider", "autostock") == "autostock",
            "supplement_snapshot": autostock_status_context.get("autostock_supplement_snapshot") or {},
            "unavailable": autostock_status_context.get("autostock_validation_unavailable") or {},
            "budget": autostock_status_context.get("autostock_validation_budget") or {},
            "source_evidence": autostock_status_context.get("autostock_validation_source_evidence") or [],
            "latest_source_evidence": autostock_status_context.get("autostock_validation_latest_source_evidence") or {},
            "rate_limit_max_calls_per_hour": int(settings.autostock_rate_limit_max_calls_per_hour),
            "rate_limit_min_interval_seconds": float(settings.autostock_rate_limit_min_interval_seconds),
            "batch_fund_default": int(settings.autostock_batch_fund_default),
        },
        "data_validation_audit": {
            "required": getattr(settings, "publication_validation_provider", "autostock") == "eastmoney_cross_endpoint",
            "provider": getattr(settings, "publication_validation_provider", "autostock"),
            "independent_source": False,
            "description": "同供应商跨接口核验，不代表独立供应商核验",
            "snapshot": autostock_status_context.get("data_validation_snapshot") or {},
            "unavailable": autostock_status_context.get("data_validation_unavailable") or {},
            "source_evidence": autostock_status_context.get("data_validation_source_evidence") or [],
        },
        "database_freshness": {
            "effective_nav_snapshot": effective_nav_snapshot,
            "risk_1y_freshness": risk_freshness,
            "risk_1y_aligned_with_effective_nav": bool(risk_freshness.get("aligned")),
            "long_term_history_snapshot": long_term_history_snapshot,
            "long_term_feature_summary": long_term_feature_summary,
        },
        "bond_universe_snapshot": universe_snapshot,
        "coverage_layers": coverage_layers,
        "counts": counts,
        "artifacts": artifacts_summary,
        **preview_summary,
        "project_publish_summary": project_publish_summary,
        "audit_files": audit_files,
        "internal_audit": internal_audit,
        "optimizations": optimizations,
    }
    if publication_control:
        summary["publication_control"] = dict(publication_control)
    return summary


def build_lightweight_report_payload(
    *,
    settings: Settings,
    run_id: str,
    artifacts: dict[str, Any],
    base_payload: dict[str, Any],
) -> dict[str, Any]:
    """基于最新 canonical report 做轻量状态刷新，避免重建整套 workbook。

    适用场景：
    - `reuse_existing_outputs=True`
    - 仅需把数据库 freshness / 长期治理 / 调度观测回写到最新 report json
    - 不值得为每日维护后的轻量刷新重新物化整套 Excel / tracker / forward validation
    """
    current = dict(base_payload or {})
    if not current:
        raise FileNotFoundError("缺少可复用的 canonical report payload，无法执行轻量报告刷新。")
    reused_from_run_id = str(current.get("run_id") or "").strip() or None

    run_dir = settings.runs_dir / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    base_artifacts = _flatten_runtime_artifacts((current.get("artifacts") or {}))
    runtime_artifacts = _flatten_runtime_artifacts(artifacts)
    if runtime_artifacts:
        base_artifacts.update(runtime_artifacts)
    localized_artifacts = _materialize_run_local_core_artifacts(
        settings=settings,
        run_dir=run_dir,
        artifacts=base_artifacts,
    )
    localized_artifacts.update(
        _build_run_local_risk_audit_artifacts(
            settings=settings,
            run_id=run_id,
            run_dir=run_dir,
        )
    )
    localized_artifacts = _normalize_lightweight_run_local_artifacts(
        settings=settings,
        run_id=run_id,
        run_dir=run_dir,
        localized_artifacts=localized_artifacts,
    )

    current["run_id"] = run_id
    current["generated_at"] = datetime.now().isoformat(timespec="seconds")
    current["publication_control"] = {
        "mode": "validation_only",
        "reason": "reuse_existing_outputs_lightweight_refresh",
        "eligible_for_formal_publication": False,
    }

    data_sources = dict(current.get("data_sources") or {})
    data_sources["database"] = str(settings.database_path)
    current["data_sources"] = data_sources

    effective_nav_snapshot = load_effective_nav_snapshot(
        settings.database_path,
        coverage_ratio=settings.effective_nav_coverage_ratio,
        weekday_only=settings.effective_nav_weekday_only,
    ).to_dict()

    counts = dict(current.get("counts") or {})
    bond_universe_snapshot = dict(current.get("bond_universe_snapshot") or {})
    if bond_universe_snapshot:
        metadata_df = _read_metadata(settings.database_path)
        returns_df = _read_return_snapshot(settings.database_path)
        risk_df = _read_risk_snapshot(settings.database_path)
        if not metadata_df.empty:
            refreshed_bond_universe_snapshot = _build_bond_universe_snapshot(
                settings=settings,
                metadata_df=metadata_df,
                returns_df=returns_df,
                risk_df=risk_df,
            )
            bond_universe_snapshot = dict(refreshed_bond_universe_snapshot or {})
    if bond_universe_snapshot:
        current["bond_universe_snapshot"] = bond_universe_snapshot
        for field in (
            "stored_universe_count",
            "active_universe_count",
            "has_latest_nav_count",
            "has_1y_risk_metrics_count",
            "screenable_universe_count",
        ):
            if field in bond_universe_snapshot:
                counts[field] = int(bond_universe_snapshot.get(field) or 0)

    metadata_for_history = _read_metadata(settings.database_path)
    returns_df_for_history = _read_return_snapshot(settings.database_path)
    risk_df_for_history = _read_risk_snapshot(settings.database_path)
    screenable_metadata_df = metadata_for_history.copy()
    if not screenable_metadata_df.empty and "fcode" in screenable_metadata_df.columns:
        code_sets = _build_bond_universe_code_sets(
            settings=settings,
            metadata_df=screenable_metadata_df,
            returns_df=returns_df_for_history,
            risk_df=risk_df_for_history,
        )
        screenable_metadata_df["fcode"] = screenable_metadata_df.get(
            "fcode", pd.Series(dtype=object)
        ).map(_normalize_code)
        screenable_metadata_df = screenable_metadata_df[
            screenable_metadata_df["fcode"].isin(code_sets.get("screenable_codes") or set())
        ].copy()
    long_term_history_snapshot = _build_long_term_history_snapshot(
        settings.database_path,
        screenable_metadata_df,
    )
    long_term_feature_summary = dict(
        ((current.get("database_freshness") or {}).get("long_term_feature_summary") or {})
    )
    if not long_term_feature_summary:
        _feature_df, long_term_feature_summary = _build_long_term_feature_frame(settings.database_path)

    database_freshness = dict(current.get("database_freshness") or {})
    database_freshness["effective_nav_snapshot"] = effective_nav_snapshot
    database_freshness["risk_1y_freshness"] = _build_risk_freshness(effective_nav_snapshot)
    database_freshness["risk_1y_aligned_with_effective_nav"] = bool(
        (database_freshness.get("risk_1y_freshness") or {}).get("aligned")
    )
    database_freshness["long_term_history_snapshot"] = long_term_history_snapshot
    database_freshness["long_term_feature_summary"] = long_term_feature_summary
    current["database_freshness"] = database_freshness

    counts["annual_return_history_fund_count"] = int(
        long_term_history_snapshot.get("annual_return_history_fund_count") or 0
    )
    counts["rank_history_fund_count"] = int(
        long_term_history_snapshot.get("rank_history_fund_count") or 0
    )
    counts["annual_feature_ready_fund_count"] = int(
        long_term_history_snapshot.get("annual_feature_ready_fund_count") or 0
    )
    counts["rank_feature_ready_fund_count"] = int(
        long_term_history_snapshot.get("rank_feature_ready_fund_count") or 0
    )
    counts["full_feature_ready_fund_count"] = int(
        long_term_history_snapshot.get("full_feature_ready_fund_count") or 0
    )
    counts["neutralized_long_term_feature_fund_count"] = int(
        long_term_history_snapshot.get("neutralized_fund_count") or 0
    )
    current["counts"] = counts
    current["coverage_layers"] = _build_coverage_layers(
        universe_snapshot=bond_universe_snapshot,
        long_term_history_snapshot=long_term_history_snapshot,
        stage_b_candidate_pool_count=int(
            counts.get("stage_b_candidate_pool_count")
            or counts.get("publishable_pool_count")
            or 0
        ),
    )

    data_source_by_code = _build_data_source_lookup(metadata_for_history)
    current["final_preview"] = _apply_data_source_to_records(
        current.get("final_preview") or [],
        data_source_by_code=data_source_by_code,
    )
    current["stable_final_preview"] = _apply_data_source_to_records(
        current.get("stable_final_preview") or [],
        data_source_by_code=data_source_by_code,
    )
    current["enhanced_final_preview"] = _apply_data_source_to_records(
        current.get("enhanced_final_preview") or [],
        data_source_by_code=data_source_by_code,
    )
    current["excluded_preview"] = _apply_data_source_to_records(
        current.get("excluded_preview") or [],
        data_source_by_code=data_source_by_code,
    )
    current["internal_watch_preview"] = _apply_data_source_to_records(
        current.get("internal_watch_preview") or [],
        data_source_by_code=data_source_by_code,
    )

    official_audit_df = _load_run_local_frame_for_data_source_audit(
        settings=settings,
        artifact_path=localized_artifacts.get("filtered_csv") or current.get("artifacts", {}).get("filtered_csv"),
        data_source_by_code=data_source_by_code,
    )
    internal_watch_audit_df = _load_run_local_frame_for_data_source_audit(
        settings=settings,
        artifact_path=localized_artifacts.get("watch_csv") or current.get("artifacts", {}).get("watch_csv"),
        data_source_by_code=data_source_by_code,
    )

    publish_summary = dict(current.get("project_publish_summary") or {})
    publish_summary["project_tracker_summary"] = _annotate_lightweight_project_tracker_summary(
        publish_summary.get("project_tracker_summary") or {},
        run_id=run_id,
    )
    publish_summary["portfolio_suggestions"] = _apply_data_source_to_portfolio_summary(
        publish_summary.get("portfolio_suggestions") or {},
        data_source_by_code=data_source_by_code,
    )
    publish_summary["bond_universe_snapshot"] = bond_universe_snapshot
    publish_summary["data_source_audit_summary"] = _build_data_source_audit_summary(
        official_df=official_audit_df,
        internal_watch_df=internal_watch_audit_df,
    )
    publish_summary = _refresh_lightweight_final_selection_contraction(
        settings=settings,
        run_id=run_id,
        generated_at=current["generated_at"],
        current_nav_snapshot_date=str(effective_nav_snapshot.get("candidate_date") or "").strip(),
        localized_artifacts=localized_artifacts,
        publish_summary=publish_summary,
        preview_top_n=int(getattr(settings, "preview_top_n", 15) or 15),
    )
    publish_summary["report_timing_summary"] = {
        "available": True,
        "generated_at": current["generated_at"],
        "total_seconds": 0.0,
        "stages": {"lightweight_refresh": 0.0},
        "mode": "lightweight_refresh",
    }
    current["project_publish_summary"] = publish_summary

    counts = dict(current.get("counts") or {})
    current["final_count"] = int(counts.get("final_count") or 0)
    current["dynamic_final_count"] = int(counts.get("dynamic_final_count") or 0)
    current["project_tracker_current_active_count"] = int(
        counts.get("project_tracker_current_active_count") or 0
    )
    current["stable_bond_final_count"] = int(counts.get("stable_bond_final_count") or 0)
    current["enhanced_bond_final_count"] = int(counts.get("enhanced_bond_final_count") or 0)

    from backend.core_execution_audit import build_core_execution_audit

    # Current execution cannot be inherited from the reused report, even when
    # its data and workbooks are intentionally reused by this lightweight run.
    internal_audit = dict(current.get("internal_audit") or {})
    internal_audit["core_flow_audit"] = build_core_execution_audit(
        settings=settings, run_id=run_id, artifacts=runtime_artifacts,
        report_mode="lightweight_refresh", publication_control=current["publication_control"],
        reused_from_run_id=reused_from_run_id,
    )
    current["internal_audit"] = internal_audit

    preview_top_n = int(getattr(settings, "preview_top_n", 15) or 15)
    final_preview_df = pd.DataFrame(current.get("final_preview") or []).head(preview_top_n).copy()
    excluded_preview_df = pd.DataFrame(current.get("excluded_preview") or []).head(preview_top_n).copy()
    internal_watch_preview_df = pd.DataFrame(current.get("internal_watch_preview") or []).head(preview_top_n).copy()
    markdown = build_markdown_report(
        settings,
        current,
        final_preview_df,
        excluded_preview_df,
        internal_watch_preview_df,
    )

    report_md = run_dir / f"bond_analysis_report_{run_id}.md"
    report_json = run_dir / f"bond_analysis_report_{run_id}.json"
    report_md.write_text(markdown, encoding="utf-8")

    artifacts_summary = dict(current.get("artifacts") or {})
    if localized_artifacts:
        artifacts_summary["core"] = localized_artifacts
        for key, value in localized_artifacts.items():
            if key == "core_output_dir":
                continue
            artifacts_summary[key] = value
    artifacts_summary["report_markdown"] = _project_relative(settings, report_md)
    artifacts_summary["report_json"] = _project_relative(settings, report_json)
    artifacts_summary["recommendations_detail_json"] = _project_relative(
        settings,
        run_dir / "recommendations_detail.json",
    )
    current["artifacts"] = artifacts_summary

    audit_files = dict(current.get("audit_files") or {})
    for key in (
        "risk_lag_audit_json",
        "risk_sync_audit_json",
        "risk_coverage_gap_audit_json",
    ):
        if localized_artifacts.get(key):
            audit_files[key] = localized_artifacts[key]
    if audit_files:
        current["audit_files"] = audit_files

    internal_audit = dict(current.get("internal_audit") or {})
    artifact_consistency = dict(internal_audit.get("artifact_consistency") or {})
    if artifact_consistency:
        artifact_consistency = _normalize_lightweight_artifact_consistency(
            artifact_consistency,
            run_id=run_id,
            tracker_summary=publish_summary.get("project_tracker_summary") or {},
        )
        internal_audit["artifact_consistency"] = artifact_consistency
        current["internal_audit"] = internal_audit

    report_json.write_text(json.dumps(current, ensure_ascii=False, indent=2), encoding="utf-8")
    return current


def _build_report_counts_summary(
    *,
    long_df: pd.DataFrame,
    short_df: pd.DataFrame,
    universe_snapshot: dict[str, Any],
    long_term_history_snapshot: dict[str, Any],
    merged: pd.DataFrame,
    excluded_df: pd.DataFrame,
    generic_holding_placeholder_watch_summary: dict[str, Any],
    low_vol_positive_ratio_shadow_watch_summary: dict[str, Any],
    candidate_df: pd.DataFrame,
    official_df: pd.DataFrame,
    stable_official_export_full: pd.DataFrame,
    enhanced_official_export_full: pd.DataFrame,
    internal_watch_df: pd.DataFrame,
    stage_b_reason_summary: dict[str, Any],
    high_score_rejected_df: pd.DataFrame,
    high_score_not_buyable_df: pd.DataFrame,
    high_score_friction_sample_df: pd.DataFrame,
    high_score_scale_gate_sample_df: pd.DataFrame,
    high_score_scale_gate_near_df: pd.DataFrame,
    high_score_scale_gate_near_review_sample_df: pd.DataFrame,
    high_score_fee_gate_sample_df: pd.DataFrame,
    dynamic_official_df: pd.DataFrame,
    dynamic_internal_watch_df: pd.DataFrame,
    overlap_codes: set[str] | list[str],
    fixed_only_codes: set[str] | list[str],
    dynamic_only_codes: set[str] | list[str],
    final_screening_summary: dict[str, Any],
    project_tracker_summary: dict[str, Any],
    forward_validation_summary: dict[str, Any],
) -> dict[str, Any]:
    return {
        "long_candidates": int(len(long_df.index)),
        "short_candidates": int(len(short_df.index)),
        "stored_universe_count": int(universe_snapshot.get("stored_universe_count") or 0),
        "active_universe_count": int(universe_snapshot.get("active_universe_count") or 0),
        "has_latest_nav_count": int(universe_snapshot.get("has_latest_nav_count") or 0),
        "has_1y_risk_metrics_count": int(universe_snapshot.get("has_1y_risk_metrics_count") or 0),
        "screenable_universe_count": int(universe_snapshot.get("screenable_universe_count") or 0),
        "annual_return_history_fund_count": int(long_term_history_snapshot.get("annual_return_history_fund_count") or 0),
        "rank_history_fund_count": int(long_term_history_snapshot.get("rank_history_fund_count") or 0),
        "annual_feature_ready_fund_count": int(long_term_history_snapshot.get("annual_feature_ready_fund_count") or 0),
        "rank_feature_ready_fund_count": int(long_term_history_snapshot.get("rank_feature_ready_fund_count") or 0),
        "full_feature_ready_fund_count": int(long_term_history_snapshot.get("full_feature_ready_fund_count") or 0),
        "neutralized_long_term_feature_fund_count": int(long_term_history_snapshot.get("neutralized_fund_count") or 0),
        "project_universe_count": int(len(merged.index)),
        "excluded_count": int(len(excluded_df.index)),
        "generic_holding_placeholder_excluded_count": int(generic_holding_placeholder_watch_summary.get("count") or 0),
        "generic_holding_placeholder_buyable_count": int(generic_holding_placeholder_watch_summary.get("buyable_count") or 0),
        "generic_holding_placeholder_high_score_count": int(generic_holding_placeholder_watch_summary.get("high_score_count") or 0),
        "low_vol_positive_ratio_focus_count": int(low_vol_positive_ratio_shadow_watch_summary.get("focus_sample_count") or 0),
        "low_vol_positive_ratio_gate_hit_count": int(low_vol_positive_ratio_shadow_watch_summary.get("positive_ratio_gate_hit_count") or 0),
        "low_vol_positive_ratio_gap_alert_count": int(low_vol_positive_ratio_shadow_watch_summary.get("positive_ratio_gap_alert_count") or 0),
        "stage_b_candidate_pool_count": int(len(candidate_df.index)),
        "publishable_pool_count": int(len(candidate_df.index)),
        "final_count": int(len(official_df.index)),
        "stable_bond_final_count": int(len(stable_official_export_full.index)),
        "enhanced_bond_final_count": int(len(enhanced_official_export_full.index)),
        "final_holding_period_no_signal_count": int(official_df.get("持有期状态", pd.Series(dtype=object)).eq(HOLDING_PERIOD_NO_SIGNAL_STATUS).sum()) if not official_df.empty else 0,
        "final_holding_period_name_unparsed_count": int(official_df.get("持有期状态", pd.Series(dtype=object)).eq("名称未识别持有期限制").sum()) if not official_df.empty else 0,
        "final_holding_period_unknown_count": int(official_df.get("持有期状态", pd.Series(dtype=object)).eq(PROJECT_HOLDING_PERIOD_DATA_UNKNOWN_STATUS).sum()) if not official_df.empty else 0,
        "internal_watch_count": int(len(internal_watch_df.index)),
        "internal_watch_holding_period_no_signal_count": int(internal_watch_df.get("持有期状态", pd.Series(dtype=object)).eq(HOLDING_PERIOD_NO_SIGNAL_STATUS).sum()) if not internal_watch_df.empty else 0,
        "internal_watch_holding_period_name_unparsed_count": int(internal_watch_df.get("持有期状态", pd.Series(dtype=object)).eq("名称未识别持有期限制").sum()) if not internal_watch_df.empty else 0,
        "internal_watch_holding_period_unknown_count": int(internal_watch_df.get("持有期状态", pd.Series(dtype=object)).eq(PROJECT_HOLDING_PERIOD_DATA_UNKNOWN_STATUS).sum()) if not internal_watch_df.empty else 0,
        "not_buyable_watch_count": int(sum(1 for item in internal_watch_df.get(AUTO_DIAGNOSIS_CATEGORY_COLUMN, []) if item == NOT_BUYABLE_DIAGNOSIS)),
        "enhanced_watch_count": int(sum(1 for item in internal_watch_df.get(AUTO_DIAGNOSIS_CATEGORY_COLUMN, []) if item == "增强风格诊断")),
        "quality_warning_watch_count": int(sum(1 for item in internal_watch_df.get(AUTO_DIAGNOSIS_CATEGORY_COLUMN, []) if item == QUALITY_WARNING_DIAGNOSIS)),
        "absolute_quality_watch_count": int(sum(1 for item in internal_watch_df.get(AUTO_DIAGNOSIS_CATEGORY_COLUMN, []) if item == ABSOLUTE_QUALITY_DIAGNOSIS)),
        "manager_stability_watch_count": int(sum(1 for item in internal_watch_df.get(AUTO_DIAGNOSIS_CATEGORY_COLUMN, []) if item == MANAGER_STABILITY_DIAGNOSIS)),
        "publish_post_review_watch_count": int(sum(1 for item in internal_watch_df.get(AUTO_DIAGNOSIS_CATEGORY_COLUMN, []) if item == AUTO_PUBLISH_GATE_DIAGNOSIS)),
        "stage_b_excluded_total": int(stage_b_reason_summary.get("excluded_total") or 0),
        "stage_b_annual_ge_5_excluded_count": _stage_b_threshold_count(stage_b_reason_summary, 5.0),
        "stage_b_annual_ge_8_excluded_count": _stage_b_threshold_count(stage_b_reason_summary, 8.0),
        "stage_b_annual_ge_10_excluded_count": _stage_b_threshold_count(stage_b_reason_summary, 10.0),
        "high_score_rejected_count": int(len(high_score_rejected_df.index)),
        "high_score_not_buyable_count": int(len(high_score_not_buyable_df.index)),
        "high_score_friction_count": int(len(high_score_friction_sample_df.index)),
        "high_score_scale_gate_count": int(len(high_score_scale_gate_sample_df.index)),
        "high_score_scale_gate_near_count": int(len(high_score_scale_gate_near_df.index)),
        "high_score_scale_gate_candidate_penalty_count": int(high_score_scale_gate_near_review_sample_df.get("复审建议", pd.Series(dtype=object)).eq("候选惩罚项复审").sum()),
        "high_score_fee_gate_count": int(len(high_score_fee_gate_sample_df.index)),
        "dynamic_final_count": int(len(dynamic_official_df.index)),
        "dynamic_internal_watch_count": int(len(dynamic_internal_watch_df.index)),
        "dynamic_quality_watch_count": int(sum(1 for item in dynamic_internal_watch_df.get(AUTO_DIAGNOSIS_CATEGORY_COLUMN, []) if item == DYNAMIC_QUALITY_DIAGNOSIS)),
        "enhanced_supplemental_pass_count": int(candidate_df.get("enhanced_supplemental_pass", pd.Series(dtype=bool)).fillna(False).astype(bool).sum()) if not candidate_df.empty else 0,
        "threshold_overlap_count": int(len(overlap_codes)),
        "threshold_fixed_only_count": int(len(fixed_only_codes)),
        "threshold_dynamic_only_count": int(len(dynamic_only_codes)),
        "intersection_count": int(final_screening_summary.get("intersection_count") or 0),
        "soft_intersection_count": int(final_screening_summary.get("soft_intersection_count") or 0),
        "only_short_count": int(final_screening_summary.get("only_short_count") or 0),
        "only_long_count": int(final_screening_summary.get("only_long_count") or 0),
        "watchlist_export_count": int(final_screening_summary.get("watchlist_export_count") or 0),
        "autostock_validated_count": int(final_screening_summary.get("autostock_cross_validation_checked_count") or 0),
        "autostock_filtered_count": int(final_screening_summary.get("autostock_cross_validation_filtered_count") or 0),
        "autostock_passed_count": int(final_screening_summary.get("autostock_cross_validation_passed_count") or 0),
        "project_tracker_total_periods": int(project_tracker_summary.get("total_periods") or 0),
        "project_tracker_fund_count": int(project_tracker_summary.get("tracked_fund_count") or 0),
        "project_tracker_repeated_fund_count": int(project_tracker_summary.get("repeated_fund_count") or 0),
        "project_tracker_current_active_count": int(project_tracker_summary.get("current_active_count") or 0),
        "forward_validation_snapshot_count": int(forward_validation_summary.get("snapshot_total") or 0),
        "forward_validation_ready_window_count": int(sum(1 for item in (forward_validation_summary.get("window_summaries") or []) if int(item.get("已就绪快照数") or 0) > 0)),
    }


def _build_report_artifacts_summary(
    *,
    settings: Settings,
    artifacts: dict[str, Any],
    report_paths: dict[str, Path],
    forward_validation_artifacts: dict[str, Any],
) -> dict[str, Any]:
    core_artifacts = {
        # Collection provenance carries identities, hashes and nullable
        # outcomes. Preserve its structure for the frozen-input validator.
        key: deepcopy(value) if key in {
            "collection_calculation_input_captures", "collection_calculation_capture",
        } else _project_relative(settings, value)
        for key, value in artifacts.items()
    }
    tracker_excel = (
        _project_relative(
            settings,
            report_paths.get("run_local_project_tracker_excel") or report_paths["project_tracker_excel"],
        )
    )
    tracker_excel_meta = (
        _project_relative(
            settings,
            report_paths.get("run_local_project_tracker_excel_meta") or report_paths["project_tracker_excel_meta"],
        )
    )
    tracker_current_detail_json = _project_relative(
        settings,
        report_paths.get("run_local_project_tracker_current_detail_json"),
    )
    recommendations_detail_json = _project_relative(
        settings,
        report_paths.get("recommendations_detail_json"),
    )
    return {
        "core": core_artifacts,
        "project_tracker_excel": tracker_excel,
        "project_tracker_excel_meta": tracker_excel_meta,
        "project_tracker_current_detail_json": tracker_current_detail_json,
        "recommendations_detail_json": recommendations_detail_json,
        "filtered_excel": _project_relative(settings, report_paths["filtered_excel"]),
        "filtered_csv": _project_relative(settings, report_paths["filtered_csv"]),
        "filtered_excel_meta": _project_relative(settings, report_paths["filtered_excel_meta"]),
        "dynamic_filtered_excel": _project_relative(settings, report_paths["dynamic_filtered_excel"]),
        "dynamic_filtered_csv": _project_relative(settings, report_paths["dynamic_filtered_csv"]),
        "dynamic_filtered_excel_meta": _project_relative(settings, report_paths["dynamic_filtered_excel_meta"]),
        "threshold_compare_excel": _project_relative(settings, report_paths["threshold_compare_excel"]),
        "threshold_compare_excel_meta": _project_relative(settings, report_paths["threshold_compare_excel_meta"]),
        "watch_excel": _project_relative(settings, report_paths["watch_excel"]),
        "watch_csv": _project_relative(settings, report_paths["watch_csv"]),
        "watch_excel_meta": _project_relative(settings, report_paths["watch_excel_meta"]),
        "dynamic_watch_excel": _project_relative(settings, report_paths["dynamic_watch_excel"]),
        "dynamic_watch_csv": _project_relative(settings, report_paths["dynamic_watch_csv"]),
        "dynamic_watch_excel_meta": _project_relative(settings, report_paths["dynamic_watch_excel_meta"]),
        "excluded_excel": _project_relative(settings, report_paths["excluded_excel"]),
        "excluded_csv": _project_relative(settings, report_paths["excluded_csv"]),
        "excluded_excel_meta": _project_relative(settings, report_paths["excluded_excel_meta"]),
        "stage_b_reason_excel": _project_relative(settings, report_paths["stage_b_reason_excel"]),
        "stage_b_reason_csv": _project_relative(settings, report_paths["stage_b_reason_csv"]),
        "stage_b_reason_excel_meta": _project_relative(settings, report_paths["stage_b_reason_excel_meta"]),
        "screening_audit_excel": _project_relative(settings, report_paths["screening_audit_excel"]),
        "screening_audit_excel_meta": _project_relative(settings, report_paths["screening_audit_excel_meta"]),
        "report_markdown": _project_relative(settings, report_paths["report_md"]),
        "report_json": _project_relative(settings, report_paths["report_json"]),
        "publish_chain_snapshot_json": _project_relative(settings, report_paths.get("publish_chain_snapshot_json")),
        "forward_validation_excel": _project_relative(settings, forward_validation_artifacts.get("excel")),
        "forward_validation_json": _project_relative(settings, forward_validation_artifacts.get("json")),
        "forward_validation_markdown": _project_relative(settings, forward_validation_artifacts.get("markdown")),
    }


def _validation_gate_rule(settings: Settings) -> str:
    """Describe the configured provider without changing its validation gates."""
    provider = str(getattr(settings, "publication_validation_provider", "autostock") or "").strip()
    if provider == "eastmoney_cross_endpoint":
        return (
            "天天基金/Eastmoney 同供应商跨接口硬校验失败样本不进入正式推荐主榜；"
            "软告警样本会保留显式标签并进入后续质量收敛，不代表独立供应商核验。"
        )
    if provider == "autostock":
        return "AutoStock 硬校验失败样本不进入正式推荐主榜；软告警样本会保留显式标签并进入后续质量收敛。"
    return f"数据校验提供方（{provider or '未配置'}）未配置或不受支持，不能据此视为校验通过。"


def _build_project_publish_summary(
    *,
    settings: Settings,
    current_generated_at: str,
    universe_snapshot: dict[str, Any],
    candidate_df: pd.DataFrame,
    fixed_official_audit_df: pd.DataFrame,
    dynamic_publish_official_df: pd.DataFrame,
    removed_after_final_selection_df: pd.DataFrame,
    dynamic_baseline_codes: set[str] | list[str],
    dynamic_reference_export_codes: set[str] | list[str],
    fixed_baseline_codes: set[str] | list[str],
    fixed_only_codes: set[str] | list[str],
    dynamic_only_codes: set[str] | list[str],
    overlap_codes: set[str] | list[str],
    formal_group_distribution: list[dict[str, Any]],
    stable_official_style_distribution: list[dict[str, Any]],
    enhanced_official_style_distribution: list[dict[str, Any]],
    official_style_distribution: list[dict[str, Any]],
    official_company_distribution: list[dict[str, Any]],
    fee_band_distribution: list[dict[str, Any]],
    internal_watch_distribution: list[dict[str, Any]],
    portfolio_summary: dict[str, Any],
    generic_holding_placeholder_watch_summary: dict[str, Any],
    low_vol_positive_ratio_shadow_watch_summary: dict[str, Any],
    project_tracker_summary: dict[str, Any],
    forward_validation_summary: dict[str, Any],
    stage_b_reason_summary: dict[str, Any],
    standard_screening_audit_summary: dict[str, Any],
    publish_stage_audit_summary: dict[str, Any],
    final_selection_contraction_summary: dict[str, Any],
    dynamic_threshold_detail_df: pd.DataFrame,
    dynamic_official_style_distribution: list[dict[str, Any]],
    dynamic_official_company_distribution: list[dict[str, Any]],
    dynamic_fee_band_distribution: list[dict[str, Any]],
    dynamic_internal_watch_distribution: list[dict[str, Any]],
    annual_return_gate_shadow_summary: dict[str, Any],
    risk_metric_cap_audit: dict[str, Any],
    threshold_comparison_preview_df: pd.DataFrame,
    data_source_audit_summary: dict[str, Any],
) -> dict[str, Any]:
    publish_reason_summary = enrich_publish_reason_summary(
        _build_publish_reason_summary(publish_stage_audit_summary),
        {
            **(standard_screening_audit_summary or {}),
            "annual_return_gate_shadow": annual_return_gate_shadow_summary or {},
        },
    )
    return {
        "front_end_policy": "仅正式推荐榜面向用户主展示，观察层仅用于内部质控与审计复核。",
        "formal_score_contract": build_formal_score_contract(),
        "positive_ratio_semantics": _build_positive_ratio_semantics_summary(
            low_vol_positive_ratio_shadow_watch_summary
        ),
        "data_source_audit_summary": data_source_audit_summary,
        "gate_rules": [
            "先排除持有期超过 90 天的定开/持有期债券基金。",
            "仅保留当前可申购基金进入正式发布主榜。",
            f"要求最新净值日期在 {DYNAMIC_NAV_FRESHNESS_DAYS} 天内且关键收益风险指标完整。",
            _validation_gate_rule(settings),
            "候选池直接来自本地全量债券型基金，而不是任何预裁剪候选池。",
            "全部债券型基金均可进入正式推荐评估，增强债基不再按风格直接排除。",
            "正式发布默认采用项目自有收益风险评分与风格动态阈值。",
            "动态阈值或增强补充准入通过后，还要求质量边际不是“边界观察”。",
            "最终展示给用户的主榜，还会再执行一次全局绝对质量底线与精选收敛。",
            f"同时要求经理最短从业天数≥{MANAGER_STABILITY_THRESHOLDS['min_total_days']}天；经理综合评分仅作弱辅助。",
            f"基金规模数据账龄需不超过 {SCALE_DATA_THRESHOLDS['max_report_age_days']} 天。",
        ],
        "formal_group_distribution": formal_group_distribution,
        "stable_official_style_distribution": stable_official_style_distribution,
        "enhanced_official_style_distribution": enhanced_official_style_distribution,
        "official_style_distribution": official_style_distribution,
        "official_company_distribution": official_company_distribution,
        "fee_band_distribution": fee_band_distribution,
        "internal_watch_distribution": internal_watch_distribution,
        "portfolio_suggestions": portfolio_summary,
        "generic_holding_placeholder_watch": generic_holding_placeholder_watch_summary,
        "low_vol_positive_ratio_shadow_watch": low_vol_positive_ratio_shadow_watch_summary,
        "project_tracker_summary": project_tracker_summary,
        "forward_validation_summary": forward_validation_summary,
        "bond_universe_snapshot": universe_snapshot,
        "report_timing_summary": {
            "available": True,
            "generated_at": current_generated_at,
            "total_seconds": 0.0,
            "stages": {},
        },
        "default_frontend_track": "official_publish",
        "high_yield_exclusion_ledger": stage_b_reason_summary,
        "standard_screening_audit": standard_screening_audit_summary,
        "annual_return_gate_shadow": annual_return_gate_shadow_summary,
        "publish_stage_audit": publish_stage_audit_summary,
        "final_selection_contraction": final_selection_contraction_summary,
        "publish_reason_summary": publish_reason_summary,
        "threshold_comparison": {
            "fixed_official_count": int(len(fixed_baseline_codes)),
            "fixed_baseline_count": int(len(fixed_baseline_codes)),
            "fixed_post_review_count": int(len(fixed_official_audit_df.index)),
            "dynamic_official_count": int(len(dynamic_baseline_codes)),
            "dynamic_baseline_count": int(len(dynamic_baseline_codes)),
            "dynamic_reference_export_count": int(len(dynamic_reference_export_codes)),
            "dynamic_post_review_count": int(len(dynamic_publish_official_df.index)),
            "final_selection_removed_count": int(len(removed_after_final_selection_df.index)),
            "overlap_count": int(len(overlap_codes)),
            "fixed_only_count": int(len(fixed_only_codes)),
            "dynamic_only_count": int(len(dynamic_only_codes)),
            "enhanced_supplemental_pass_count": int(candidate_df.get("enhanced_supplemental_pass", pd.Series(dtype=bool)).fillna(False).astype(bool).sum()) if not candidate_df.empty else 0,
            "style_thresholds": dynamic_threshold_detail_df.fillna("").to_dict(orient="records"),
            "dynamic_official_style_distribution": dynamic_official_style_distribution,
            "dynamic_official_company_distribution": dynamic_official_company_distribution,
            "dynamic_fee_band_distribution": dynamic_fee_band_distribution,
            "dynamic_internal_watch_distribution": dynamic_internal_watch_distribution,
            "risk_metric_cap_audit": risk_metric_cap_audit,
            "difference_preview": threshold_comparison_preview_df.fillna("").astype(str).to_dict(orient="records"),
        },
    }


def _build_internal_audit_summary(
    *,
    settings: Settings,
    run_id: str,
    artifacts: dict[str, Any],
    publication_control: dict[str, Any] | None,
    core_base_dir: Path,
    final_meta: dict[str, Any],
    final_screening_summary: dict[str, Any],
    accumulation_summary: dict[str, Any],
    quality_gate_summary: dict[str, Any],
) -> dict[str, Any]:
    from backend.core_execution_audit import build_core_execution_audit

    return {
        "core_flow_audit": build_core_execution_audit(
            settings=settings, run_id=run_id, artifacts=artifacts, report_mode="full_report",
            publication_control=publication_control,
        ),
        "core_architecture": {
            "scope": "architecture_description",
            "stages": [
                {
                    "name": "数据采集与数据库入库",
                    "details": "由项目内置采集链执行，输出统一归档到本项目目录。",
                },
                {
                    "name": "单周期分析与10层筛选",
                    "details": "由项目内置债券筛选引擎执行长短周期分析、主榜收敛和输出生成。",
                },
                {
                    "name": "长期/短期多周期合并",
                    "details": "统一消费本项目生成的长期优质、近期优质及对比报告产物。",
                },
                {
                    "name": "正式发布主榜与质量门禁",
                    "details": "从本地全量债基池独立构建候选集，再执行项目级收益风险门禁与正式推荐收敛。",
                },
                {
                    "name": "累积跟踪",
                    "details": "已纳入本项目报告摘要、前端入口与独立 Excel 预览，不再只是静态附属文件。",
                },
            ],
            "optimizations_applied": [
                "最终发布层增加结构化 holding_days 排除，避免仅依赖名称关键词识别定开/持有期产品。",
                "本项目正式进榜不再使用固定 TopN 数量截断，也不再依赖预裁剪候选池作为唯一来源。",
                "本项目新增基于年化收益、卡玛、正收益率、回撤、波动、费率和规模的绝对质量门槛，夏普/索提诺保留为评分与 shadow 观察信号。",
                "正式发布主榜覆盖全部债券型基金，不再因一级债增强、二级债增强或转债增强风格直接排除。",
                "按债基风格分别计算项目自有分数和动态阈值，修正统一评分对增强风格的系统性失真。",
                "高分但不适合直接推荐的样本分流到自动诊断池，而不再直接暴露给终端用户。",
                "自动诊断池、审计账本、AutoStock 审计文件已统一透出到本项目产物层。",
                "累积跟踪摘要已写入 JSON/Markdown，并在前端与 Excel 中心统一展示。",
            ],
        },
        "core_details": {
            "final_screening_summary": final_screening_summary,
            "accumulation_summary": accumulation_summary,
        },
        "core_quality_gate": quality_gate_summary,
        "core_watchlists": {
            "only_short_simple_csv": _project_relative(settings, artifacts.get("only_short_simple_csv")),
            "only_long_simple_csv": _project_relative(settings, artifacts.get("only_long_simple_csv")),
            "only_short_watchlist_csv": _project_relative(settings, artifacts.get("only_short_watchlist_csv")),
            "only_long_watchlist_csv": _project_relative(settings, artifacts.get("only_long_watchlist_csv")),
        },
        "core_sidecar_paths": {
            "comparison_report_file": _to_project_relative_path(settings, core_base_dir, final_meta.get("comparison_report_file")),
            "autostock_validation_audit_file": _to_project_relative_path(settings, core_base_dir, final_meta.get("autostock_validation_audit_file")),
            "bond_mainlist_quality_audit_json_file": _to_project_relative_path(settings, core_base_dir, final_meta.get("bond_mainlist_quality_audit_json_file")),
            "bond_mainlist_quality_audit_md_file": _to_project_relative_path(settings, core_base_dir, final_meta.get("bond_mainlist_quality_audit_md_file")),
            "final_screening_decision_ledger_file": _to_project_relative_path(settings, core_base_dir, final_meta.get("final_screening_decision_ledger_file")),
        },
    }


def _build_report_preview_summary(
    *,
    final_preview: pd.DataFrame,
    stable_final_preview: pd.DataFrame,
    enhanced_final_preview: pd.DataFrame,
    internal_watch_preview: pd.DataFrame,
    excluded_preview: pd.DataFrame,
    generic_holding_placeholder_preview: pd.DataFrame,
    stage_b_reason_preview: pd.DataFrame,
    accumulation_preview: pd.DataFrame,
    project_tracker_preview: pd.DataFrame,
    project_tracker_current_preview: pd.DataFrame,
    forward_validation_preview: pd.DataFrame,
) -> dict[str, Any]:
    return {
        "final_preview": _preview_records(final_preview),
        "stable_final_preview": _preview_records(stable_final_preview),
        "enhanced_final_preview": _preview_records(enhanced_final_preview),
        "internal_watch_preview": _preview_records(internal_watch_preview),
        "excluded_preview": _preview_records(excluded_preview),
        "generic_holding_placeholder_preview": _preview_records(generic_holding_placeholder_preview),
        "stage_b_reason_preview": _preview_records(stage_b_reason_preview),
        "accumulation_preview": _preview_records(accumulation_preview),
        "project_tracker_preview": _preview_records(project_tracker_preview),
        "project_tracker_current_preview": _preview_records(project_tracker_current_preview),
        "forward_validation_preview": _preview_records(forward_validation_preview),
    }


def _build_report_audit_files_summary(
    *,
    settings: Settings,
    artifacts: dict[str, Any],
    report_paths: dict[str, Path],
) -> dict[str, Any]:
    return {
        "autostock_validation_json": _project_relative(settings, artifacts.get("autostock_validation_audit_file")),
        "bond_mainlist_quality_json": _project_relative(settings, artifacts.get("bond_mainlist_quality_audit_json_file")),
        "bond_mainlist_quality_markdown": _project_relative(settings, artifacts.get("bond_mainlist_quality_audit_md_file")),
        "final_screening_decision_ledger_json": _project_relative(settings, artifacts.get("final_screening_decision_ledger_file")),
        "risk_lag_audit_json": _project_relative(settings, artifacts.get("risk_lag_audit_json")),
        "risk_sync_audit_json": _project_relative(settings, artifacts.get("risk_sync_audit_json")),
        "risk_coverage_gap_audit_json": _project_relative(settings, artifacts.get("risk_coverage_gap_audit_json")),
        "standard_screening_audit_excel": _project_relative(settings, report_paths["screening_audit_excel"]),
    }


def _build_report_optimizations_summary() -> list[str]:
    return [
        "项目内统一维护数据采集、单周期分析、长短期合并与最终收敛主链，避免多套实现并存。",
        "在正式发布主榜之前增加结构化持有期复核，依据 holding_days 字段排除持有期超过 90 天的债券基金。",
        "本项目正式进榜不再沿用固定 TopN 数量截断，也不再依赖预裁剪候选池作为唯一候选入口。",
        "基于本地全量债基池重建候选宇宙，并建立项目自有收益风险评分体系。",
        "在项目自有评分基础上增加绝对质量门槛（年化、卡玛、正收益率、回撤、波动、费率、规模），夏普/索提诺保留为评分与 shadow 观察信号。",
        "保留固定阈值与动态阈值的内部审计底稿，用于校验全局阈值对不同债基风格的潜在偏差，但前端对外只展示单一正式榜单。",
        "新增项目治理候选池门禁，面向全部债券型基金统一执行可申购、校验、收益风险与经理从业与评分复核，不再直接排除增强债基。",
        "按债基风格分别计算动态阈值，避免用纯债口径压制增强债基，也避免增强债低风险错觉污染纯债比较。",
        "新增债基风格标签、基准归类、费率合计和推荐理由字段，增强用户侧可解释性。",
        "补齐 Markdown 报告、JSON 摘要和过滤后产物，便于前端直接展示与复盘。",
        "将内部筛选运行输出统一重定向到根目录 reports/core_output，保持根目录整洁。",
        "自动诊断池保留在审计产物中，前端主视图只聚焦正式推荐榜，避免把研究候选池误读成最终投资建议。",
        "把优质基金累积跟踪、观察池与质量审计文件纳入本项目报告和前端导航，形成完整发布闭环。",
        "新增债基持有期补强脚本，补齐滚动持有、中文数字月份/年份和定开年限等名称规则漏识别样本。",
    ]


def _context_values(context: dict[str, Any], *keys: str) -> tuple[Any, ...]:
    return tuple(context[key] for key in keys)


def _prepare_report_export_materialization_context(
    *,
    settings: Settings,
    run_id: str,
    current_generated_at: str,
    run_dir: Path,
    snapshot: dict[str, Any],
    filters: dict[str, Any],
    finish_report_sub_stage: Any,
    stage_name: str,
) -> dict[str, Any]:
    materialize_sub_stage_started_at = perf_counter()
    report_materialization_assets = _prepare_report_materialization_assets(
        settings=settings,
        run_id=run_id,
        run_dir=run_dir,
        universe_snapshot=snapshot["universe_snapshot"],
        long_term_history_snapshot=snapshot["long_term_history_snapshot"],
        publish_stage_audit_summary=filters["publish_stage_audit_summary"],
        standard_screening_audit_summary=filters["standard_screening_audit_summary"],
        merged=filters["merged"],
        excluded_df=filters["excluded_df"],
        candidate_df=filters["candidate_df"],
        official_df=filters["official_df"],
        internal_watch_df=filters["internal_watch_df"],
        generic_holding_placeholder_watch_df=filters["generic_holding_placeholder_watch_df"],
        generic_holding_placeholder_watch_summary=filters["generic_holding_placeholder_watch_summary"],
        fixed_baseline_codes=filters["fixed_baseline_codes"],
        dynamic_baseline_codes=filters["dynamic_baseline_codes"],
        dynamic_reference_export_codes=filters["dynamic_reference_export_codes"],
        fixed_official_audit_df=filters["fixed_official_audit_df"],
        dynamic_publish_official_df=filters["dynamic_publish_official_df"],
        fixed_internal_watch_audit_df=filters["fixed_internal_watch_audit_df"],
        dynamic_internal_watch_df=filters["dynamic_internal_watch_df"],
        removed_after_final_selection_df=filters["removed_after_final_selection_df"],
        overlap_codes=filters["overlap_codes"],
        fixed_only_codes=filters["fixed_only_codes"],
        dynamic_only_codes=filters["dynamic_only_codes"],
        stable_official_export_full=filters["stable_official_export_full"],
        enhanced_official_export_full=filters["enhanced_official_export_full"],
        official_export_full=filters["official_export_full"],
    )
    report_paths = report_materialization_assets["report_paths"]
    materialized_export_context = _prepare_materialized_export_context(
        settings=settings,
        run_id=run_id,
        current_generated_at=current_generated_at,
        effective_nav_candidate_date=filters["effective_nav_candidate_date"],
        filtered_excel=report_paths["filtered_excel"],
        watch_excel=report_paths["watch_excel"],
        candidate_df=filters["candidate_df"],
        excluded_df=filters["excluded_df"],
        official_df=filters["official_df"],
        dynamic_publish_track_df=filters["dynamic_publish_track_df"],
        dynamic_publish_official_df=filters["dynamic_publish_official_df"],
        dynamic_official_df=filters["dynamic_official_df"],
        dynamic_internal_watch_df=filters["dynamic_internal_watch_df"],
        dynamic_internal_watch_export_full=filters["dynamic_internal_watch_export_full"],
        stable_official_export_full=filters["stable_official_export_full"],
        enhanced_official_export_full=filters["enhanced_official_export_full"],
        dynamic_threshold_track_df=filters["dynamic_threshold_track_df"],
        fixed_threshold_track_df=filters["fixed_threshold_track_df"],
        fixed_publish_track_df=filters["fixed_publish_track_df"],
        style_meta=filters["style_meta"],
        global_thresholds=filters["global_thresholds"],
        removed_after_final_selection_df=filters["removed_after_final_selection_df"],
        final_selection_contraction_df=filters["final_selection_contraction_df"],
        internal_watch_df=filters["internal_watch_df"],
    )
    finish_report_sub_stage(
        stage_name,
        "prepare_export_frames_and_portfolio",
        materialize_sub_stage_started_at,
    )
    return {
        "report_materialization_assets": report_materialization_assets,
        "report_paths": report_paths,
        "materialized_export_context": materialized_export_context,
    }


def _materialize_report_workbooks_and_validation(
    *,
    settings: Settings,
    run_id: str,
    current_generated_at: str,
    candidate: dict[str, Any],
    snapshot: dict[str, Any],
    filters: dict[str, Any],
    report_input_context: dict[str, Any],
    report_paths: dict[str, Path],
    report_materialization_assets: dict[str, Any],
    materialized_export_context: dict[str, Any],
    finish_report_sub_stage: Any,
    stage_name: str,
) -> dict[str, Any]:
    materialize_sub_stage_started_at = perf_counter()
    primary_workbook_context = _materialize_primary_report_workbooks(
        settings=settings,
        run_id=run_id,
        current_generated_at=current_generated_at,
        effective_nav_candidate_date=filters["effective_nav_candidate_date"],
        universe_snapshot=snapshot["universe_snapshot"],
        merged=filters["merged"],
        excluded_df=filters["excluded_df"],
        candidate_df=filters["candidate_df"],
        official_df=materialized_export_context["official_df"],
        dynamic_official_df=filters["dynamic_official_df"],
        dynamic_internal_watch_df=filters["dynamic_internal_watch_df"],
        publish_summary_df=report_materialization_assets["publish_summary_df"],
        dynamic_threshold_detail_df=filters["dynamic_threshold_detail_df"],
        dynamic_official_style_df=filters["dynamic_official_style_df"],
        dynamic_official_company_df=filters["dynamic_official_company_df"],
        dynamic_fee_band_df=filters["dynamic_fee_band_df"],
        dynamic_internal_watch_summary_df=filters["dynamic_internal_watch_summary_df"],
        export_context=materialized_export_context,
        report_paths=report_paths,
    )
    materialize_sub_stage_started_at = finish_report_sub_stage(
        stage_name,
        "write_primary_workbooks",
        materialize_sub_stage_started_at,
    )

    materialize_sub_stage_started_at = perf_counter()
    forward_validation_package = build_forward_validation_package(
        settings,
        run_id=run_id,
        generated_at=datetime.now().isoformat(timespec="seconds"),
        snapshot_date=filters["effective_nav_candidate_date"],
        filtered_excel=report_paths["filtered_excel"],
        watch_excel=report_paths["watch_excel"],
        final_count=int(len(materialized_export_context["official_df"].index)),
        stable_final_count=int(len(materialized_export_context["stable_official_export_full"].index)),
        enhanced_final_count=int(len(materialized_export_context["enhanced_official_export_full"].index)),
    )
    materialize_sub_stage_started_at = finish_report_sub_stage(
        stage_name,
        "build_forward_validation_package",
        materialize_sub_stage_started_at,
    )

    materialize_sub_stage_started_at = perf_counter()
    secondary_workbook_context = _materialize_secondary_report_workbooks(
        settings=settings,
        effective_nav_candidate_date=filters["effective_nav_candidate_date"],
        report_paths=report_paths,
        excluded_export=report_materialization_assets["excluded_export"],
        stage_b_reason_ledger_df=materialized_export_context["stage_b_reason_ledger_df"],
        stage_b_reason_summary=materialized_export_context["stage_b_reason_summary"],
        quality_overlap_pair_df=filters["quality_overlap_pair_df"],
        quality_overlap_combo_df=filters["quality_overlap_combo_df"],
        quality_overlap_sample_df=filters["quality_overlap_sample_df"],
        high_score_friction_sample_df=filters["high_score_friction_sample_df"],
        high_score_friction_reason_df=filters["high_score_friction_reason_df"],
        high_score_friction_style_df=filters["high_score_friction_style_df"],
        high_score_scale_gate_sample_df=filters["high_score_scale_gate_sample_df"],
        high_score_scale_gate_style_df=filters["high_score_scale_gate_style_df"],
        high_score_scale_gate_band_df=filters["high_score_scale_gate_band_df"],
        high_score_scale_gate_age_df=filters["high_score_scale_gate_age_df"],
        high_score_scale_gate_near_df=filters["high_score_scale_gate_near_df"],
        high_score_scale_gate_near_review_df=filters["high_score_scale_gate_near_review_df"],
        high_score_scale_gate_near_review_sample_df=filters["high_score_scale_gate_near_review_sample_df"],
        high_score_fee_gate_sample_df=filters["high_score_fee_gate_sample_df"],
        high_score_fee_gate_style_df=filters["high_score_fee_gate_style_df"],
        style_pass_rate_df=filters["style_pass_rate_df"],
        style_veto_reason_df=filters["style_veto_reason_df"],
        reason_group_audit_df=filters["reason_group_audit_df"],
        quality_factor_group_audit_df=filters["quality_factor_group_audit_df"],
        quality_factor_detail_audit_df=filters["quality_factor_detail_audit_df"],
        status_split_audit_df=filters["status_split_audit_df"],
        high_score_rejected_df=filters["high_score_rejected_df"],
        high_score_not_buyable_df=filters["high_score_not_buyable_df"],
        risk_metric_cap_audit=candidate["risk_metric_cap_audit"],
        comparison_df=report_materialization_assets["comparison_df"],
        threshold_comparison_full_df=report_materialization_assets["threshold_comparison_full_df"],
        dynamic_threshold_detail_df=filters["dynamic_threshold_detail_df"],
        finish_report_sub_stage=finish_report_sub_stage,
        stage_name=stage_name,
    )
    project_tracker_context = _materialize_project_tracker_assets(
        settings=settings,
        run_id=run_id,
        current_generated_at=current_generated_at,
        project_tracker_excel=report_paths["project_tracker_excel"],
        project_tracker_excel_meta=report_paths["project_tracker_excel_meta"],
        run_local_project_tracker_excel=report_paths.get("run_local_project_tracker_excel"),
        run_local_project_tracker_excel_meta=report_paths.get("run_local_project_tracker_excel_meta"),
        run_local_project_tracker_current_detail_json=report_paths.get("run_local_project_tracker_current_detail_json"),
        project_tracker_df=primary_workbook_context["project_tracker_df"],
        project_tracker_process_df=primary_workbook_context["project_tracker_process_df"],
        project_tracker_summary=primary_workbook_context["project_tracker_summary"],
        data_source_by_code=_build_data_source_lookup(
            report_input_context.get("metadata_df", pd.DataFrame()),
        ),
        finish_report_sub_stage=finish_report_sub_stage,
        stage_name=stage_name,
    )
    finish_report_sub_stage(
        stage_name,
        "write_audit_and_tracker_workbooks",
        materialize_sub_stage_started_at,
    )
    return {
        "primary_workbook_context": primary_workbook_context,
        "secondary_workbook_context": secondary_workbook_context,
        "project_tracker_context": project_tracker_context,
        "forward_validation_package": forward_validation_package,
    }


def _finalize_report_materialization_preview_context(
    *,
    settings: Settings,
    run_id: str,
    current_generated_at: str,
    artifacts: dict[str, Any],
    candidate: dict[str, Any],
    snapshot: dict[str, Any],
    filters: dict[str, Any],
    report_paths: dict[str, Path],
    report_materialization_assets: dict[str, Any],
    materialized_export_context: dict[str, Any],
    primary_workbook_context: dict[str, Any],
    secondary_workbook_context: dict[str, Any],
    project_tracker_context: dict[str, Any],
    forward_validation_package: dict[str, Any],
    finish_report_sub_stage: Any,
    stage_name: str,
) -> dict[str, Any]:
    publish_chain_snapshot_json = _write_publish_chain_snapshot(
        settings=settings,
        run_dir=report_paths["report_json"].parent,
        run_id=run_id,
        current_generated_at=current_generated_at,
        dynamic_reference_export_df=filters["dynamic_official_df"],
        dynamic_post_review_df=filters["dynamic_publish_official_df"],
        final_official_df=materialized_export_context["official_df"],
        final_selection_removed_df=filters["removed_after_final_selection_df"],
        final_selection_contraction_df=filters["final_selection_contraction_df"],
    )
    report_paths["publish_chain_snapshot_json"] = publish_chain_snapshot_json

    _write_report_artifact_meta_files(
        settings=settings,
        run_id=run_id,
        artifacts=artifacts,
        report_paths=report_paths,
        workbook_results={
            "filtered_workbook_result": primary_workbook_context["filtered_workbook_result"],
            "dynamic_filtered_workbook_result": primary_workbook_context["dynamic_filtered_workbook_result"],
            "watch_workbook_result": primary_workbook_context["watch_workbook_result"],
            "dynamic_watch_workbook_result": primary_workbook_context["dynamic_watch_workbook_result"],
            "excluded_workbook_result": secondary_workbook_context["excluded_workbook_result"],
            "stage_b_reason_workbook_result": secondary_workbook_context["stage_b_reason_workbook_result"],
            "screening_audit_workbook_result": secondary_workbook_context["screening_audit_workbook_result"],
            "threshold_compare_workbook_result": secondary_workbook_context["threshold_compare_workbook_result"],
        },
        universe_snapshot=snapshot["universe_snapshot"],
        merged=filters["merged"],
        excluded_df=filters["excluded_df"],
        candidate_df=filters["candidate_df"],
        official_df=materialized_export_context["official_df"],
        dynamic_official_df=filters["dynamic_official_df"],
        fixed_baseline_codes=filters["fixed_baseline_codes"],
        fixed_official_audit_df=filters["fixed_official_audit_df"],
        dynamic_publish_official_df=filters["dynamic_publish_official_df"],
        overlap_codes=filters["overlap_codes"],
        fixed_only_codes=filters["fixed_only_codes"],
        dynamic_only_codes=filters["dynamic_only_codes"],
        removed_after_final_selection_df=filters["removed_after_final_selection_df"],
        internal_watch_df=materialized_export_context["internal_watch_df"],
        dynamic_internal_watch_df=filters["dynamic_internal_watch_df"],
        generic_holding_placeholder_watch_summary=filters["generic_holding_placeholder_watch_summary"],
        stage_b_reason_summary=materialized_export_context["stage_b_reason_summary"],
        style_pass_rate_df=filters["style_pass_rate_df"],
        high_score_rejected_df=filters["high_score_rejected_df"],
        high_score_not_buyable_df=filters["high_score_not_buyable_df"],
        high_score_friction_sample_df=filters["high_score_friction_sample_df"],
        high_score_scale_gate_sample_df=filters["high_score_scale_gate_sample_df"],
        high_score_scale_gate_near_df=filters["high_score_scale_gate_near_df"],
        high_score_scale_gate_near_review_sample_df=filters["high_score_scale_gate_near_review_sample_df"],
        high_score_fee_gate_sample_df=filters["high_score_fee_gate_sample_df"],
        risk_metric_cap_audit=candidate["risk_metric_cap_audit"],
    )

    materialize_sub_stage_started_at = perf_counter()
    preview_summary_context = _prepare_report_preview_and_summary_context(
        settings=settings,
        official_export_full=materialized_export_context["official_export_full"],
        internal_watch_export=materialized_export_context["internal_watch_export"],
        excluded_export=report_materialization_assets["excluded_export"],
        generic_holding_placeholder_watch_export=report_materialization_assets["generic_holding_placeholder_watch_export"],
        stage_b_reason_ledger_df=materialized_export_context["stage_b_reason_ledger_df"],
        project_tracker_df=primary_workbook_context["project_tracker_df"],
        project_tracker_current_view_df=project_tracker_context["project_tracker_current_view_df"],
        forward_validation_package=forward_validation_package,
        stable_official_export_full=materialized_export_context["stable_official_export_full"],
        enhanced_official_export_full=materialized_export_context["enhanced_official_export_full"],
        official_style_df=materialized_export_context["official_style_df"],
        formal_group_distribution_df=materialized_export_context["formal_group_distribution_df"],
        stable_official_style_df=materialized_export_context["stable_official_style_df"],
        enhanced_official_style_df=materialized_export_context["enhanced_official_style_df"],
        internal_watch_summary_df=materialized_export_context["internal_watch_summary_df"],
        official_company_df=materialized_export_context["official_company_df"],
        fee_band_df=materialized_export_context["fee_band_df"],
        dynamic_official_style_df=filters["dynamic_official_style_df"],
        dynamic_internal_watch_summary_df=filters["dynamic_internal_watch_summary_df"],
        dynamic_official_company_df=filters["dynamic_official_company_df"],
        dynamic_fee_band_df=filters["dynamic_fee_band_df"],
        final_screening_summary=snapshot["final_screening_summary"],
        effective_nav_snapshot=filters["effective_nav_snapshot"],
        universe_snapshot=snapshot["universe_snapshot"],
        long_term_history_snapshot=snapshot["long_term_history_snapshot"],
        candidate_df=filters["candidate_df"],
    )
    finish_report_sub_stage(
        stage_name,
        "build_preview_and_summary_records",
        materialize_sub_stage_started_at,
    )
    final_preview, internal_watch_preview, excluded_preview = _context_values(
        preview_summary_context,
        "final_preview",
        "internal_watch_preview",
        "excluded_preview",
    )
    return {
        "publish_chain_snapshot_json": publish_chain_snapshot_json,
        "preview_summary_context": preview_summary_context,
        "final_preview": final_preview,
        "internal_watch_preview": internal_watch_preview,
        "excluded_preview": excluded_preview,
    }


def _materialize_report_payload_stage(
    *,
    settings: Settings,
    run_id: str,
    current_generated_at: str,
    run_dir: Path,
    artifacts: dict[str, Any],
    report_input_context: dict[str, Any],
    report_snapshot_context: dict[str, Any],
    report_candidate_universe_context: dict[str, Any],
    report_filters_and_audits_context: dict[str, Any],
    finish_report_sub_stage: Any,
    stage_name: str,
) -> dict[str, Any]:
    snapshot = report_snapshot_context
    candidate = report_candidate_universe_context
    filters = report_filters_and_audits_context

    export_materialization_context = _prepare_report_export_materialization_context(
        settings=settings,
        run_id=run_id,
        current_generated_at=current_generated_at,
        run_dir=run_dir,
        snapshot=snapshot,
        filters=filters,
        finish_report_sub_stage=finish_report_sub_stage,
        stage_name=stage_name,
    )
    report_materialization_assets, report_paths, materialized_export_context = _context_values(
        export_materialization_context,
        "report_materialization_assets",
        "report_paths",
        "materialized_export_context",
    )

    workbook_materialization_context = _materialize_report_workbooks_and_validation(
        settings=settings,
        run_id=run_id,
        current_generated_at=current_generated_at,
        candidate=candidate,
        snapshot=snapshot,
        filters=filters,
        report_input_context=report_input_context,
        report_paths=report_paths,
        report_materialization_assets=report_materialization_assets,
        materialized_export_context=materialized_export_context,
        finish_report_sub_stage=finish_report_sub_stage,
        stage_name=stage_name,
    )
    primary_workbook_context, secondary_workbook_context, project_tracker_context, forward_validation_package = _context_values(
        workbook_materialization_context,
        "primary_workbook_context",
        "secondary_workbook_context",
        "project_tracker_context",
        "forward_validation_package",
    )

    preview_materialization_context = _finalize_report_materialization_preview_context(
        settings=settings,
        run_id=run_id,
        current_generated_at=current_generated_at,
        artifacts=artifacts,
        candidate=candidate,
        snapshot=snapshot,
        filters=filters,
        report_paths=report_paths,
        report_materialization_assets=report_materialization_assets,
        materialized_export_context=materialized_export_context,
        primary_workbook_context=primary_workbook_context,
        secondary_workbook_context=secondary_workbook_context,
        project_tracker_context=project_tracker_context,
        forward_validation_package=forward_validation_package,
        finish_report_sub_stage=finish_report_sub_stage,
        stage_name=stage_name,
    )
    preview_summary_context, final_preview, internal_watch_preview, excluded_preview = _context_values(
        preview_materialization_context,
        "preview_summary_context",
        "final_preview",
        "internal_watch_preview",
        "excluded_preview",
    )
    return {
        "report_paths": report_paths,
        "threshold_comparison_preview_df": report_materialization_assets["threshold_comparison_preview_df"],
        "materialized_export_context": materialized_export_context,
        "primary_workbook_context": primary_workbook_context,
        "preview_summary_context": preview_summary_context,
        "final_preview": final_preview,
        "internal_watch_preview": internal_watch_preview,
        "excluded_preview": excluded_preview,
    }


@report_history_store.capture_reads
def build_report_payload(
    *,
    settings: Settings,
    run_id: str,
    artifacts: dict[str, Any],
    publication_control: dict[str, Any] | None = None,
) -> dict[str, Any]:
    from backend.publication_transaction import PROTOCOL_VERSION, new_report_revision

    run_dir = new_report_revision(settings, run_id)
    publication_control = {**(publication_control or {}), "commit_protocol": PROTOCOL_VERSION,
                           "revision": run_dir.name, "artifact_checks_required": True,
                           "full_value_checks_required": True, "csv_return_provenance_checks_required": True,
                           "drawdown_source_checks_required": True, "truthfulness_checks_required": True}
    artifacts = _flatten_runtime_artifacts(artifacts)
    artifacts = _materialize_run_local_core_artifacts(
        settings=settings,
        run_dir=run_dir,
        artifacts=artifacts,
    )
    artifacts.update(
        _build_run_local_risk_audit_artifacts(
            settings=settings,
            run_id=run_id,
            run_dir=run_dir,
        )
    )
    current_generated_at = datetime.now().isoformat(timespec="seconds")
    report_build_started_at = perf_counter()
    report_timing_stages: dict[str, float] = {}
    report_timing_stage_details: dict[str, dict[str, float]] = {}

    def _finish_report_stage(stage_name: str, stage_started_at: float) -> float:
        report_timing_stages[stage_name] = round(perf_counter() - stage_started_at, 4)
        return perf_counter()

    def _finish_report_sub_stage(
        stage_name: str,
        sub_stage_name: str,
        sub_stage_started_at: float,
    ) -> float:
        stage_details = report_timing_stage_details.setdefault(stage_name, {})
        stage_details[sub_stage_name] = round(perf_counter() - sub_stage_started_at, 4)
        return perf_counter()

    stage_started_at = perf_counter()
    report_input_context = _load_report_input_context(settings, artifacts)
    stage_started_at = _finish_report_stage("load_inputs_and_metadata", stage_started_at)

    stage_started_at = perf_counter()
    report_candidate_universe_context = _build_report_candidate_universe_context(settings, artifacts, report_input_context)
    stage_started_at = _finish_report_stage("build_candidate_universe", stage_started_at)

    stage_started_at = perf_counter()
    report_snapshot_context = _build_report_snapshot_context(
        settings,
        artifacts,
        report_input_context,
        report_candidate_universe_context,
    )
    stage_started_at = _finish_report_stage("build_universe_snapshots", stage_started_at)

    stage_started_at = perf_counter()
    report_filters_and_audits_context = _run_report_filters_and_audits(
        settings=settings,
        run_id=run_id,
        current_generated_at=current_generated_at,
        candidate_universe_df=report_candidate_universe_context["candidate_universe_df"],
        decision_map=report_input_context["decision_map"],
        soft_warning_map=report_input_context["soft_warning_map"],
        risk_metric_cap_audit=report_candidate_universe_context["risk_metric_cap_audit"],
        finish_report_sub_stage=_finish_report_sub_stage,
    )
    autostock_status_context = report_filters_and_audits_context["autostock_status_context"]
    if getattr(settings, "publication_validation_provider", "autostock") == "eastmoney_cross_endpoint":
        autostock_status_context["data_validation_snapshot"] = write_data_validation_snapshot(
            project_root=settings.project_root, run_dir=run_dir, run_id=run_id,
            generated_at=current_generated_at, context=autostock_status_context,
        )
    else:
        autostock_status_context["autostock_supplement_snapshot"] = write_supplement_snapshot(
            project_root=settings.project_root, run_dir=run_dir, run_id=run_id,
            generated_at=current_generated_at, context=autostock_status_context,
        )
    stage_started_at = _finish_report_stage("build_filters_and_audits", stage_started_at)

    stage_started_at = perf_counter()
    materialize_stage_context = _materialize_report_payload_stage(
        settings=settings,
        run_id=run_id,
        current_generated_at=current_generated_at,
        run_dir=run_dir,
        artifacts=artifacts,
        report_input_context=report_input_context,
        report_snapshot_context=report_snapshot_context,
        report_candidate_universe_context=report_candidate_universe_context,
        report_filters_and_audits_context=report_filters_and_audits_context,
        finish_report_sub_stage=_finish_report_sub_stage,
        stage_name="materialize_summary_payload",
    )
    report_paths = materialize_stage_context["report_paths"]
    threshold_comparison_preview_df = materialize_stage_context["threshold_comparison_preview_df"]
    materialized_export_context = materialize_stage_context["materialized_export_context"]
    primary_workbook_context = materialize_stage_context["primary_workbook_context"]
    preview_summary_context = materialize_stage_context["preview_summary_context"]
    final_preview = materialize_stage_context["final_preview"]
    internal_watch_preview = materialize_stage_context["internal_watch_preview"]
    excluded_preview = materialize_stage_context["excluded_preview"]
    report_md = report_paths["report_md"]
    report_json = report_paths["report_json"]
    stage_started_at = _finish_report_stage("materialize_summary_payload", stage_started_at)

    summary = _build_report_summary_payload(
        settings=settings,
        run_id=run_id,
        current_generated_at=current_generated_at,
        artifacts=artifacts,
        report_paths=report_paths,
        report_input_context=report_input_context,
        report_snapshot_context=report_snapshot_context,
        report_candidate_universe_context=report_candidate_universe_context,
        report_filters_and_audits_context=report_filters_and_audits_context,
        materialized_export_context=materialized_export_context,
        primary_workbook_context=primary_workbook_context,
        preview_summary_context=preview_summary_context,
        threshold_comparison_preview_df=threshold_comparison_preview_df,
        publication_control=publication_control,
    )
    from backend.publication_checks import write_publication_basis
    from backend.screening_replay_snapshot import write_screening_input_snapshot
    from backend.manager_metrics import manager_metric_contract

    summary["manager_metric_semantics"] = manager_metric_contract()
    if autostock_status_context.get("data_validation_snapshot"):
        summary["artifacts"]["data_validation_snapshot_json_gz"] = autostock_status_context["data_validation_snapshot"]["path"]
    else:
        summary["artifacts"]["autostock_supplement_snapshot_json_gz"] = autostock_status_context["autostock_supplement_snapshot"]["path"]
    from backend.prospective_autostock import capture_strategy_observations, capture_summary

    observation_started = perf_counter()
    prospective_capture = capture_strategy_observations(
        settings, run_id=run_id, revision=run_dir.name, generated_at=summary["generated_at"],
        universe=report_candidate_universe_context["candidate_universe_df"],
        filters=report_filters_and_audits_context, report_inputs=report_input_context,
    )
    report_filters_and_audits_context["prospective_autostock_capture"] = prospective_capture
    summary["strategy_observation_capture"] = capture_summary(prospective_capture)
    _finish_report_stage("prospective_strategy_observations", observation_started)
    print(f"[strategy-observations] status={prospective_capture['status']} "
          f"targets={prospective_capture.get('target_count')} compared={prospective_capture.get('compared_count')}", flush=True)
    screening_snapshot_path = write_screening_input_snapshot(
        settings, summary, universe=report_candidate_universe_context["candidate_universe_df"],
        filters=report_filters_and_audits_context, materialized=materialized_export_context,
        report_inputs=report_input_context,
    )
    summary["artifacts"]["screening_input_snapshot_json_gz"] = _project_relative(settings, screening_snapshot_path)

    from backend.report_history import freeze_read_inputs
    history_inputs=freeze_read_inputs(settings,summary,run_dir)
    if history_inputs is not None:
        summary['artifacts']['history_read_inputs_json_gz']=_project_relative(settings,history_inputs)
    basis_path = write_publication_basis(
        settings, summary,
        official_records=_stringify_frame(materialized_export_context["official_export_full"]).to_dict(orient="records"),
        risk_records=_stringify_frame(report_candidate_universe_context["risk_df"]).to_dict(orient="records"),
        dynamic_records=_stringify_frame(materialized_export_context["dynamic_official_export_full"]).to_dict(orient="records"),
    )
    summary["artifacts"]["publication_basis_json"] = _project_relative(settings, basis_path)
    summary["internal_audit"]["artifact_consistency"] = _build_artifact_consistency_audit(
        settings=settings,
        summary=summary,
    )
    stage_started_at = _finish_report_stage("artifact_consistency_audit", stage_started_at)

    # Backward-compatible top-level counters:
    # some runtime tooling still probes these fields directly instead of going through
    # summary["counts"], which can otherwise read as empty/None and create false audit noise.
    counts = dict(summary.get("counts") or {})
    summary["final_count"] = int(counts.get("final_count") or 0)
    summary["dynamic_final_count"] = int(counts.get("dynamic_final_count") or 0)
    summary["project_tracker_current_active_count"] = int(
        counts.get("project_tracker_current_active_count") or 0
    )
    summary["stable_bond_final_count"] = int(counts.get("stable_bond_final_count") or 0)
    summary["enhanced_bond_final_count"] = int(counts.get("enhanced_bond_final_count") or 0)

    stage_started_at = perf_counter()
    markdown = build_markdown_report(settings, summary, final_preview, excluded_preview, internal_watch_preview)
    report_md.write_text(markdown, encoding="utf-8")
    _finish_report_stage("write_markdown_and_json", stage_started_at)
    report_timing_summary = {
        "available": True,
        "generated_at": current_generated_at,
        "total_seconds": round(perf_counter() - report_build_started_at, 4),
        "stages": report_timing_stages,
        "sub_stages": report_timing_stage_details,
    }
    summary["project_publish_summary"]["report_timing_summary"] = report_timing_summary
    # Registry readers must never observe a partially serialized candidate.
    from backend.publication import _atomic_write_json

    from backend.state_normalizer import normalize_snapshot_payload

    summary, _ = normalize_snapshot_payload(summary)
    _atomic_write_json(report_json, summary)
    return summary



def _append_markdown_preview_table(
    lines: list[str],
    *,
    rows: list[dict[str, Any]] | None,
    columns: list[str],
    label: str | None = None,
    leading_blank: bool = False,
) -> None:
    preview_df = pd.DataFrame(rows or [])
    if preview_df.empty:
        return
    if leading_blank:
        lines.append("")
    if label:
        lines.append(label)
    lines.append(_markdown_table(preview_df, columns))


def _build_markdown_overview_lines(summary: dict[str, Any]) -> list[str]:
    from backend.core_execution_audit import core_execution_markdown

    counts = summary["counts"]
    validation_pool_label = ("跨接口校验后核心池" if (summary.get("data_validation_audit") or {}).get("required")
                             else "双源校验后核心池")
    coverage_layers = summary.get("coverage_layers") or {}
    table_coverage = coverage_layers.get("table_coverage") or {}
    bond_universe_coverage = coverage_layers.get("bond_universe_coverage") or {}
    screenable_universe_coverage = coverage_layers.get("screenable_universe_coverage") or {}
    publishable_universe_coverage = coverage_layers.get("publishable_universe_coverage") or {}
    stage_b_candidate_coverage = coverage_layers.get("stage_b_candidate_coverage") or publishable_universe_coverage
    governance_candidate_pool_count = (
        stage_b_candidate_coverage.get("governance_candidate_pool_count")
        or stage_b_candidate_coverage.get("stage_b_candidate_pool_count")
        or stage_b_candidate_coverage.get("publishable_pool_count")
        or counts.get("stage_b_candidate_pool_count")
        or counts.get("publishable_pool_count")
        or 0
    )
    governance_scope_note = stage_b_candidate_coverage.get("scope_note") or (
        "项目治理候选池沿项目内部治理/对账口径构建，不等同于正式可筛选宇宙。"
    )
    lines = [
        "# 债券基金筛选分析报告",
        "",
        f"- 运行批次：`{summary['run_id']}`",
        f"- 生成时间：{summary['generated_at']}",
        f"- 天天基金 API：`{summary['data_sources']['tiantian']}`",
        (f"- 数据校验：{summary['data_sources'].get('validation_description')}（AutoStock 已停用）"
         if (summary.get("data_validation_audit") or {}).get("required")
         else f"- AutoStock API：`{summary['data_sources']['autostock']}`"),
        f"- 数据库：`{summary['data_sources']['database']}`",
        "",
        "## 1. 本轮执行范围",
        "",
        *core_execution_markdown((summary.get("internal_audit") or {}).get("core_flow_audit")),
        f"项目治理候选池、{validation_pool_label}与正式发布主榜属于不同层级，前者用于治理、审计和差异追踪，后者才是前端默认展示给用户的正式结果。",
        "",
        "## 2. 本轮筛选摘要",
        "",
        f"- 长期优质基金候选数：**{counts['long_candidates']}**",
        f"- 近期优质基金候选数：**{counts['short_candidates']}**",
        f"- table_coverage / 债基存储全集数量：**{table_coverage.get('base_fund_count', counts.get('stored_universe_count', 0))}**",
        f"- bond_universe_coverage / 活跃债基宇宙数量：**{bond_universe_coverage.get('base_fund_count', counts.get('active_universe_count', 0))}**",
        f"- bond_universe_coverage / 具备最新净值数量：**{bond_universe_coverage.get('has_latest_nav_count', counts.get('has_latest_nav_count', 0))}**",
        f"- bond_universe_coverage / 具备1年风险数量：**{bond_universe_coverage.get('has_1y_risk_metrics_count', counts.get('has_1y_risk_metrics_count', 0))}**",
        f"- screenable_universe_coverage / 具备年度涨幅历史数量：**{screenable_universe_coverage.get('annual_return_history_fund_count', counts.get('annual_return_history_fund_count', 0))}**",
        f"- screenable_universe_coverage / 具备同类排名历史数量：**{screenable_universe_coverage.get('rank_history_fund_count', counts.get('rank_history_fund_count', 0))}**",
        f"- screenable_universe_coverage / 正式可筛选宇宙数量：**{screenable_universe_coverage.get('base_fund_count', counts.get('screenable_universe_count', 0))}**",
        f"- 本地全量债基候选数：**{counts.get('project_universe_count', 0)}**",
        f"- 结构化排除数量（持有期 > 90 天）：**{counts['excluded_count']}**",
        f"- 项目治理候选池数量：**{governance_candidate_pool_count}**",
        f"- {validation_pool_label}数量：**{counts.get('autostock_passed_count', 0)}**",
        f"- 正式发布主榜数量：**{counts['final_count']}**",
        f"- 动态阈值参考榜数量：**{counts.get('dynamic_final_count', 0)}**",
        f"- 纯债稳健榜数量：**{counts.get('stable_bond_final_count', 0)}**",
        f"- 增强债弹性榜数量：**{counts.get('enhanced_bond_final_count', 0)}**",
        f"- 自动诊断池数量：**{counts['internal_watch_count']}**",
        f"- 自动诊断分流：不可申购 **{counts['not_buyable_watch_count']}**，质量告警 **{counts['quality_warning_watch_count']}**，绝对质量未达标 **{counts.get('absolute_quality_watch_count', 0)}**，经理从业与评分 **{counts.get('manager_stability_watch_count', 0)}**，自动发布门禁 **{counts.get('publish_post_review_watch_count', 0)}**",
        f"- 增强债基补充准入通过数量：**{counts.get('enhanced_supplemental_pass_count', 0)}**",
        f"- 长短期交集数量：**{counts.get('intersection_count', 0)}**",
        f"- 软交集补充数量：**{counts.get('soft_intersection_count', 0)}**",
        f"- 仅短期候选数量：**{counts.get('only_short_count', 0)}**",
        f"- 仅长期候选数量：**{counts.get('only_long_count', 0)}**",
        f"- {'跨接口核验' if (summary.get('data_validation_audit') or {}).get('required') else 'AutoStock'} 已校验候选：**{counts.get('autostock_validated_count', 0)}**，通过：**{counts.get('autostock_passed_count', 0)}**，拦截：**{counts.get('autostock_filtered_count', 0)}**",
        f"- 项目累积跟踪期数：**{counts.get('project_tracker_total_periods', 0)}**，当前仍在榜基金：**{counts.get('project_tracker_current_active_count', 0)}**，重复出现基金：**{counts.get('project_tracker_repeated_fund_count', 0)}**",
        f"- 项目治理候选池说明：{governance_scope_note}",
        "",
        "## 3. 优化项",
        "",
    ]
    for item in summary["optimizations"]:
        lines.append(f"- {item}")
    return lines


def _build_markdown_readiness_and_gate_lines(
    summary: dict[str, Any],
    *,
    settings: Settings,
) -> list[str]:
    project_publish_summary = summary.get("project_publish_summary") or {}
    forward_validation_summary = project_publish_summary.get("forward_validation_summary") or {}
    long_term_snapshot = (summary.get("database_freshness") or {}).get("long_term_history_snapshot") or {}
    long_term_governance_payload = _build_long_term_governance_markdown_payload(
        settings=settings,
        long_term_snapshot=long_term_snapshot,
    )
    ready_for_long_term_enhancement = bool(
        long_term_snapshot.get("ready_for_long_term_enhancement", long_term_snapshot.get("ready_for_scoring"))
    )
    long_term_blocking_reason = (
        long_term_snapshot.get("long_term_enhancement_blocking_reason")
        or long_term_snapshot.get("blocking_reason")
    )
    lines = [
        "",
        "## 3.1 长期稳定性数据就绪情况",
        "",
        f"- 治理摘要：{long_term_governance_payload.get('full_feature_ready_coverage_ratio', 0.0) * 100:.2f}% 样本已具备长周期全特征，"
        f"当前仍有 **{long_term_governance_payload.get('neutralized_fund_count', 0)}** 只基金按中性化方式进入后续链路。",
        f"- 治理分流：等待历史成熟 **{long_term_governance_payload.get('await_history_maturity_count', 0)}** 只，"
        f"本地重试池 **{long_term_governance_payload.get('local_retry_pool_count', 0)}** 只，"
        f"上游受限观察池 **{long_term_governance_payload.get('upstream_limited_watchlist_count', 0)}** 只。",
        "",
        "## 3.2 前瞻验证闭环",
        "",
        f"- 当前前瞻验证状态：**{forward_validation_summary.get('status_label', '未生成')}**",
        f"- 历史快照数：**{forward_validation_summary.get('snapshot_total', 0)}**",
        f"- 最新快照日期：**{forward_validation_summary.get('latest_snapshot_date', '') or '暂无'}**",
        f"- 基准口径：{forward_validation_summary.get('candidate_pool_definition', '未定义')}",
    ]
    if forward_validation_summary.get("blocking_reason"):
        lines.append(f"- 当前阻塞原因：`{forward_validation_summary.get('blocking_reason')}`")
    for item in (forward_validation_summary.get("window_summaries") or []):
        lines.append(
            f"- {item.get('窗口')}：已就绪快照 **{item.get('已就绪快照数', 0)}** / **{item.get('快照总数', 0)}**，"
            f"最新正式榜有效基金 **{item.get('最新正式榜有效基金数', 0)}**，"
            f"最新候选池有效基金 **{item.get('最新候选池有效基金数', 0)}**，状态 **{item.get('状态', '')}**"
        )
    lines.extend(
        [
            f"- 年度涨幅历史覆盖基金数：**{long_term_snapshot.get('annual_return_history_fund_count', 0)}**",
            f"- 同类排名历史覆盖基金数：**{long_term_snapshot.get('rank_history_fund_count', 0)}**",
            f"- 年度稳定性特征可用基金数：**{long_term_snapshot.get('annual_feature_ready_fund_count', 0)}**",
            f"- 排名持续性特征可用基金数：**{long_term_snapshot.get('rank_feature_ready_fund_count', 0)}**",
            f"- 长期特征完整可用基金数：**{long_term_snapshot.get('full_feature_ready_fund_count', 0)}**",
            f"- 长期特征按中性处理基金数：**{long_term_snapshot.get('neutralized_fund_count', 0)}**",
            f"- 年度历史缺口：**{long_term_governance_payload.get('annual_history_missing_total', 0)}**",
            f"- 排名历史缺口：**{long_term_governance_payload.get('rank_history_missing_total', 0)}**",
            f"- 等待历史成熟：**{long_term_governance_payload.get('await_history_maturity_count', 0)}**",
            f"- 本地重试池：**{long_term_governance_payload.get('local_retry_pool_count', 0)}**",
            f"- 上游受限观察池：**{long_term_governance_payload.get('upstream_limited_watchlist_count', 0)}**",
            f"- 上游空载荷观察：**{long_term_governance_payload.get('upstream_limited_empty_payload_total', 0)}**",
            f"- 年度涨幅历史记录数：**{long_term_snapshot.get('annual_return_record_count', 0)}**",
            f"- 同类排名历史记录数：**{long_term_snapshot.get('rank_history_record_count', 0)}**",
            f"- 当前长期增强特征是否具备充分覆盖：**{'是' if ready_for_long_term_enhancement else '否'}**",
            "- 当前策略：长期稳定性特征已开始计算与审计，但覆盖不足样本仍按中性处理，不直接拉低正式推荐结果。",
        ]
    )
    if long_term_blocking_reason and str(long_term_blocking_reason) != "ready":
        lines.append(f"- 当前长期增强特征覆盖不足原因：`{long_term_blocking_reason}`")
    if long_term_governance_payload.get("blocking_reason") and str(long_term_governance_payload.get("blocking_reason")) != str(long_term_blocking_reason):
        lines.append(f"- 治理阻塞原因补充：`{long_term_governance_payload.get('blocking_reason')}`")
    lines.extend(["", "## 4. 正式推荐门禁", ""])
    for item in project_publish_summary.get("gate_rules", []):
        lines.append(f"- {item}")
    return lines


def _build_markdown_threshold_audit_lines(summary: dict[str, Any]) -> list[str]:
    project_publish_summary = summary.get("project_publish_summary") or {}
    threshold_comparison = project_publish_summary.get("threshold_comparison") or {}
    risk_metric_cap_audit = threshold_comparison.get("risk_metric_cap_audit") or {}
    lines = [
        "",
        "## 5. 内部阈值审计",
        "",
        f"- 固定阈值基准通过：**{threshold_comparison.get('fixed_baseline_count', threshold_comparison.get('fixed_official_count', 0))}**",
        f"- 固定阈值自动发布门禁通过：**{threshold_comparison.get('fixed_post_review_count', 0)}**",
        f"- 动态阈值基准通过：**{threshold_comparison.get('dynamic_baseline_count', threshold_comparison.get('dynamic_official_count', 0))}**",
        f"- 动态阈值导出参考榜：**{threshold_comparison.get('dynamic_reference_export_count', threshold_comparison.get('dynamic_baseline_count', threshold_comparison.get('dynamic_official_count', 0)))}**",
        f"- 动态阈值自动发布门禁通过：**{threshold_comparison.get('dynamic_post_review_count', threshold_comparison.get('dynamic_official_count', 0))}**",
        f"- 双轨基准重合：**{threshold_comparison.get('overlap_count', 0)}**",
        f"- 仅固定阈值基准入选：**{threshold_comparison.get('fixed_only_count', 0)}**",
        f"- 仅动态阈值基准入选：**{threshold_comparison.get('dynamic_only_count', 0)}**",
        f"- 最终精选二次收敛剔除：**{threshold_comparison.get('final_selection_removed_count', 0)}**",
        f"- 增强补充准入通过：**{threshold_comparison.get('enhanced_supplemental_pass_count', 0)}**",
    ]
    if risk_metric_cap_audit:
        lines.extend(
            [
                f"- 风险指标 cap-hit / Sharpe：**{risk_metric_cap_audit.get('sharpe_cap_hit_count', 0)}** 只（**{risk_metric_cap_audit.get('sharpe_cap_hit_ratio', 0)}%**）",
                f"- 风险指标 cap-hit / Sortino：**{risk_metric_cap_audit.get('sortino_cap_hit_count', 0)}** 只（**{risk_metric_cap_audit.get('sortino_cap_hit_ratio', 0)}%**）",
                f"- 风险指标 cap-hit / 任一命中：**{risk_metric_cap_audit.get('either_cap_hit_count', 0)}** 只（**{risk_metric_cap_audit.get('either_cap_hit_ratio', 0)}%**）",
                f"- raw vs capped Top{risk_metric_cap_audit.get('top_n', 0)} 重排变化：**{risk_metric_cap_audit.get('top_n_changed_count', 0)}** 只，重合 **{risk_metric_cap_audit.get('top_n_overlap_count', 0)}** 只",
            ]
        )
    lines.append("")
    lines.append(
        _markdown_table(
            pd.DataFrame(threshold_comparison.get("difference_preview") or []),
            ["基金代码", "基金名称", "差异类型", "债基风格标签", "固定阈值理由", "动态阈值理由"],
        )
    )
    _append_markdown_preview_table(
        lines,
        rows=risk_metric_cap_audit.get("style_hit_rows"),
        columns=["债基风格标签", "样本数", "Sharpe cap-hit 数", "Sortino cap-hit 数", "任一 cap-hit 数", "任一 cap-hit 比例(%)"],
        label="风险指标 cap-hit 分风格分布：",
        leading_blank=True,
    )
    return lines


def _build_markdown_standard_screening_audit_lines(summary: dict[str, Any]) -> list[str]:
    artifacts = summary["artifacts"]
    project_publish_summary = summary.get("project_publish_summary") or {}
    standard_screening_audit = project_publish_summary.get("standard_screening_audit") or {}
    positive_ratio_semantics = project_publish_summary.get("positive_ratio_semantics") or {}
    low_vol_positive_ratio_shadow_watch = project_publish_summary.get("low_vol_positive_ratio_shadow_watch") or {}
    cap_hit_overview = standard_screening_audit.get("cap_hit_overview") or {}
    lines = [
        "",
        "## 5.1 标准筛选审计",
        "",
        f"- 高分被拒阈值：**{standard_screening_audit.get('high_score_threshold', HIGH_SCORE_AUDIT_THRESHOLD)}**",
        f"- 高分被拒样本数：**{standard_screening_audit.get('high_score_rejected_count', 0)}**",
        f"- 高分不可买样本数：**{standard_screening_audit.get('high_score_not_buyable_count', 0)}**",
        f"- 高分摩擦样本数：**{standard_screening_audit.get('high_score_friction_count', 0)}**",
        f"- 高分规模门槛样本数：**{standard_screening_audit.get('high_score_scale_gate_count', 0)}**",
        f"- 高分近门槛样本数：**{standard_screening_audit.get('high_score_scale_gate_near_count', 0)}**",
        f"- 候选惩罚项复审样本数：**{standard_screening_audit.get('high_score_scale_gate_candidate_penalty_count', 0)}**",
        f"- 高分费率门槛样本数：**{standard_screening_audit.get('high_score_fee_gate_count', 0)}**",
        f"- 任一 cap-hit 样本数：**{cap_hit_overview.get('either_cap_hit_count', 0)}**（**{cap_hit_overview.get('either_cap_hit_ratio', 0)}%**）",
        f"- TopN 重排变化：**{cap_hit_overview.get('top_n_changed_count', 0)}**",
        f"- 标准筛选审计工作簿：`{artifacts.get('screening_audit_excel')}`",
    ]
    score_cluster_explainability = standard_screening_audit.get("score_cluster_explainability") or {}
    annual_return_score_sharpe_coupling = (
        standard_screening_audit.get("annual_return_score_sharpe_coupling") or {}
    )
    risk_adjusted_metric_stack_review = (
        standard_screening_audit.get("risk_adjusted_metric_stack_review") or {}
    )
    if score_cluster_explainability.get("available"):
        judgement = score_cluster_explainability.get("judgement") or {}
        lines.extend(
            [
                f"- 得分簇命中样本数：**{score_cluster_explainability.get('score_cluster_hit_total', 0)}**（占质量拦截 **{score_cluster_explainability.get('score_cluster_hit_share_pct', 0)}%**）",
                f"- 得分簇疑似冗余：**{'是' if judgement.get('suspected_redundancy_present') else '否'}**；"
                f"综合加权得分覆盖比：**{judgement.get('weighted_anchor_coverage_ratio', 0)}**；"
                f"脱离综合加权得分的近期/长期独立命中：**{judgement.get('short_without_weighted_total', 0)} / {judgement.get('long_without_weighted_total', 0)}**",
                f"- 得分簇处置建议：{judgement.get('reason') or '-'}",
            ]
        )
    if annual_return_score_sharpe_coupling.get("available"):
        judgement = annual_return_score_sharpe_coupling.get("judgement") or {}
        dominant_local_boundary_review = (
            annual_return_score_sharpe_coupling.get("dominant_local_boundary_review") or {}
        )
        lines.extend(
            [
                f"- 年化/得分/夏普聚焦样本数：**{annual_return_score_sharpe_coupling.get('focus_scope_total', 0)}**（占质量拦截 **{annual_return_score_sharpe_coupling.get('focus_scope_share_pct', 0)}%**）",
                f"- 年化收益独立命中：**{judgement.get('annual_return_independent_hit_total', 0)}**；"
                f"与得分共现：**{judgement.get('annual_return_with_score_total', 0)}**；"
                f"与夏普共现：**{judgement.get('annual_return_with_sharpe_total', 0)}**；"
                f"三者共同命中：**{judgement.get('annual_return_with_score_and_sharpe_total', 0)}**",
                f"- 年化收益耦合占比：**{judgement.get('annual_return_coupled_share_pct', 0)}%**；"
                f"得分依赖年化收益占比：**{judgement.get('score_requires_annual_return_share_pct', 0)}%**；"
                f"夏普依赖年化收益占比：**{judgement.get('sharpe_requires_annual_return_share_pct', 0)}%**",
                f"- 年化收益局部边界复核：**{'是' if judgement.get('local_boundary_review_recommended_now') else '否'}**；"
                f"范围：**{judgement.get('local_boundary_review_scope') or '-'}**；"
                f"优先风格数：**{judgement.get('high_priority_local_review_style_total', 0)}**",
                f"- 年化收益局部边界主焦点：**{judgement.get('dominant_style_band_label') or '-'}**；"
                f"样本数：**{judgement.get('dominant_style_band_count', 0)}**；"
                f"占年化单独命中：**{judgement.get('dominant_style_band_share_pct', 0)}%**",
                f"- 主焦点局部复核建议：**{'是' if judgement.get('dominant_local_boundary_shadow_review_recommended_now') else '否'}**；"
                f"主焦点样本数：**{judgement.get('dominant_local_boundary_sample_count', 0)}**；"
                f"综合得分中位缓冲：**{judgement.get('dominant_local_boundary_weighted_margin_median', 0)}**；"
                f"年化中位差值：**{judgement.get('dominant_local_boundary_annual_gap_median_pct', 0)}pct**",
                f"- 年化/得分/夏普处置建议：{judgement.get('reason') or '-'}",
                f"- 局部边界复核说明：{judgement.get('local_boundary_review_reason') or '-'}",
            ]
        )
        if dominant_local_boundary_review.get("available"):
            lines.extend(
                [
                    f"- 主焦点局部复核动作：{dominant_local_boundary_review.get('reason') or '-'}",
                ]
            )
    if risk_adjusted_metric_stack_review.get("available"):
        judgement = risk_adjusted_metric_stack_review.get("judgement") or {}
        dominant_pair = judgement.get("dominant_intra_cluster_pair") or {}
        positive_ratio_governance = (
            risk_adjusted_metric_stack_review.get("positive_ratio_governance") or {}
        )
        lines.extend(
            [
                f"- 风险调整收益栈聚焦样本数：**{risk_adjusted_metric_stack_review.get('focus_scope_total', 0)}**",
                f"- 簇内高重叠 Pair 数：**{judgement.get('high_overlap_pair_total', 0)}**；"
                f"主导 Pair：**{dominant_pair.get('factor_a_label', '-')}/{dominant_pair.get('factor_b_label', '-')}**；"
                f"重叠比：**{dominant_pair.get('overlap_vs_smaller_ratio', 0)}**",
                f"- 夏普复核优先级：**{judgement.get('sharpe_review_priority', 'P3')}**；"
                f"`positive_ratio` 策略实质性：**{positive_ratio_governance.get('strategy_materiality_status') or 'unknown'}**",
                f"- 风险调整收益栈处置建议：{judgement.get('reason') or '-'}",
            ]
        )
    if positive_ratio_semantics:
        lines.extend(
            [
                f"- `positive_ratio` 项目口径：{positive_ratio_semantics.get('definition') or '1年日频正收益天数占比'}",
                f"- `positive_ratio` 指标性质：{positive_ratio_semantics.get('scope') or '项目内部稳定性代理指标'}，"
                f"{'不是' if not bool(positive_ratio_semantics.get('industry_standard')) else '是'}行业公共标准风险指标名词。",
            ]
        )
    if low_vol_positive_ratio_shadow_watch.get("available"):
        lines.extend(
            [
                f"- 低波 shadow 观察样本：**{low_vol_positive_ratio_shadow_watch.get('focus_sample_count', 0)}**",
                f"- 低波 shadow 可复算样本：**{low_vol_positive_ratio_shadow_watch.get('monthly_shadow_ready_count', 0)}**",
                f"- 低波自然月 vs 日频正收益率差 >= {int(LOW_VOL_POSITIVE_RATIO_GAP_ALERT_PCT)}pct 样本：**{low_vol_positive_ratio_shadow_watch.get('positive_ratio_gap_alert_count', 0)}**",
                f"- 低波 shadow 说明：{low_vol_positive_ratio_shadow_watch.get('explanation') or positive_ratio_semantics.get('explanation') or '当前仅作为解释增强，不替换生产阈值。'}",
            ]
        )
    preview_blocks = [
        ("reason_group_preview", "- 主原因分组 Top：", ["主原因分组", "数量", "占未入榜样本比例(%)"], False),
        ("quality_factor_group_preview", "- 质量筛选因子组 Top：", ["质量因子组", "数量", "占质量拦截样本比例(%)"], False),
        ("quality_factor_detail_preview", "- 质量细分因子命中 Top：", ["质量细分因子", "所属因子组", "命中样本数", "占质量拦截样本比例(%)"], False),
        ("quality_overlap_pair_preview", "- 质量高重叠 Pair Top：", ["因子A", "因子B", "共同命中样本数", "占质量拦截样本比例(%)"], False),
        ("quality_overlap_combo_preview", "- 质量高重叠 Combo Top：", ["重叠组合", "因子数", "样本数", "占质量拦截样本比例(%)"], False),
        ("high_score_friction_reason_preview", "- 高分摩擦主因 Top：", ["主排除原因", "样本数", "占高分摩擦样本比例(%)"], False),
        ("high_score_friction_style_preview", "- 高分摩擦风格 Top：", ["债基风格标签", "样本数", "占高分摩擦样本比例(%)"], False),
        ("high_score_scale_gate_style_preview", "- 高分规模门槛风格 Top：", ["债基风格标签", "样本数", "占专题样本比例(%)"], False),
        ("high_score_scale_gate_band_preview", "- 高分规模分层：", ["规模分层", "样本数", "占规模门槛专题比例(%)"], False),
        ("high_score_scale_gate_age_preview", "- 高分规模样本成立年限：", ["成立年限分层", "样本数", "占规模门槛专题比例(%)"], False),
        ("high_score_scale_gate_near_review_preview", "- 近门槛复审结论：", ["复审建议", "样本数", "占近门槛样本比例(%)"], False),
        ("high_score_fee_gate_style_preview", "- 高分费率门槛风格 Top：", ["债基风格标签", "样本数", "占专题样本比例(%)"], False),
        ("status_split_preview", "- 状态拆分概览：", ["状态维度", "状态值", "数量", "占未入榜样本比例(%)"], False),
        ("style_pass_rate_preview", None, ["债基风格标签", "治理候选数", "动态基准通过数", "正式发布数", "动态基准通过率(%)", "正式发布率(%)"], True),
    ]
    for key, label, columns, leading_blank in preview_blocks:
        _append_markdown_preview_table(
            lines,
            rows=standard_screening_audit.get(key),
            columns=columns,
            label=label,
            leading_blank=leading_blank,
        )
    if score_cluster_explainability.get("available"):
        lines.extend(
            [
                "",
                "- 得分簇组合拆解：",
                _markdown_table(
                    pd.DataFrame(score_cluster_explainability.get("combo_breakdown") or []),
                    ["combo_label", "count", "share_pct_of_score_cluster"],
                ),
            ]
        )
    if annual_return_score_sharpe_coupling.get("available"):
        lines.extend(
            [
                "",
                "- 年化/得分/夏普组合拆解：",
                _markdown_table(
                    pd.DataFrame(annual_return_score_sharpe_coupling.get("combo_breakdown") or []),
                    ["combo_label", "count", "share_pct_of_focus_scope"],
                ),
            ]
        )
        lines.extend(
            [
                "",
                "- 年化单独命中风格/得分带局部复核：",
                _markdown_table(
                    pd.DataFrame(
                        annual_return_score_sharpe_coupling.get(
                            "annual_return_only_local_review_priority_preview"
                        )
                        or []
                    ),
                    [
                        "债基风格标签",
                        "annual_return_only_count",
                        "dominant_score_band",
                        "dominant_score_band_count",
                        "dominant_score_band_share_within_style_pct",
                        "review_priority",
                        "review_action",
                    ],
                ),
            ]
        )
        lines.extend(
            [
                "",
                "- 年化单独命中风格 x 得分带 Top：",
                _markdown_table(
                    pd.DataFrame(
                        annual_return_score_sharpe_coupling.get(
                            "annual_return_only_style_band_preview"
                        )
                        or []
                    ),
                    [
                        "债基风格标签",
                        "score_band",
                        "annual_return_only_count",
                        "share_pct_of_annual_return_only",
                    ],
                ),
            ]
        )
        if dominant_local_boundary_review.get("available"):
            lines.extend(
                [
                    "",
                    "- 主焦点局部复核样本：",
                    _markdown_table(
                        pd.DataFrame(dominant_local_boundary_review.get("sample_preview") or []),
                        [
                            "基金代码",
                            "基金名称",
                            "综合加权得分",
                            "1年-年化收益(%)",
                            "annual_gap_pct",
                            "weighted_margin",
                            "short_margin",
                            "long_margin",
                            "是否可申购",
                            "AutoStock原始校验状态",
                        ],
                    ),
                ]
            )
    if risk_adjusted_metric_stack_review.get("available"):
        lines.extend(
            [
                "",
                "- 风险调整收益栈簇内 Pair 拆解：",
                _markdown_table(
                    pd.DataFrame(risk_adjusted_metric_stack_review.get("intra_cluster_pair_preview") or []),
                    ["factor_a_label", "factor_b_label", "pair_count", "overlap_vs_smaller_ratio"],
                ),
            ]
        )
    return lines


def _build_markdown_publish_and_recommendation_lines(
    summary: dict[str, Any],
    final_preview: pd.DataFrame,
) -> list[str]:
    counts = summary["counts"]
    project_publish_summary = summary.get("project_publish_summary") or {}
    publish_stage_audit = project_publish_summary.get("publish_stage_audit") or {}
    publish_reason_payload = _build_publish_reason_markdown_payload(summary)
    publish_reason_summary = publish_reason_payload["publish_reason_summary"]
    high_yield_ledger = project_publish_summary.get("high_yield_exclusion_ledger") or {}
    lines = [
        "",
        "## 6. 发布链阶段审计",
        "",
    ]
    for item in publish_stage_audit.get("headline_highlights") or []:
        lines.append(f"- {item}")
    if publish_reason_summary.get("headline_highlights"):
        lines.extend(
            [
                f"- 发布责任链摘要：{publish_reason_payload.get('responsibility_chain_headline') or publish_reason_summary.get('headline_highlights')[0]}",
                f"- 发布主链摘要：{(publish_reason_summary.get('narrative_summary') or {}).get('main_chain', '')}",
            ]
        )
    for item in publish_reason_payload.get("focus_metric_summary") or []:
        lines.append(
            f"- {item.get('label')}：**{item.get('intercepted_count', 0)}**（口径 `{item.get('count_basis', '-')}`）"
        )
    lines.append("")
    lines.append(
        _markdown_table(
            _build_publish_stage_audit_frame(publish_stage_audit),
            ["阶段", "范围", "输入样本数", "通过数", "淘汰数", "淘汰率(%)", "主原因1", "主原因2", "说明"],
        )
    )
    lines.extend(
        [
            "",
            "## 7. 高收益未入榜原因总账",
            "",
            f"- 未入正式榜总数：**{counts.get('stage_b_excluded_total', 0)}**",
            f"- 年化 ≥ 5% 未入榜：**{counts.get('stage_b_annual_ge_5_excluded_count', 0)}**",
            f"- 年化 ≥ 8% 未入榜：**{counts.get('stage_b_annual_ge_8_excluded_count', 0)}**",
            f"- 年化 ≥ 10% 未入榜：**{counts.get('stage_b_annual_ge_10_excluded_count', 0)}**",
        ]
    )
    for item in (high_yield_ledger.get("top_primary_reasons") or [])[:5]:
        lines.append(f"- 主排除原因 {item.get('reason')}: {item.get('count')} 只")
    lines.extend(
        [
            "",
            _markdown_table(
                pd.DataFrame(summary.get("stage_b_reason_preview") or []),
                ["基金代码", "基金名称", "主排除原因", "次排除原因", "1年-年化收益(%)", "推荐理由"],
            ),
            "",
            "## 8. 正式推荐基金预览",
            "",
            _markdown_table(
                final_preview,
                ["基金代码", "基金名称", FORMAL_GROUP_LABEL, "持有期状态", "债基风格标签", "长期稳定性标签", "是否可申购", "正式投资评分", "项目质量信心", "推荐备注"],
            ),
            "",
            "## 8. 正式推荐结构摘要",
            "",
        ]
    )
    for item in project_publish_summary.get("formal_group_distribution", []):
        lines.append(f"- 分组 {item.get(FORMAL_GROUP_LABEL)}: {item.get('数量')} 只，占比 {item.get('占正式推荐比例(%)')}%")
    for item in project_publish_summary.get("official_style_distribution", []):
        lines.append(f"- 风格 {item.get('风格标签')}: {item.get('数量')} 只，占比 {item.get('占正式推荐比例(%)')}%")
    for item in project_publish_summary.get("official_company_distribution", [])[:5]:
        lines.append(f"- 公司 {item.get('基金公司')}: {item.get('数量')} 只，占比 {item.get('占正式推荐比例(%)')}%")
    lines.extend(
        [
            "",
            "### 纯债稳健榜预览",
            "",
            _markdown_table(
                pd.DataFrame(summary.get("stable_final_preview") or []),
                ["基金代码", "基金名称", "持有期状态", "债基风格标签", "长期稳定性标签", "正式投资评分", "项目质量信心", "推荐备注"],
            ),
            "",
            "### 增强债弹性榜预览",
            "",
            _markdown_table(
                pd.DataFrame(summary.get("enhanced_final_preview") or []),
                ["基金代码", "基金名称", "持有期状态", "债基风格标签", "长期稳定性标签", "正式投资评分", "项目质量信心", "推荐备注"],
            ),
            "",
        ]
    )
    return lines


def _build_markdown_portfolio_lines(summary: dict[str, Any]) -> list[str]:
    project_publish_summary = summary.get("project_publish_summary") or {}
    portfolio_groups = (project_publish_summary.get("portfolio_suggestions") or {}).get("groups", [])
    lines = [
        "",
        "## 9. 组合建议（仅基于当前可获取字段）",
        "",
    ]
    if not portfolio_groups:
        lines.append("_暂无组合建议_")
        return lines
    for group in portfolio_groups:
        lines.extend(
            [
                f"### {group.get('name')}",
                "",
                str(group.get("description") or ""),
                "",
                _markdown_table(
                    pd.DataFrame(group.get("funds") or []),
                    [
                        "基金代码",
                        "基金名称",
                        "基金公司",
                        FORMAL_GROUP_LABEL,
                        "债基风格标签",
                        "基金规模(亿)",
                        "最新净值日期",
                        "费率合计(%)",
                        "正式投资评分",
                        "投资优先池得分",
                        "项目质量信心",
                        "出现次数",
                        "出现频率(%)",
                        "1年-年化收益(%)",
                        "近1月(%)",
                        "近3月(%)",
                        "1年-夏普比率",
                        "1年-最大回撤(%)",
                        "1年-波动率",
                        "AutoStock原始校验状态",
                        "长期稳定性标签",
                        "建议权重",
                        "入选原因",
                        "风险提示",
                    ],
                ),
                "",
            ]
        )
    return lines


def _build_markdown_watch_artifact_and_conclusion_lines(
    summary: dict[str, Any],
    excluded_preview: pd.DataFrame,
    internal_watch_preview: pd.DataFrame,
) -> list[str]:
    counts = summary["counts"]
    artifacts = summary["artifacts"]
    lines = [
        "## 10. 自动诊断摘要",
        "",
        f"- 自动诊断池文件：`{artifacts.get('watch_excel')}`",
        f"- 该池仅用于系统质控、自动诊断与候选分流，不作为用户主推荐列表展示。",
        "",
        _markdown_table(
            internal_watch_preview,
            ["基金代码", "基金名称", AUTO_DIAGNOSIS_CATEGORY_COLUMN, "持有期状态", "推荐理由", "债基风格标签", "是否可申购", "AutoStock原始校验状态"],
        ),
        "",
        "## 11. 已排除的长持有期债券基金预览",
        "",
        _markdown_table(
            excluded_preview,
            ["基金代码", "基金名称", "持有期天数", "持有期关键词", "排除原因"],
        ),
        "",
        "## 11.1 通用占位持有期观察样本",
        "",
        f"- 通用占位持有期排除数量：**{counts.get('generic_holding_placeholder_excluded_count', 0)}**",
        f"- 其中可申购数量：**{counts.get('generic_holding_placeholder_buyable_count', 0)}**",
        f"- 其中正式投资评分 ≥ 80 数量：**{counts.get('generic_holding_placeholder_high_score_count', 0)}**",
        "",
        _markdown_table(
            pd.DataFrame(summary.get("generic_holding_placeholder_preview") or []),
            ["基金代码", "基金名称", "持有期状态", "holding_period_source", "债基风格标签", "是否可申购", "正式投资评分", "项目质量信心", "推荐备注"],
        ),
        "",
        "## 12. 交付产物",
        "",
        f"- 正式发布主榜：`{artifacts['filtered_excel']}`",
        f"- 动态阈值审计底稿：`{artifacts.get('dynamic_filtered_excel')}`",
        f"- 阈值对比底稿：`{artifacts.get('threshold_compare_excel')}`",
        f"- 正式发布主榜 CSV：`{artifacts['filtered_csv']}`",
        f"- 自动诊断池：`{artifacts.get('watch_excel')}`",
        f"- 动态自动诊断池：`{artifacts.get('dynamic_watch_excel')}`",
        f"- 排除清单：`{artifacts['excluded_excel']}`",
        f"- 高收益未入榜原因总账：`{artifacts.get('stage_b_reason_excel')}`",
        f"- 标准筛选审计工作簿：`{artifacts.get('screening_audit_excel')}`",
        f"- 长短期合并主榜产物：`{artifacts['core'].get('final_excel')}`",
        f"- 长短期对比报告：`{artifacts['core'].get('comparison_excel')}`",
        f"- 优质基金累积跟踪：`{artifacts['core'].get('accumulation_tracker_excel')}`",
        (f"- 数据校验快照：`{artifacts.get('data_validation_snapshot_json_gz')}`"
         if (summary.get("data_validation_audit") or {}).get("required")
         else f"- AutoStock 审计文件：`{summary.get('audit_files', {}).get('autostock_validation_json')}`"),
        "",
        "## 13. 累积跟踪摘要",
        "",
        f"- 当前已累积 **{counts.get('project_tracker_total_periods', 0)}** 期正式发布结果。",
        f"- 当前在项目跟踪表中的基金 **{counts.get('project_tracker_fund_count', 0)}** 只。",
        f"- 当前最新一期仍在榜基金 **{counts.get('project_tracker_current_active_count', 0)}** 只。",
        f"- 达到“重复出现”标准的基金 **{counts.get('project_tracker_repeated_fund_count', 0)}** 只。",
        "",
        "## 14. 结论",
        "",
        "最终正式发布主榜由本项目债券筛选主链生成，再叠加结构化持有期排除与项目治理候选池门禁。对外主展示只保留更接近“可直接研究和考虑买入”的正式发布结果，避免把高分但不可买、存在校验告警或质量边际不足的基金直接暴露成最终推荐。",
        "",
    ]
    return lines


def build_markdown_report(
    settings: Settings,
    summary: dict[str, Any],
    final_preview: pd.DataFrame,
    excluded_preview: pd.DataFrame,
    internal_watch_preview: pd.DataFrame,
) -> str:
    lines: list[str] = []
    lines.extend(_build_markdown_overview_lines(summary))
    lines.extend(_build_markdown_readiness_and_gate_lines(summary, settings=settings))
    lines.extend(_build_markdown_threshold_audit_lines(summary))
    lines.extend(_build_markdown_standard_screening_audit_lines(summary))
    lines.extend(_build_markdown_publish_and_recommendation_lines(summary, final_preview))
    lines.extend(_build_markdown_portfolio_lines(summary))
    lines.extend(_build_markdown_watch_artifact_and_conclusion_lines(summary, excluded_preview, internal_watch_preview))
    return "\n".join(lines)
