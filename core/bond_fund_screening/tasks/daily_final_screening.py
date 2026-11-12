#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
每日最终筛选任务 - daily_final_screening

功能：
1. 读取长期优质基金Excel文件
2. 读取近期优质基金Excel文件
3. 计算两者的交集和差集
4. 生成完整的6工作表对比报告（即使交集为0）
5. 生成简化CSV文件

输出文件：
- 近期与长期优质基金对比报告_{timestamp}.xlsx（6个工作表）
- 仅近期优质_简版_{timestamp}.csv
- 仅长期优质_简版_{timestamp}.csv

执行顺序：
- 查找最新的长期优质基金文件
- 查找最新的近期优质基金文件
- 交集对比分析（增强版，支持0交集）
- 生成完整对比报告

使用方法：
    python3 -m bond_fund_screening.tasks.daily_final_screening
"""

import sys
import os
import argparse
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
CORE_ROOT = PROJECT_ROOT / "core"
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(CORE_ROOT) not in sys.path:
    sys.path.insert(0, str(CORE_ROOT))


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """尽早解析命令行参数，确保 `--help` 不触发后续重型导入。"""
    parser = argparse.ArgumentParser(description='债券基金每日最终筛选任务')
    return parser.parse_args(argv)


if __name__ == "__main__" and any(arg in {"-h", "--help"} for arg in sys.argv[1:]):
    parse_args()
    raise SystemExit(0)

from bond_fund_screening.tasks.bootstrap import ensure_project_venv


ensure_project_venv(PROJECT_ROOT)

import traceback
from functools import partial
from datetime import datetime
import pandas as pd
from bond_fund_screening.tasks.runtime_env import screening_report_date

from bond_fund_screening.config import (
    DAILY_SCREENING_CONFIG,
    OUTPUT_DIR,
    FINAL_SCREENING_POLICY,
    FINAL_SCREENING_POLICY_BY_FUND_TYPE,
)
from bond_fund_screening.core.data_loader import DataLoader
from bond_fund_screening.core.risk_calculator import RiskCalculator
from utils.calculation_capture import capture_call, capture_session
from bond_fund_screening.share_class_utils import normalize_base_fund_name
from bond_fund_screening.report_artifact_metadata import (
    collect_source_runs_from_report_meta,
    serialize_output_path_reference,
    write_derived_report_meta,
)
from bond_fund_screening.tasks.output_retention import (
    cleanup_same_day_versions_for_file,
    remove_same_day_family_versions,
)
from bond_fund_screening.report_catalog import load_report_manifest_entries, atomic_write_json_file
from bond_fund_screening.tasks import final_screening_autostock_helpers as autostock_helpers
from bond_fund_screening.tasks import final_screening_artifact_helpers as artifact_helpers
from bond_fund_screening.tasks import final_screening_candidate_helpers as candidate_helpers
from bond_fund_screening.tasks import final_screening_comparison_stage_helpers as comparison_stage_helpers
from bond_fund_screening.tasks import final_screening_common_helpers as common_helpers
from bond_fund_screening.tasks import final_screening_cross_source_helpers as cross_source_helpers
from bond_fund_screening.tasks import final_screening_database_helpers as database_helpers
from bond_fund_screening.tasks import final_screening_final_fund_helpers as final_fund_helpers
from bond_fund_screening.tasks import final_screening_input_lineage_helpers as input_lineage_helpers
from bond_fund_screening.tasks import final_screening_logging_helpers as logging_helpers
from bond_fund_screening.tasks import final_screening_mainlist_stage_helpers as mainlist_stage_helpers
from bond_fund_screening.tasks import final_screening_output_helpers as output_helpers
from bond_fund_screening.tasks import final_screening_quality_gate_helpers as quality_gate_helpers
from bond_fund_screening.tasks import final_screening_runtime_config_helpers as runtime_config_helpers
from bond_fund_screening.tasks import final_screening_sheet_helpers as sheet_helpers
from bond_fund_screening.tasks import final_screening_summary_helpers as summary_helpers
from bond_fund_screening.tasks import final_screening_watchlist_helpers as watchlist_helpers
from api.autostock_client import AutoStockClient
from api.autostock_endpoints import DataConverter
from database.models import FundNavHistory, Fund, FundReturns, FundRiskMetrics
from database.connection import resolve_db_path
from utils.bond_mainlist_audit import (
    build_bond_mainlist_audit_payload,
    write_bond_mainlist_audit_artifacts,
)
from utils.config_manager import get_config_manager
import re


_REPORT_DATE_COMPACT_RE = re.compile(r'(?<!\d)(?P<date>20\d{6})(?!\d)')
_REPORT_DATE_ISO_RE = re.compile(r'(?<!\d)(?P<date>20\d{2}-\d{2}-\d{2})(?!\d)')
STALE_RISK_METRIC_1Y_LAG_DAYS = runtime_config_helpers.STALE_RISK_METRIC_1Y_LAG_DAYS
FINAL_MAINLIST_REQUIRED_COLUMNS = runtime_config_helpers.FINAL_MAINLIST_REQUIRED_COLUMNS
AUTOSTOCK_MAINLIST_REQUIRED_LOCAL_COLUMNS = (
    runtime_config_helpers.AUTOSTOCK_MAINLIST_REQUIRED_LOCAL_COLUMNS
)
AUTOSTOCK_MAINLIST_RETURN_FIELD_MAPPINGS = (
    runtime_config_helpers.AUTOSTOCK_MAINLIST_RETURN_FIELD_MAPPINGS
)
FINAL_SCREENING_PRIMARY_SCORE_COLUMNS = (
    runtime_config_helpers.FINAL_SCREENING_PRIMARY_SCORE_COLUMNS
)
FINAL_SCREENING_FALLBACK_SCORE_COLUMNS = (
    runtime_config_helpers.FINAL_SCREENING_FALLBACK_SCORE_COLUMNS
)
FINAL_SCREENING_SOFT_INTERSECTION_MIN_CROSS_PERIODS = (
    runtime_config_helpers.FINAL_SCREENING_SOFT_INTERSECTION_MIN_CROSS_PERIODS
)
AUTOSTOCK_VALIDATION_EXPORT_COLUMNS = (
    runtime_config_helpers.AUTOSTOCK_VALIDATION_EXPORT_COLUMNS
)
AUTOSTOCK_VALIDATION_CACHE_TTL_SECONDS = (
    runtime_config_helpers.AUTOSTOCK_VALIDATION_CACHE_TTL_SECONDS
)
AUTOSTOCK_VALIDATION_CACHE_SCHEMA_VERSION = (
    runtime_config_helpers.AUTOSTOCK_VALIDATION_CACHE_SCHEMA_VERSION
)
FINAL_SCREENING_POLICY_SNAPSHOT_SCHEMA_VERSION = (
    runtime_config_helpers.FINAL_SCREENING_POLICY_SNAPSHOT_SCHEMA_VERSION
)
FINAL_SCREENING_DECISION_LEDGER_SCHEMA_VERSION = (
    runtime_config_helpers.FINAL_SCREENING_DECISION_LEDGER_SCHEMA_VERSION
)
FINAL_FUND_TYPE_LABEL_MAP = runtime_config_helpers.FINAL_FUND_TYPE_LABEL_MAP
_LAST_CROSS_SCORE_BASE_MAPS = {
    'short': {},
    'long': {},
}
_LAST_CROSS_PERIOD_COUNT_BASE_MAPS = {
    'short': {},
    'long': {},
}


def _reset_cross_fallback_base_maps() -> None:
    """重置最近一次 cross-score/cross-period 的母基金级缓存。"""
    _LAST_CROSS_SCORE_BASE_MAPS['short'] = {}
    _LAST_CROSS_SCORE_BASE_MAPS['long'] = {}
    _LAST_CROSS_PERIOD_COUNT_BASE_MAPS['short'] = {}
    _LAST_CROSS_PERIOD_COUNT_BASE_MAPS['long'] = {}


def _peek_cross_score_base_maps() -> tuple[dict, dict]:
    """返回最近一次加载的母基金级 cross-score 映射。"""
    return (
        dict(_LAST_CROSS_SCORE_BASE_MAPS.get('short') or {}),
        dict(_LAST_CROSS_SCORE_BASE_MAPS.get('long') or {}),
    )


def _peek_cross_period_count_base_maps() -> tuple[dict, dict]:
    """返回最近一次加载的母基金级 cross-period 映射。"""
    return (
        dict(_LAST_CROSS_PERIOD_COUNT_BASE_MAPS.get('short') or {}),
        dict(_LAST_CROSS_PERIOD_COUNT_BASE_MAPS.get('long') or {}),
    )


def _load_autostock_cross_validation_config() -> dict:
    """读取 AutoStock 交叉验证配置，统一对齐现有验证器口径。"""
    config = autostock_helpers.load_autostock_cross_validation_config(
        config_loader=lambda: get_config_manager().load_config(),
        env=os.environ,
        default_base_url=AutoStockClient.BASE_URL,
        safe_batch_size=AutoStockClient.SAFE_FUND_BATCH_SIZE,
        print_fn=print,
    )
    from backend.config import load_settings
    settings = load_settings()
    config['validation_provider'] = settings.publication_validation_provider
    if settings.publication_validation_provider == 'eastmoney_cross_endpoint':
        config['enabled'] = True
        config['supplier_group'] = 'eastmoney_tiantian'
        config['independent_source'] = False
    return config


def _build_autostock_validation_summary(reason: str | None = None) -> dict:
    """构建 AutoStock 主榜交叉验证摘要。"""
    return autostock_helpers.build_autostock_validation_summary(reason)


def _autostock_validation_cache_dir() -> Path:
    """返回 AutoStock 主榜交叉验证缓存目录。"""
    return autostock_helpers.autostock_validation_cache_dir(output_dir=OUTPUT_DIR)


def _build_autostock_validation_cache_key(
    candidate_codes: list[str],
    df_local: pd.DataFrame,
    *,
    fund_type: str | None = None,
    config_fingerprint: dict | None = None,
) -> str:
    """基于候选池、本地净值日期与基金类型构造稳定缓存键。"""
    return autostock_helpers.build_autostock_validation_cache_key(
        candidate_codes,
        df_local,
        fund_type=fund_type,
        config_fingerprint=config_fingerprint,
        normalize_fcode=_normalize_fcode,
        normalize_iso_date=autostock_helpers.normalize_iso_date,
        safe_float=autostock_helpers.safe_float,
        schema_version=AUTOSTOCK_VALIDATION_CACHE_SCHEMA_VERSION,
    )


def _load_autostock_validation_cache(cache_key: str) -> dict | None:
    """读取 AutoStock 主榜交叉验证缓存。"""
    return autostock_helpers.load_autostock_validation_cache(
        cache_key,
        cache_dir=_autostock_validation_cache_dir(),
        ttl_seconds=_resolve_autostock_validation_cache_ttl_seconds(),
        required_schema_version=AUTOSTOCK_VALIDATION_CACHE_SCHEMA_VERSION,
    )


def _load_recent_autostock_validation_caches() -> list[dict]:
    """加载 TTL 窗口内可用的 AutoStock 主榜交叉验证缓存，用于增量复用。"""
    return autostock_helpers.load_recent_autostock_validation_caches(
        cache_dir=_autostock_validation_cache_dir(),
        ttl_seconds=_resolve_autostock_validation_cache_ttl_seconds(),
        required_schema_version=AUTOSTOCK_VALIDATION_CACHE_SCHEMA_VERSION,
    )


def _build_autostock_cache_entry_map(payloads: list[dict] | None) -> dict[str, dict]:
    """按基金代码聚合最近缓存条目，供本轮校验做增量复用。"""
    return autostock_helpers.build_autostock_cache_entry_map(
        payloads,
        normalize_fcode=_normalize_fcode,
    )


def _write_autostock_validation_cache(cache_key: str, payload: dict) -> Path:
    """写出 AutoStock 主榜交叉验证缓存。"""
    return autostock_helpers.write_autostock_validation_cache(
        cache_key,
        payload,
        cache_dir=_autostock_validation_cache_dir(),
        json_writer=atomic_write_json_file,
    )


def _resolve_autostock_validation_cache_ttl_seconds() -> int:
    """解析 AutoStock 主榜交叉验证缓存 TTL。"""
    return autostock_helpers.resolve_autostock_validation_cache_ttl_seconds(
        config_loader=lambda: get_config_manager().load_config(),
        env=os.environ,
        default_ttl_seconds=AUTOSTOCK_VALIDATION_CACHE_TTL_SECONDS,
    )


def _warning_reasons_to_display_text(warning_reasons: list[str] | tuple[str, ...] | None) -> str:
    """将校验告警原因格式化为适合 Excel 展示的中文文本。"""
    return autostock_helpers.warning_reasons_to_display_text(warning_reasons)


def _build_autostock_validation_status_map(
    validation_rows: pd.DataFrame | None,
    *,
    failed_codes: set[str] | None = None,
) -> dict[str, dict]:
    """按基金代码汇总主榜校验状态，供导出和摘要复用。"""
    if isinstance(validation_rows, pd.DataFrame) and '校验来源' in validation_rows.columns:
        from backend.fund_cross_validation import validation_status_map
        status_map = validation_status_map(validation_rows)
        # Private compatibility keys keep selection rules identical. Public
        # exports use neutral fields and the actual provider below.
        for item in status_map.values():
            item['autostock_validation_status'] = item.get('validation_status', 'not_checked')
            item['autostock_warning_display'] = item.get('validation_warning_display', '')
            item['autostock_warning_reasons'] = item.get('validation_warning_reasons', [])
            item['autostock_hard_reasons'] = item.get('validation_hard_reasons', [])
        return status_map
    return autostock_helpers.build_autostock_validation_status_map(
        validation_rows,
        failed_codes=failed_codes,
        normalize_fcode=_normalize_fcode,
        warning_text_builder=_warning_reasons_to_display_text,
    )


def _build_autostock_local_signature(row) -> str:
    """为单只基金构造本地校验签名，用于同日缓存增量复用。"""
    return autostock_helpers.build_autostock_local_signature(
        row,
        normalize_fcode=_normalize_fcode,
        normalize_iso_date=autostock_helpers.normalize_iso_date,
        safe_float=autostock_helpers.safe_float,
    )


def _write_autostock_validation_audit_file(
    *,
    fund_type: str,
    timestamp: str,
    payload: dict,
    output_dir: Path | None = None,
) -> Path:
    """写出完整 AutoStock 审计 JSON，不面向用户下载，仅供审计与复核。"""
    return autostock_helpers.write_autostock_validation_audit_file(
        fund_type=fund_type,
        timestamp=timestamp,
        payload=payload,
        output_dir=Path(output_dir or OUTPUT_DIR),
        json_writer=atomic_write_json_file,
    )


def _build_autostock_validation_audit_payload(
    *,
    generated_at: datetime,
    fund_type: str,
    candidate_pool_count: int,
    validated_count: int,
    failed_codes: set[str],
    summary: dict,
    failed_df: pd.DataFrame,
    detail_df: pd.DataFrame,
) -> dict:
    """构造完整 AutoStock 校验审计载荷。"""
    return autostock_helpers.build_autostock_validation_audit_payload(
        generated_at=generated_at,
        fund_type=fund_type,
        candidate_pool_count=candidate_pool_count,
        validated_count=validated_count,
        failed_codes=failed_codes,
        summary=summary,
        failed_df=failed_df,
        detail_df=detail_df,
        normalize_fcode=_normalize_fcode,
    )


def _build_final_screening_policy_snapshot(
    *,
    fund_type: str,
    final_policy: dict,
    single_side_watchlist_limit: int,
    short_direct_score_column: str | None,
    long_direct_score_column: str | None,
    autostock_config: dict | None,
    min_cross_periods: int = FINAL_SCREENING_SOFT_INTERSECTION_MIN_CROSS_PERIODS,
) -> dict:
    """构造最终筛选策略快照，固化本次运行的关键参数。"""
    return artifact_helpers.build_final_screening_policy_snapshot(
        fund_type=fund_type,
        final_policy=final_policy,
        single_side_watchlist_limit=single_side_watchlist_limit,
        short_direct_score_column=short_direct_score_column,
        long_direct_score_column=long_direct_score_column,
        autostock_config=autostock_config,
        min_cross_periods=min_cross_periods,
        schema_version=FINAL_SCREENING_POLICY_SNAPSHOT_SCHEMA_VERSION,
        should_filter_holding_period_fn=_should_filter_holding_period_funds,
        should_apply_autostock_cross_validation_fn=_should_apply_autostock_cross_validation,
    )


def _write_final_screening_decision_ledger_file(
    *,
    fund_type: str,
    timestamp: str,
    payload: dict,
    output_dir: Path | None = None,
) -> Path:
    """写出最终筛选候选决策账本，仅供内部审计复盘。"""
    return artifact_helpers.write_final_screening_decision_ledger_file(
        fund_type=fund_type,
        timestamp=timestamp,
        payload=payload,
        output_dir=Path(output_dir or OUTPUT_DIR),
        json_writer=atomic_write_json_file,
    )


def _write_bond_mainlist_quality_audit_file(
    *,
    timestamp: str,
    payload: dict,
    output_dir: Path | None = None,
) -> tuple[Path, Path]:
    """写出债基正式主榜专项审计，仅供内部复盘。"""
    return artifact_helpers.write_bond_mainlist_quality_audit_file(
        timestamp=timestamp,
        payload=payload,
        output_dir=Path(output_dir or OUTPUT_DIR),
        audit_writer=write_bond_mainlist_audit_artifacts,
    )


def _build_bond_mainlist_quality_audit_payload(
    *,
    decision_ledger_file: Path,
) -> dict:
    """基于本轮决策账本构造债基正式主榜专项审计载荷。"""
    return build_bond_mainlist_audit_payload(
        db_path=str(resolve_db_path()),
        decision_path=decision_ledger_file,
    )


def _build_final_screening_decision_ledger_payload(
    *,
    generated_at: datetime,
    fund_type: str,
    policy_snapshot: dict,
    input_lineage: dict,
    final_screening_summary: dict,
    candidate_records: list[dict],
) -> dict:
    """构造最终筛选候选决策账本。"""
    return artifact_helpers.build_final_screening_decision_ledger_payload(
        generated_at=generated_at,
        fund_type=fund_type,
        policy_snapshot=policy_snapshot,
        input_lineage=input_lineage,
        final_screening_summary=final_screening_summary,
        candidate_records=candidate_records,
        schema_version=FINAL_SCREENING_DECISION_LEDGER_SCHEMA_VERSION,
    )


def _build_drawdown_duration_summary(reason: str | None = None) -> dict:
    """构建 1 年最长回撤观测数分析摘要。"""
    return quality_gate_helpers.build_drawdown_duration_summary(reason)


def _write_final_screening_report_sidecars(
    *,
    output_dir: Path,
    output_file: Path,
    final_fund_file: Path,
    short_term_file: Path,
    long_term_file: Path,
    fund_type: str,
    input_lineage: dict,
    final_screening_policy_snapshot: dict,
    final_screening_summary: dict,
    single_side_watchlist_limit: int,
    csv_short: Path | None,
    csv_long: Path | None,
    short_watchlist_csv: Path | None,
    long_watchlist_csv: Path | None,
    bond_short_term_one_year_observation_artifacts: list[dict] | None,
    autostock_validation_audit_file: Path | None,
    final_screening_decision_ledger_file: Path,
    bond_mainlist_quality_audit_json_file: Path | None,
    bond_mainlist_quality_audit_md_file: Path | None,
    refresh_manifest: bool = True,
) -> dict:
    """写出最终筛选主报告 sidecar，并返回元数据文件路径与序列化引用。"""
    return artifact_helpers.write_final_screening_report_sidecars(
        output_dir=output_dir,
        comparison_report_file=output_file,
        final_fund_file=final_fund_file,
        short_term_file=short_term_file,
        long_term_file=long_term_file,
        fund_type=fund_type,
        source_run_collector=collect_source_runs_from_report_meta,
        meta_writer=write_derived_report_meta,
        serializer=serialize_output_path_reference,
        input_lineage=input_lineage,
        final_screening_policy_snapshot=final_screening_policy_snapshot,
        final_screening_summary=final_screening_summary,
        single_side_watchlist_limit=single_side_watchlist_limit,
        csv_short=csv_short,
        csv_long=csv_long,
        short_watchlist_csv=short_watchlist_csv,
        long_watchlist_csv=long_watchlist_csv,
        bond_short_term_one_year_observation_artifacts=bond_short_term_one_year_observation_artifacts,
        autostock_validation_audit_file=autostock_validation_audit_file,
        final_screening_decision_ledger_file=final_screening_decision_ledger_file,
        bond_mainlist_quality_audit_json_file=bond_mainlist_quality_audit_json_file,
        bond_mainlist_quality_audit_md_file=bond_mainlist_quality_audit_md_file,
        refresh_manifest=refresh_manifest,
    )


def _cross_validate_candidate_pool_with_autostock(
    df_candidates: pd.DataFrame,
    *,
    df_local_complete: pd.DataFrame | None = None,
):
    """
    使用 AutoStock 对最终候选池做批量交叉验证。

    设计原则：
    1. 仅对最终主榜候选池做验证，避免全市场调用，控制免费额度；
    2. 必须使用批量接口，不逐只请求；
    3. 数值无法核验、缺失或超差的基金，不进入最终主榜与观察池。
    """
    from backend.config import load_settings
    settings = load_settings()
    if settings.publication_validation_provider == 'eastmoney_cross_endpoint':
        from backend.fund_cross_validation import validate_candidate_frame
        codes = [_normalize_fcode(value) for value in df_candidates.get('fcode', [])]
        local = df_local_complete if isinstance(df_local_complete, pd.DataFrame) else get_fund_complete_info(codes)
        return validate_candidate_frame(settings, df_candidates, local, _load_autostock_cross_validation_config())
    return quality_gate_helpers.cross_validate_candidate_pool_with_autostock(
        df_candidates,
        df_local_complete=df_local_complete,
        build_validation_summary_fn=_build_autostock_validation_summary,
        normalize_fcode_fn=_normalize_fcode,
        get_fund_complete_info_fn=get_fund_complete_info,
        required_local_columns=AUTOSTOCK_MAINLIST_REQUIRED_LOCAL_COLUMNS,
        load_autostock_cross_validation_config_fn=_load_autostock_cross_validation_config,
        build_autostock_validation_cache_key_fn=_build_autostock_validation_cache_key,
        load_autostock_validation_cache_fn=_load_autostock_validation_cache,
        build_autostock_local_signature_fn=_build_autostock_local_signature,
        build_autostock_cache_entry_map_fn=_build_autostock_cache_entry_map,
        load_recent_autostock_validation_caches_fn=_load_recent_autostock_validation_caches,
        autostock_client_cls=AutoStockClient,
        compare_candidate_with_autostock_fn=partial(
            autostock_helpers.compare_candidate_with_autostock,
            converter_cls=DataConverter,
            normalize_fcode=_normalize_fcode,
            normalize_iso_date_fn=autostock_helpers.normalize_iso_date,
            safe_float_fn=autostock_helpers.safe_float,
            is_type_match_fn=autostock_helpers.is_type_match,
            compare_return_with_autostock_fn=autostock_helpers.compare_return_with_autostock,
            return_field_mappings=AUTOSTOCK_MAINLIST_RETURN_FIELD_MAPPINGS,
        ),
        write_autostock_validation_cache_fn=_write_autostock_validation_cache,
        cache_schema_version=AUTOSTOCK_VALIDATION_CACHE_SCHEMA_VERSION,
        shared_state_path=(
            os.environ.get('AUTOSTOCK_RATE_LIMIT_STATE_PATH')
            or str(PROJECT_ROOT / 'backend' / 'data' / 'autostock_rate_limit_state.json')
        ),
    )


def _infer_fund_type(short_file: Path, long_file: Path) -> str:
    """从文件名推断基金类型。"""
    return common_helpers.infer_fund_type(short_file, long_file)


def _normalize_fcode(value):
    """统一基金代码格式，避免 Excel/SQLite 键格式不一致导致交叉匹配失败。"""
    return common_helpers.normalize_fcode(value)


def _normalize_fcodes_in_df(df: pd.DataFrame, dataset_name: str) -> pd.DataFrame:
    """标准化 DataFrame 的 fcode 列并清理非法/重复代码。"""
    return common_helpers.normalize_fcodes_in_df(
        df,
        dataset_name,
        normalize_fcode_fn=_normalize_fcode,
        print_fn=print,
    )


def _should_apply_autostock_cross_validation(fund_type: str) -> bool:
    """
    是否对最终主榜候选池执行 AutoStock 双源交叉验证。

    当前仅对两个外部 API 都能稳定提供可比字段的基金类型开启：
    - bond
    - stock
    - hybrid
    """
    return runtime_config_helpers.should_apply_autostock_cross_validation(fund_type)


def _get_project_db_path() -> Path:
    """返回项目数据库路径。"""
    return Path(resolve_db_path())


def _create_project_session():
    """创建项目数据库 Session。"""
    return database_helpers.create_project_session(
        db_path=_get_project_db_path(),
    )


def _resolve_display_annual_return_1y(risk_1y, returns):
    """为最终报告选择更贴近当前净值窗口的 1 年收益口径。"""
    return common_helpers.resolve_display_annual_return_1y(
        risk_1y,
        returns,
        stale_risk_metric_1y_lag_days=STALE_RISK_METRIC_1Y_LAG_DAYS,
    )


def _prepare_lookup_fcodes(fcodes: list) -> tuple:
    """
    预处理基金代码列表。

    Returns:
        (原始代码与标准代码映射列表, 去重后的标准代码列表)
    """
    return common_helpers.prepare_lookup_fcodes(
        fcodes,
        normalize_fcode_fn=_normalize_fcode,
    )


def _load_latest_nav_records(session, normalized_fcodes: list) -> dict:
    """批量加载每只基金的最新净值记录。"""
    return database_helpers.load_latest_nav_records(
        session,
        normalized_fcodes,
        fund_nav_history_model=FundNavHistory,
    )


def _resolve_final_screening_policy(policy: dict = None, fund_type: str | None = None) -> dict:
    """
    解析最终筛选策略配置并提供安全默认值。

    Args:
        policy: 可选覆盖配置，优先级高于默认配置和 fund_type 覆盖
        fund_type: 基金类型，可触发类型专用默认覆盖

    Returns:
        标准化后的策略字典
    """
    cfg = dict(FINAL_SCREENING_POLICY)
    normalized_fund_type = str(fund_type or '').strip().lower()
    if normalized_fund_type:
        cfg.update(FINAL_SCREENING_POLICY_BY_FUND_TYPE.get(normalized_fund_type, {}))
    if isinstance(policy, dict):
        cfg.update(policy)
    mainlist_selection_mode = str(
        cfg.get('mainlist_selection_mode', 'all_eligible') or 'all_eligible'
    ).strip() or 'all_eligible'
    anchor_limit_raw = cfg.get('mainlist_anchor_limit', cfg.get('top_n'))
    if anchor_limit_raw in (None, '', 0):
        mainlist_anchor_limit = None
    else:
        mainlist_anchor_limit = max(int(anchor_limit_raw), 1)
    watchlist_limit_raw = cfg.get('watchlist_limit', 20)
    return {
        'mainlist_anchor_limit': mainlist_anchor_limit,
        'watchlist_limit': max(int(watchlist_limit_raw or 20), 1),
        'short_score_weight': float(cfg.get('short_score_weight', 0.35)),
        'long_score_weight': float(cfg.get('long_score_weight', 0.50)),
        'hard_intersection_bonus': float(cfg.get('hard_intersection_bonus', 15.0)),
        'soft_intersection_bonus': float(cfg.get('soft_intersection_bonus', 8.0)),
        'mainlist_selection_mode': mainlist_selection_mode,
        'mainlist_score_band_delta': max(
            float(cfg.get('mainlist_score_band_delta', 0.0) or 0.0),
            0.0,
        ),
        'bond_subtype_edge_governance_enabled': bool(
            cfg.get('bond_subtype_edge_governance_enabled', False)
        ),
        'bond_subtype_edge_warn_share_pct': max(
            float(cfg.get('bond_subtype_edge_warn_share_pct', 70.0) or 70.0),
            0.0,
        ),
        'bond_subtype_edge_max_replacements': max(
            int(cfg.get('bond_subtype_edge_max_replacements', 0) or 0),
            0,
        ),
        'bond_subtype_edge_score_gap_tolerance': max(
            float(cfg.get('bond_subtype_edge_score_gap_tolerance', 0.0) or 0.0),
            0.0,
        ),
        'drawdown_duration_analysis_window_days': max(
            int(cfg.get('drawdown_duration_analysis_window_days', 365)),
            30,
        ),
        'drawdown_duration_nav_buffer_days': max(
            int(cfg.get('drawdown_duration_nav_buffer_days', 30)),
            0,
        ),
    }


def _should_filter_holding_period_funds(fund_type: str) -> bool:
    """
    是否在最终名单阶段过滤长持有期/定开基金。

    该过滤本质上是债券最终筛选的流动性约束，不应误伤股票型、混合型的
    合法持有期产品。
    """
    return runtime_config_helpers.should_filter_holding_period_funds(fund_type)


def _select_weighted_final_candidates(
    df_weighted: pd.DataFrame,
    fund_type: str,
    mainlist_anchor_limit: int,
    name_col: str = 'fund_name',
    collapse_share_classes_enabled: bool = False,
    excluded_fcodes: set[str] | None = None,
) -> pd.DataFrame:
    """
    从加权排序结果中选出最终候选。

    设计目标：
    1. 不改变原始排序语义，仍以加权得分降序为准
    2. 债券最终筛选在“选候选”阶段就过滤定开/持有期基金
    3. 在需要锚点模式时尽量维持 mainlist_anchor_limit 的目标规模，避免先截断后过滤导致数量缩水

    Returns:
        最终候选 DataFrame
    """
    return candidate_helpers.select_weighted_final_candidates(
        df_weighted,
        fund_type=fund_type,
        top_n_limit=mainlist_anchor_limit,
        name_col=name_col,
        collapse_share_classes_enabled=collapse_share_classes_enabled,
        excluded_fcodes=excluded_fcodes,
        normalize_fcode_fn=_normalize_fcode,
        should_filter_holding_period_fn=_should_filter_holding_period_funds,
        has_fixed_holding_period_fn=has_fixed_holding_period,
    )


def _summarize_candidate_pool_reduction(
    df_weighted: pd.DataFrame,
    *,
    fund_type: str,
    name_col: str = 'fund_name',
    collapse_share_classes_enabled: bool = False,
) -> tuple[pd.DataFrame, dict]:
    """返回候选池压缩后的数据与结构化压缩来源统计。"""
    return candidate_helpers.summarize_candidate_pool_reduction(
        df_weighted,
        fund_type=fund_type,
        name_col=name_col,
        collapse_share_classes_enabled=collapse_share_classes_enabled,
        should_filter_holding_period_fn=_should_filter_holding_period_funds,
        has_fixed_holding_period_fn=has_fixed_holding_period,
    )


def _sort_weighted_candidates(df: pd.DataFrame, score_col: str = 'weighted_score') -> pd.DataFrame:
    """最终名单统一按加权分稳定排序，同分时按基金代码升序。"""
    return candidate_helpers.sort_weighted_candidates(df, score_col)


def _resolve_final_screening_score_column(df: pd.DataFrame) -> str | None:
    """
    解析最终筛选主权重应使用的评分列。

    设计原则：
    1. 优先使用“平均综合评分/avg_combo_score”，与 screening_picks 的
       selected_combo_score 聚合口径保持一致；
    2. 仅在历史产物缺少平均评分列时，才回退到“综合稳定性得分”；
    3. 避免主榜直接分与软交集补充分落在不同分值尺度上。
    """
    return candidate_helpers.resolve_final_screening_score_column(
        df,
        primary_score_columns=FINAL_SCREENING_PRIMARY_SCORE_COLUMNS,
        fallback_score_columns=FINAL_SCREENING_FALLBACK_SCORE_COLUMNS,
    )


def _resolve_final_screening_direct_score_column(
    df: pd.DataFrame,
    *,
    fund_type: str | None = None,
) -> str | None:
    """
    解析最终主榜对“直接入围候选”应使用的评分列。

    设计原则：
    1. 债券链的近期/长期详细报告本质是多周期稳定性总榜，直接候选应优先沿用
       综合稳定性得分，避免在最终主榜阶段把周期数与稳定性信息再次弱化为平均分；
    2. 股票/混合链仍保持现有平均分优先口径，避免在未完成专项审计前误改生产语义；
    3. 若目标列不存在，再回退到通用解析逻辑，保持向后兼容。
    """
    return candidate_helpers.resolve_final_screening_direct_score_column(
        df,
        fund_type=fund_type,
        primary_score_columns=FINAL_SCREENING_PRIMARY_SCORE_COLUMNS,
        fallback_score_columns=FINAL_SCREENING_FALLBACK_SCORE_COLUMNS,
    )


def _resolve_frame_name_column(df: pd.DataFrame) -> str | None:
    """解析 DataFrame 中可用于母基金归一化的名称列。"""
    return candidate_helpers.resolve_frame_name_column(df)


def _ensure_base_fund_name_column(df: pd.DataFrame) -> pd.DataFrame:
    """确保 DataFrame 包含可用的 base_fund_name 列。"""
    return candidate_helpers.ensure_base_fund_name_column(df)


def _build_base_representative_lookup(
    df: pd.DataFrame,
    *,
    score_col: str | None,
) -> dict[str, pd.Series]:
    """
    按母基金构建代表记录映射。

    说明：
    1. 输入详细报告理论上已经过母基金去重，但为稳妥起见仍按当前评分列排序后再取首条；
    2. 这样可避免测试桩或历史产物仍包含 A/C 份额重复时，代表记录不稳定。
    """
    return candidate_helpers.build_base_representative_lookup(df, score_col=score_col)


def _build_weighted_final_candidate_frame(
    *,
    df_short: pd.DataFrame,
    df_long: pd.DataFrame,
    fund_type: str,
    short_score_col: str | None,
    long_score_col: str | None,
    cross_short_scores: dict,
    cross_long_scores: dict,
    cross_short_period_counts: dict | None = None,
    cross_long_period_counts: dict | None = None,
    cross_short_base_scores: dict | None = None,
    cross_long_base_scores: dict | None = None,
    cross_short_base_period_counts: dict | None = None,
    cross_long_base_period_counts: dict | None = None,
    policy: dict | None = None,
    min_cross_periods: int = FINAL_SCREENING_SOFT_INTERSECTION_MIN_CROSS_PERIODS,
) -> tuple[pd.DataFrame, dict]:
    """
    构建最终主榜候选池。

    设计原则：
    1. 分类统计以母基金维度为准，避免短/长期分别挑中不同份额时被误判为单边/邻近交集；
    2. 行级候选仍保留代码维度，后续继续沿用现有持有期过滤、份额折叠与导出链路；
    3. 真正只在单边出现的基金，才允许使用 screening_picks 的 cross-score 做软交集回填。
    """
    return candidate_helpers.build_weighted_final_candidate_frame(
        df_short=df_short,
        df_long=df_long,
        fund_type=fund_type,
        short_score_col=short_score_col,
        long_score_col=long_score_col,
        cross_short_scores=cross_short_scores,
        cross_long_scores=cross_long_scores,
        cross_short_period_counts=cross_short_period_counts,
        cross_long_period_counts=cross_long_period_counts,
        cross_short_base_scores=cross_short_base_scores,
        cross_long_base_scores=cross_long_base_scores,
        cross_short_base_period_counts=cross_short_base_period_counts,
        cross_long_base_period_counts=cross_long_base_period_counts,
        policy=policy,
        min_cross_periods=min_cross_periods,
        resolve_policy_fn=_resolve_final_screening_policy,
        normalize_fcode_fn=_normalize_fcode,
    )


def _build_soft_intersection_audit_map(
    df_weighted: pd.DataFrame,
    *,
    cross_short_scores: dict | None,
    cross_long_scores: dict | None,
    cross_short_period_counts: dict | None = None,
    cross_long_period_counts: dict | None = None,
    cross_short_base_scores: dict | None = None,
    cross_long_base_scores: dict | None = None,
    cross_short_base_period_counts: dict | None = None,
    cross_long_base_period_counts: dict | None = None,
) -> dict[str, dict]:
    """为邻近交集候选构造解释字段，说明其由哪侧、哪种口径补齐。"""
    return candidate_helpers.build_soft_intersection_audit_map(
        df_weighted,
        cross_short_scores=cross_short_scores,
        cross_long_scores=cross_long_scores,
        cross_short_period_counts=cross_short_period_counts,
        cross_long_period_counts=cross_long_period_counts,
        cross_short_base_scores=cross_short_base_scores,
        cross_long_base_scores=cross_long_base_scores,
        cross_short_base_period_counts=cross_short_base_period_counts,
        cross_long_base_period_counts=cross_long_base_period_counts,
        normalize_fcode_fn=_normalize_fcode,
        safe_float_fn=autostock_helpers.safe_float,
    )


def _filter_final_candidates_by_data_completeness(
    df_weighted: pd.DataFrame,
    *,
    required_columns: tuple[str, ...] = FINAL_MAINLIST_REQUIRED_COLUMNS,
) -> tuple[set[str], dict, pd.DataFrame, pd.DataFrame]:
    """
    识别关键投资指标不完整的候选基金。

    设计原则：
    1. 最终主榜与累积跟踪不能接纳关键指标缺失的基金；
    2. 缺失时直接排除，不做任何估算或填补；
    3. 若校验链路本身异常，则回退原有候选池，避免把“校验失败”误判为“数据缺失”。
    """
    return quality_gate_helpers.filter_final_candidates_by_data_completeness(
        df_weighted,
        required_columns=required_columns,
        normalize_fcode_fn=_normalize_fcode,
        get_fund_complete_info_fn=get_fund_complete_info,
        print_fn=print,
    )


@capture_call('final_screening.drawdown_duration')
def _analyze_candidate_pool_drawdown_duration(
    df_candidates: pd.DataFrame,
    *,
    analysis_window_days: int,
    nav_buffer_days: int = 30,
) -> tuple[pd.DataFrame, dict]:
    """
    基于本地 1 年净值历史重算最长回撤观测数，并回写到候选池。

    设计目标：
    1. 只依赖本地 fund_nav_history，避免新增外部 API 成本；
    2. 与 RiskCalculator 保持同一计算口径；
    3. 当前阶段只做分析与披露，不直接引入主榜硬过滤，避免误杀浅回撤但恢复慢的债基。
    """
    return quality_gate_helpers.analyze_candidate_pool_drawdown_duration(
        df_candidates,
        analysis_window_days=analysis_window_days,
        nav_buffer_days=nav_buffer_days,
        build_drawdown_duration_summary_fn=_build_drawdown_duration_summary,
        normalize_fcode_fn=_normalize_fcode,
        data_loader_factory=DataLoader,
        db_path_getter=_get_project_db_path,
        risk_calculator_factory=RiskCalculator,
        print_fn=print,
    )


def _build_strategy_validation_note(fund_type: str) -> str:
    """返回经实跑验证后的分类型策略说明。"""
    return watchlist_helpers.build_strategy_validation_note(fund_type)


def _build_final_screening_explanations(
    *,
    fund_type: str,
    union_candidate_count: int,
    eligible_pool_count: int,
    dedup_or_filtered_removed: int,
    top_n_trimmed: int,
    final_output_count: int,
    intersection_count: int,
    soft_intersection_count: int,
    only_short_count: int,
    only_long_count: int,
    final_top_category_counts: list[dict] | None,
    watchlist_reason_counts: list[dict] | None,
    candidate_pool_reduction: dict | None,
    data_completeness_filtered: int = 0,
    autostock_filtered: int = 0,
    drawdown_duration_distribution: dict | None = None,
    mainlist_selection_mode: str = 'fixed_top_n',
    mainlist_selection_anchor_top_n: int | None = None,
    mainlist_score_band_delta: float = 0.0,
    mainlist_selection_expanded_count: int = 0,
    bond_subtype_edge_governance: dict | None = None,
) -> dict:
    """构建最终筛选面向用户的解释摘要。"""
    return watchlist_helpers.build_final_screening_explanations(
        fund_type=fund_type,
        union_candidate_count=union_candidate_count,
        eligible_pool_count=eligible_pool_count,
        dedup_or_filtered_removed=dedup_or_filtered_removed,
        top_n_trimmed=top_n_trimmed,
        final_output_count=final_output_count,
        intersection_count=intersection_count,
        soft_intersection_count=soft_intersection_count,
        only_short_count=only_short_count,
        only_long_count=only_long_count,
        final_top_category_counts=final_top_category_counts,
        watchlist_reason_counts=watchlist_reason_counts,
        candidate_pool_reduction=candidate_pool_reduction,
        data_completeness_filtered=data_completeness_filtered,
        autostock_filtered=autostock_filtered,
        drawdown_duration_distribution=drawdown_duration_distribution,
        mainlist_selection_mode=mainlist_selection_mode,
        mainlist_selection_anchor_top_n=mainlist_selection_anchor_top_n,
        mainlist_score_band_delta=mainlist_score_band_delta,
        mainlist_selection_expanded_count=mainlist_selection_expanded_count,
        bond_subtype_edge_governance=bond_subtype_edge_governance,
    )


def _export_single_side_watchlist(
    df_watchlist: pd.DataFrame,
    *,
    output_dir: Path,
    timestamp: str,
    filename_prefix: str,
) -> Path | None:
    """导出单边候选观察池 CSV，保留解释所需的份额聚合信息。"""
    return watchlist_helpers.export_single_side_watchlist(
        df_watchlist,
        output_dir=output_dir,
        timestamp=timestamp,
        filename_prefix=filename_prefix,
    )


def _get_score_from_df(df: pd.DataFrame, fcode: str, score_col: str | None) -> float | None:
    """
    从 DataFrame 获取基金得分。

    Returns:
        float: 找到的实际得分，允许合法 0.0
        None: 不存在该基金/列缺失/值为空
    """
    return candidate_helpers.get_score_from_df(df, fcode, score_col)


def _resolve_score_with_fallback(primary_score: float | None, fallback_scores: dict, fcode: str) -> float | None:
    """仅当主来源缺失时才回退到补充分值，避免把合法 0 分误当成缺失。"""
    return candidate_helpers.resolve_score_with_fallback(primary_score, fallback_scores, fcode)


def _resolve_score_with_base_fallback(
    primary_score: float | None,
    fallback_scores: dict | None,
    fcode: str,
    *,
    base_name: str | None = None,
    fallback_base_scores: dict | None = None,
) -> float | None:
    """在代码级缺失时允许按母基金口径回退补充分值。"""
    return candidate_helpers.resolve_score_with_base_fallback(
        primary_score,
        fallback_scores,
        fcode,
        base_name=base_name,
        fallback_base_scores=fallback_base_scores,
        normalize_fcode_fn=_normalize_fcode,
    )


def _resolve_cross_period_count_with_base_fallback(
    fallback_period_counts: dict | None,
    fcode: str,
    *,
    base_name: str | None = None,
    fallback_base_period_counts: dict | None = None,
) -> int:
    """在代码级缺失时允许按母基金口径回退多周期支撑数。"""
    return candidate_helpers.resolve_cross_period_count_with_base_fallback(
        fallback_period_counts,
        fcode,
        base_name=base_name,
        fallback_base_period_counts=fallback_base_period_counts,
        normalize_fcode_fn=_normalize_fcode,
    )


def _build_soft_intersection(
    *,
    codes_short: set,
    codes_long: set,
    intersection: set,
    df_short: pd.DataFrame,
    df_long: pd.DataFrame,
    short_score_col: str | None,
    long_score_col: str | None,
    cross_short_scores: dict,
    cross_long_scores: dict,
    cross_short_period_counts: dict | None = None,
    cross_long_period_counts: dict | None = None,
    cross_short_base_scores: dict | None = None,
    cross_long_base_scores: dict | None = None,
    cross_short_base_period_counts: dict | None = None,
    cross_long_base_period_counts: dict | None = None,
    min_cross_periods: int = 1,
) -> set:
    """
    计算软交集。

    语义：
    1. 不在硬交集中
    2. 长短期双方都存在可用得分
    3. 合法 0.0 分也应视为“有得分”，不能误判为缺失
    4. 若某一侧得分仅来自 cross-score 回填，则该侧至少应具备最小多周期支撑数
    """
    return candidate_helpers.build_soft_intersection(
        codes_short=codes_short,
        codes_long=codes_long,
        intersection=intersection,
        df_short=df_short,
        df_long=df_long,
        short_score_col=short_score_col,
        long_score_col=long_score_col,
        cross_short_scores=cross_short_scores,
        cross_long_scores=cross_long_scores,
        cross_short_period_counts=cross_short_period_counts,
        cross_long_period_counts=cross_long_period_counts,
        cross_short_base_scores=cross_short_base_scores,
        cross_long_base_scores=cross_long_base_scores,
        cross_short_base_period_counts=cross_short_base_period_counts,
        cross_long_base_period_counts=cross_long_base_period_counts,
        min_cross_periods=min_cross_periods,
        normalize_fcode_fn=_normalize_fcode,
    )


def _normalize_report_date(value) -> str | None:
    """将多种日期输入规范化为 YYYY-MM-DD。"""
    return input_lineage_helpers.normalize_report_date(value)


def extract_report_date(report_file: Path) -> str | None:
    """从详细报告文件名中提取发布日期。"""
    return input_lineage_helpers.extract_report_date(
        report_file,
        compact_pattern=_REPORT_DATE_COMPACT_RE,
        iso_pattern=_REPORT_DATE_ISO_RE,
        normalize_report_date_fn=_normalize_report_date,
    )


def _build_input_lineage_meta(
    short_term_file: Path,
    long_term_file: Path,
    today_date: str | None = None,
) -> dict:
    """构建最终筛选输入文件的来源血缘信息。"""
    return input_lineage_helpers.build_input_lineage_meta(
        short_term_file,
        long_term_file,
        today_date=today_date,
        normalize_report_date_fn=_normalize_report_date,
        extract_report_date_fn=extract_report_date,
    )


def _load_cross_scores(
    fund_type: str = 'bond',
    short_target_date: str | None = None,
    long_target_date: str | None = None,
) -> tuple:
    """
    从 screening_picks 表加载指定基金类型的近期和长期评分。

    v1.9.69 P1: 用于修复软交集死锁——非硬交集基金只存在于一侧 Excel，
    从 Excel DataFrame 查找对侧得分永远为 0，导致软交集永远输出 0 只。
    改为从数据库直接取双侧平均得分，跨越 Excel 文件边界。

    v1.9.70 P1: 增加 fund_type 参数，SQL PARTITION BY 加入 fund_type 维度，
    避免不同类型基金共享 period_years 导致只取到最后运��类型的数据。

    Args:
        fund_type: 基金类型 ('bond', 'stock', 'hybrid')

    Returns:
        (short_scores, long_scores): 两个 {fcode: avg_score} 字典
        近期对应 period_years < 1，长期对应 period_years >= 1。
        分数口径优先 selected_combo_score，缺失时回退 combo_score。
        若传入 short_target_date / long_target_date，则分别与输入报告日期对齐。
        失败时返回两个空字典
    """
    _reset_cross_fallback_base_maps()
    runtime = cross_source_helpers.build_cross_source_runtime(
        project_db_path_getter=_get_project_db_path,
        normalize_fcode_fn=_normalize_fcode,
        normalize_report_date_fn=_normalize_report_date,
        normalize_base_fund_name_fn=normalize_base_fund_name,
    )
    short_scores, long_scores, short_base_scores, long_base_scores = (
        cross_source_helpers.load_cross_scores_with_base_maps(
            fund_type=fund_type,
            short_target_date=short_target_date,
            long_target_date=long_target_date,
            **runtime,
        )
    )
    _LAST_CROSS_SCORE_BASE_MAPS['short'] = short_base_scores
    _LAST_CROSS_SCORE_BASE_MAPS['long'] = long_base_scores
    return short_scores, long_scores


def _load_cross_period_counts(
    fund_type: str = 'bond',
    short_target_date: str | None = None,
    long_target_date: str | None = None,
) -> tuple[dict, dict]:
    """
    加载 cross-score 候选在短期/长期侧分别覆盖了多少个周期。

    用途：
    1. 软交集不能只因为某只基金在对侧单个周期偶然入围就被认定为“双侧有效”；
    2. 当得分来自 screening_picks 回填时，至少要求它在对应侧达到与主输入文件一致的
       多周期支撑下限，避免把单周期噪声放大成邻近交集。
    """
    _LAST_CROSS_PERIOD_COUNT_BASE_MAPS['short'] = {}
    _LAST_CROSS_PERIOD_COUNT_BASE_MAPS['long'] = {}
    runtime = cross_source_helpers.build_cross_source_runtime(
        project_db_path_getter=_get_project_db_path,
        normalize_fcode_fn=_normalize_fcode,
        normalize_report_date_fn=_normalize_report_date,
        normalize_base_fund_name_fn=normalize_base_fund_name,
    )
    short_counts, long_counts, short_base_counts, long_base_counts = (
        cross_source_helpers.load_cross_period_counts_with_base_maps(
            fund_type=fund_type,
            short_target_date=short_target_date,
            long_target_date=long_target_date,
            **runtime,
        )
    )
    _LAST_CROSS_PERIOD_COUNT_BASE_MAPS['short'] = short_base_counts
    _LAST_CROSS_PERIOD_COUNT_BASE_MAPS['long'] = long_base_counts
    return short_counts, long_counts


def find_required_input_file(
    today_pattern: str,
    fallback_pattern: str,
    directory: Path,
    label: str,
    allow_stale: bool = None,
) -> Path:
    """
    查找最终筛选所需输入文件。

    默认强制要求使用当天产物；仅在显式开启 allow_stale 时才回退到最近文件。
    """
    return input_lineage_helpers.find_required_input_file(
        today_pattern=today_pattern,
        fallback_pattern=fallback_pattern,
        directory=directory,
        label=label,
        allow_stale=allow_stale,
        allow_stale_fn=lambda: input_lineage_helpers.allow_stale_final_inputs(env=os.environ),
        find_latest_file_fn=input_lineage_helpers.find_latest_file,
        print_fn=print,
    )


def find_required_input_pair(
    *,
    short_today_pattern: str,
    short_fallback_pattern: str,
    short_label: str,
    long_today_pattern: str,
    long_fallback_pattern: str,
    long_label: str,
    directory: Path,
    allow_stale: bool | None = None,
) -> tuple[Path, Path]:
    """
    为长短期对比选择同一业务日的一对输入文件。

    默认要求当天两侧产物都存在；显式开启 stale 后，回退到最近共同业务日，
    避免分别回退各自最新文件而混用不同业务日数据。
    """
    return input_lineage_helpers.find_required_input_pair(
        short_today_pattern=short_today_pattern,
        short_fallback_pattern=short_fallback_pattern,
        short_label=short_label,
        long_today_pattern=long_today_pattern,
        long_fallback_pattern=long_fallback_pattern,
        long_label=long_label,
        directory=directory,
        allow_stale=allow_stale,
        allow_stale_fn=lambda: input_lineage_helpers.allow_stale_final_inputs(env=os.environ),
        collect_preferred_lineage_files_by_report_date_fn=partial(
            input_lineage_helpers.collect_preferred_lineage_files_by_report_date,
            manifest_loader=load_report_manifest_entries,
            extract_report_date_fn=extract_report_date,
        ),
        collect_latest_files_by_report_date_fn=partial(
            input_lineage_helpers.collect_latest_files_by_report_date,
            extract_report_date_fn=extract_report_date,
        ),
        find_latest_file_fn=input_lineage_helpers.find_latest_file,
        normalize_report_date_fn=_normalize_report_date,
        compact_date_pattern=_REPORT_DATE_COMPACT_RE,
        iso_date_pattern=_REPORT_DATE_ISO_RE,
        print_fn=print,
    )


def get_bond_final_input_patterns(today: str) -> dict:
    """返回债券日终筛选所依赖的长短期详细报告文件模式。"""
    return runtime_config_helpers.get_bond_final_input_patterns(today)


def get_latest_nav_data(fcodes: list, session=None) -> dict:
    """
    从数据库获取基金的最新日增长率和日期

    Args:
        fcodes: 基金代码列表

    Returns:
        字典: {fcode: {'daily_growth': float, 'nav_date': date}}
    """
    return database_helpers.fetch_latest_nav_data(
        fcodes,
        session=session,
        project_db_path_getter=_get_project_db_path,
        create_project_session_fn=_create_project_session,
        prepare_lookup_fcodes_fn=_prepare_lookup_fcodes,
        load_latest_nav_records_fn=_load_latest_nav_records,
        print_fn=print,
    )


def has_fixed_holding_period(fund_name: str) -> bool:
    """
    判断基金是否有固定持有期

    Args:
        fund_name: 基金名称

    Returns:
        是否有持有期
    """
    from bond_fund_screening.config import LONG_HOLDING_KEYWORDS
    return common_helpers.has_fixed_holding_period(
        fund_name,
        holding_keywords=LONG_HOLDING_KEYWORDS,
    )


@capture_call('final_screening.fund_rows', omit=('session',))
def get_fund_complete_info(fcodes: list, session=None) -> pd.DataFrame:
    """
    从数据库获取基金的完整信息

    包括:
    - 基金基础信息 (Fund表)
    - 收益表现 (FundReturns表)
    - 近一年风险指标 (FundRiskMetrics表, period='1y')
    - 最新日增长率和日期 (FundNavHistory表)

    Args:
        fcodes: 基金代码列表

    Returns:
        DataFrame包含所有基金信息
    """
    return database_helpers.fetch_fund_complete_info(
        fcodes,
        session=session,
        project_db_path_getter=_get_project_db_path,
        create_project_session_fn=_create_project_session,
        prepare_lookup_fcodes_fn=_prepare_lookup_fcodes,
        load_latest_nav_records_fn=_load_latest_nav_records,
        fund_model=Fund,
        returns_model=FundReturns,
        risk_model=FundRiskMetrics,
        resolve_display_annual_return_1y_fn=_resolve_display_annual_return_1y,
        print_fn=print,
        traceback_print_fn=traceback.print_exc,
    )


def generate_enhanced_comparison_report(
    short_term_file: Path,
    long_term_file: Path,
    output_dir: Path,
    *,
    output_prefix: str | None = None,
    output_timestamp: str | None = None,
    today_date: str | None = None,
) -> dict:
    """
    生成增强版对比报告（支持0交集）

    Args:
        short_term_file: 近期优质基金文件路径
        long_term_file: 长期优质基金文件路径
        output_dir: 输出目录
        output_prefix: 可选输出前缀，用于隔离不同基金类型的并发产物
        output_timestamp: 可选输出时间戳，未传入时使用当前秒级时间
        today_date: 可选目标业务日（YYYY-MM-DD），用于回测/历史审计时覆盖输入对齐判断

    Returns:
        结果字典，包含统计信息
    """
    logging_helpers.print_enhanced_comparison_report_header(print_fn=print)

    comparison_analysis = comparison_stage_helpers.run_comparison_input_analysis_stage(
        short_term_file=short_term_file,
        long_term_file=long_term_file,
        today_date=today_date,
        min_cross_periods=FINAL_SCREENING_SOFT_INTERSECTION_MIN_CROSS_PERIODS,
        load_final_screening_input_frame_fn=partial(
            sheet_helpers.load_final_screening_input_frame,
            print_fn=print,
        ),
        normalize_fcodes_in_df_fn=_normalize_fcodes_in_df,
        ensure_base_fund_name_column_fn=_ensure_base_fund_name_column,
        build_final_screening_code_stats_fn=sheet_helpers.build_final_screening_code_stats,
        infer_fund_type_fn=_infer_fund_type,
        resolve_final_screening_direct_score_column_fn=_resolve_final_screening_direct_score_column,
        build_input_lineage_meta_fn=_build_input_lineage_meta,
        load_cross_scores_fn=_load_cross_scores,
        load_cross_period_counts_fn=_load_cross_period_counts,
        peek_cross_score_base_maps_fn=_peek_cross_score_base_maps,
        peek_cross_period_count_base_maps_fn=_peek_cross_period_count_base_maps,
        resolve_final_screening_policy_fn=_resolve_final_screening_policy,
        build_weighted_final_candidate_frame_fn=_build_weighted_final_candidate_frame,
        normalize_fcode_fn=_normalize_fcode,
        print_report_step_header_fn=partial(
            logging_helpers.print_report_step_header,
            print_fn=print,
        ),
        print_code_stats_fn=partial(
            logging_helpers.print_code_stats,
            print_fn=print,
        ),
        print_input_lineage_summary_fn=partial(
            logging_helpers.print_input_lineage_summary,
            print_fn=print,
        ),
        print_cross_score_summary_fn=partial(
            logging_helpers.print_cross_score_summary,
            print_fn=print,
        ),
        print_cross_score_degraded_fn=partial(
            logging_helpers.print_cross_score_degraded,
            print_fn=print,
        ),
        print_candidate_summary_fn=partial(
            logging_helpers.print_candidate_summary,
            print_fn=print,
        ),
    )
    df_short = comparison_analysis.df_short
    df_long = comparison_analysis.df_long
    code_stats = comparison_analysis.code_stats
    codes_short = code_stats['codes_short']
    codes_long = code_stats['codes_long']
    fund_type = comparison_analysis.fund_type
    input_lineage = comparison_analysis.input_lineage
    short_report_date = comparison_analysis.short_report_date
    long_report_date = comparison_analysis.long_report_date
    short_score_col = comparison_analysis.short_score_col
    long_score_col = comparison_analysis.long_score_col
    cross_short_scores = comparison_analysis.cross_source.short_scores
    cross_long_scores = comparison_analysis.cross_source.long_scores
    cross_short_period_counts = comparison_analysis.cross_source.short_period_counts
    cross_long_period_counts = comparison_analysis.cross_source.long_period_counts
    cross_short_base_scores = comparison_analysis.cross_source.short_base_scores
    cross_long_base_scores = comparison_analysis.cross_source.long_base_scores
    cross_short_base_period_counts = (
        comparison_analysis.cross_source.short_base_period_counts
    )
    cross_long_base_period_counts = (
        comparison_analysis.cross_source.long_base_period_counts
    )
    final_policy = comparison_analysis.final_policy
    df_weighted = comparison_analysis.df_weighted
    candidate_summary = comparison_analysis.candidate_summary

    comparison_export = comparison_stage_helpers.run_comparison_report_export_stage(
        output_dir=output_dir,
        output_prefix=output_prefix,
        output_timestamp=output_timestamp,
        short_term_file=short_term_file,
        long_term_file=long_term_file,
        df_short=df_short,
        df_long=df_long,
        code_stats=code_stats,
        candidate_summary=candidate_summary,
        input_lineage=input_lineage,
        short_report_date=short_report_date,
        long_report_date=long_report_date,
        intersection_base_names=comparison_analysis.intersection_base_names,
        effective_only_short=comparison_analysis.effective_only_short,
        effective_only_long=comparison_analysis.effective_only_long,
        build_intersection_sheet_fn=partial(
            sheet_helpers.build_intersection_sheet,
            normalize_fcode_fn=_normalize_fcode,
            get_latest_nav_data_fn=get_latest_nav_data,
            print_fn=print,
        ),
        build_single_side_sheet_fn=sheet_helpers.build_single_side_sheet,
        build_summary_sheet_fn=partial(
            sheet_helpers.build_summary_sheet,
            generated_at=datetime.now(),
        ),
        build_final_output_name_fn=artifact_helpers.build_final_output_name,
        write_comparison_report_workbook_fn=partial(
            output_helpers.write_comparison_report_workbook,
            holding_period_detector=has_fixed_holding_period,
        ),
        export_simple_csv_fn=output_helpers.export_simple_csv,
        print_report_step_header_fn=partial(
            logging_helpers.print_report_step_header,
            print_fn=print,
        ),
        print_sheet_preparation_done_fn=partial(
            logging_helpers.print_sheet_preparation_done,
            print_fn=print,
        ),
        print_comparison_report_written_fn=partial(
            logging_helpers.print_comparison_report_written,
            print_fn=print,
        ),
        print_markdown_skipped_fn=partial(
            logging_helpers.print_markdown_skipped,
            print_fn=print,
        ),
        print_simple_csv_written_fn=partial(
            logging_helpers.print_simple_csv_written,
            print_fn=print,
        ),
    )
    timestamp = comparison_export.timestamp
    output_file = comparison_export.output_file
    csv_short = comparison_export.csv_short
    csv_long = comparison_export.csv_long
    short_watchlist_csv = None
    long_watchlist_csv = None
    autostock_validation_audit_file = None
    final_screening_decision_ledger_file = None
    bond_mainlist_quality_audit_json_file = None
    bond_mainlist_quality_audit_md_file = None

    # ========================================================================
    # 步骤7: 生成当日最终优质基金文件（供累积跟踪使用）
    # 🔧 优化：采用加权并集策略，不仅限于交集
    # ========================================================================
    logging_helpers.print_report_step_header(
        "步骤7: 生成当日最终优质基金文件（从数据库提取完整信息）",
        print_fn=print,
    )

    final_fund_file = output_dir / artifact_helpers.build_final_output_name(
        f"当日最终优质基金_{timestamp}.xlsx",
        output_prefix=output_prefix,
    )

    mainlist_anchor_limit = final_policy.get('mainlist_anchor_limit')
    single_side_watchlist_limit = int(final_policy.get('watchlist_limit', 20) or 20)
    autostock_enabled = _should_apply_autostock_cross_validation(fund_type)
    autostock_config = (
        _load_autostock_cross_validation_config()
        if autostock_enabled
        else None
    )
    final_screening_policy_snapshot = _build_final_screening_policy_snapshot(
        fund_type=fund_type,
        final_policy=final_policy,
        single_side_watchlist_limit=single_side_watchlist_limit,
        short_direct_score_column=short_score_col,
        long_direct_score_column=long_score_col,
        autostock_config=autostock_config,
        min_cross_periods=FINAL_SCREENING_SOFT_INTERSECTION_MIN_CROSS_PERIODS,
    )

    logging_helpers.print_weighted_union_policy(
        mainlist_anchor_limit=mainlist_anchor_limit,
        watchlist_limit=single_side_watchlist_limit,
        short_weight=final_policy['short_score_weight'],
        long_weight=final_policy['long_score_weight'],
        hard_intersection_bonus=final_policy['hard_intersection_bonus'],
        soft_intersection_bonus=final_policy['soft_intersection_bonus'],
        mainlist_selection_mode=str(
            final_policy.get('mainlist_selection_mode', 'fixed_top_n') or 'fixed_top_n'
        ),
        mainlist_score_band_delta=float(
            final_policy.get('mainlist_score_band_delta', 0.0) or 0.0
        ),
        print_fn=print,
    )

    try:
        mainlist_pipeline = mainlist_stage_helpers.run_mainlist_pipeline(
            final_fund_file=final_fund_file,
            df_short=df_short,
            df_long=df_long,
            df_weighted=df_weighted,
            fund_type=fund_type,
            final_policy=final_policy,
            mainlist_anchor_limit=mainlist_anchor_limit,
            single_side_watchlist_limit=single_side_watchlist_limit,
            candidate_summary=candidate_summary,
            autostock_enabled=autostock_enabled,
            raw_intersection_count=candidate_summary.get(
                'raw_intersection_count',
                len(code_stats['raw_intersection']),
            ),
            autostock_validation_status_map=None,
            short_report_date=short_report_date,
            long_report_date=long_report_date,
            input_pair_status_label=input_lineage['input_pair_status_label'],
            cross_short_scores=cross_short_scores,
            cross_long_scores=cross_long_scores,
            cross_short_period_counts=cross_short_period_counts,
            cross_long_period_counts=cross_long_period_counts,
            cross_short_base_scores=cross_short_base_scores,
            cross_long_base_scores=cross_long_base_scores,
            cross_short_base_period_counts=cross_short_base_period_counts,
            cross_long_base_period_counts=cross_long_base_period_counts,
            output_dir=output_dir,
            timestamp=timestamp,
            output_prefix=output_prefix,
            normalize_fcode_fn=_normalize_fcode,
            filter_final_candidates_by_data_completeness_fn=_filter_final_candidates_by_data_completeness,
            summarize_candidate_pool_reduction_fn=_summarize_candidate_pool_reduction,
            build_autostock_validation_summary_fn=_build_autostock_validation_summary,
            cross_validate_candidate_pool_with_autostock_fn=_cross_validate_candidate_pool_with_autostock,
            build_autostock_validation_status_map_fn=_build_autostock_validation_status_map,
            analyze_candidate_pool_drawdown_duration_fn=_analyze_candidate_pool_drawdown_duration,
            build_drawdown_duration_summary_fn=_build_drawdown_duration_summary,
            build_final_selection_summary_fn=partial(
                summary_helpers.build_final_selection_summary,
                final_mainlist_required_columns=FINAL_MAINLIST_REQUIRED_COLUMNS,
            ),
            build_bond_mainlist_structure_audit_fn=(
                summary_helpers.build_bond_mainlist_structure_audit
            ),
            build_bond_fee_semantics_audit_fn=(
                summary_helpers.build_bond_fee_semantics_audit
            ),
            build_watchlist_bundle_fn=partial(
                summary_helpers.build_watchlist_bundle,
                build_single_side_watchlist_fn=partial(
                    candidate_helpers.build_single_side_watchlist,
                    normalize_fcode_fn=_normalize_fcode,
                    should_filter_holding_period_fn=_should_filter_holding_period_funds,
                    has_fixed_holding_period_fn=has_fixed_holding_period,
                ),
                rebind_watchlist_fn=watchlist_helpers.rebind_watchlist_to_validated_representatives,
                annotate_watchlist_fn=watchlist_helpers.annotate_watchlist_with_mainlist_context,
                summarize_watchlist_context_fn=watchlist_helpers.summarize_watchlist_context,
                build_explanations_fn=_build_final_screening_explanations,
                build_soft_intersection_audit_map_fn=_build_soft_intersection_audit_map,
                build_decision_ledger_records_fn=partial(
                    watchlist_helpers.build_final_screening_decision_ledger_records,
                    final_mainlist_required_columns=FINAL_MAINLIST_REQUIRED_COLUMNS,
                    normalize_fcode_fn=_normalize_fcode,
                    should_filter_holding_period_fn=_should_filter_holding_period_funds,
                    has_fixed_holding_period_fn=has_fixed_holding_period,
                ),
            ),
            export_single_side_watchlist_fn=watchlist_helpers.export_single_side_watchlist,
            build_final_output_name_fn=artifact_helpers.build_final_output_name,
            get_fund_complete_info_fn=get_fund_complete_info,
            load_sales_fee_validation_classification_map_fn=(
                database_helpers.load_sales_fee_validation_classification_map
            ),
            sort_weighted_candidates_fn=_sort_weighted_candidates,
            should_filter_holding_period_fn=_should_filter_holding_period_funds,
            has_fixed_holding_period_fn=has_fixed_holding_period,
            fund_type_label_map=FINAL_FUND_TYPE_LABEL_MAP,
            write_final_fund_workbook_fn=partial(
                output_helpers.write_final_fund_workbook,
                holding_period_detector=has_fixed_holding_period,
            ),
            print_completeness_summary_fn=partial(
                logging_helpers.print_completeness_summary,
                print_fn=print,
            ),
            print_autostock_validation_summary_fn=partial(
                logging_helpers.print_autostock_validation_summary,
                print_fn=print,
            ),
            print_autostock_validation_skipped_fn=partial(
                logging_helpers.print_autostock_validation_skipped,
                print_fn=print,
            ),
            print_drawdown_duration_summary_fn=partial(
                logging_helpers.print_drawdown_duration_summary,
                print_fn=print,
            ),
            print_weighted_union_statistics_fn=partial(
                logging_helpers.print_weighted_union_statistics,
                print_fn=print,
            ),
            print_watchlist_exports_fn=partial(
                logging_helpers.print_watchlist_exports,
                print_fn=print,
            ),
            print_complete_info_fetch_start_fn=partial(
                logging_helpers.print_complete_info_fetch_start,
                print_fn=print,
            ),
            print_complete_info_fetch_result_fn=partial(
                logging_helpers.print_complete_info_fetch_result,
                print_fn=print,
            ),
            print_holding_period_filter_status_fn=partial(
                logging_helpers.print_holding_period_filter_status,
                print_fn=print,
            ),
            print_export_cleanup_summary_fn=partial(
                logging_helpers.print_export_cleanup_summary,
                print_fn=print,
            ),
            print_final_fund_workbook_written_fn=partial(
                logging_helpers.print_final_fund_workbook_written,
                print_fn=print,
            ),
        )
        quality_gate_stage = mainlist_pipeline.quality_gate
        selection_stage = mainlist_pipeline.selection
        df_weighted_selected_pool = selection_stage.df_weighted_selected_pool
        final_screening_summary = selection_stage.final_screening_summary
        short_watchlist_csv = selection_stage.short_watchlist_csv
        long_watchlist_csv = selection_stage.long_watchlist_csv
        final_screening_decision_records = selection_stage.final_screening_decision_records
        prevalidation_selected_pool_count = (
            quality_gate_stage.prevalidation_selected_pool_count
        )
        autostock_failed_fcodes = quality_gate_stage.autostock_failed_fcodes
        autostock_validation_summary = quality_gate_stage.autostock_validation_summary
        autostock_failed_candidates_df = (
            quality_gate_stage.autostock_failed_candidates_df
        )
        autostock_validation_detail_df = (
            quality_gate_stage.autostock_validation_detail_df
        )
        bond_short_term_one_year_observation_artifacts = (
            artifact_helpers.collect_bond_short_term_one_year_observation_artifacts(short_term_file)
        )

        artifact_finalize = artifact_helpers.finalize_final_screening_artifacts(
            fund_type=fund_type,
            timestamp=timestamp,
            output_dir=output_dir,
            output_prefix=output_prefix,
            output_file=output_file,
            final_fund_file=final_fund_file,
            short_term_file=short_term_file,
            long_term_file=long_term_file,
            input_lineage=input_lineage,
            final_screening_policy_snapshot=final_screening_policy_snapshot,
            final_screening_summary=final_screening_summary,
            single_side_watchlist_limit=single_side_watchlist_limit,
            csv_short=csv_short,
            csv_long=csv_long,
            short_watchlist_csv=short_watchlist_csv,
            long_watchlist_csv=long_watchlist_csv,
            bond_short_term_one_year_observation_artifacts=bond_short_term_one_year_observation_artifacts,
            autostock_validation_audit_enabled=autostock_enabled,
            autostock_validation_audit_payload=(
                _build_autostock_validation_audit_payload(
                    generated_at=datetime.now(),
                    fund_type=fund_type,
                    candidate_pool_count=prevalidation_selected_pool_count,
                    validated_count=len(df_weighted_selected_pool),
                    failed_codes=autostock_failed_fcodes,
                    summary=autostock_validation_summary,
                    failed_df=autostock_failed_candidates_df,
                    detail_df=autostock_validation_detail_df,
                )
                if autostock_enabled
                else None
            ),
            decision_ledger_payload=_build_final_screening_decision_ledger_payload(
                generated_at=datetime.now(),
                fund_type=fund_type,
                policy_snapshot=final_screening_policy_snapshot,
                input_lineage=input_lineage,
                final_screening_summary=final_screening_summary,
                candidate_records=final_screening_decision_records,
            ),
            build_bond_mainlist_quality_audit_payload_fn=(
                _build_bond_mainlist_quality_audit_payload
                if fund_type == 'bond' else None
            ),
            write_autostock_validation_audit_file_fn=partial(
                _write_autostock_validation_audit_file,
                output_dir=output_dir,
            ),
            write_final_screening_decision_ledger_file_fn=partial(
                _write_final_screening_decision_ledger_file,
                output_dir=output_dir,
            ),
            write_bond_mainlist_quality_audit_file_fn=partial(
                _write_bond_mainlist_quality_audit_file,
                output_dir=output_dir,
            ),
            write_final_screening_report_sidecars_fn=_write_final_screening_report_sidecars,
            cleanup_final_screening_same_day_outputs_fn=partial(
                artifact_helpers.cleanup_final_screening_same_day_outputs,
                cleanup_file_versions=cleanup_same_day_versions_for_file,
                cleanup_family_versions=remove_same_day_family_versions,
            ),
            print_fn=print,
        )
        autostock_validation_audit_file = artifact_finalize['autostock_validation_audit_file']
        final_screening_decision_ledger_file = artifact_finalize['final_screening_decision_ledger_file']
        bond_mainlist_quality_audit_json_file = artifact_finalize['bond_mainlist_quality_audit_json_file']
        bond_mainlist_quality_audit_md_file = artifact_finalize['bond_mainlist_quality_audit_md_file']
    except Exception:
        artifact_helpers.cleanup_partial_final_screening_outputs(
            comparison_report_file=output_file,
            final_fund_file=final_fund_file,
            csv_short=csv_short,
            csv_long=csv_long,
            short_watchlist_csv=short_watchlist_csv,
            long_watchlist_csv=long_watchlist_csv,
            autostock_validation_audit_file=autostock_validation_audit_file,
            final_screening_decision_ledger_file=final_screening_decision_ledger_file,
            bond_mainlist_quality_audit_json_file=bond_mainlist_quality_audit_json_file,
            bond_mainlist_quality_audit_md_file=bond_mainlist_quality_audit_md_file,
            print_fn=print,
        )
        raise

    return artifact_helpers.build_final_screening_result_payload(
        candidate_summary=candidate_summary,
        codes_short=codes_short,
        codes_long=codes_long,
        output_file=output_file,
        final_fund_file=final_fund_file,
        csv_short=csv_short,
        csv_long=csv_long,
        short_watchlist_csv=short_watchlist_csv,
        long_watchlist_csv=long_watchlist_csv,
        bond_short_term_one_year_observation_artifacts=bond_short_term_one_year_observation_artifacts,
        input_lineage=input_lineage,
        short_report_date=short_report_date,
        long_report_date=long_report_date,
        comparison_report_meta_file=artifact_finalize['comparison_report_meta_file'],
        final_fund_report_meta_file=artifact_finalize['final_fund_report_meta_file'],
        autostock_validation_audit_file=artifact_finalize['autostock_validation_audit_file'],
        final_screening_policy_snapshot=final_screening_policy_snapshot,
        final_screening_decision_ledger_file=artifact_finalize['final_screening_decision_ledger_file'],
        bond_mainlist_quality_audit_json_file=artifact_finalize['bond_mainlist_quality_audit_json_file'],
        bond_mainlist_quality_audit_md_file=artifact_finalize['bond_mainlist_quality_audit_md_file'],
        final_screening_summary=final_screening_summary,
        same_day_cleanup_deleted_count=artifact_finalize['same_day_cleanup_deleted_count'],
    )


@capture_session('final_screening')
def main():
    """主函数"""
    start_time = datetime.now()
    logging_helpers.print_main_task_start(start_time, print_fn=print)

    # 获取配置
    config = DAILY_SCREENING_CONFIG
    output_dir = config['output_dir']

    # 步骤1: 查找长期优质基金文件
    logging_helpers.print_main_step_header(1, "查找长期优质基金文件", print_fn=print)

    # 先尝试查找今天生成的文件
    # Use the parent workflow's business date when a run crosses midnight.
    today = screening_report_date()
    patterns = get_bond_final_input_patterns(today)
    long_term_pattern = patterns['long_today']
    fallback_long_term_pattern = patterns['long_fallback']
    short_term_pattern = patterns['short_today']
    fallback_short_term_pattern = patterns['short_fallback']
    try:
        short_term_file, long_term_file = find_required_input_pair(
            short_today_pattern=short_term_pattern,
            short_fallback_pattern=fallback_short_term_pattern,
            short_label='近期优质基金',
            long_today_pattern=long_term_pattern,
            long_fallback_pattern=fallback_long_term_pattern,
            long_label='长期优质基金',
            directory=output_dir,
        )
    except FileNotFoundError as e:
        logging_helpers.print_required_input_pair_not_found(
            output_dir=output_dir,
            long_term_pattern=long_term_pattern,
            short_term_pattern=short_term_pattern,
            error=e,
            print_fn=print,
        )
        return 1

    logging_helpers.print_found_input_file('长期优质基金', long_term_file, print_fn=print)

    # 步骤2: 查找近期优质基金文件
    logging_helpers.print_main_step_header(2, "查找近期优质基金文件", print_fn=print)

    logging_helpers.print_found_input_file('近期优质基金', short_term_file, print_fn=print)

    # 步骤3: 生成增强版对比报告
    try:
        result = generate_enhanced_comparison_report(short_term_file, long_term_file, output_dir)

        if not result['success']:
            logging_helpers.print_report_generation_failed(print_fn=print)
            return 1

    except Exception as e:
        logging_helpers.print_report_generation_exception(e, print_fn=print)
        import traceback
        traceback.print_exc()
        return 1

    # 完成
    logging_helpers.print_main_task_completion(datetime.now(), result, print_fn=print)
    final_output_path = Path(result['final_fund_file']).resolve()
    print(f"FUND_ANA_FINAL_OUTPUT={final_output_path}")

    return 0
if __name__ == "__main__":
    sys.exit(main())
