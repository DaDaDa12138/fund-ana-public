#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""最终筛选产物写入、sidecar 与同日清理辅助工具。"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from pathlib import Path

from bond_fund_screening.report_artifact_metadata import (
    resolve_output_path_reference,
    resolve_report_meta_path,
)
from bond_fund_screening.report_catalog import rebuild_report_manifests_for_paths


def _load_json_dict(file_path: str | Path | None) -> dict:
    """以最小容错读取 JSON 字典。"""
    if not file_path:
        return {}

    path_obj = Path(file_path)
    if not path_obj.exists():
        return {}

    try:
        payload = json.loads(path_obj.read_text(encoding='utf-8'))
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        return {}

    return payload if isinstance(payload, dict) else {}


def collect_bond_short_term_one_year_observation_artifacts(
    report_file: str | Path | None,
) -> list[dict]:
    """从短期详细报告 sidecar 中提取债基短周期未满1Y观察产物引用。"""
    if not report_file:
        return []

    report_path = Path(report_file)
    meta_path = report_path.with_name(f'{report_path.stem}_report_meta.json')
    payload = _load_json_dict(meta_path)
    raw_items = payload.get('bond_short_term_one_year_observation_artifacts')
    if not isinstance(raw_items, list):
        return []

    artifacts = []
    seen = set()
    for item in raw_items:
        if not isinstance(item, dict):
            continue
        key = str(item.get('key') or '').strip()
        file_path = resolve_output_path_reference(
            item.get('file_path'),
            output_dir=meta_path.parent,
        )
        if not key or file_path is None:
            continue
        report_meta_file = resolve_output_path_reference(
            item.get('report_meta_file'),
            output_dir=meta_path.parent,
        )
        dedupe_key = (key, file_path)
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        artifacts.append({
            'key': key,
            'label': str(item.get('label') or '债基短期未满1Y观察').strip(),
            'kind': str(item.get('kind') or 'csv').strip() or 'csv',
            'period': str(item.get('period') or '').strip() or None,
            'period_label': str(item.get('period_label') or '').strip() or None,
            'file_path': str(file_path),
            'report_meta_file': str(report_meta_file) if report_meta_file else None,
        })
    return artifacts


def build_final_output_name(base_name: str, *, output_prefix: str | None = None) -> str:
    """按需为最终筛选输出补充类型前缀。"""
    normalized_prefix = str(output_prefix or '').strip()
    if not normalized_prefix:
        return base_name
    return f"{normalized_prefix}_{base_name}"


def build_watchlist_output_name(side_label: str, limit: int, *, output_prefix: str | None = None) -> str:
    """统一构造观察池导出文件名前缀。"""
    normalized_side_label = str(side_label or '').strip()
    normalized_limit = max(int(limit or 0), 1)
    return build_final_output_name(
        f"仅{normalized_side_label}优质观察池_{normalized_limit}只",
        output_prefix=output_prefix,
    )


def build_final_screening_policy_snapshot(
    *,
    fund_type: str,
    final_policy: dict,
    single_side_watchlist_limit: int,
    short_direct_score_column: str | None,
    long_direct_score_column: str | None,
    autostock_config: dict | None,
    min_cross_periods: int,
    schema_version: str,
    should_filter_holding_period_fn,
    should_apply_autostock_cross_validation_fn,
) -> dict:
    """构造最终筛选策略快照，固化本次运行的关键参数。"""
    normalized_fund_type = str(fund_type or '').strip().lower()
    policy_snapshot = {
        'schema_version': schema_version,
        'fund_type': normalized_fund_type,
        'mainlist_anchor_limit': (
            int(final_policy.get('mainlist_anchor_limit'))
            if final_policy.get('mainlist_anchor_limit') not in (None, '', 0)
            else None
        ),
        'watchlist_limit': int(final_policy.get('watchlist_limit', single_side_watchlist_limit) or single_side_watchlist_limit),
        'short_score_weight': float(final_policy.get('short_score_weight', 0.0) or 0.0),
        'long_score_weight': float(final_policy.get('long_score_weight', 0.0) or 0.0),
        'hard_intersection_bonus': float(final_policy.get('hard_intersection_bonus', 0.0) or 0.0),
        'soft_intersection_bonus': float(final_policy.get('soft_intersection_bonus', 0.0) or 0.0),
        'mainlist_selection_mode': (
            str(final_policy.get('mainlist_selection_mode', 'fixed_top_n') or 'fixed_top_n').strip()
            or 'fixed_top_n'
        ),
        'mainlist_score_band_delta': float(final_policy.get('mainlist_score_band_delta', 0.0) or 0.0),
        'bond_subtype_edge_governance_enabled': bool(
            final_policy.get('bond_subtype_edge_governance_enabled', False)
        ),
        'bond_subtype_edge_warn_share_pct': float(
            final_policy.get('bond_subtype_edge_warn_share_pct', 0.0) or 0.0
        ),
        'bond_subtype_edge_max_replacements': int(
            final_policy.get('bond_subtype_edge_max_replacements', 0) or 0
        ),
        'bond_subtype_edge_score_gap_tolerance': float(
            final_policy.get('bond_subtype_edge_score_gap_tolerance', 0.0) or 0.0
        ),
        'soft_intersection_min_cross_periods': int(min_cross_periods or 1),
        'single_side_watchlist_limit': int(single_side_watchlist_limit or 0),
        'short_direct_score_column': str(short_direct_score_column or '').strip() or None,
        'long_direct_score_column': str(long_direct_score_column or '').strip() or None,
        'drawdown_duration_analysis_window_days': int(
            final_policy.get('drawdown_duration_analysis_window_days', 0) or 0
        ),
        'drawdown_duration_nav_buffer_days': int(
            final_policy.get('drawdown_duration_nav_buffer_days', 0) or 0
        ),
        'holding_period_filter_enabled': bool(
            should_filter_holding_period_fn(normalized_fund_type)
        ),
        'share_class_collapse_enabled': True,
        'autostock_cross_validation_enabled': bool(
            should_apply_autostock_cross_validation_fn(normalized_fund_type)
            and (autostock_config or {}).get('validation_provider', 'autostock') == 'autostock'
        ),
        'data_validation_provider': (autostock_config or {}).get('validation_provider', 'autostock'),
        'data_validation_independent_source': (autostock_config or {}).get('independent_source'),
        'autostock_validation_tolerance': {
            'nav_relative_tolerance_pct': (
                float(autostock_config.get('nav_relative_tolerance_pct', 0.0) or 0.0)
                if isinstance(autostock_config, dict) else None
            ),
            'nav_absolute_tolerance': (
                float(autostock_config.get('nav_absolute_tolerance', 0.0) or 0.0)
                if isinstance(autostock_config, dict) else None
            ),
            'return_relative_tolerance_pct': (
                float(autostock_config.get('return_relative_tolerance_pct', 0.0) or 0.0)
                if isinstance(autostock_config, dict) else None
            ),
            'return_absolute_tolerance': (
                float(autostock_config.get('return_absolute_tolerance', 0.0) or 0.0)
                if isinstance(autostock_config, dict) else None
            ),
            'return_small_base_threshold': (
                float(autostock_config.get('return_small_base_threshold', 0.0) or 0.0)
                if isinstance(autostock_config, dict) else None
            ),
        },
    }
    policy_payload = {
        key: value
        for key, value in policy_snapshot.items()
        if key not in {'policy_id', 'policy_version', 'policy_hash'}
    }
    policy_snapshot['policy_id'] = f'final_screening_{normalized_fund_type or "unknown"}'
    policy_snapshot['policy_version'] = str(schema_version or 'v1')
    policy_snapshot['policy_hash'] = hashlib.sha256(
        json.dumps(policy_payload, ensure_ascii=False, sort_keys=True).encode('utf-8')
    ).hexdigest()
    return policy_snapshot


def build_final_screening_risk_metric_governance() -> dict:
    """固化最终筛选产物中的风险指标事实来源说明。"""
    return {
        'official_runtime_source': 'bond_fund_screening/core/risk_calculator.py',
        'stored_metric_source': 'analyzers/risk_analyzer.py',
        'official_runtime_usage': (
            '正式筛选与正式主榜解释优先使用基于当轮净值窗口实时重算的风险指标。'
        ),
        'stored_metric_usage_boundary': (
            'fund_risk_metrics 中的 Sortino/Calmar 当前仅应用于展示补充、离线审计和异常排查，不直接作为正式主榜硬门槛。'
        ),
        'annual_return_boundary': (
            'annual_return 视为风险窗口年化展示值，不等同于 fund_returns.return_1y。'
        ),
        'type_code_32_boundary': (
            'type_code=32 当前不应进入正式 1Y 风险分位门槛，只能单独审计或进入观察语义。'
        ),
    }


def write_final_screening_decision_ledger_file(
    *,
    fund_type: str,
    timestamp: str,
    payload: dict,
    output_dir: Path,
    json_writer,
) -> Path:
    """写出最终筛选候选决策账本，仅供内部审计复盘。"""
    audit_dir = Path(output_dir) / '.audit' / 'final_screening_decisions'
    audit_dir.mkdir(parents=True, exist_ok=True)
    audit_path = audit_dir / f'{fund_type}_final_screening_decisions_{timestamp}.json'
    json_writer(audit_path, payload)
    return audit_path


def write_bond_mainlist_quality_audit_file(
    *,
    timestamp: str,
    payload: dict,
    output_dir: Path,
    audit_writer,
) -> tuple[Path, Path]:
    """写出债基正式主榜专项审计产物，仅供内部审计复盘。"""
    audit_dir = Path(output_dir) / '.audit' / 'bond_mainlist_quality'
    audit_dir.mkdir(parents=True, exist_ok=True)
    return audit_writer(
        payload,
        output_dir=audit_dir,
        timestamp=timestamp,
    )


def build_final_screening_decision_ledger_payload(
    *,
    generated_at: datetime,
    fund_type: str,
    policy_snapshot: dict,
    input_lineage: dict,
    final_screening_summary: dict,
    candidate_records: list[dict],
    schema_version: str,
) -> dict:
    """构造最终筛选候选决策账本。"""
    return {
        'schema_version': schema_version,
        'generated_at': generated_at.isoformat(),
        'fund_type': str(fund_type or '').strip().lower(),
        'policy_snapshot': dict(policy_snapshot or {}),
        'risk_metric_governance': build_final_screening_risk_metric_governance(),
        'policy_id': (
            str(policy_snapshot.get('policy_id') or '').strip()
            if isinstance(policy_snapshot, dict) else None
        ),
        'policy_version': (
            str(policy_snapshot.get('policy_version') or '').strip()
            if isinstance(policy_snapshot, dict) else None
        ),
        'policy_hash': (
            str(policy_snapshot.get('policy_hash') or '').strip()
            if isinstance(policy_snapshot, dict) else None
        ),
        'input_lineage': dict(input_lineage or {}),
        'final_screening_summary': dict(final_screening_summary or {}),
        'candidate_count': int(len(candidate_records or [])),
        'candidate_records': list(candidate_records or []),
    }


def build_serialized_artifact_references(
    *,
    output_dir: Path,
    short_term_file: Path,
    long_term_file: Path,
    comparison_report_file: Path,
    final_fund_file: Path,
    csv_short: Path | None,
    csv_long: Path | None,
    short_watchlist_csv: Path | None,
    long_watchlist_csv: Path | None,
    bond_short_term_one_year_observation_artifacts: list[dict] | None,
    autostock_validation_audit_file: Path | None,
    final_screening_decision_ledger_file: Path,
    bond_mainlist_quality_audit_json_file: Path | None,
    bond_mainlist_quality_audit_md_file: Path | None,
    serializer,
) -> dict:
    """统一序列化最终筛选相关产物路径引用。"""
    serialized_observation_artifacts = []
    for item in bond_short_term_one_year_observation_artifacts or []:
        if not isinstance(item, dict):
            continue
        serialized_file_path = serializer(item.get('file_path'), output_dir=output_dir)
        if not serialized_file_path:
            continue
        serialized_observation_artifacts.append({
            'key': str(item.get('key') or '').strip() or 'bond_short_term_one_year_observation_csv',
            'label': str(item.get('label') or '债基短期未满1Y观察').strip(),
            'kind': str(item.get('kind') or 'csv').strip() or 'csv',
            'period': str(item.get('period') or '').strip() or None,
            'period_label': str(item.get('period_label') or '').strip() or None,
            'file_path': serialized_file_path,
            'report_meta_file': serializer(item.get('report_meta_file'), output_dir=output_dir)
            if item.get('report_meta_file') else None,
        })

    return {
        'short_report_file': serializer(short_term_file, output_dir=output_dir),
        'long_report_file': serializer(long_term_file, output_dir=output_dir),
        'comparison_report_file': serializer(comparison_report_file, output_dir=output_dir),
        'final_fund_file': serializer(final_fund_file, output_dir=output_dir),
        'only_short_simple_csv': serializer(csv_short, output_dir=output_dir) if csv_short else None,
        'only_long_simple_csv': serializer(csv_long, output_dir=output_dir) if csv_long else None,
        'only_short_watchlist_csv': serializer(short_watchlist_csv, output_dir=output_dir) if short_watchlist_csv else None,
        'only_long_watchlist_csv': serializer(long_watchlist_csv, output_dir=output_dir) if long_watchlist_csv else None,
        'bond_short_term_one_year_observation_artifacts': serialized_observation_artifacts,
        'autostock_validation_audit_file': (
            serializer(autostock_validation_audit_file, output_dir=output_dir)
            if autostock_validation_audit_file else None
        ),
        'final_screening_decision_ledger_file': (
            serializer(final_screening_decision_ledger_file, output_dir=output_dir)
            if final_screening_decision_ledger_file else None
        ),
        'bond_mainlist_quality_audit_json_file': (
            serializer(bond_mainlist_quality_audit_json_file, output_dir=output_dir)
            if bond_mainlist_quality_audit_json_file else None
        ),
        'bond_mainlist_quality_audit_md_file': (
            serializer(bond_mainlist_quality_audit_md_file, output_dir=output_dir)
            if bond_mainlist_quality_audit_md_file else None
        ),
    }


def write_final_screening_report_sidecars(
    *,
    output_dir: Path,
    comparison_report_file: Path,
    final_fund_file: Path,
    short_term_file: Path,
    long_term_file: Path,
    fund_type: str,
    source_run_collector,
    meta_writer,
    serializer,
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
    """写出最终筛选主报告 sidecar，并返回相关元数据。"""
    source_runs = []
    source_runs.extend(source_run_collector(short_term_file))
    source_runs.extend(source_run_collector(long_term_file))
    derived_from_files = [short_term_file, long_term_file]

    serialized_refs = build_serialized_artifact_references(
        output_dir=output_dir,
        short_term_file=short_term_file,
        long_term_file=long_term_file,
        comparison_report_file=comparison_report_file,
        final_fund_file=final_fund_file,
        csv_short=csv_short,
        csv_long=csv_long,
        short_watchlist_csv=short_watchlist_csv,
        long_watchlist_csv=long_watchlist_csv,
        bond_short_term_one_year_observation_artifacts=bond_short_term_one_year_observation_artifacts,
        autostock_validation_audit_file=autostock_validation_audit_file,
        final_screening_decision_ledger_file=final_screening_decision_ledger_file,
        bond_mainlist_quality_audit_json_file=bond_mainlist_quality_audit_json_file,
        bond_mainlist_quality_audit_md_file=bond_mainlist_quality_audit_md_file,
        serializer=serializer,
    )

    shared_meta = {
        'fund_type': fund_type,
        'policy_id': final_screening_policy_snapshot.get('policy_id'),
        'policy_version': final_screening_policy_snapshot.get('policy_version'),
        'policy_hash': final_screening_policy_snapshot.get('policy_hash'),
        'risk_metric_governance': build_final_screening_risk_metric_governance(),
        'input_pair_status': input_lineage['input_pair_status'],
        'input_pair_status_label': input_lineage['input_pair_status_label'],
        'used_stale_inputs': input_lineage['used_stale_inputs'],
        'input_lineage': dict(input_lineage),
        'policy_snapshot': dict(final_screening_policy_snapshot),
        'final_screening_policy_snapshot': dict(final_screening_policy_snapshot),
        'only_short_simple_csv': serialized_refs['only_short_simple_csv'],
        'only_long_simple_csv': serialized_refs['only_long_simple_csv'],
        'only_short_watchlist_csv': serialized_refs['only_short_watchlist_csv'],
        'only_long_watchlist_csv': serialized_refs['only_long_watchlist_csv'],
        'bond_short_term_one_year_observation_artifacts': list(
            serialized_refs['bond_short_term_one_year_observation_artifacts']
        ),
        'single_side_watchlist_limit': single_side_watchlist_limit,
        'final_screening_summary': dict(final_screening_summary),
        'final_screening_decision_ledger_file': serialized_refs['final_screening_decision_ledger_file'],
        'bond_mainlist_quality_audit_json_file': serialized_refs['bond_mainlist_quality_audit_json_file'],
        'bond_mainlist_quality_audit_md_file': serialized_refs['bond_mainlist_quality_audit_md_file'],
    }

    comparison_report_meta_file = meta_writer(
        comparison_report_file,
        artifact_type='daily_final_comparison_report',
        source_runs=source_runs,
        derived_from_files=derived_from_files,
        extra_meta={
            **shared_meta,
            'short_report_file': serialized_refs['short_report_file'],
            'long_report_file': serialized_refs['long_report_file'],
        },
        refresh_manifest=False,
    )
    final_fund_report_meta_file = meta_writer(
        final_fund_file,
        artifact_type='daily_final_fund_report',
        source_runs=source_runs,
        derived_from_files=derived_from_files,
        extra_meta={
            **shared_meta,
            'comparison_report_file': serialized_refs['comparison_report_file'],
            'autostock_validation_audit_file': serialized_refs['autostock_validation_audit_file'],
        },
        refresh_manifest=False,
    )
    if refresh_manifest:
        rebuild_report_manifests_for_paths(comparison_report_file, final_fund_file)
    return {
        'source_runs': source_runs,
        'derived_from_files': derived_from_files,
        'serialized_refs': serialized_refs,
        'comparison_report_meta_file': comparison_report_meta_file,
        'final_fund_report_meta_file': final_fund_report_meta_file,
    }


def cleanup_final_screening_same_day_outputs(
    *,
    output_dir: Path,
    timestamp: str,
    output_prefix: str | None,
    single_side_watchlist_limit: int,
    comparison_report_file: Path,
    final_fund_file: Path,
    csv_short: Path | None,
    csv_long: Path | None,
    short_watchlist_csv: Path | None,
    long_watchlist_csv: Path | None,
    autostock_validation_audit_file: Path | None = None,
    final_screening_decision_ledger_file: Path | None = None,
    bond_mainlist_quality_audit_json_file: Path | None = None,
    bond_mainlist_quality_audit_md_file: Path | None = None,
    cleanup_file_versions,
    cleanup_family_versions,
) -> list[dict]:
    """清理最终筛选链路同业务日旧版本。"""
    cleanup_results = [
        cleanup_file_versions(comparison_report_file),
        cleanup_file_versions(final_fund_file),
    ]

    optional_files = [
        csv_short,
        csv_long,
        short_watchlist_csv,
        long_watchlist_csv,
    ]
    for path in optional_files:
        if path:
            cleanup_results.append(cleanup_file_versions(path))

    if autostock_validation_audit_file is not None:
        cleanup_results.append(cleanup_file_versions(autostock_validation_audit_file))
    if final_screening_decision_ledger_file is not None:
        cleanup_results.append(cleanup_file_versions(final_screening_decision_ledger_file))
    if bond_mainlist_quality_audit_json_file is not None:
        cleanup_results.append(cleanup_file_versions(bond_mainlist_quality_audit_json_file))
    if bond_mainlist_quality_audit_md_file is not None:
        cleanup_results.append(cleanup_file_versions(bond_mainlist_quality_audit_md_file))

    day_token = str(timestamp).split('_', 1)[0]
    optional_cleanup_specs = (
        ('仅近期优质_简版', '.csv', csv_short, False),
        ('仅长期优质_简版', '.csv', csv_long, False),
        (build_watchlist_output_name('近期', single_side_watchlist_limit, output_prefix=output_prefix), '.csv', short_watchlist_csv, False),
        (build_watchlist_output_name('长期', single_side_watchlist_limit, output_prefix=output_prefix), '.csv', long_watchlist_csv, False),
        (build_final_output_name(f'仅近期优质观察池_{single_side_watchlist_limit}只', output_prefix=output_prefix), '.csv', short_watchlist_csv, True),
        (build_final_output_name(f'仅长期优质观察池_{single_side_watchlist_limit}只', output_prefix=output_prefix), '.csv', long_watchlist_csv, True),
    )
    for prefix, suffix, generated_file, force_cleanup_legacy_family in optional_cleanup_specs:
        if generated_file is not None and not force_cleanup_legacy_family:
            continue
        cleanup_results.append(cleanup_family_versions(
            output_dir,
            prefix=prefix,
            day_token=day_token,
            suffix=suffix,
        ))

    return cleanup_results


def _delete_artifact_file(
    file_path: Path | None,
    *,
    delete_sidecar: bool,
) -> list[str]:
    """删除单个产物文件，并按需同步删除 sidecar。"""
    if file_path is None:
        return []

    deleted_files: list[str] = []
    artifact_path = Path(file_path)

    try:
        if artifact_path.exists():
            artifact_path.unlink()
            deleted_files.append(str(artifact_path))
    except FileNotFoundError:
        pass

    if not delete_sidecar:
        return deleted_files

    meta_path = resolve_report_meta_path(artifact_path)
    if meta_path == artifact_path:
        return deleted_files

    try:
        if meta_path.exists():
            meta_path.unlink()
            deleted_files.append(str(meta_path))
    except FileNotFoundError:
        pass

    return deleted_files


def cleanup_partial_final_screening_outputs(
    *,
    comparison_report_file: Path | None,
    final_fund_file: Path | None,
    csv_short: Path | None,
    csv_long: Path | None,
    short_watchlist_csv: Path | None,
    long_watchlist_csv: Path | None,
    autostock_validation_audit_file: Path | None = None,
    final_screening_decision_ledger_file: Path | None = None,
    bond_mainlist_quality_audit_json_file: Path | None = None,
    bond_mainlist_quality_audit_md_file: Path | None = None,
    print_fn=print,
    refresh_manifest: bool = True,
) -> dict:
    """最终筛选失败时清理本轮半成品，避免页面误识别为最新正式产物。"""
    deleted_files: list[str] = []

    for file_path in (
        comparison_report_file,
        final_fund_file,
        csv_short,
        csv_long,
        short_watchlist_csv,
        long_watchlist_csv,
    ):
        deleted_files.extend(
            _delete_artifact_file(file_path, delete_sidecar=True)
        )

    for file_path in (
        autostock_validation_audit_file,
        final_screening_decision_ledger_file,
        bond_mainlist_quality_audit_json_file,
        bond_mainlist_quality_audit_md_file,
    ):
        deleted_files.extend(
            _delete_artifact_file(file_path, delete_sidecar=False)
        )

    if deleted_files and refresh_manifest:
        rebuild_report_manifests_for_paths(
            comparison_report_file,
            final_fund_file,
            csv_short,
            csv_long,
            short_watchlist_csv,
            long_watchlist_csv,
        )

    if deleted_files:
        print_fn(f"  🧹 最终筛选失败，已清理本轮半成品产物: {len(deleted_files)} 个文件")

    return {
        'deleted_files': deleted_files,
        'deleted_count': len(deleted_files),
    }


def finalize_final_screening_artifacts(
    *,
    fund_type: str,
    timestamp: str,
    output_dir: Path,
    output_prefix: str | None,
    output_file: Path,
    final_fund_file: Path,
    short_term_file: Path,
    long_term_file: Path,
    input_lineage: dict,
    final_screening_policy_snapshot: dict,
    final_screening_summary: dict,
    single_side_watchlist_limit: int,
    csv_short: Path | None,
    csv_long: Path | None,
    short_watchlist_csv: Path | None,
    long_watchlist_csv: Path | None,
    bond_short_term_one_year_observation_artifacts: list[dict] | None,
    autostock_validation_audit_enabled: bool,
    autostock_validation_audit_payload: dict | None,
    decision_ledger_payload: dict,
    build_bond_mainlist_quality_audit_payload_fn,
    write_autostock_validation_audit_file_fn,
    write_final_screening_decision_ledger_file_fn,
    write_bond_mainlist_quality_audit_file_fn,
    write_final_screening_report_sidecars_fn,
    cleanup_final_screening_same_day_outputs_fn,
    print_fn,
) -> dict:
    """串联最终筛选产物写出、sidecar 绑定与同日清理。"""
    autostock_validation_audit_file = None
    final_screening_decision_ledger_file = None
    bond_mainlist_quality_audit_json_file = None
    bond_mainlist_quality_audit_md_file = None
    deleted_artifact_count = 0

    try:
        if autostock_validation_audit_enabled and autostock_validation_audit_payload is not None:
            autostock_validation_audit_file = write_autostock_validation_audit_file_fn(
                fund_type=fund_type,
                timestamp=timestamp,
                payload=autostock_validation_audit_payload,
            )

        final_screening_decision_ledger_file = write_final_screening_decision_ledger_file_fn(
            fund_type=fund_type,
            timestamp=timestamp,
            payload=decision_ledger_payload,
        )
        if str(fund_type or '').strip().lower() == 'bond' and build_bond_mainlist_quality_audit_payload_fn is not None:
            bond_mainlist_quality_audit_payload = build_bond_mainlist_quality_audit_payload_fn(
                decision_ledger_file=final_screening_decision_ledger_file,
            )
            (
                bond_mainlist_quality_audit_json_file,
                bond_mainlist_quality_audit_md_file,
            ) = write_bond_mainlist_quality_audit_file_fn(
                timestamp=timestamp,
                payload=bond_mainlist_quality_audit_payload,
            )

        artifact_meta = write_final_screening_report_sidecars_fn(
            output_dir=output_dir,
            output_file=output_file,
            final_fund_file=final_fund_file,
            short_term_file=short_term_file,
            long_term_file=long_term_file,
            fund_type=fund_type,
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
            refresh_manifest=False,
        )
        comparison_report_meta_file = artifact_meta['comparison_report_meta_file']
        final_fund_report_meta_file = artifact_meta['final_fund_report_meta_file']
        print_fn(f"  💾 对比报告元数据: {comparison_report_meta_file.name}")
        print_fn(f"  💾 当日最终优质基金元数据: {final_fund_report_meta_file.name}")
        if autostock_validation_audit_file is not None:
            print_fn(f"  💾 AutoStock完整审计: {autostock_validation_audit_file.name}")
        print_fn(f"  💾 候选决策账本: {final_screening_decision_ledger_file.name}")
        if bond_mainlist_quality_audit_json_file is not None:
            print_fn(f"  💾 债基主榜专项审计(JSON): {bond_mainlist_quality_audit_json_file.name}")
        if bond_mainlist_quality_audit_md_file is not None:
            print_fn(f"  💾 债基主榜专项审计(MD): {bond_mainlist_quality_audit_md_file.name}")

        cleanup_results = cleanup_final_screening_same_day_outputs_fn(
            output_dir=output_dir,
            timestamp=timestamp,
            output_prefix=output_prefix,
            single_side_watchlist_limit=single_side_watchlist_limit,
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
        )
        deleted_artifact_count = sum(int(item.get('deleted_count', 0) or 0) for item in cleanup_results)
        if deleted_artifact_count > 0:
            print_fn(f"  🧹 同业务日旧版最终筛选产物已清理: {deleted_artifact_count} 个文件")

        rebuild_report_manifests_for_paths(output_file, final_fund_file)
    except Exception:
        cleanup_partial_final_screening_outputs(
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
            print_fn=print_fn,
        )
        raise

    return {
        'artifact_meta': artifact_meta,
        'comparison_report_meta_file': comparison_report_meta_file,
        'final_fund_report_meta_file': final_fund_report_meta_file,
        'autostock_validation_audit_file': autostock_validation_audit_file,
        'final_screening_decision_ledger_file': final_screening_decision_ledger_file,
        'bond_mainlist_quality_audit_json_file': bond_mainlist_quality_audit_json_file,
        'bond_mainlist_quality_audit_md_file': bond_mainlist_quality_audit_md_file,
        'same_day_cleanup_deleted_count': deleted_artifact_count,
    }


def build_final_screening_result_payload(
    *,
    candidate_summary: dict,
    codes_short: set,
    codes_long: set,
    output_file: Path,
    final_fund_file: Path,
    csv_short: Path | None,
    csv_long: Path | None,
    short_watchlist_csv: Path | None,
    long_watchlist_csv: Path | None,
    bond_short_term_one_year_observation_artifacts: list[dict] | None,
    input_lineage: dict,
    short_report_date: str | None,
    long_report_date: str | None,
    comparison_report_meta_file: Path,
    final_fund_report_meta_file: Path,
    autostock_validation_audit_file: Path | None,
    final_screening_policy_snapshot: dict,
    final_screening_decision_ledger_file: Path,
    bond_mainlist_quality_audit_json_file: Path | None,
    bond_mainlist_quality_audit_md_file: Path | None,
    final_screening_summary: dict,
    same_day_cleanup_deleted_count: int,
) -> dict:
    """构造增强版对比报告返回 payload。"""
    return {
        'success': True,
        'intersection_count': candidate_summary['intersection_count'],
        'soft_intersection_count': candidate_summary['soft_intersection_count'],
        'short_term_count': len(codes_short),
        'long_term_count': len(codes_long),
        'only_short_count': candidate_summary['only_short_count'],
        'only_long_count': candidate_summary['only_long_count'],
        'raw_only_short_count': candidate_summary['raw_only_short_count'],
        'raw_only_long_count': candidate_summary['raw_only_long_count'],
        'output_file': str(output_file),
        'final_fund_file': str(final_fund_file),
        'only_short_simple_csv': str(csv_short) if csv_short else None,
        'only_long_simple_csv': str(csv_long) if csv_long else None,
        'only_short_watchlist_csv': str(short_watchlist_csv) if short_watchlist_csv else None,
        'only_long_watchlist_csv': str(long_watchlist_csv) if long_watchlist_csv else None,
        'bond_short_term_one_year_observation_artifacts': [
            dict(item) for item in (bond_short_term_one_year_observation_artifacts or [])
        ],
        'today_date': input_lineage['today_date'],
        'short_report_date': short_report_date,
        'long_report_date': long_report_date,
        'input_pair_status': input_lineage['input_pair_status'],
        'input_pair_status_label': input_lineage['input_pair_status_label'],
        'used_stale_inputs': input_lineage['used_stale_inputs'],
        'input_lineage_note': input_lineage['input_lineage_note'],
        'comparison_report_meta_file': str(comparison_report_meta_file),
        'final_fund_report_meta_file': str(final_fund_report_meta_file),
        'autostock_validation_audit_file': (
            str(autostock_validation_audit_file) if autostock_validation_audit_file else None
        ),
        'policy_snapshot': dict(final_screening_policy_snapshot),
        'final_screening_policy_snapshot': dict(final_screening_policy_snapshot),
        'risk_metric_governance': build_final_screening_risk_metric_governance(),
        'final_screening_decision_ledger_file': str(final_screening_decision_ledger_file),
        'bond_mainlist_quality_audit_json_file': (
            str(bond_mainlist_quality_audit_json_file)
            if bond_mainlist_quality_audit_json_file else None
        ),
        'bond_mainlist_quality_audit_md_file': (
            str(bond_mainlist_quality_audit_md_file)
            if bond_mainlist_quality_audit_md_file else None
        ),
        'final_screening_summary': dict(final_screening_summary),
        'same_day_cleanup_deleted_count': int(same_day_cleanup_deleted_count or 0),
    }
