#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
任务副产物审计工具。

只读扫描 logs/ 与 logs/tasks 目录，输出磁盘占用、文件分布和建议保留窗口摘要，
用于审计和预览，不执行任何删除动作。
"""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta
import os
from pathlib import Path
import re
import shutil
import threading
import time
from typing import Dict, Iterable, List, Optional, Sequence, Set

from utils.database_backup_cleanup_preview import (
    DEFAULT_SCHEDULED_BACKUP_KEEP_COUNT,
    build_database_backup_cleanup_preview_payload,
    get_database_backup_cleanup_policy,
)
from utils.database_backup_manual_review_control_pack import (
    load_latest_database_backup_manual_review_control_pack_payload,
)
from utils.database_backup_manual_review_runbook import (
    load_latest_database_backup_manual_review_runbook_payload,
)
from utils.database_backup_cleanup_execution import (
    load_latest_database_backup_cleanup_execution_payload,
)
from utils.audit_artifact_cleanup_preview import (
    build_audit_artifact_cleanup_preview_payload,
)
from utils.audit_artifact_cleanup_execution import (
    load_latest_audit_artifact_cleanup_execution_payload,
)
from utils.screening_output_cleanup_preview import (
    build_screening_output_cleanup_preview_payload,
    get_screening_output_cleanup_policy,
)
from utils.screening_output_cleanup_execution import (
    load_latest_screening_output_cleanup_execution_payload,
)
from utils.screening_history_cleanup_execution import (
    load_latest_screening_history_cleanup_execution_payload,
)
from utils.nav_history_archive_runbook import (
    load_latest_nav_history_archive_runbook_payload,
)
from utils.nav_history_archive_preflight import (
    load_latest_nav_history_archive_preflight_payload,
)
from utils.nav_history_archive_control_pack import (
    load_latest_nav_history_archive_control_pack_payload,
)
from utils.nav_history_archive_maintenance_window_pack import (
    load_latest_nav_history_archive_maintenance_window_pack_payload,
)
from utils.nav_history_archive_execution_checklist import (
    load_latest_nav_history_archive_execution_checklist_payload,
)
from utils.nav_history_archive_maintenance_readiness_audit import (
    load_latest_nav_history_archive_maintenance_readiness_audit_payload,
)
from utils.nav_history_archive_post_maintenance_evidence import (
    load_latest_nav_history_archive_post_maintenance_evidence_payload,
)
from utils.nav_history_archive_manual_window_sop import (
    load_latest_nav_history_archive_manual_window_sop_payload,
)
from utils.nav_history_archive_apply_runner import (
    load_latest_nav_history_archive_apply_runner_payload,
)
from utils.fund_nav_history_governance_audit import (
    load_latest_fund_nav_history_governance_audit_payload,
)


_CACHE_LOCK = threading.Lock()
_SUMMARY_CACHE: Dict[str, object] = {
    'key': None,
    'expires_at': 0.0,
    'payload': None,
}


TASK_ARTIFACT_RULES: Sequence[Dict[str, object]] = (
    {
        'key': 'application_logs',
        'label': '应用日志',
        'relative_dir': 'logs',
        'pattern': '*.log',
        'suggested_retention_days': 30,
        'runtime_dependency': '仅用于运行排障，不参与业务计算或数据库写入',
    },
    {
        'key': 'task_logs',
        'label': '任务执行日志',
        'relative_dir': 'logs/tasks',
        'pattern': 'task_*.log',
        'exclude_prefixes': ('task_pending_',),
        'suggested_retention_days': 30,
        'runtime_dependency': '仅用于任务历史日志查看，不参与运行期计算',
    },
    {
        'key': 'pending_task_logs',
        'label': '待绑定任务日志',
        'relative_dir': 'logs/tasks',
        'pattern': 'task_pending_*.log',
        'suggested_retention_days': 7,
        'runtime_dependency': '临时过渡文件，不参与运行期计算',
    },
    {
        'key': 'missing_nav_no_data_reports',
        'label': '缺失净值空数据报告',
        'relative_dir': 'logs/tasks',
        'pattern': 'supplement_missing_nav_no_data_*.json',
        'suggested_retention_days': 14,
        'runtime_dependency': '仅补净值脚本读取最近72小时窗口用于冷却跳过',
    },
    {
        'key': 'missing_nav_timeout_reports',
        'label': '缺失净值超时报告',
        'relative_dir': 'logs/tasks',
        'pattern': 'supplement_missing_nav_timeouts_*.tsv',
        'suggested_retention_days': 14,
        'runtime_dependency': '仅用于排障，不参与运行期计算',
    },
    {
        'key': 'detail_empty_reports',
        'label': '详情空数据报告',
        'relative_dir': 'logs/tasks',
        'pattern': 'detail_empty_funds_*.json',
        'suggested_retention_days': 14,
        'runtime_dependency': '仅用于排障，不参与运行期计算',
    },
)


LOG_FAMILY_RULES: Sequence[Dict[str, object]] = (
    {
        'key': 'collector_runtime_logs',
        'label': 'Collector运行日志',
        'pattern': r'^logs/collector_\d{4}-\d{2}-\d{2}(?:_new)?\.log(?:\.\d+)?$',
        'suggested_retention_days': 14,
        'runtime_dependency': '仅用于运行排障，不参与业务计算',
    },
    {
        'key': 'api_runtime_logs',
        'label': 'API运行日志',
        'pattern': r'^logs/api_\d{4}-\d{2}-\d{2}(?:_new)?\.log(?:\.\d+)?$',
        'suggested_retention_days': 14,
        'runtime_dependency': '仅用于运行排障，不参与业务计算',
    },
    {
        'key': 'database_runtime_logs',
        'label': '数据库运行日志',
        'pattern': r'^logs/database_\d{4}-\d{2}-\d{2}(?:_new)?\.log(?:\.\d+)?$',
        'suggested_retention_days': 14,
        'runtime_dependency': '仅用于数据库排障，不参与业务计算',
    },
    {
        'key': 'error_runtime_logs',
        'label': '错误运行日志',
        'pattern': r'^logs/error_\d{4}-\d{2}-\d{2}(?:_new)?\.log(?:\.\d+)?$',
        'suggested_retention_days': 14,
        'runtime_dependency': '仅用于错误排障，不参与业务计算',
    },
    {
        'key': 'performance_runtime_logs',
        'label': '性能运行日志',
        'pattern': r'^logs/performance_\d{4}-\d{2}-\d{2}(?:_new)?\.log(?:\.\d+)?$',
        'suggested_retention_days': 14,
        'runtime_dependency': '仅用于性能分析，不参与业务计算',
    },
    {
        'key': 'task_runtime_logs',
        'label': '任务运行日志',
        'pattern': r'^logs/task_\d{4}-\d{2}-\d{2}(?:_new)?\.log(?:\.\d+)?$',
        'suggested_retention_days': 14,
        'runtime_dependency': '仅用于任务运行排障，不参与业务计算',
    },
    {
        'key': 'task_execution_logs',
        'label': '任务执行输出日志',
        'pattern': r'^logs/tasks/task_\d+_.*\.log$',
        'suggested_retention_days': 30,
        'runtime_dependency': '仅用于任务执行日志查看，不参与业务计算',
    },
    {
        'key': 'task_pending_logs',
        'label': '待绑定任务输出日志',
        'pattern': r'^logs/tasks/task_pending_.*\.log$',
        'suggested_retention_days': 7,
        'runtime_dependency': '临时过渡文件，不参与业务计算',
    },
    {
        'key': 'service_logs',
        'label': '服务进程日志',
        'pattern': r'^logs/(?:web|scheduler|supervisord|tiantianapi)(?:_error)?\.log(?:\.\d+)?$',
        'suggested_retention_days': 14,
        'runtime_dependency': '仅用于容器内服务进程排障，不参与业务计算',
    },
)


PROJECT_ARTIFACT_RULES: Sequence[Dict[str, object]] = (
    {
        'key': 'database_backups',
        'label': '数据库备份',
        'relative_dir': 'backend/data/backups',
        'patterns': ('*.db', '*.db.zst', '*.sqlite', '*.sqlite.zst', '*.sqlite3', '*.sqlite3.zst'),
        'suggested_retention_days': 14,
        'runtime_dependency': '仅用于维护窗口回滚；确认最新备份可用后，可评估清理更早备份',
    },
    {
        'key': 'audit_artifacts',
        'label': '审计产物',
        'relative_dir': 'audit',
        'patterns': ('*',),
        'suggested_retention_days': 30,
        'runtime_dependency': '仅用于审计留痕与问题追溯，不参与运行期业务计算',
    },
    {
        'key': 'screening_output_artifacts',
        'label': '筛选输出',
        'relative_dir': 'bond_fund_screening/output',
        'patterns': ('*',),
        'suggested_retention_days': 14,
        'runtime_dependency': '用于历史报告预览、下载与人工复核，清理前需确认不再需要旧产物',
    },
    {
        'key': 'root_generated_artifacts',
        'label': '根目录历史产物',
        'relative_dir': '.',
        'patterns': (
            'COMPREHENSIVE_ANALYSIS_REPORT_*',
            'COMPARISON_REPORT_*',
            'test_report_*',
            'test_report.txt',
            'numeric_ftype_backfill_preview_*',
        ),
        'top_level_only': True,
        'suggested_retention_days': 14,
        'runtime_dependency': '历史调试/审计产物，不参与运行期业务计算，建议迁入 audit 后再清理',
    },
)


def invalidate_task_artifact_summary_cache() -> None:
    """清空任务副产物摘要缓存，供维护动作后立即刷新概览。"""
    with _CACHE_LOCK:
        _SUMMARY_CACHE.update({
            'key': None,
            'expires_at': 0.0,
            'payload': None,
        })


def _format_datetime(value: Optional[datetime]) -> Optional[str]:
    if value is None:
        return None
    return value.isoformat(timespec='seconds')


def _to_relative_path(path: Path, project_root: Path) -> str:
    root_text = os.path.abspath(os.fspath(project_root))
    path_text = os.path.abspath(os.fspath(path))
    if path_text == root_text:
        return '.'

    root_prefix = root_text.rstrip(os.sep) + os.sep
    if path_text.startswith(root_prefix):
        return path_text[len(root_prefix) :]

    try:
        return os.path.relpath(path_text, root_text)
    except ValueError:
        return path_text


def _empty_retention_preview() -> Dict[str, object]:
    return {
        'candidate_group_count': 0,
        'candidate_file_count': 0,
        'candidate_size_bytes': 0,
        'candidate_size_mb': 0.0,
        'groups': [],
    }


def _normalize_task_artifact_detail_level(value: Optional[str]) -> str:
    normalized = str(value or 'full').strip().lower()
    if normalized == 'overview':
        return 'overview'
    return 'full'


def _iter_top_level_files(
    target_dir: Path,
    pattern: str,
    *,
    exclude_prefixes: Sequence[str] = (),
) -> Iterable[Path]:
    if not target_dir.exists():
        return ()
    return (
        path
        for path in target_dir.glob(pattern)
        if path.is_file() and not any(path.name.startswith(prefix) for prefix in exclude_prefixes)
    )


def _summarize_files(
    files: Sequence[Path],
    *,
    project_root: Path,
    now: datetime,
    label: str,
    runtime_dependency: str,
    suggested_retention_days: int,
) -> Dict[str, object]:
    entries: List[Dict[str, object]] = []
    total_size_bytes = 0
    cutoff = now - timedelta(days=max(int(suggested_retention_days), 0))

    for path in files:
        try:
            stat = path.stat()
        except OSError:
            continue

        modified_at = datetime.fromtimestamp(stat.st_mtime)
        entry = {
            'path': _to_relative_path(path, project_root),
            'size_bytes': int(stat.st_size),
            'modified_at': modified_at,
        }
        total_size_bytes += int(stat.st_size)
        entries.append(entry)

    if not entries:
        return {
            'label': label,
            'count': 0,
            'total_size_bytes': 0,
            'total_size_mb': 0.0,
            'oldest_at': None,
            'newest_at': None,
            'largest_file': None,
            'runtime_dependency': runtime_dependency,
            'suggested_retention_days': int(suggested_retention_days),
            'older_than_suggested_count': 0,
            'older_than_suggested_size_bytes': 0,
            'older_than_suggested_size_mb': 0.0,
            'older_entries': [],
        }

    entries.sort(key=lambda item: item['modified_at'])
    largest_entry = max(entries, key=lambda item: item['size_bytes'])
    older_entries = [item for item in entries if item['modified_at'] < cutoff]

    return {
        'label': label,
        'count': len(entries),
        'total_size_bytes': total_size_bytes,
        'total_size_mb': round(total_size_bytes / (1024 * 1024), 2),
        'oldest_at': _format_datetime(entries[0]['modified_at']),
        'newest_at': _format_datetime(entries[-1]['modified_at']),
        'largest_file': {
            'path': largest_entry['path'],
            'size_bytes': largest_entry['size_bytes'],
            'modified_at': _format_datetime(largest_entry['modified_at']),
        },
        'runtime_dependency': runtime_dependency,
        'suggested_retention_days': int(suggested_retention_days),
        'older_than_suggested_count': len(older_entries),
        'older_than_suggested_size_bytes': sum(int(item['size_bytes']) for item in older_entries),
        'older_than_suggested_size_mb': round(
            sum(int(item['size_bytes']) for item in older_entries) / (1024 * 1024),
            2,
        ),
        'older_entries': [
            {
                'path': item['path'],
                'size_bytes': int(item['size_bytes']),
                'modified_at': _format_datetime(item['modified_at']),
            }
            for item in older_entries
        ],
    }


def _summarize_other_files(
    files: Sequence[Path],
    *,
    project_root: Path,
    now: datetime,
    label: str,
) -> Dict[str, object]:
    return _summarize_files(
        files,
        project_root=project_root,
        now=now,
        label=label,
        runtime_dependency='未归类副产物，仅用于审计定位',
        suggested_retention_days=30,
    )


def _parse_artifact_timestamp(value: Optional[str]) -> datetime:
    text = str(value or '').strip()
    if not text:
        return datetime.min
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return datetime.min


def _first_finding(payload: Dict[str, object]) -> str:
    findings = payload.get('findings') if isinstance(payload, dict) else None
    if not isinstance(findings, list):
        return ''
    for item in findings:
        text = str(item or '').strip()
        if text:
            return text
    return ''


def _build_file_cleanup_execution_item(
    *,
    key: str,
    label: str,
    artifact_path: Path,
    payload: Dict[str, object],
    scope_total_key: str,
    deleted_count_key: str,
    missing_count_key: str,
    failed_count_key: str,
    processed_count_key: str,
) -> Dict[str, object]:
    summary = dict(payload.get('summary') or {})
    parameters = dict(payload.get('parameters') or {})
    return {
        'key': key,
        'label': label,
        'artifact_path': str(artifact_path),
        'generated_at': str(payload.get('generated_at') or '').strip(),
        'status': str(payload.get('status') or '').strip(),
        'mode': str(payload.get('mode') or '').strip(),
        'scope_total': int(summary.get(scope_total_key) or parameters.get(scope_total_key) or 0),
        'deleted_target_count': int(summary.get(deleted_count_key) or 0),
        'missing_target_count': int(summary.get(missing_count_key) or 0),
        'failed_target_count': int(summary.get(failed_count_key) or 0),
        'processed_target_count': int(summary.get(processed_count_key) or 0),
        'deleted_size_mb': round(float(summary.get('deleted_size_mb') or 0.0), 4),
        'message': _first_finding(payload),
    }


def _build_screening_history_execution_item(
    artifact_path: Path,
    payload: Dict[str, object],
) -> Dict[str, object]:
    summary = dict(payload.get('summary') or {})
    parameters = dict(payload.get('parameters') or {})
    return {
        'key': 'screening_history',
        'label': '筛选历史',
        'artifact_path': str(artifact_path),
        'generated_at': str(payload.get('generated_at') or '').strip(),
        'status': str(payload.get('status') or '').strip(),
        'mode': str(payload.get('mode') or '').strip(),
        'scope_total': int(summary.get('selected_run_count') or parameters.get('selected_run_count') or 0),
        'deleted_target_count': int(summary.get('deleted_total') or 0),
        'missing_target_count': 0,
        'failed_target_count': int(summary.get('failed_batch_count') or 0),
        'processed_target_count': int(summary.get('completed_batch_count') or 0),
        'deleted_size_mb': 0.0,
        'message': _first_finding(payload),
    }


def _load_latest_execution_item(
    *,
    output_dir: Path,
    loader,
    builder,
) -> Optional[Dict[str, object]]:
    try:
        artifact_path, payload = loader(output_dir=str(output_dir))
    except FileNotFoundError:
        return None
    except Exception as exc:
        return {
            'key': 'unknown',
            'label': '维护执行',
            'artifact_path': '',
            'generated_at': '',
            'status': 'error',
            'mode': '',
            'scope_total': 0,
            'deleted_target_count': 0,
            'missing_target_count': 0,
            'failed_target_count': 1,
            'processed_target_count': 0,
            'deleted_size_mb': 0.0,
            'message': f'{type(exc).__name__}: {exc}',
        }
    return builder(artifact_path, dict(payload or {}))


def _build_maintenance_execution_rollup(project_root: Path) -> Dict[str, object]:
    audit_dir = project_root / 'audit'
    items = [
        item
        for item in (
            _load_latest_execution_item(
                output_dir=audit_dir,
                loader=load_latest_database_backup_cleanup_execution_payload,
                builder=lambda artifact_path, payload: _build_file_cleanup_execution_item(
                    key='database_backups',
                    label='数据库备份',
                    artifact_path=artifact_path,
                    payload=payload,
                    scope_total_key='scope_file_total',
                    deleted_count_key='deleted_file_count',
                    missing_count_key='missing_file_count',
                    failed_count_key='failed_file_count',
                    processed_count_key='processed_file_count',
                ),
            ),
            _load_latest_execution_item(
                output_dir=audit_dir,
                loader=load_latest_audit_artifact_cleanup_execution_payload,
                builder=lambda artifact_path, payload: _build_file_cleanup_execution_item(
                    key='audit_artifacts',
                    label='审计产物',
                    artifact_path=artifact_path,
                    payload=payload,
                    scope_total_key='scope_entry_total',
                    deleted_count_key='deleted_entry_count',
                    missing_count_key='missing_entry_count',
                    failed_count_key='failed_entry_count',
                    processed_count_key='processed_entry_count',
                ),
            ),
            _load_latest_execution_item(
                output_dir=audit_dir,
                loader=load_latest_screening_output_cleanup_execution_payload,
                builder=lambda artifact_path, payload: _build_file_cleanup_execution_item(
                    key='screening_outputs',
                    label='筛选输出',
                    artifact_path=artifact_path,
                    payload=payload,
                    scope_total_key='scope_file_total',
                    deleted_count_key='deleted_file_count',
                    missing_count_key='missing_file_count',
                    failed_count_key='failed_file_count',
                    processed_count_key='processed_file_count',
                ),
            ),
            _load_latest_execution_item(
                output_dir=audit_dir,
                loader=load_latest_screening_history_cleanup_execution_payload,
                builder=_build_screening_history_execution_item,
            ),
        )
        if item is not None
    ]

    items.sort(
        key=lambda item: _parse_artifact_timestamp(str(item.get('generated_at') or '')),
        reverse=True,
    )

    if not items:
        return {
            'available': False,
            'item_count': 0,
            'items': [],
        }

    latest_item = dict(items[0] or {})
    latest_status = str(latest_item.get('status') or '').strip()
    applied_count = sum(1 for item in items if str(item.get('status') or '').strip() == 'applied')
    failed_count = sum(1 for item in items if str(item.get('status') or '').strip() == 'failed')
    running_count = sum(
        1 for item in items if str(item.get('status') or '').strip() in {'running', 'pending'}
    )
    dry_run_count = sum(
        1 for item in items if str(item.get('status') or '').strip() in {'dry-run', 'planned'}
    )

    return {
        'available': True,
        'generated_at': latest_item.get('generated_at') or '',
        'item_count': len(items),
        'applied_count': applied_count,
        'failed_count': failed_count,
        'running_count': running_count,
        'dry_run_count': dry_run_count,
        'latest_execution_key': latest_item.get('key') or '',
        'latest_execution_label': latest_item.get('label') or '',
        'latest_status': latest_status,
        'latest_mode': latest_item.get('mode') or '',
        'latest_artifact_path': latest_item.get('artifact_path') or '',
        'latest_scope_total': int(latest_item.get('scope_total') or 0),
        'latest_deleted_target_count': int(latest_item.get('deleted_target_count') or 0),
        'latest_deleted_size_mb': round(float(latest_item.get('deleted_size_mb') or 0.0), 4),
        'latest_failed_target_count': int(latest_item.get('failed_target_count') or 0),
        'latest_missing_target_count': int(latest_item.get('missing_target_count') or 0),
        'latest_processed_target_count': int(latest_item.get('processed_target_count') or 0),
        'latest_message': str(latest_item.get('message') or '').strip(),
        'items': items[:4],
    }


def _normalize_summary_int(value) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _normalize_summary_size_mb(value) -> float:
    try:
        return round(float(value or 0.0), 2)
    except (TypeError, ValueError):
        return 0.0


def _extract_preview_summary(payload: Optional[Dict[str, object]]) -> Dict[str, object]:
    if not isinstance(payload, dict):
        return {}
    summary = payload.get('summary')
    return dict(summary or {}) if isinstance(summary, dict) else {}


def _build_screening_storage_pressure_signature(
    screening_storage_pressure: Optional[Dict[str, object]],
) -> tuple[object, ...]:
    if not isinstance(screening_storage_pressure, dict):
        return ()
    summary = _extract_preview_summary(screening_storage_pressure)
    interpretation = dict(screening_storage_pressure.get('interpretation') or {})
    return (
        bool(screening_storage_pressure.get('available')),
        str(interpretation.get('status') or '').strip(),
        _normalize_summary_int(summary.get('candidate_run_count')),
        _normalize_summary_int(summary.get('decisions_to_delete')),
        _normalize_summary_int(summary.get('picks_to_delete')),
        _normalize_summary_size_mb(summary.get('database_size_mb')),
        _normalize_summary_size_mb(summary.get('database_live_size_mb')),
        _normalize_summary_size_mb(summary.get('database_reclaimable_mb')),
    )


def _build_file_maintenance_effect_delta(
    *,
    latest_item: Dict[str, object],
    current_total_size_mb: float,
    current_total_path: str,
    remaining_candidate_count: int,
    remaining_candidate_size_mb: float,
) -> Dict[str, object]:
    latest_label = str(latest_item.get('label') or '维护').strip() or '维护'
    latest_status = str(latest_item.get('status') or '').strip()
    latest_scope_total = _normalize_summary_int(latest_item.get('scope_total'))
    deleted_target_count = _normalize_summary_int(latest_item.get('deleted_target_count'))
    deleted_size_mb = _normalize_summary_size_mb(latest_item.get('deleted_size_mb'))
    failed_target_count = _normalize_summary_int(latest_item.get('failed_target_count'))
    missing_target_count = _normalize_summary_int(latest_item.get('missing_target_count'))
    candidate_cleared = remaining_candidate_count <= 0

    status = 'review_required'
    alert_type = 'info'
    tag_type = 'info'
    badge_text = '待复核'
    headline = f'最近一次{latest_label}维护效果待复核'
    message = f'当前{latest_label}目录约 {current_total_size_mb} MB，仍有 {remaining_candidate_count} 个候选待处理。'
    action_hint = '建议对照最新 checkpoint、目录体积和剩余候选，确认本轮删除是否达到预期。'

    if latest_status == 'failed':
        status = 'execution_failed'
        alert_type = 'error'
        tag_type = 'danger'
        badge_text = '执行失败'
        headline = f'最近一次{latest_label}维护执行失败'
        message = (
            f'最近一次{latest_label}维护已尝试处理 {latest_scope_total} 个目标，'
            f'当前仍有 {failed_target_count} 个失败项，剩余候选 {remaining_candidate_count} 个。'
        )
        action_hint = '先处理失败项和残留对象，再决定是否重新生成 dry-run 或进入下一轮维护。'
    elif latest_status in {'running', 'pending'}:
        status = 'running'
        alert_type = 'warning'
        tag_type = 'warning'
        badge_text = '执行中'
        headline = f'最近一次{latest_label}维护仍在执行'
        message = (
            f'最近一次{latest_label}维护当前 scope {latest_scope_total} 个目标，'
            f'当前剩余候选 {remaining_candidate_count} 个，需等待 checkpoint 稳定后再复核。'
        )
        action_hint = '当前不应据此下结论，先等待本轮执行结束并重新拉取摘要。'
    elif latest_status in {'dry-run', 'planned'}:
        status = 'await_apply'
        alert_type = 'warning'
        tag_type = 'warning'
        badge_text = '仅预演'
        headline = f'最近一次{latest_label}仅完成 dry-run'
        message = (
            f'最近一次{latest_label}维护尚未进入真实删除，当前仍有 {remaining_candidate_count} 个候选，'
            f'累计约 {remaining_candidate_size_mb} MB。'
        )
        action_hint = '若要评估真实释放效果，需要先执行 apply，再回看目录体积与剩余候选。'
    elif latest_status == 'applied' and candidate_cleared and failed_target_count == 0:
        status = 'effective'
        alert_type = 'success'
        tag_type = 'success'
        badge_text = '达到预期'
        headline = f'最近一次{latest_label}维护已基本达到预期'
        message = (
            f'最近一次{latest_label}维护已删 {deleted_target_count} 个目标，约 {deleted_size_mb} MB；'
            f'当前对应候选已清空。'
        )
        action_hint = '继续观察新产物增长即可；如后续再次累积，再进入下一轮 dry-run。'
    elif latest_status == 'applied' and (
        deleted_target_count > 0 or failed_target_count > 0 or missing_target_count > 0
    ):
        status = 'partial'
        alert_type = 'warning'
        tag_type = 'warning'
        badge_text = '部分生效'
        headline = f'最近一次{latest_label}维护已生效，但仍有残留候选'
        message = (
            f'最近一次{latest_label}维护已删 {deleted_target_count} 个目标，约 {deleted_size_mb} MB；'
            f'当前仍有 {remaining_candidate_count} 个候选，累计约 {remaining_candidate_size_mb} MB。'
        )
        action_hint = '建议区分这是新增长量还是本轮残留，再决定是否补一轮 dry-run/apply。'
    elif latest_scope_total == 0:
        status = 'no_candidate'
        alert_type = 'success'
        tag_type = 'success'
        badge_text = '无候选'
        headline = f'最近一次{latest_label}维护无实际候选'
        message = f'最近一次{latest_label}维护 scope 为 0，当前目录约 {current_total_size_mb} MB。'
        action_hint = '当前无需继续清理，保持保留策略与只读审计即可。'

    return {
        'available': True,
        'status': status,
        'alert_type': alert_type,
        'tag_type': tag_type,
        'badge_text': badge_text,
        'headline': headline,
        'message': message,
        'action_hint': action_hint,
        'remaining_candidate_count': remaining_candidate_count,
        'remaining_candidate_size_mb': remaining_candidate_size_mb,
        'remaining_candidate_exists': remaining_candidate_count > 0,
        'current_total_size_mb': current_total_size_mb,
        'current_total_path': current_total_path,
        'candidate_cleared': candidate_cleared,
    }


def _build_screening_history_maintenance_effect_delta(
    *,
    latest_item: Dict[str, object],
    screening_storage_pressure: Optional[Dict[str, object]],
) -> Dict[str, object]:
    latest_label = str(latest_item.get('label') or '筛选历史').strip() or '筛选历史'
    latest_status = str(latest_item.get('status') or '').strip()
    latest_scope_total = _normalize_summary_int(latest_item.get('scope_total'))
    deleted_target_count = _normalize_summary_int(latest_item.get('deleted_target_count'))
    failed_target_count = _normalize_summary_int(latest_item.get('failed_target_count'))

    payload = dict(screening_storage_pressure or {})
    summary = _extract_preview_summary(payload)
    interpretation = dict(payload.get('interpretation') or {})
    pressure_status = str(interpretation.get('status') or '').strip()
    candidate_run_count = _normalize_summary_int(summary.get('candidate_run_count'))
    decisions_to_delete = _normalize_summary_int(summary.get('decisions_to_delete'))
    picks_to_delete = _normalize_summary_int(summary.get('picks_to_delete'))
    database_size_mb = _normalize_summary_size_mb(summary.get('database_size_mb'))
    database_live_size_mb = _normalize_summary_size_mb(summary.get('database_live_size_mb'))
    database_reclaimable_mb = _normalize_summary_size_mb(summary.get('database_reclaimable_mb'))
    candidate_cleared = candidate_run_count <= 0 and decisions_to_delete <= 0 and picks_to_delete <= 0

    status = 'review_required'
    alert_type = 'info'
    tag_type = 'info'
    badge_text = '待复核'
    headline = f'最近一次{latest_label}维护效果待复核'
    message = f'当前主库约 {database_size_mb} MB，仍需结合重复 run 与可回收空页判断清理效果。'
    action_hint = '建议先核对最新 checkpoint，再结合筛选历史存储压力摘要判断是否需要继续维护。'

    if payload.get('available') is False:
        status = 'pressure_unavailable'
        alert_type = 'warning'
        tag_type = 'warning'
        badge_text = '缺实时态'
        headline = '缺少实时筛选历史压力摘要，暂无法复核维护效果'
        message = '最近一次筛选历史维护已有执行结果，但当前未拿到实时 screening_storage_pressure。'
        action_hint = '请先确保数据库可读，再重新拉取系统概览或数据库页摘要。'
    elif latest_status == 'failed':
        status = 'execution_failed'
        alert_type = 'error'
        tag_type = 'danger'
        badge_text = '执行失败'
        headline = f'最近一次{latest_label}维护执行失败'
        message = (
            f'最近一次{latest_label}维护原计划处理 {latest_scope_total} 个 run，'
            f'当前仍有 {failed_target_count} 个失败批次，重复候选 {candidate_run_count} 个 run。'
        )
        action_hint = '先处理失败批次与残留 run，再决定是否继续下一轮真实删除。'
    elif latest_status in {'running', 'pending'}:
        status = 'running'
        alert_type = 'warning'
        tag_type = 'warning'
        badge_text = '执行中'
        headline = f'最近一次{latest_label}维护仍在执行'
        message = (
            f'最近一次{latest_label}维护仍在执行，当前重复候选 {candidate_run_count} 个 run，'
            f'主库约 {database_size_mb} MB。'
        )
        action_hint = '等待 checkpoint 完整落盘后再复核，不要在执行中判断最终效果。'
    elif latest_status in {'dry-run', 'planned'}:
        status = 'await_apply'
        alert_type = 'warning'
        tag_type = 'warning'
        badge_text = '仅预演'
        headline = f'最近一次{latest_label}仅完成 dry-run'
        message = (
            f'当前重复候选仍有 {candidate_run_count} 个 run，涉及 {decisions_to_delete} 条 decision、'
            f'{picks_to_delete} 条 pick。'
        )
        action_hint = '若要验证真实释放效果，需要先进入 apply；当前只能说明候选规模，不代表已释放空间。'
    elif latest_status == 'applied' and candidate_cleared and database_reclaimable_mb >= 512:
        status = 'post_cleanup_reclaimable'
        alert_type = 'warning'
        tag_type = 'warning'
        badge_text = '已裁剪待回收'
        headline = '最近一次筛选历史结构清理已完成，但主库仍有可回收空页'
        message = (
            f'最近一次{latest_label}维护已删 {deleted_target_count} 个目标；当前重复候选已清空，'
            f'但主库仍有约 {database_reclaimable_mb} MB 可回收空页。'
        )
        action_hint = '说明结构性重复历史已基本裁剪，下一步应独立评估 VACUUM，而不是继续复用旧 runbook。'
    elif latest_status == 'applied' and candidate_cleared:
        status = 'effective'
        alert_type = 'success'
        tag_type = 'success'
        badge_text = '达到预期'
        headline = '最近一次筛选历史结构清理已基本达到预期'
        message = (
            f'最近一次{latest_label}维护已删 {deleted_target_count} 个目标；当前重复 run 候选已清空，'
            f'主库 live 体积约 {database_live_size_mb} MB。'
        )
        action_hint = '继续观察新写入增长即可；若后续重复候选重新出现，再重新生成 preview/runbook。'
    elif latest_status == 'applied' and deleted_target_count > 0:
        status = 'partial'
        alert_type = 'warning'
        tag_type = 'warning'
        badge_text = '部分生效'
        headline = '最近一次筛选历史结构清理已生效，但仍有残留候选'
        message = (
            f'最近一次{latest_label}维护已删 {deleted_target_count} 个目标；'
            f'当前仍有 {candidate_run_count} 个重复 run，涉及 {decisions_to_delete} 条 decision、'
            f'{picks_to_delete} 条 pick。'
        )
        action_hint = '需要区分这些候选是本轮残留还是维护后新产生，再决定是否补一轮 runbook。'
    elif latest_scope_total == 0 and candidate_cleared:
        status = 'no_candidate'
        alert_type = 'success'
        tag_type = 'success'
        badge_text = '无候选'
        headline = '最近一次筛选历史维护无实际候选'
        message = f'当前重复 run 候选为 0，主库约 {database_size_mb} MB，可回收空页 {database_reclaimable_mb} MB。'
        action_hint = '当前无需继续做筛选历史删除，如仍关注体积，应转向主库热区或 VACUUM 评估。'

    return {
        'available': True,
        'status': status,
        'alert_type': alert_type,
        'tag_type': tag_type,
        'badge_text': badge_text,
        'headline': headline,
        'message': message,
        'action_hint': action_hint,
        'remaining_candidate_count': candidate_run_count,
        'remaining_candidate_size_mb': 0.0,
        'remaining_candidate_exists': candidate_run_count > 0,
        'current_total_size_mb': database_size_mb,
        'current_total_live_size_mb': database_live_size_mb,
        'current_total_path': 'database/bond_funds.db',
        'candidate_cleared': candidate_cleared,
        'screening_history_candidate_run_count': candidate_run_count,
        'screening_history_decisions_to_delete': decisions_to_delete,
        'screening_history_picks_to_delete': picks_to_delete,
        'screening_history_pressure_status': pressure_status,
        'database_reclaimable_mb': database_reclaimable_mb,
    }


def _build_maintenance_effect_delta_summary(
    project_root: Path,
    *,
    maintenance_execution_rollup: Dict[str, object],
    project_directories: Dict[str, Dict[str, object]],
    database_backup_retention_preview: Dict[str, object],
    audit_artifact_retention_preview: Dict[str, object],
    screening_output_retention_preview: Dict[str, object],
    screening_storage_pressure: Optional[Dict[str, object]] = None,
) -> Dict[str, object]:
    rollup = dict(maintenance_execution_rollup or {})
    items = list(rollup.get('items') or [])
    latest_item = dict(items[0] or {}) if items else {}
    if not latest_item and rollup.get('available'):
        latest_item = {
            'key': rollup.get('latest_execution_key') or '',
            'label': rollup.get('latest_execution_label') or '',
            'artifact_path': rollup.get('latest_artifact_path') or '',
            'generated_at': rollup.get('generated_at') or '',
            'status': rollup.get('latest_status') or '',
            'mode': rollup.get('latest_mode') or '',
            'scope_total': rollup.get('latest_scope_total') or 0,
            'deleted_target_count': rollup.get('latest_deleted_target_count') or 0,
            'missing_target_count': rollup.get('latest_missing_target_count') or 0,
            'failed_target_count': rollup.get('latest_failed_target_count') or 0,
            'processed_target_count': rollup.get('latest_processed_target_count') or 0,
            'deleted_size_mb': rollup.get('latest_deleted_size_mb') or 0.0,
            'message': rollup.get('latest_message') or '',
        }

    if not latest_item:
        return {
            'available': False,
            'status': 'no_execution',
            'alert_type': 'info',
            'tag_type': 'info',
            'badge_text': '暂无执行',
            'headline': '尚无维护执行效果可复核',
            'message': '当前还没有 maintenance execution checkpoint，无法评估本轮维护效果。',
            'action_hint': '先完成 dry-run 或 apply，再回看目录体积、候选变化和主库压力。',
        }

    latest_key_raw = str(latest_item.get('key') or '').strip()
    latest_key = {
        'screening_outputs': 'screening_output',
    }.get(latest_key_raw, latest_key_raw)
    latest_label = str(latest_item.get('label') or '维护').strip() or '维护'
    base_payload = {
        'available': True,
        'generated_at': str(latest_item.get('generated_at') or '').strip(),
        'latest_execution_key': latest_key,
        'latest_execution_label': latest_label,
        'latest_status': str(latest_item.get('status') or '').strip(),
        'latest_artifact_path': str(latest_item.get('artifact_path') or '').strip(),
        'scope_total': _normalize_summary_int(latest_item.get('scope_total')),
        'deleted_target_count': _normalize_summary_int(latest_item.get('deleted_target_count')),
        'missing_target_count': _normalize_summary_int(latest_item.get('missing_target_count')),
        'failed_target_count': _normalize_summary_int(latest_item.get('failed_target_count')),
        'processed_target_count': _normalize_summary_int(latest_item.get('processed_target_count')),
        'deleted_size_mb': _normalize_summary_size_mb(latest_item.get('deleted_size_mb')),
        'latest_message': str(latest_item.get('message') or '').strip(),
    }

    if latest_key == 'database_backups':
        preview_summary = _extract_preview_summary(database_backup_retention_preview)
        directory_summary = dict(project_directories.get('database_backups') or {})
        effect_payload = _build_file_maintenance_effect_delta(
            latest_item=latest_item,
            current_total_size_mb=_normalize_summary_size_mb(directory_summary.get('total_size_mb')),
            current_total_path=str(directory_summary.get('path') or 'database/backups'),
            remaining_candidate_count=_normalize_summary_int(preview_summary.get('candidate_file_count')),
            remaining_candidate_size_mb=_normalize_summary_size_mb(preview_summary.get('candidate_size_mb')),
        )
    elif latest_key == 'audit_artifacts':
        preview_summary = _extract_preview_summary(audit_artifact_retention_preview)
        directory_summary = dict(project_directories.get('audit_artifacts') or {})
        effect_payload = _build_file_maintenance_effect_delta(
            latest_item=latest_item,
            current_total_size_mb=_normalize_summary_size_mb(directory_summary.get('total_size_mb')),
            current_total_path=str(directory_summary.get('path') or 'audit'),
            remaining_candidate_count=_normalize_summary_int(preview_summary.get('candidate_entry_count')),
            remaining_candidate_size_mb=_normalize_summary_size_mb(preview_summary.get('candidate_size_mb')),
        )
    elif latest_key == 'screening_output':
        preview_summary = _extract_preview_summary(screening_output_retention_preview)
        directory_summary = dict(project_directories.get('screening_output_artifacts') or {})
        effect_payload = _build_file_maintenance_effect_delta(
            latest_item=latest_item,
            current_total_size_mb=_normalize_summary_size_mb(directory_summary.get('total_size_mb')),
            current_total_path=str(directory_summary.get('path') or 'bond_fund_screening/output'),
            remaining_candidate_count=_normalize_summary_int(preview_summary.get('candidate_file_count')),
            remaining_candidate_size_mb=_normalize_summary_size_mb(preview_summary.get('candidate_size_mb')),
        )
    elif latest_key == 'screening_history':
        effect_payload = _build_screening_history_maintenance_effect_delta(
            latest_item=latest_item,
            screening_storage_pressure=screening_storage_pressure,
        )
    else:
        effect_payload = {
            'available': True,
            'status': 'review_required',
            'alert_type': 'info',
            'tag_type': 'info',
            'badge_text': '待复核',
            'headline': f'最近一次{latest_label}维护效果待复核',
            'message': '当前缺少对应类型的效果对照逻辑，请结合 execution checkpoint 人工复核。',
            'action_hint': '先核对最新 execution checkpoint、目标目录和当前候选状态。',
            'remaining_candidate_count': 0,
            'remaining_candidate_size_mb': 0.0,
            'remaining_candidate_exists': False,
            'current_total_size_mb': 0.0,
            'current_total_path': '',
            'candidate_cleared': False,
        }

    findings = [
        f"最近类型 {latest_label}，状态 {base_payload['latest_status'] or 'unknown'}，scope {base_payload['scope_total']}。",
        f"本轮已删 {base_payload['deleted_target_count']} 个目标，删除体积 {base_payload['deleted_size_mb']} MB。",
        (
            f"当前剩余候选 {effect_payload.get('remaining_candidate_count', 0)} 个，"
            f"当前体积 {effect_payload.get('current_total_size_mb', 0.0)} MB。"
        ),
    ]
    if latest_key == 'screening_history':
        findings.append(
            f"主库可回收空页 {effect_payload.get('database_reclaimable_mb', 0.0)} MB，"
            f"压力状态 {effect_payload.get('screening_history_pressure_status') or 'unknown'}。"
        )

    return {
        **base_payload,
        **effect_payload,
        'findings': findings,
    }


def _build_nav_history_archive_summary(project_root: Path) -> Dict[str, object]:
    audit_dir = project_root / 'audit'

    runbook_path = None
    runbook_payload: Dict[str, object] = {}
    runbook_error = ''
    try:
        runbook_path, loaded_runbook_payload = load_latest_nav_history_archive_runbook_payload(
            output_dir=str(audit_dir)
        )
        runbook_payload = dict(loaded_runbook_payload or {})
    except FileNotFoundError:
        pass
    except Exception as exc:
        runbook_error = f'{type(exc).__name__}: {exc}'

    preflight_path = None
    preflight_payload: Dict[str, object] = {}
    preflight_error = ''
    try:
        preflight_path, loaded_preflight_payload = load_latest_nav_history_archive_preflight_payload(
            output_dir=str(audit_dir)
        )
        preflight_payload = dict(loaded_preflight_payload or {})
    except FileNotFoundError:
        pass
    except Exception as exc:
        preflight_error = f'{type(exc).__name__}: {exc}'

    control_pack_path = None
    control_pack_payload: Dict[str, object] = {}
    control_pack_error = ''
    try:
        control_pack_path, loaded_control_pack_payload = load_latest_nav_history_archive_control_pack_payload(
            output_dir=str(audit_dir)
        )
        control_pack_payload = dict(loaded_control_pack_payload or {})
    except FileNotFoundError:
        pass
    except Exception as exc:
        control_pack_error = f'{type(exc).__name__}: {exc}'

    maintenance_window_pack_path = None
    maintenance_window_pack_payload: Dict[str, object] = {}
    maintenance_window_pack_error = ''
    try:
        maintenance_window_pack_path, loaded_maintenance_window_pack_payload = (
            load_latest_nav_history_archive_maintenance_window_pack_payload(
                output_dir=str(audit_dir)
            )
        )
        maintenance_window_pack_payload = dict(loaded_maintenance_window_pack_payload or {})
    except FileNotFoundError:
        pass
    except Exception as exc:
        maintenance_window_pack_error = f'{type(exc).__name__}: {exc}'

    execution_checklist_path = None
    execution_checklist_payload: Dict[str, object] = {}
    execution_checklist_error = ''
    try:
        execution_checklist_path, loaded_execution_checklist_payload = (
            load_latest_nav_history_archive_execution_checklist_payload(
                output_dir=str(audit_dir)
            )
        )
        execution_checklist_payload = dict(loaded_execution_checklist_payload or {})
    except FileNotFoundError:
        pass
    except Exception as exc:
        execution_checklist_error = f'{type(exc).__name__}: {exc}'

    readiness_audit_path = None
    readiness_audit_payload: Dict[str, object] = {}
    readiness_audit_error = ''
    try:
        readiness_audit_path, loaded_readiness_audit_payload = (
            load_latest_nav_history_archive_maintenance_readiness_audit_payload(
                output_dir=str(audit_dir)
            )
        )
        readiness_audit_payload = dict(loaded_readiness_audit_payload or {})
    except FileNotFoundError:
        pass
    except Exception as exc:
        readiness_audit_error = f'{type(exc).__name__}: {exc}'

    post_maintenance_evidence_path = None
    post_maintenance_evidence_payload: Dict[str, object] = {}
    post_maintenance_evidence_error = ''
    try:
        post_maintenance_evidence_path, loaded_post_maintenance_evidence_payload = (
            load_latest_nav_history_archive_post_maintenance_evidence_payload(
                output_dir=str(audit_dir)
            )
        )
        post_maintenance_evidence_payload = dict(
            loaded_post_maintenance_evidence_payload or {}
        )
    except FileNotFoundError:
        pass
    except Exception as exc:
        post_maintenance_evidence_error = f'{type(exc).__name__}: {exc}'

    manual_window_sop_path = None
    manual_window_sop_payload: Dict[str, object] = {}
    manual_window_sop_error = ''
    try:
        manual_window_sop_path, loaded_manual_window_sop_payload = (
            load_latest_nav_history_archive_manual_window_sop_payload(
                output_dir=str(audit_dir)
            )
        )
        manual_window_sop_payload = dict(loaded_manual_window_sop_payload or {})
    except FileNotFoundError:
        pass
    except Exception as exc:
        manual_window_sop_error = f'{type(exc).__name__}: {exc}'

    apply_runner_path = None
    apply_runner_payload: Dict[str, object] = {}
    apply_runner_error = ''
    try:
        apply_runner_path, loaded_apply_runner_payload = (
            load_latest_nav_history_archive_apply_runner_payload(
                output_dir=str(audit_dir)
            )
        )
        apply_runner_payload = dict(loaded_apply_runner_payload or {})
    except FileNotFoundError:
        pass
    except Exception as exc:
        apply_runner_error = f'{type(exc).__name__}: {exc}'

    runbook_available = runbook_path is not None
    preflight_available = preflight_path is not None
    control_pack_available = control_pack_path is not None
    maintenance_window_pack_available = maintenance_window_pack_path is not None
    execution_checklist_available = execution_checklist_path is not None
    readiness_audit_available = readiness_audit_path is not None
    post_maintenance_evidence_available = post_maintenance_evidence_path is not None
    manual_window_sop_available = manual_window_sop_path is not None
    apply_runner_available = apply_runner_path is not None
    available = runbook_available or preflight_available or apply_runner_available

    runbook_summary = dict(runbook_payload.get('summary') or {})
    preflight_summary = dict(preflight_payload.get('summary') or {})
    preflight_checks = dict(preflight_payload.get('checks') or {})
    maintenance_window_pack_summary = dict(maintenance_window_pack_payload.get('summary') or {})
    execution_checklist_summary = dict(execution_checklist_payload.get('summary') or {})
    readiness_audit_summary = dict(readiness_audit_payload.get('summary') or {})
    post_maintenance_evidence_summary = dict(
        post_maintenance_evidence_payload.get('summary') or {}
    )
    manual_window_sop_summary = dict(
        manual_window_sop_payload.get('summary') or {}
    )
    apply_runner_summary = dict(apply_runner_payload.get('summary') or {})
    operator_placeholder_checks = [
        item
        for item in (readiness_audit_payload.get('operator_placeholder_checks') or [])
        if isinstance(item, dict)
    ]
    readiness_blockers = [
        str(item or '').strip()
        for item in (readiness_audit_payload.get('blockers') or [])
        if str(item or '').strip()
    ]
    readiness_review_items = [
        str(item or '').strip()
        for item in (readiness_audit_payload.get('review_items') or [])
        if str(item or '').strip()
    ]
    final_gate_checks = [
        str(item or '').strip()
        for item in (execution_checklist_payload.get('final_gate_checks') or [])
        if str(item or '').strip()
    ]
    execution_order = [
        str(item or '').strip()
        for item in (execution_checklist_payload.get('execution_order') or [])
        if str(item or '').strip()
    ]
    rollback_triggers = [
        str(item or '').strip()
        for item in (execution_checklist_payload.get('rollback_triggers') or [])
        if str(item or '').strip()
    ]

    ready_for_manual_signoff = bool(
        preflight_checks.get(
            'ready_for_manual_signoff',
            preflight_summary.get('ready_for_manual_signoff'),
        )
    )
    runbook_status = str(runbook_payload.get('status') or '').strip()
    recommended_retention_years = _normalize_summary_int(
        runbook_summary.get('recommended_retention_years')
        or preflight_summary.get('runbook_recommended_retention_years')
    )
    archive_cutoff_date = str(
        runbook_summary.get('archive_cutoff_date')
        or preflight_summary.get('runbook_archive_cutoff_date')
        or ''
    ).strip()
    estimated_archive_row_count = _normalize_summary_int(
        runbook_summary.get('estimated_archive_row_count')
        or preflight_summary.get('runbook_estimated_archive_row_count')
    )
    estimated_archive_size_mb = _normalize_summary_size_mb(
        runbook_summary.get('estimated_archive_size_mb')
        or preflight_summary.get('runbook_estimated_archive_size_mb')
    )
    archive_target_path = str(runbook_summary.get('archive_target_path') or '').strip()
    archive_table_name = str(runbook_summary.get('archive_table_name') or '').strip()
    live_cold_row_count = _normalize_summary_int(preflight_summary.get('live_cold_row_count'))
    live_cold_fund_count = _normalize_summary_int(preflight_summary.get('live_cold_fund_count'))
    live_min_nav_date = str(preflight_summary.get('live_min_nav_date') or '').strip()
    live_max_nav_date = str(preflight_summary.get('live_max_nav_date') or '').strip()
    control_pack_status = str(
        (control_pack_payload.get('summary') or {}).get('package_status') or ''
    ).strip()
    maintenance_window_pack_status = str(
        maintenance_window_pack_summary.get('window_status') or ''
    ).strip()
    execution_checklist_status = str(
        execution_checklist_summary.get('checklist_status') or ''
    ).strip()
    readiness_audit_status = str(
        readiness_audit_summary.get('readiness_status') or ''
    ).strip()
    blocker_count = _normalize_summary_int(readiness_audit_summary.get('blocker_count'))
    review_item_count = _normalize_summary_int(readiness_audit_summary.get('review_item_count'))
    archive_target_absolute_path = str(
        readiness_audit_summary.get('archive_target_absolute_path') or ''
    ).strip()
    archive_target_disk_free_mb = _normalize_summary_size_mb(
        readiness_audit_summary.get('archive_target_disk_free_mb')
    )
    project_disk_usage_percent = _normalize_summary_size_mb(
        readiness_audit_summary.get('project_disk_usage_percent')
    )
    operator_placeholders_complete = bool(
        readiness_audit_summary.get('operator_placeholders_complete')
    )
    backup_prerequisites_complete = bool(
        readiness_audit_summary.get('backup_prerequisites_complete')
    )
    vacuum_decision_recorded = bool(
        readiness_audit_summary.get('vacuum_decision_recorded')
    )
    execution_mode = str(
        apply_runner_summary.get('execution_mode')
        or readiness_audit_summary.get('execution_mode')
        or execution_checklist_summary.get('execution_mode')
        or maintenance_window_pack_summary.get('execution_mode')
        or manual_window_sop_summary.get('execution_mode')
        or ''
    ).strip()
    page_apply_supported = bool(
        readiness_audit_summary.get('page_apply_supported')
        or execution_checklist_summary.get('page_apply_supported')
        or maintenance_window_pack_summary.get('page_apply_supported')
        or manual_window_sop_summary.get('page_apply_supported')
        or apply_runner_summary.get('page_apply_supported')
    )
    final_reference_label = ''
    final_reference_artifact_path = ''
    final_reference_generated_at = ''
    apply_runner_status = str(apply_runner_summary.get('runner_status') or '').strip()
    if apply_runner_available and apply_runner_status:
        final_reference_label = 'archive apply runner'
        final_reference_artifact_path = str(apply_runner_path or '').strip()
        final_reference_generated_at = str(apply_runner_payload.get('generated_at') or '').strip()
    elif readiness_audit_available and readiness_audit_status:
        final_reference_label = 'maintenance readiness audit'
        final_reference_artifact_path = str(readiness_audit_path or '').strip()
        final_reference_generated_at = str(readiness_audit_payload.get('generated_at') or '').strip()
    elif manual_window_sop_available:
        final_reference_label = 'manual window sop'
        final_reference_artifact_path = str(manual_window_sop_path or '').strip()
        final_reference_generated_at = str(manual_window_sop_payload.get('generated_at') or '').strip()
    elif post_maintenance_evidence_available:
        final_reference_label = 'post maintenance evidence'
        final_reference_artifact_path = str(post_maintenance_evidence_path or '').strip()
        final_reference_generated_at = str(
            post_maintenance_evidence_payload.get('generated_at') or ''
        ).strip()
    elif execution_checklist_available:
        final_reference_label = 'execution checklist'
        final_reference_artifact_path = str(execution_checklist_path or '').strip()
        final_reference_generated_at = str(execution_checklist_payload.get('generated_at') or '').strip()
    elif maintenance_window_pack_available:
        final_reference_label = 'maintenance window pack'
        final_reference_artifact_path = str(maintenance_window_pack_path or '').strip()
        final_reference_generated_at = str(maintenance_window_pack_payload.get('generated_at') or '').strip()
    elif control_pack_available:
        final_reference_label = 'control pack'
        final_reference_artifact_path = str(control_pack_path or '').strip()
        final_reference_generated_at = str(control_pack_payload.get('generated_at') or '').strip()
    next_step_label = '继续补齐归档链路'

    status = 'unavailable'
    headline = '主库冷数据归档摘要暂不可用'
    message = '当前还没有 NAV 历史归档 runbook/preflight 产物，无法判断是否已进入人工签字前状态。'
    action_hint = '先在数据库页生成 NAV 历史剖面、保留预演、归档方案预演与 runbook，再补 preflight。'

    if apply_runner_available and apply_runner_status == 'archive_binding_health_blocked':
        status = 'blocked'
        headline = 'NAV 热冷库归档绑定保活未通过'
        message = _first_finding(apply_runner_payload) or (
            '最新 apply runner 发现已生效冷库绑定的主库、冷库或热冷计数保活门禁存在阻断项。'
        )
        action_hint = '先检查主库/冷库 quick_check、archive_target_path、cutoff 前热库剩余行和冷库计数，不要继续自动归档、删除或 VACUUM。'
        next_step_label = '先修复冷库绑定保活'
    elif apply_runner_available and apply_runner_status == 'archive_binding_active':
        status = 'archive_binding_active'
        headline = 'NAV 热冷库归档绑定已生效'
        message = _first_finding(apply_runner_payload) or (
            '最新 apply runner 证明归档库已对齐且热库冷数据已移除，运行期可启用 cold binding。'
        )
        action_hint = '继续保留周维护 dry-run 和 postcheck 证据；后续可单独评估 VACUUM INTO 或低频压缩窗口。'
        next_step_label = '进入归档后治理'
    elif apply_runner_available and apply_runner_status == 'dry_run_ready_for_archive_export':
        status = 'apply_dry_run_ready'
        headline = 'NAV 热冷库归档 dry-run 已通过导出门禁'
        message = _first_finding(apply_runner_payload) or (
            '最新 apply runner 已通过 readiness、热冷查询兼容、备份、cutoff 和计数门禁，但当前仍未执行写入或删除。'
        )
        action_hint = '下一步应实现 page-limited archive export；删除和 VACUUM 仍需等待归档计数核验闭合。'
        next_step_label = '实现 page-limited archive export'
    elif apply_runner_available and apply_runner_status == 'archive_export_in_progress':
        status = 'archive_export_in_progress'
        headline = 'NAV 归档导出正在分页推进'
        message = _first_finding(apply_runner_payload) or (
            '最新 apply runner 已向独立冷库写入部分历史 NAV，热库冷数据仍保留，运行期不会启用 cold binding。'
        )
        action_hint = '继续用 page-limited archive_export 续跑，直到归档库计数与预期冷数据完全对齐；仍禁止 hot delete 和 VACUUM。'
        next_step_label = '继续分页导出冷库'
    elif apply_runner_available and apply_runner_status == 'dry_run_ready_for_delete_gate_review':
        status = 'delete_gate_review'
        headline = 'NAV 归档库已对齐，等待删除门禁复核'
        message = _first_finding(apply_runner_payload) or (
            '归档库计数已与冷数据预期一致，但热库冷数据仍存在；运行期暂不启用 cold binding，避免重复读取。'
        )
        action_hint = '下一步应补齐删除门禁与 postcheck 自动证据，确认后再考虑热库裁剪。'
        next_step_label = '补齐删除门禁与 postcheck'
    elif apply_runner_available and apply_runner_status == 'archive_export_failed':
        status = 'blocked'
        headline = 'NAV 归档导出失败'
        message = _first_finding(apply_runner_payload) or (
            '最新 apply runner 在写入独立归档库时失败，已停止后续动作；热库删除和 VACUUM 均未执行。'
        )
        action_hint = '先检查 apply runner 的 archive_export_result、blockers 与归档库文件状态，再决定是否续跑或人工处理。'
        next_step_label = '先处理归档导出失败'
    elif apply_runner_available and apply_runner_status == 'blocked':
        status = 'blocked'
        headline = 'NAV 热冷库归档 dry-run 门禁未通过'
        message = _first_finding(apply_runner_payload) or (
            '最新 apply runner 发现 readiness、查询兼容、备份、目标库或计数门禁存在阻断项。'
        )
        action_hint = '先处理 apply runner blockers，不要执行真实归档、删除或 VACUUM。'
        next_step_label = '先消除 runner blockers'
    elif readiness_audit_available and readiness_audit_status == 'ready_for_manual_window_execution':
        status = 'execution_ready'
        headline = '主库冷数据归档已通过维护窗口准备度审计'
        message = _first_finding(readiness_audit_payload) or _first_finding(execution_checklist_payload) or (
            '当前 execution checklist、维护窗口包与环境准备度均已闭合，可进入人工维护窗口执行。'
        )
        action_hint = (
            '页面不提供 apply；应以最新 readiness audit + execution checklist 作为最终执行参考，在维护窗口内人工归档、删除、回退核对与补证。'
        )
        next_step_label = '进入人工维护窗口'
    elif readiness_audit_available and readiness_audit_status == 'blocked':
        status = 'blocked'
        headline = '主库冷数据归档维护准备度未通过'
        message = _first_finding(readiness_audit_payload) or (
            '当前 execution checklist 已存在，但路径、空间或来源一致性仍存在阻断项，不应进入真实维护窗口。'
        )
        action_hint = '先处理 readiness audit 阻断项，再讨论真实维护窗口。'
        next_step_label = '先消除阻断项'
    elif readiness_audit_available and readiness_audit_status == 'review_required':
        status = 'checklist_ready'
        headline = '主库冷数据归档已形成最终执行清单，但准备度仍待补齐'
        message = _first_finding(readiness_audit_payload) or _first_finding(execution_checklist_payload) or (
            '当前 execution checklist 已存在，但环境准备、责任人占位或 VACUUM 决策仍未闭合。'
        )
        action_hint = (
            '先处理 readiness audit 中的 review items，再进入真实维护窗口；页面仍不提供 apply。'
        )
        next_step_label = '先补齐 review items'
    elif manual_window_sop_available and ready_for_manual_signoff:
        status = 'window_ready'
        headline = '主库冷数据归档已形成人工维护窗口 SOP'
        message = _first_finding(manual_window_sop_payload) or _first_finding(readiness_audit_payload) or (
            '当前 execution checklist、准备度审计与补证模板已进一步收敛为单页维护窗口 SOP，可直接作为人工窗口执行对照表。'
        )
        action_hint = '页面不提供 apply；应以最新 manual window SOP 作为单页执行参考，在维护窗口内人工归档、删除、回滚核对与补证。'
        next_step_label = '进入人工维护窗口'
    elif execution_checklist_available and ready_for_manual_signoff:
        status = 'checklist_ready'
        headline = '主库冷数据归档已形成最终执行清单，等待准备度与补证模板闭合'
        message = _first_finding(execution_checklist_payload) or _first_finding(maintenance_window_pack_payload) or (
            '当前 runbook / preflight / control pack / maintenance window pack 已进一步收敛为最终执行清单，但仍缺少最后的环境准备度审计或执行后补证模板。'
        )
        action_hint = (
            '下一步应生成 maintenance readiness audit 与 post maintenance evidence，核对路径、磁盘、备份目录、责任人占位与补证模板；页面仍不提供 apply。'
        )
        next_step_label = '先生成 readiness audit / post evidence'
    elif maintenance_window_pack_available and ready_for_manual_signoff:
        status = 'window_ready'
        headline = '主库冷数据归档已形成维护窗口包'
        message = _first_finding(maintenance_window_pack_payload) or _first_finding(control_pack_payload) or (
            '当前 runbook / preflight / control pack 已进一步收敛为维护窗口包，可直接用于人工排窗与留痕。'
        )
        action_hint = (
            '页面不提供 apply；应以最新 maintenance window pack 作为单一执行模板，在维护窗口内人工归档、删除与补证。'
        )
        next_step_label = '先生成最终执行清单'
    elif control_pack_available and ready_for_manual_signoff:
        status = 'preflight_ready'
        headline = '主库冷数据归档已形成可签字控制包'
        message = _first_finding(control_pack_payload) or _first_finding(preflight_payload) or (
            '当前 runbook / preflight 已收敛为单一签字包，可进入人工维护窗口排期。'
        )
        action_hint = '优先以最新 control pack 作为签字材料；页面不提供 apply，真实执行仍需在维护窗口内人工完成。'
        next_step_label = '先生成维护窗口包'
    elif runbook_available and ready_for_manual_signoff:
        status = 'preflight_ready'
        headline = '主库冷数据归档已进入人工签字前状态'
        message = _first_finding(preflight_payload) or (
            '当前 live 冷数据范围与 runbook 一致，可进入人工签字与维护窗口审批。'
        )
        action_hint = '真实归档前仍需先做主库备份、归档目标校验和人工签字，不要直接在运行时执行删除。'
        next_step_label = '先生成 control pack'
    elif runbook_available and preflight_available:
        status = 'review_required'
        headline = '主库冷数据归档链路需要重新复核'
        message = _first_finding(preflight_payload) or (
            '当前 preflight 已发现 live 冷数据范围与 runbook 存在漂移，暂不适合进入真实归档。'
        )
        action_hint = '建议重新生成 runbook 或重新评估 cutoff，待 preflight ready_for_manual_signoff 后再推进。'
        next_step_label = '先重新生成 runbook / preflight'
    elif runbook_available and runbook_status == 'dense_fund_review':
        status = 'review_required'
        headline = '主库冷数据归档手册已生成，但仍需复核高密度异常'
        message = str(runbook_payload.get('message') or '').strip() or (
            '当前 runbook 已生成，但策略建议先复核高密度异常基金，再决定是否进入真实归档。'
        )
        action_hint = '先处理局部异常稠密基金，再重新生成 preflight，确认冷数据范围稳定。'
        next_step_label = '先复核高密度异常'
    elif runbook_available:
        status = 'runbook_ready'
        headline = '主库冷数据归档执行手册已成册'
        message = str(runbook_payload.get('message') or '').strip() or (
            '当前已生成归档 runbook，但还没有形成可签字的 preflight 结论。'
        )
        action_hint = '下一步应生成 preflight，确认 live 冷数据范围与 runbook 一致后再决定是否进入维护窗口。'
        next_step_label = '先生成 preflight'
    elif preflight_available:
        status = 'review_required'
        headline = '主库冷数据归档 preflight 已存在，但缺少对应 runbook'
        message = _first_finding(preflight_payload) or (
            '当前 preflight 缺少稳定的 runbook 对照来源，链路不完整。'
        )
        action_hint = '请先重新生成 runbook，再用同一批摘要重新生成 preflight。'
        next_step_label = '先补 runbook'
    elif (
        runbook_error
        or preflight_error
        or control_pack_error
        or maintenance_window_pack_error
        or execution_checklist_error
        or readiness_audit_error
        or post_maintenance_evidence_error
        or manual_window_sop_error
        or apply_runner_error
    ):
        message = '；'.join(
            item
            for item in (
                runbook_error,
                preflight_error,
                control_pack_error,
                maintenance_window_pack_error,
                execution_checklist_error,
                readiness_audit_error,
                post_maintenance_evidence_error,
                manual_window_sop_error,
                apply_runner_error,
            )
            if item
        )
        action_hint = '先排查 audit 目录产物格式与读取错误，再重新生成 NAV 归档摘要链路。'

    return {
        'available': available,
        'runbook_available': runbook_available,
        'preflight_available': preflight_available,
        'control_pack_available': control_pack_available,
        'maintenance_window_pack_available': maintenance_window_pack_available,
        'execution_checklist_available': execution_checklist_available,
        'readiness_audit_available': readiness_audit_available,
        'post_maintenance_evidence_available': post_maintenance_evidence_available,
        'manual_window_sop_available': manual_window_sop_available,
        'apply_runner_available': apply_runner_available,
        'runbook_generated_at': str(runbook_payload.get('generated_at') or '').strip(),
        'preflight_generated_at': str(preflight_payload.get('generated_at') or '').strip(),
        'control_pack_generated_at': str(control_pack_payload.get('generated_at') or '').strip(),
        'maintenance_window_pack_generated_at': str(
            maintenance_window_pack_payload.get('generated_at') or ''
        ).strip(),
        'execution_checklist_generated_at': str(
            execution_checklist_payload.get('generated_at') or ''
        ).strip(),
        'readiness_audit_generated_at': str(
            readiness_audit_payload.get('generated_at') or ''
        ).strip(),
        'post_maintenance_evidence_generated_at': str(
            post_maintenance_evidence_payload.get('generated_at') or ''
        ).strip(),
        'manual_window_sop_generated_at': str(
            manual_window_sop_payload.get('generated_at') or ''
        ).strip(),
        'apply_runner_generated_at': str(
            apply_runner_payload.get('generated_at') or ''
        ).strip(),
        'runbook_artifact_path': str(runbook_path or '').strip(),
        'preflight_artifact_path': str(preflight_path or '').strip(),
        'control_pack_artifact_path': str(control_pack_path or '').strip(),
        'maintenance_window_pack_artifact_path': str(maintenance_window_pack_path or '').strip(),
        'execution_checklist_artifact_path': str(execution_checklist_path or '').strip(),
        'readiness_audit_artifact_path': str(readiness_audit_path or '').strip(),
        'post_maintenance_evidence_artifact_path': str(
            post_maintenance_evidence_path or ''
        ).strip(),
        'manual_window_sop_artifact_path': str(
            manual_window_sop_path or ''
        ).strip(),
        'apply_runner_artifact_path': str(apply_runner_path or '').strip(),
        'runbook_status': runbook_status,
        'control_pack_status': control_pack_status,
        'maintenance_window_pack_status': maintenance_window_pack_status,
        'execution_checklist_status': execution_checklist_status,
        'readiness_audit_status': readiness_audit_status,
        'post_maintenance_evidence_status': str(
            post_maintenance_evidence_summary.get('template_status') or ''
        ).strip(),
        'manual_window_sop_status': str(
            manual_window_sop_summary.get('template_status') or ''
        ).strip(),
        'apply_runner_status': apply_runner_status,
        'auto_governance_stage': str(
            apply_runner_summary.get('auto_governance_stage') or ''
        ).strip(),
        'archive_export_in_progress': bool(
            apply_runner_summary.get('archive_export_in_progress')
        ),
        'archive_export_result_status': str(
            apply_runner_summary.get('archive_export_result_status') or ''
        ).strip(),
        'archive_export_inserted_row_count': _normalize_summary_int(
            apply_runner_summary.get('archive_export_inserted_row_count')
        ),
        'archive_sql_executed': bool(apply_runner_summary.get('archive_sql_executed')),
        'delete_sql_executed': bool(apply_runner_summary.get('delete_sql_executed')),
        'vacuum_executed': bool(apply_runner_summary.get('vacuum_executed')),
        'archive_binding_active': bool(apply_runner_summary.get('archive_binding_active')),
        'archive_materialized_and_reconciled': bool(
            apply_runner_summary.get('archive_materialized_and_reconciled')
        ),
        'archive_missing_row_count': _normalize_summary_int(
            apply_runner_summary.get('archive_missing_row_count')
        ),
        'apply_runner_blocker_count': _normalize_summary_int(
            apply_runner_summary.get('blocker_count')
        ),
        'preflight_ready_for_manual_signoff': ready_for_manual_signoff,
        'recommended_retention_years': recommended_retention_years if recommended_retention_years > 0 else None,
        'archive_cutoff_date': archive_cutoff_date,
        'estimated_archive_row_count': estimated_archive_row_count,
        'estimated_archive_size_mb': estimated_archive_size_mb,
        'archive_target_path': archive_target_path,
        'archive_table_name': archive_table_name,
        'archive_target_absolute_path': archive_target_absolute_path,
        'live_cold_row_count': live_cold_row_count,
        'live_cold_fund_count': live_cold_fund_count,
        'live_min_nav_date': live_min_nav_date,
        'live_max_nav_date': live_max_nav_date,
        'readiness_ready_for_manual_window_execution': bool(
            readiness_audit_summary.get('ready_for_manual_window_execution')
        ),
        'blocker_count': blocker_count,
        'review_item_count': review_item_count,
        'archive_target_disk_free_mb': archive_target_disk_free_mb,
        'project_disk_usage_percent': project_disk_usage_percent,
        'operator_placeholders_complete': operator_placeholders_complete,
        'backup_prerequisites_complete': backup_prerequisites_complete,
        'vacuum_decision_recorded': vacuum_decision_recorded,
        'page_apply_supported': page_apply_supported,
        'execution_mode': execution_mode,
        'final_reference_label': final_reference_label,
        'final_reference_artifact_path': final_reference_artifact_path,
        'final_reference_generated_at': final_reference_generated_at,
        'next_step_label': next_step_label,
        'blockers': readiness_blockers[:4],
        'review_items': readiness_review_items[:4],
        'final_gate_checks': final_gate_checks[:4],
        'execution_order': execution_order[:4],
        'rollback_triggers': rollback_triggers[:4],
        'operator_roles': [
            str(item.get('role') or '').strip()
            for item in operator_placeholder_checks[:4]
            if str(item.get('role') or '').strip()
        ],
        'status': status,
        'headline': headline,
        'message': message,
        'action_hint': action_hint,
    }


def _build_fund_nav_history_governance_summary(project_root: Path) -> Dict[str, object]:
    audit_dir = project_root / 'audit'

    artifact_path = None
    payload: Dict[str, object] = {}
    load_error = ''
    try:
        artifact_path, loaded_payload = load_latest_fund_nav_history_governance_audit_payload(
            output_dir=str(audit_dir)
        )
        payload = dict(loaded_payload or {})
    except FileNotFoundError:
        pass
    except Exception as exc:
        load_error = f'{type(exc).__name__}: {exc}'

    summary = dict(payload.get('summary') or {})
    findings = [
        str(item or '').strip()
        for item in (payload.get('findings') or [])
        if str(item or '').strip()
    ]
    recommendations = [
        str(item or '').strip()
        for item in (payload.get('recommendations') or [])
        if str(item or '').strip()
    ]
    available = artifact_path is not None
    status = str(summary.get('status') or '').strip()
    recommended_direction_key = str(summary.get('recommended_direction_key') or '').strip()
    recommended_direction_label = str(summary.get('recommended_direction_label') or '').strip()
    headline = str(summary.get('headline') or '').strip()
    message = str(summary.get('message') or '').strip()
    action_hint = str(summary.get('action_hint') or '').strip()

    if not available:
        status = 'unavailable'
        headline = 'fund_nav_history 治理审计摘要暂不可用'
        message = '当前还没有最新 fund_nav_history 治理审计产物，无法在首页收口下一步优化方向。'
        action_hint = '先执行 fund_nav_history 治理审计脚本，生成最新 JSON/Markdown 产物后再回看统一结论。'
        if load_error:
            message = load_error
            action_hint = '先修复治理审计产物的读取或格式问题，再重新生成最新产物。'

    return {
        'available': available,
        'artifact_path': str(artifact_path or '').strip(),
        'generated_at': str(payload.get('generated_at') or '').strip(),
        'status': status,
        'recommended_direction_key': recommended_direction_key,
        'recommended_direction_label': recommended_direction_label,
        'headline': headline,
        'message': message,
        'action_hint': action_hint,
        'database_size_mb': _normalize_summary_size_mb(summary.get('database_size_mb')),
        'database_path': str(summary.get('database_path') or '').strip(),
        'nav_history_table_size_mb': _normalize_summary_size_mb(summary.get('nav_history_table_size_mb')),
        'nav_history_index_total_size_mb': _normalize_summary_size_mb(summary.get('nav_history_index_total_size_mb')),
        'nav_history_hot_zone_size_mb': _normalize_summary_size_mb(summary.get('nav_history_hot_zone_size_mb')),
        'nav_history_hot_zone_share_of_db_pct': _normalize_summary_size_mb(
            summary.get('nav_history_hot_zone_share_of_db_pct')
        ),
        'recommended_retention_years': _normalize_summary_int(summary.get('recommended_retention_years')) or None,
        'archive_cutoff_date': str(summary.get('archive_cutoff_date') or '').strip(),
        'estimated_archive_row_count': _normalize_summary_int(summary.get('estimated_archive_row_count')),
        'estimated_archive_fund_count': _normalize_summary_int(summary.get('estimated_archive_fund_count')),
        'estimated_archive_size_mb': _normalize_summary_size_mb(summary.get('estimated_archive_size_mb')),
        'estimated_archive_share_pct': _normalize_summary_size_mb(summary.get('estimated_archive_share_pct')),
        'estimated_archive_share_of_db_pct': _normalize_summary_size_mb(
            summary.get('estimated_archive_share_of_db_pct')
        ),
        'estimated_archive_share_of_nav_hot_zone_pct': _normalize_summary_size_mb(
            summary.get('estimated_archive_share_of_nav_hot_zone_pct')
        ),
        'profile_status': str(summary.get('profile_status') or '').strip(),
        'strategy_status': str(summary.get('strategy_status') or '').strip(),
        'index_status': str(summary.get('index_status') or '').strip(),
        'index_runtime_drop_candidate': bool(summary.get('index_runtime_drop_candidate')),
        'index_legacy_hit_count': _normalize_summary_int(summary.get('index_legacy_hit_count')),
        'findings': findings[:4],
        'recommendations': recommendations[:4],
    }


def _build_database_backup_manual_review_summary(
    project_root: Path,
    *,
    database_backup_retention_preview: Optional[Dict[str, object]] = None,
) -> Dict[str, object]:
    audit_dir = project_root / 'audit'
    preview_payload = dict(database_backup_retention_preview or {})
    preview_summary = dict(preview_payload.get('summary') or {})
    manual_review_expected = _normalize_summary_int(
        preview_summary.get('manual_review_candidate_file_count')
    ) > 0

    control_pack_path = None
    control_pack_payload: Dict[str, object] = {}
    control_pack_error = ''
    try:
        control_pack_path, loaded_control_pack_payload = load_latest_database_backup_manual_review_control_pack_payload(
            output_dir=str(audit_dir)
        )
        control_pack_payload = dict(loaded_control_pack_payload or {})
    except FileNotFoundError:
        pass
    except Exception as exc:
        control_pack_error = f'{type(exc).__name__}: {exc}'

    control_pack_available = control_pack_path is not None
    summary = dict(control_pack_payload.get('summary') or {})
    package_status = str(summary.get('package_status') or '').strip()
    ready_for_manual_signoff = bool(summary.get('ready_for_manual_signoff'))

    status = 'unavailable'
    headline = '数据库备份 manual review 摘要暂不可用'
    message = '当前还没有数据库备份 manual review 控制包，无法收敛人工签字范围。'
    action_hint = '先基于最新数据库备份 preview 生成 manual review 控制包，再进入人工复核。'

    if control_pack_available and ready_for_manual_signoff:
        status = 'manual_review_ready'
        headline = '数据库备份 manual review 已形成可签字控制包'
        message = _first_finding(control_pack_payload) or (
            '当前数据库备份 manual review 候选已收敛为单一签字包，可进入人工维护窗口复核。'
        )
        action_hint = '页面不提供 manual review 文件自动删除；应以 control pack 作为签字材料，在维护窗口内人工确认并处置。'
    elif control_pack_available:
        status = 'observe_only'
        headline = '数据库备份 manual review 控制包已生成'
        message = _first_finding(control_pack_payload) or (
            '当前 manual review 控制包已生成，但暂未形成需要签字删除的备份范围。'
        )
        action_hint = '继续保留为只读记录即可；如后续再次出现超大快照，再重新进入人工复核。'
    elif manual_review_expected:
        status = 'control_pack_missing'
        headline = '数据库备份 manual review 签字包尚未生成'
        message = (
            '当前数据库备份 preview 已识别出 manual review 候选，但还缺少单一签字控制包。'
        )
        action_hint = '建议先基于最新 preview 生成数据库备份 manual review control pack，再进入人工复核与维护窗口签字。'
    elif preview_payload:
        status = 'not_required'
        headline = '当前数据库备份无需进入 manual review 签字'
        message = '当前备份 preview 未识别出需要人工复核的备份范围，manual review 控制包不是本轮刚需。'
        action_hint = '继续保留 preview 作为只读依据即可；若后续出现超大快照或未识别备份，再生成 control pack。'
    elif control_pack_error:
        message = control_pack_error
        action_hint = '先排查 audit 目录产物格式与读取错误，再重新生成数据库备份 manual review 控制包。'

    fallback_summary = summary or preview_summary
    return {
        'available': control_pack_available or manual_review_expected,
        'manual_review_expected': manual_review_expected,
        'control_pack_available': control_pack_available,
        'control_pack_generated_at': str(control_pack_payload.get('generated_at') or '').strip(),
        'control_pack_artifact_path': str(control_pack_path or '').strip(),
        'package_status': package_status,
        'ready_for_manual_signoff': ready_for_manual_signoff,
        'manual_review_candidate_file_count': _normalize_summary_int(
            fallback_summary.get('manual_review_candidate_file_count')
        ),
        'manual_review_candidate_size_mb': _normalize_summary_size_mb(
            fallback_summary.get('manual_review_candidate_size_mb')
        ),
        'oversized_manual_snapshot_review_file_count': _normalize_summary_int(
            fallback_summary.get('oversized_manual_snapshot_review_file_count')
        ),
        'oversized_manual_snapshot_review_size_mb': _normalize_summary_size_mb(
            fallback_summary.get('oversized_manual_snapshot_review_size_mb')
        ),
        'manual_snapshot_count': _normalize_summary_int(fallback_summary.get('manual_snapshot_count')),
        'manual_snapshot_size_mb': _normalize_summary_size_mb(fallback_summary.get('manual_snapshot_size_mb')),
        'target_backup_dir': str(
            summary.get('target_backup_dir')
            or preview_payload.get('backup_dir')
            or ''
        ).strip(),
        'active_database_path': str(fallback_summary.get('active_database_path') or '').strip(),
        'active_database_size_mb': _normalize_summary_size_mb(fallback_summary.get('active_database_size_mb')),
        'status': status,
        'headline': headline,
        'message': message,
        'action_hint': action_hint,
    }


def _build_database_backup_manual_review_runbook_summary(
    project_root: Path,
    *,
    database_backup_manual_review_summary: Optional[Dict[str, object]] = None,
) -> Dict[str, object]:
    audit_dir = project_root / 'audit'
    task_summary = dict(database_backup_manual_review_summary or {})

    runbook_path = None
    runbook_payload: Dict[str, object] = {}
    runbook_error = ''
    try:
        runbook_path, loaded_runbook_payload = load_latest_database_backup_manual_review_runbook_payload(
            output_dir=str(audit_dir)
        )
        runbook_payload = dict(loaded_runbook_payload or {})
    except FileNotFoundError:
        pass
    except Exception as exc:
        runbook_error = f'{type(exc).__name__}: {exc}'

    runbook_available = runbook_path is not None
    summary = dict(runbook_payload.get('summary') or {})
    runbook_status = str(summary.get('runbook_status') or '').strip()
    ready_for_manual_signoff = bool(summary.get('ready_for_manual_signoff'))
    control_pack_expected = bool(task_summary.get('control_pack_available'))
    control_pack_artifact_path = str(task_summary.get('control_pack_artifact_path') or '').strip()

    status = 'unavailable'
    headline = '数据库备份人工复核 runbook 暂不可用'
    message = '当前还没有数据库备份 manual review 执行手册，无法沉淀最终签字记录模板。'
    action_hint = '建议先基于最新 control pack 生成 runbook，固化删前核对、签字记录和删后补证要求。'

    if runbook_available and ready_for_manual_signoff:
        status = 'runbook_ready'
        headline = '数据库备份人工复核执行手册已就绪'
        message = str(runbook_payload.get('message') or '').strip() or (
            '当前数据库备份 manual review 已形成只读 runbook，可在维护窗口内据此完成人工处置记录。'
        )
        action_hint = str(runbook_payload.get('action_hint') or '').strip() or (
            '页面不提供删除入口；应按 runbook 模板完成人工签字、删除和补证。'
        )
    elif runbook_available:
        status = 'observe_only'
        headline = '数据库备份人工复核执行手册已生成'
        message = str(runbook_payload.get('message') or '').strip() or (
            '当前 manual review runbook 已生成，但本轮仍以只读记录为主。'
        )
        action_hint = str(runbook_payload.get('action_hint') or '').strip() or '继续保留为只读记录即可。'
    elif control_pack_expected:
        status = 'runbook_missing'
        headline = '数据库备份人工复核 runbook 尚未生成'
        message = '当前已有 manual review control pack，但缺少最终执行手册和处置记录模板。'
        action_hint = '建议先生成数据库备份 manual review runbook，再进入维护窗口人工签字。'
    elif runbook_error:
        message = runbook_error
        action_hint = '先排查 audit 目录产物格式与读取错误，再重新生成数据库备份 manual review runbook。'

    return {
        'available': runbook_available or control_pack_expected,
        'runbook_available': runbook_available,
        'generated_at': str(runbook_payload.get('generated_at') or '').strip(),
        'artifact_path': str(runbook_path or '').strip(),
        'source_control_pack_artifact': str(
            runbook_payload.get('source_database_backup_manual_review_control_pack_artifact') or ''
        ).strip(),
        'source_control_pack_generated_at': str(
            runbook_payload.get('source_control_pack_generated_at') or ''
        ).strip(),
        'control_pack_artifact_path': control_pack_artifact_path,
        'runbook_status': runbook_status,
        'ready_for_manual_signoff': ready_for_manual_signoff,
        'manual_review_candidate_file_count': _normalize_summary_int(
            summary.get('manual_review_candidate_file_count')
        ),
        'manual_review_candidate_size_mb': _normalize_summary_size_mb(
            summary.get('manual_review_candidate_size_mb')
        ),
        'target_backup_dir': str(summary.get('target_backup_dir') or '').strip(),
        'active_database_path': str(summary.get('active_database_path') or '').strip(),
        'active_database_size_mb': _normalize_summary_size_mb(summary.get('active_database_size_mb')),
        'status': status,
        'headline': headline,
        'message': message,
        'action_hint': action_hint,
    }


def _list_all_files(root_dir: Path) -> List[Path]:
    if not root_dir.exists():
        return []

    files: List[Path] = []
    for dirpath, _, filenames in os.walk(root_dir, onerror=lambda _: None):
        dir_path = Path(dirpath)
        for filename in filenames:
            files.append(dir_path / filename)
    return files


def _summarize_directory(path: Path, *, project_root: Path) -> Dict[str, object]:
    files = _list_all_files(path)
    total_size_bytes = 0
    for file_path in files:
        try:
            total_size_bytes += int(file_path.stat().st_size)
        except OSError:
            continue

    return {
        'path': _to_relative_path(path, project_root),
        'exists': path.exists(),
        'file_count': len(files),
        'total_size_bytes': total_size_bytes,
        'total_size_mb': round(total_size_bytes / (1024 * 1024), 2),
    }


def _summarize_file_collection(
    files: Sequence[Path],
    *,
    project_root: Path,
    path_label: str,
    exists: bool,
) -> Dict[str, object]:
    total_size_bytes = 0
    for file_path in files:
        try:
            total_size_bytes += int(file_path.stat().st_size)
        except OSError:
            continue

    return {
        'path': path_label,
        'exists': exists,
        'file_count': len(files),
        'total_size_bytes': total_size_bytes,
        'total_size_mb': round(total_size_bytes / (1024 * 1024), 2),
    }


def _build_largest_files(files: Sequence[Path], *, project_root: Path, limit: int) -> List[Dict[str, object]]:
    entries: List[Dict[str, object]] = []
    for path in files:
        try:
            stat = path.stat()
        except OSError:
            continue

        entries.append(
            {
                'path': _to_relative_path(path, project_root),
                'size_bytes': int(stat.st_size),
                'modified_at': _format_datetime(datetime.fromtimestamp(stat.st_mtime)),
            }
        )

    entries.sort(key=lambda item: (item['size_bytes'], item['path']), reverse=True)
    return entries[: max(int(limit), 0)]


def _classify_disk_usage(usage_percent: float) -> str:
    if usage_percent >= 95:
        return 'critical'
    if usage_percent >= 85:
        return 'warning'
    return 'healthy'


def _match_relative_paths(files: Sequence[Path], *, project_root: Path, pattern: str) -> List[Path]:
    compiled = re.compile(pattern)
    matched: List[Path] = []
    for path in files:
        relative_path = _to_relative_path(path, project_root)
        if compiled.match(relative_path):
            matched.append(path)
    return matched


def _build_retention_preview(
    groups: Dict[str, Dict[str, object]],
    *,
    limit_per_group: int = 5,
) -> Dict[str, object]:
    preview_groups: List[Dict[str, object]] = []
    for key, group in groups.items():
        older_count = int(group.get('older_than_suggested_count', 0) or 0)
        if older_count <= 0:
            continue

        older_entries = sorted(
            (group.get('older_entries') or []),
            key=lambda item: (item.get('size_bytes', 0), item.get('path', '')),
            reverse=True,
        )
        preview_groups.append(
            {
                'key': key,
                'label': group.get('label'),
                'suggested_retention_days': group.get('suggested_retention_days'),
                'runtime_dependency': group.get('runtime_dependency'),
                'candidate_file_count': older_count,
                'candidate_size_bytes': int(group.get('older_than_suggested_size_bytes', 0) or 0),
                'candidate_size_mb': round(
                    int(group.get('older_than_suggested_size_bytes', 0) or 0) / (1024 * 1024),
                    2,
                ),
                'sample_files': older_entries[: max(int(limit_per_group), 0)],
            }
        )

    preview_groups.sort(
        key=lambda item: (item['candidate_size_bytes'], item['candidate_file_count'], item['key']),
        reverse=True,
    )
    return {
        'candidate_group_count': len(preview_groups),
        'candidate_file_count': sum(item['candidate_file_count'] for item in preview_groups),
        'candidate_size_bytes': sum(item['candidate_size_bytes'] for item in preview_groups),
        'candidate_size_mb': round(
            sum(item['candidate_size_bytes'] for item in preview_groups) / (1024 * 1024),
            2,
        ),
        'groups': preview_groups,
    }


def _merge_retention_preview_groups(
    base_preview: Dict[str, object],
    extra_groups: Sequence[Dict[str, object]],
) -> Dict[str, object]:
    merged_groups = list(base_preview.get('groups') or []) + list(extra_groups or [])
    merged_groups.sort(
        key=lambda item: (
            int(item.get('candidate_size_bytes', 0) or 0),
            int(item.get('candidate_file_count', 0) or 0),
            str(item.get('key', '')),
        ),
        reverse=True,
    )
    return {
        'candidate_group_count': len(merged_groups),
        'candidate_file_count': sum(int(item.get('candidate_file_count', 0) or 0) for item in merged_groups),
        'candidate_size_bytes': sum(int(item.get('candidate_size_bytes', 0) or 0) for item in merged_groups),
        'candidate_size_mb': round(
            sum(int(item.get('candidate_size_bytes', 0) or 0) for item in merged_groups) / (1024 * 1024),
            2,
        ),
        'groups': merged_groups,
    }


def _extract_specialized_group_rows(
    candidate_groups: Sequence[Dict[str, object]],
    *,
    prefix: str,
    label_prefix: str,
    suggested_retention_days: Optional[int],
) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    for item in candidate_groups or []:
        candidate_file_count = int(
            item.get('candidate_file_count', item.get('candidate_entry_count', 0)) or 0
        )
        candidate_size_bytes = int(
            item.get('candidate_size_bytes', 0) or 0
        )
        runtime_dependency = str(item.get('runtime_dependency') or '').strip()
        rows.append(
            {
                'key': f"{prefix}:{item.get('key') or item.get('family_key') or item.get('label') or 'unknown'}",
                'label': f"{label_prefix} / {item.get('label') or '未分类'}",
                'suggested_retention_days': suggested_retention_days,
                'runtime_dependency': runtime_dependency,
                'candidate_file_count': candidate_file_count,
                'candidate_size_bytes': candidate_size_bytes,
                'candidate_size_mb': round(candidate_size_bytes / (1024 * 1024), 2),
                'sample_files': list(item.get('sample_files') or item.get('sample_entries') or []),
                'disposition': item.get('disposition'),
                'reason': item.get('reason'),
            }
        )
    return rows


def _strip_internal_entries(groups: Dict[str, Dict[str, object]]) -> Dict[str, Dict[str, object]]:
    cleaned: Dict[str, Dict[str, object]] = {}
    for key, group in groups.items():
        cloned = dict(group)
        cloned.pop('older_entries', None)
        cleaned[key] = cloned
    return cleaned


def _collect_rule_files(
    target_dir: Path,
    patterns: Sequence[str],
    *,
    top_level_only: bool = False,
) -> List[Path]:
    if not target_dir.exists():
        return []

    normalized_patterns = tuple(str(pattern).strip() for pattern in (patterns or ()) if str(pattern).strip())
    if not normalized_patterns or normalized_patterns == ('*',):
        if top_level_only:
            return sorted([path for path in target_dir.iterdir() if path.is_file()])
        return _list_all_files(target_dir)

    discovered: List[Path] = []
    seen: Set[Path] = set()
    for pattern in normalized_patterns:
        iterator = target_dir.glob(pattern) if top_level_only else target_dir.rglob(pattern)
        for path in iterator:
            if not path.is_file():
                continue
            resolved = path.resolve()
            if resolved in seen:
                continue
            seen.add(resolved)
            discovered.append(path)
    return discovered


def build_log_retention_plan(
    project_root: Path | str,
    *,
    now: Optional[datetime] = None,
    top_n: int = 5,
) -> Dict[str, object]:
    """
    生成日志清理 dry-run 计划。

    仅返回建议保留窗口之外的候选文件清单，不执行任何删除动作。
    """
    summary = summarize_task_artifacts(
        project_root,
        now=now,
        top_n=top_n,
        cache_ttl_seconds=0,
    )

    root = Path(project_root).resolve()
    effective_now = now or datetime.now()
    all_log_files = _list_all_files(root / 'logs')

    raw_families: Dict[str, Dict[str, object]] = {}
    for rule in LOG_FAMILY_RULES:
        matched_family_files = _match_relative_paths(
            all_log_files,
            project_root=root,
            pattern=str(rule['pattern']),
        )
        raw_families[str(rule['key'])] = _summarize_files(
            matched_family_files,
            project_root=root,
            now=effective_now,
            label=str(rule['label']),
            runtime_dependency=str(rule['runtime_dependency']),
            suggested_retention_days=int(rule['suggested_retention_days']),
        )

    groups: List[Dict[str, object]] = []
    for key, family in raw_families.items():
        older_entries = family.get('older_entries') or []
        if not older_entries:
            continue
        groups.append(
            {
                'key': key,
                'label': family.get('label'),
                'runtime_dependency': family.get('runtime_dependency'),
                'suggested_retention_days': family.get('suggested_retention_days'),
                'candidate_file_count': family.get('older_than_suggested_count', 0),
                'candidate_size_bytes': family.get('older_than_suggested_size_bytes', 0),
                'candidate_files': older_entries,
            }
        )

    groups.sort(
        key=lambda item: (
            int(item.get('candidate_size_bytes', 0) or 0),
            int(item.get('candidate_file_count', 0) or 0),
            str(item.get('key', '')),
        ),
        reverse=True,
    )

    return {
        'generated_at': summary.get('generated_at'),
        'disk_usage': summary.get('disk_usage'),
        'retention_preview': summary.get('retention_preview'),
        'largest_files': summary.get('largest_files'),
        'groups': groups,
    }


def summarize_task_artifacts(
    project_root: Path | str,
    *,
    now: Optional[datetime] = None,
    top_n: int = 5,
    cache_ttl_seconds: int = 60,
    detail_level: str = 'full',
    runtime_config: Optional[dict] = None,
    screening_storage_pressure: Optional[dict] = None,
) -> Dict[str, object]:
    """
    汇总日志与任务副产物占用情况。

    Args:
        project_root: 项目根目录。
        now: 当前时间，测试可注入。
        top_n: 返回最大的前 N 个文件。
        cache_ttl_seconds: 缓存秒数；0 表示禁用缓存。
    """
    root = Path(project_root).resolve()
    effective_now = now or datetime.now()
    normalized_detail_level = _normalize_task_artifact_detail_level(detail_level)
    overview_only = normalized_detail_level == 'overview'
    backup_cleanup_policy = get_database_backup_cleanup_policy(runtime_config)
    screening_output_cleanup_policy = get_screening_output_cleanup_policy(runtime_config)
    cache_key = (
        str(root),
        int(top_n),
        normalized_detail_level,
        int(backup_cleanup_policy.get('scheduled_keep_count', 0)),
        int(backup_cleanup_policy.get('unmanaged_retention_days', 0)),
        int(backup_cleanup_policy.get('keep_latest_manual_snapshot_per_family', 0)),
        int(screening_output_cleanup_policy.get('retention_days', 0)),
        bool(screening_output_cleanup_policy.get('keep_latest_per_family', True)),
        _build_screening_storage_pressure_signature(screening_storage_pressure),
    )
    now_ts = time.time()

    if cache_ttl_seconds > 0:
        with _CACHE_LOCK:
            if (
                _SUMMARY_CACHE.get('key') == cache_key
                and float(_SUMMARY_CACHE.get('expires_at', 0.0)) > now_ts
                and _SUMMARY_CACHE.get('payload') is not None
            ):
                return deepcopy(_SUMMARY_CACHE['payload'])  # type: ignore[arg-type]

    logs_dir = root / 'logs'
    tasks_dir = logs_dir / 'tasks'

    categories: Dict[str, Dict[str, object]] = {}
    families: Dict[str, Dict[str, object]] = {}
    all_log_files: List[Path] = []
    project_categories: Dict[str, Dict[str, object]] = {}
    if not overview_only:
        matched_paths: Set[Path] = set()

        for rule in TASK_ARTIFACT_RULES:
            target_dir = root / str(rule['relative_dir'])
            files = list(
                _iter_top_level_files(
                    target_dir,
                    str(rule['pattern']),
                    exclude_prefixes=tuple(rule.get('exclude_prefixes', ())),
                )
            )
            matched_paths.update(files)
            categories[str(rule['key'])] = _summarize_files(
                files,
                project_root=root,
                now=effective_now,
                label=str(rule['label']),
                runtime_dependency=str(rule['runtime_dependency']),
                suggested_retention_days=int(rule['suggested_retention_days']),
            )

        all_log_files = _list_all_files(logs_dir)
        other_files = [path for path in all_log_files if path not in matched_paths]
        categories['other_artifacts'] = _summarize_other_files(
            other_files,
            project_root=root,
            now=effective_now,
            label='其他日志副产物',
        )

        for rule in LOG_FAMILY_RULES:
            matched_family_files = _match_relative_paths(
                all_log_files,
                project_root=root,
                pattern=str(rule['pattern']),
            )
            families[str(rule['key'])] = _summarize_files(
                matched_family_files,
                project_root=root,
                now=effective_now,
                label=str(rule['label']),
                runtime_dependency=str(rule['runtime_dependency']),
                suggested_retention_days=int(rule['suggested_retention_days']),
            )

    project_directories: Dict[str, Dict[str, object]] = {}
    project_artifact_files: List[Path] = []
    for rule in PROJECT_ARTIFACT_RULES:
        target_dir = root / str(rule['relative_dir'])
        top_level_only = bool(rule.get('top_level_only', False))
        files = _collect_rule_files(
            target_dir,
            tuple(rule.get('patterns', ('*',))),
            top_level_only=top_level_only,
        )
        project_artifact_files.extend(files)
        project_categories[str(rule['key'])] = _summarize_files(
            files,
            project_root=root,
            now=effective_now,
            label=str(rule['label']),
            runtime_dependency=str(rule['runtime_dependency']),
            suggested_retention_days=int(rule['suggested_retention_days']),
        )
        directory_summary = (
            _summarize_file_collection(
                files,
                project_root=root,
                path_label='.',
                exists=target_dir.exists(),
            )
            if top_level_only
            else _summarize_directory(target_dir, project_root=root)
        )
        project_directories[str(rule['key'])] = {
            **directory_summary,
            'label': str(rule['label']),
            'runtime_dependency': str(rule['runtime_dependency']),
            'suggested_retention_days': int(rule['suggested_retention_days']),
        }

    database_backup_retention_preview = build_database_backup_cleanup_preview_payload(
        backup_dir=root / 'backend' / 'data' / 'backups',
        active_database_path=root / 'backend' / 'data' / 'bond_funds.db',
        scheduled_keep_count=int(
            backup_cleanup_policy.get('scheduled_keep_count', DEFAULT_SCHEDULED_BACKUP_KEEP_COUNT)
        ),
        unmanaged_retention_days=int(
            backup_cleanup_policy.get(
                'unmanaged_retention_days',
                project_directories.get('database_backups', {}).get('suggested_retention_days', 14) or 14,
            )
        ),
        keep_latest_manual_snapshot_per_family=int(
            backup_cleanup_policy.get('keep_latest_manual_snapshot_per_family', 1)
        ),
        now=effective_now,
    )
    audit_artifact_retention_preview = build_audit_artifact_cleanup_preview_payload(
        target_dir=root / 'audit',
        retention_days=int(
            project_directories.get('audit_artifacts', {}).get('suggested_retention_days', 30) or 30
        ),
        now=effective_now,
    )
    screening_output_retention_preview = build_screening_output_cleanup_preview_payload(
        output_dir=root / 'bond_fund_screening' / 'output',
        retention_days=int(
            screening_output_cleanup_policy.get(
                'retention_days',
                project_directories.get('screening_output_artifacts', {}).get('suggested_retention_days', 14) or 14,
            )
        ),
        keep_latest_per_family=bool(screening_output_cleanup_policy.get('keep_latest_per_family', True)),
        detail_level='overview' if overview_only else 'full',
        now=effective_now,
    )
    maintenance_execution_rollup = _build_maintenance_execution_rollup(root)
    maintenance_effect_delta_summary = _build_maintenance_effect_delta_summary(
        root,
        maintenance_execution_rollup=maintenance_execution_rollup,
        project_directories=project_directories,
        database_backup_retention_preview=database_backup_retention_preview,
        audit_artifact_retention_preview=audit_artifact_retention_preview,
        screening_output_retention_preview=screening_output_retention_preview,
        screening_storage_pressure=screening_storage_pressure,
    )
    nav_history_archive_summary = _build_nav_history_archive_summary(root)
    fund_nav_history_governance_summary = _build_fund_nav_history_governance_summary(root)
    database_backup_manual_review_summary = _build_database_backup_manual_review_summary(
        root,
        database_backup_retention_preview=database_backup_retention_preview,
    )
    database_backup_manual_review_runbook_summary = _build_database_backup_manual_review_runbook_summary(
        root,
        database_backup_manual_review_summary=database_backup_manual_review_summary,
    )

    disk_usage = shutil.disk_usage(root)
    usage_percent = (
        round((disk_usage.used / disk_usage.total) * 100, 2)
        if disk_usage.total
        else 0.0
    )

    retention_preview = _build_retention_preview(families) if not overview_only else _empty_retention_preview()
    generic_project_retention_preview = _build_retention_preview(
        {
            key: value
            for key, value in project_categories.items()
            if key not in {'database_backups', 'audit_artifacts', 'screening_output_artifacts'}
        }
    )
    database_backup_preview_groups = [
        {
            'key': f"database_backups:{item.get('key')}",
            'label': f"数据库备份 / {item.get('label')}",
            'suggested_retention_days': project_directories.get('database_backups', {}).get('suggested_retention_days'),
            'runtime_dependency': item.get('runtime_dependency'),
            'candidate_file_count': int(item.get('candidate_file_count', 0) or 0),
            'candidate_size_bytes': int(item.get('candidate_size_bytes', 0) or 0),
            'candidate_size_mb': round(
                int(item.get('candidate_size_bytes', 0) or 0) / (1024 * 1024),
                2,
            ),
            'sample_files': list(item.get('sample_files') or []),
            'disposition': item.get('disposition'),
            'reason': item.get('reason'),
        }
        for item in (database_backup_retention_preview.get('candidate_groups') or [])
    ]
    audit_preview_groups = _extract_specialized_group_rows(
        audit_artifact_retention_preview.get('candidate_groups') or [],
        prefix='audit_artifacts',
        label_prefix='审计产物',
        suggested_retention_days=project_directories.get('audit_artifacts', {}).get('suggested_retention_days'),
    )
    screening_output_preview_groups = _extract_specialized_group_rows(
        screening_output_retention_preview.get('candidate_groups') or [],
        prefix='screening_output_artifacts',
        label_prefix='筛选输出',
        suggested_retention_days=project_directories.get('screening_output_artifacts', {}).get('suggested_retention_days'),
    )
    project_retention_preview = _merge_retention_preview_groups(
        generic_project_retention_preview,
        database_backup_preview_groups + audit_preview_groups + screening_output_preview_groups,
    )

    payload = {
        'generated_at': _format_datetime(effective_now),
        'disk_usage': {
            'total_bytes': int(disk_usage.total),
            'used_bytes': int(disk_usage.used),
            'free_bytes': int(disk_usage.free),
            'usage_percent': usage_percent,
            'status': _classify_disk_usage(usage_percent),
        },
        'directories': (
            {
                'logs': _summarize_directory(logs_dir, project_root=root),
                'tasks': _summarize_directory(tasks_dir, project_root=root),
            }
            if not overview_only
            else {}
        ),
        'categories': _strip_internal_entries(categories) if not overview_only else {},
        'families': _strip_internal_entries(families) if not overview_only else {},
        'retention_preview': retention_preview,
        'largest_files': (
            _build_largest_files(all_log_files, project_root=root, limit=top_n)
            if not overview_only
            else []
        ),
        'project_directories': project_directories if not overview_only else {},
        'project_categories': _strip_internal_entries(project_categories) if not overview_only else {},
        'database_backup_retention_preview': database_backup_retention_preview,
        'audit_artifact_retention_preview': audit_artifact_retention_preview,
        'screening_output_retention_preview': screening_output_retention_preview,
        'maintenance_execution_rollup': maintenance_execution_rollup,
        'maintenance_effect_delta_summary': maintenance_effect_delta_summary,
        'nav_history_archive_summary': nav_history_archive_summary,
        'fund_nav_history_governance_summary': fund_nav_history_governance_summary,
        'database_backup_manual_review_summary': database_backup_manual_review_summary,
        'database_backup_manual_review_runbook_summary': database_backup_manual_review_runbook_summary,
        'project_retention_preview': project_retention_preview,
        'project_largest_files': (
            _build_largest_files(project_artifact_files, project_root=root, limit=top_n)
            if not overview_only
            else []
        ),
    }

    if cache_ttl_seconds > 0:
        with _CACHE_LOCK:
            _SUMMARY_CACHE['key'] = cache_key
            _SUMMARY_CACHE['expires_at'] = now_ts + int(cache_ttl_seconds)
            _SUMMARY_CACHE['payload'] = deepcopy(payload)

    return payload
