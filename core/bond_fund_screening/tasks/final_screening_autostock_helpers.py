#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""最终筛选 AutoStock 交叉验证辅助工具。"""

from __future__ import annotations

from utils.autostock_quota_state import read_quota_state
from utils.autostock_cache_storage import cache_keys, read_cache, write_cache

import hashlib
import json
import math
import time
from datetime import datetime
from datetime import date as date_type
from pathlib import Path

import pandas as pd

try:
    import fcntl
except ImportError:  # pragma: no cover - Windows
    fcntl = None

AUTOSTOCK_QUOTA_WINDOW_SECONDS = 3600

AUTOSTOCK_VALIDATION_DETAIL_REQUIRED_FIELDS = (
    '基金代码',
    '基金名称',
    '本地基金类型',
    'AutoStock基金类型',
    '类型一致',
    '失败原因',
    '告警原因',
)

AUTOSTOCK_VALIDATION_RETURN_DETAIL_FIELDS = (
    'return_1w',
    'return_1m',
    'return_3m',
    'return_6m',
    'return_1y',
)


def _normalize_positive_int(value, default: int) -> int:
    """将输入规整为正整数，非法时回退默认值。"""
    try:
        parsed = int(value)
        if parsed > 0:
            return parsed
    except (TypeError, ValueError):
        pass
    return int(default)


def load_autostock_cross_validation_config(
    *,
    config_loader,
    env,
    default_base_url: str,
    safe_batch_size: int,
    print_fn=print,
) -> dict:
    """读取 AutoStock 交叉验证配置，统一对齐现有验证器口径。"""
    try:
        config = config_loader() if callable(config_loader) else {}
    except Exception as exc:
        print_fn(f"  ⚠️ 读取 AutoStock 配置失败，使用默认配置: {exc}")
        config = {}

    autostock_cfg = config.get('api', {}).get('autostock', {}) if isinstance(config, dict) else {}
    rate_limit_cfg = autostock_cfg.get('rate_limit', {}) or {}
    batch_cfg = autostock_cfg.get('batch', {}) or {}
    verification_cfg = autostock_cfg.get('verification', {}) or {}
    tolerance_cfg = verification_cfg.get('tolerance', {}) or {}
    budget_cfg = verification_cfg.get('budget', {}) or {}
    residual_missing_cfg = verification_cfg.get('residual_missing', {}) or {}

    return {
        'enabled': autostock_cfg.get('enabled', True) is True,
        'base_url': str(
            env.get(
                'AUTOSTOCK_API_BASE_URL',
                autostock_cfg.get('base_url', default_base_url),
            )
        ).strip(),
        'min_interval': float(
            env.get(
                'AUTOSTOCK_MIN_INTERVAL',
                rate_limit_cfg.get('min_interval', autostock_cfg.get('min_interval', 2.0)),
            )
        ),
        'timeout': int(
            env.get(
                'AUTOSTOCK_TIMEOUT',
                autostock_cfg.get('retry', {}).get('timeout', 60),
            )
        ),
        'max_calls_per_hour': _normalize_positive_int(
            env.get(
                'AUTOSTOCK_MAX_CALLS_PER_HOUR',
                rate_limit_cfg.get('max_calls_per_hour', autostock_cfg.get('max_calls_per_hour', 100)),
            ),
            100,
        ),
        'batch_size': _normalize_positive_int(
            env.get(
                'AUTOSTOCK_VERIFY_BATCH_SIZE',
                batch_cfg.get('verify_size', batch_cfg.get('size', safe_batch_size)),
            ),
            safe_batch_size,
        ),
        'budget_reserve_calls': _normalize_positive_int(
            env.get(
                'AUTOSTOCK_WORKFLOW_VERIFY_RESERVE_CALLS',
                budget_cfg.get('reserve_calls', 10),
            ),
            10,
        ),
        'max_fetch_calls_per_run': _normalize_positive_int(
            env.get(
                'AUTOSTOCK_WORKFLOW_VERIFY_MAX_CALLS_PER_RUN',
                budget_cfg.get('max_fetch_calls_per_run', 50),
            ),
            50,
        ),
        'residual_missing_max_count': _normalize_positive_int(
            env.get(
                'AUTOSTOCK_VERIFY_RESIDUAL_MISSING_MAX_COUNT',
                residual_missing_cfg.get('max_count', 2),
            ),
            2,
        ),
        'residual_missing_max_ratio': max(
            0.0,
            float(
                env.get(
                    'AUTOSTOCK_VERIFY_RESIDUAL_MISSING_MAX_RATIO',
                    residual_missing_cfg.get('max_ratio', 0.002),
                )
            ),
        ),
        'nav_relative_tolerance_pct': float(
            tolerance_cfg.get('nav_relative', tolerance_cfg.get('nav', 1.0))
        ),
        'nav_absolute_tolerance': float(tolerance_cfg.get('nav_absolute', 0.001)),
        'return_relative_tolerance_pct': float(
            tolerance_cfg.get('return_relative', tolerance_cfg.get('return', 5.0))
        ),
        'return_absolute_tolerance': float(tolerance_cfg.get('return_absolute', 0.5)),
        'return_small_base_threshold': float(
            tolerance_cfg.get('return_small_base_threshold', 1.0)
        ),
        'nav_date_soft_lag_days': _normalize_positive_int(
            env.get(
                'AUTOSTOCK_NAV_DATE_SOFT_LAG_DAYS',
                tolerance_cfg.get('nav_date_soft_lag_days', 5),
            ),
            5,
        ),
    }


def read_autostock_quota_snapshot(
    *,
    shared_state_path: str | Path | None,
    max_calls_per_hour: int,
    min_interval_seconds: float,
    now: float | None = None,
    fail_closed: bool = True,
) -> dict:
    """读取 AutoStock 共享配额窗口，口径与 AutoStockClient 的限速器一致。"""
    current_time = time.time() if now is None else float(now)
    max_calls = int(max_calls_per_hour or 100)
    if max_calls <= 0:
        max_calls = 100
    state_path = Path(shared_state_path) if shared_state_path else None
    timestamps: list[float] = []
    last_call_time = 0.0
    blocked_until = 0.0
    read_error: str | None = None

    if state_path:
        try:
            payload = read_quota_state(state_path)
            last_call_time = payload["last_call_time"]
            blocked_until = payload["blocked_until"]
            timestamps = [value for value in payload["recent_call_timestamps"]
                          if current_time - value < AUTOSTOCK_QUOTA_WINDOW_SECONDS]
        except Exception as exc:
            read_error = str(exc)

    quota_limited = max_calls > 0
    recent_call_count = len(timestamps)
    quota_remaining = max(max_calls - recent_call_count, 0) if quota_limited else None
    upstream_cooldown_active = blocked_until > current_time
    if (upstream_cooldown_active or (read_error and fail_closed)) and quota_limited:
        quota_remaining = 0
    quota_exhausted = bool(upstream_cooldown_active or (quota_limited and quota_remaining == 0))
    wait_seconds = max(0.0, blocked_until - current_time)
    if quota_limited and recent_call_count >= max_calls and timestamps:
        wait_seconds = max(
            wait_seconds,
            AUTOSTOCK_QUOTA_WINDOW_SECONDS - (current_time - min(timestamps)),
        )

    return {
        'quota_limited': quota_limited,
        'max_calls_per_hour': max_calls if quota_limited else None,
        'recent_call_count': recent_call_count,
        'quota_remaining': quota_remaining,
        'quota_exhausted': quota_exhausted,
        'rate_limit_wait_seconds': round(wait_seconds, 3),
        'min_interval_seconds': float(min_interval_seconds),
        'shared_state_path': str(state_path) if state_path else '',
        'last_call_time': last_call_time,
        'state_read_error': read_error,
        'upstream_cooldown_active': upstream_cooldown_active,
        'blocked_until': blocked_until or None,
    }


def plan_autostock_call_budget(
    *,
    quota_snapshot: dict,
    needed_call_count: int,
    reserve_calls: int,
    max_fetch_calls_per_run: int | None = None,
) -> dict:
    """按共享配额、自动化保留额度和单轮上限计算本轮可调用次数。"""
    needed = max(0, int(needed_call_count or 0))
    quota_limited = bool(quota_snapshot.get('quota_limited'))
    max_calls = int(quota_snapshot.get('max_calls_per_hour') or 0)
    reserve = max(0, min(int(reserve_calls or 0), max_calls)) if quota_limited else 0
    if quota_limited:
        remaining = max(int(quota_snapshot.get('quota_remaining') or 0), 0)
        quota_available_after_reserve = max(remaining - reserve, 0)
        quota_available_for_full_fetch = remaining
    else:
        remaining = None
        quota_available_after_reserve = needed
        quota_available_for_full_fetch = needed

    max_run_calls = max(0, int(max_fetch_calls_per_run or 0))
    run_available = max_run_calls if max_run_calls > 0 else needed
    reserved_available = min(quota_available_after_reserve, run_available)
    full_fetch_available = min(quota_available_for_full_fetch, run_available)
    can_complete_full_fetch = needed <= full_fetch_available
    available = full_fetch_available if can_complete_full_fetch else reserved_available
    allowed = min(needed, available)
    return {
        'needed_call_count': needed,
        'allowed_call_count': allowed,
        'skipped_call_count': max(0, needed - allowed),
        'reserved_allowed_call_count': min(needed, reserved_available),
        'can_complete_full_fetch': bool(can_complete_full_fetch),
        'reserve_overridden_for_full_fetch': bool(can_complete_full_fetch and needed > reserved_available),
        'quota_limited': quota_limited,
        'quota_remaining': remaining,
        'quota_reserved_for_automation': reserve,
        'max_fetch_calls_per_run': max_run_calls,
        'run_call_limit_exceeded': bool(max_run_calls > 0 and needed > max_run_calls),
        'quota_exhausted': bool(quota_snapshot.get('quota_exhausted')),
        'quota_unavailable': bool(needed > 0 and quota_limited and full_fetch_available <= 0),
        'quota_degraded': bool(needed > allowed),
    }


def resolve_autostock_validation_cache_ttl_seconds(
    *,
    config_loader,
    env,
    default_ttl_seconds: int,
) -> int:
    """解析 AutoStock 主榜校验缓存 TTL，支持环境变量和配置覆盖。"""
    try:
        config = config_loader() if callable(config_loader) else {}
    except Exception:
        config = {}

    autostock_cfg = config.get('api', {}).get('autostock', {}) if isinstance(config, dict) else {}
    verification_cfg = autostock_cfg.get('verification', {}) or {}
    cache_cfg = verification_cfg.get('cache', {}) or {}

    ttl_value = env.get(
        'AUTOSTOCK_VALIDATION_CACHE_TTL_SECONDS',
        cache_cfg.get(
            'ttl_seconds',
            verification_cfg.get('cache_ttl_seconds', default_ttl_seconds),
        ),
    )
    return _normalize_positive_int(ttl_value, int(default_ttl_seconds))


def build_autostock_validation_summary(reason: str | None = None) -> dict:
    """构建 AutoStock 主榜交叉验证摘要。"""
    return {
        'enabled': False,
        'reason': reason,
        'checked_count': 0,
        'filtered_count': 0,
        'passed_count': 0,
        'sample_fcodes': [],
        'reason_counts': [],
        'warning_counts': [],
        'batch_meta': {},
    }


def autostock_validation_cache_dir(*, output_dir: Path) -> Path:
    """返回 AutoStock 主榜交叉验证缓存目录。"""
    cache_dir = Path(output_dir) / '.cache' / 'autostock_validation'
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir


def build_autostock_validation_cache_key(
    candidate_codes: list[str],
    df_local: pd.DataFrame,
    *,
    fund_type: str | None = None,
    config_fingerprint: dict | None = None,
    normalize_fcode,
    normalize_iso_date,
    safe_float,
    schema_version: str,
) -> str:
    """基于候选池与本地校验字段构造稳定缓存键。"""
    normalized_codes = [normalize_fcode(code) for code in candidate_codes if normalize_fcode(code)]
    local_parts: list[str] = []
    if isinstance(df_local, pd.DataFrame) and not df_local.empty:
        for _, row in df_local.iterrows():
            fcode = normalize_fcode(row.get('基金代码'))
            if not fcode:
                continue
            local_parts.append(
                build_autostock_local_signature(
                    row,
                    normalize_fcode=normalize_fcode,
                    normalize_iso_date=normalize_iso_date,
                    safe_float=safe_float,
                )
            )
    payload = {
        'schema_version': schema_version,
        'fund_type': str(fund_type or '').strip(),
        'candidate_codes': normalized_codes,
        'local_rows': sorted(local_parts),
        'config_fingerprint': dict(config_fingerprint or {}),
    }
    return hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, sort_keys=True).encode('utf-8')
    ).hexdigest()


def load_autostock_validation_cache(
    cache_key: str,
    *,
    cache_dir: Path,
    ttl_seconds: int,
    required_schema_version: str,
    now: datetime | None = None,
) -> dict | None:
    """读取 AutoStock 主榜交叉验证缓存。"""
    try:
        payload = read_cache(Path(cache_dir), cache_key)
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        return None

    if not isinstance(payload, dict):
        return None

    generated_at = str(payload.get('generated_at') or '').strip()
    if not generated_at:
        return None

    current_time = now or datetime.now()
    try:
        age_seconds = (current_time - datetime.fromisoformat(generated_at)).total_seconds()
    except ValueError:
        return None

    if age_seconds < 0 or age_seconds > ttl_seconds:
        return None

    if not is_autostock_validation_cache_payload_compatible(
        payload,
        required_schema_version=required_schema_version,
    ):
        return None
    return payload


def load_recent_autostock_validation_caches(
    *,
    cache_dir: Path,
    ttl_seconds: int,
    required_schema_version: str,
    now: datetime | None = None,
) -> list[dict]:
    """加载 TTL 窗口内可用的 AutoStock 主榜交叉验证缓存，用于增量复用。"""
    current_time = now or datetime.now()
    payloads: list[dict] = []
    for cache_key in cache_keys(Path(cache_dir)):
        try:
            payload = read_cache(Path(cache_dir), cache_key)
        except (OSError, json.JSONDecodeError, TypeError, ValueError):
            continue
        if not isinstance(payload, dict):
            continue

        generated_at = str(payload.get('generated_at') or '').strip()
        if not generated_at:
            continue
        try:
            age_seconds = (current_time - datetime.fromisoformat(generated_at)).total_seconds()
        except ValueError:
            continue
        if age_seconds < 0 or age_seconds > ttl_seconds:
            continue
        if not is_autostock_validation_cache_payload_compatible(
            payload,
            required_schema_version=required_schema_version,
        ):
            continue
        payloads.append(payload)

    payloads.sort(key=lambda item: str(item.get('generated_at') or ''), reverse=True)
    return payloads


def build_autostock_cache_entry_map(payloads: list[dict] | None, *, normalize_fcode) -> dict[str, dict]:
    """按基金代码聚合最近缓存条目，供本轮校验做增量复用。"""
    entry_map: dict[str, dict] = {}
    for payload in payloads or []:
        local_row_map = payload.get('local_row_map')
        result_by_fcode = payload.get('result_by_fcode')
        if not isinstance(local_row_map, dict) or not isinstance(result_by_fcode, dict):
            continue

        generated_at = str(payload.get('generated_at') or '').strip()
        for raw_code, local_signature in local_row_map.items():
            fcode = normalize_fcode(raw_code)
            if not fcode or fcode in entry_map:
                continue
            result_entry = result_by_fcode.get(fcode)
            if not isinstance(result_entry, dict):
                continue
            entry_map[fcode] = {
                'generated_at': generated_at,
                'local_signature': str(local_signature or ''),
                'result': dict(result_entry),
            }
    return entry_map


def has_autostock_validation_detail_schema(detail: dict | None) -> bool:
    """判断单条审计明细是否具备当前缓存 schema 必要字段。"""
    if not isinstance(detail, dict):
        return False
    if not all(field in detail for field in AUTOSTOCK_VALIDATION_DETAIL_REQUIRED_FIELDS):
        return False
    for prefix in AUTOSTOCK_VALIDATION_RETURN_DETAIL_FIELDS:
        for suffix in ('_abs_diff_pct_point', '_relative_diff_pct', '_diff_mode'):
            if f'{prefix}{suffix}' not in detail:
                return False
    return True


def is_autostock_validation_cache_payload_compatible(
    payload: dict | None,
    *,
    required_schema_version: str,
) -> bool:
    """判断缓存载荷是否可被当前版本复用。"""
    if not isinstance(payload, dict):
        return False

    if str(payload.get('schema_version') or '').strip() != str(required_schema_version or '').strip():
        return False

    local_row_map = payload.get('local_row_map')
    result_by_fcode = payload.get('result_by_fcode')
    detail_records = payload.get('detail_records')
    if not isinstance(local_row_map, dict) or not isinstance(result_by_fcode, dict):
        return False
    if detail_records is not None and not isinstance(detail_records, list):
        return False

    if isinstance(detail_records, list):
        for detail in detail_records:
            if not has_autostock_validation_detail_schema(detail):
                return False

    for result in result_by_fcode.values():
        if not isinstance(result, dict):
            return False
        if not has_autostock_validation_detail_schema(result.get('details')):
            return False
        if not isinstance(result.get('hard_reasons'), list):
            return False
        if not isinstance(result.get('warning_reasons'), list):
            return False
        if 'passed' not in result:
            return False

    return True


def write_autostock_validation_cache(
    cache_key: str,
    payload: dict,
    *,
    cache_dir: Path,
    json_writer,
) -> Path:
    """写出 AutoStock 主榜交叉验证缓存。"""
    return write_cache(Path(cache_dir), cache_key, payload, json_writer=json_writer)


def warning_reasons_to_display_text(warning_reasons: list[str] | tuple[str, ...] | None) -> str:
    """将校验告警原因格式化为适合 Excel 展示的中文文本。"""
    normalized = [str(item).strip() for item in (warning_reasons or []) if str(item).strip()]
    if not normalized:
        return ''

    label_map = {
        'latest_nav_date_soft_lag': '双源净值日期存在可容忍滞后',
        'return_1w_soft_mismatch': '近1周收益口径差异',
        'return_1m_soft_due_to_nav_date_lag': '近1月收益受净值日期差异影响',
        'return_3m_soft_due_to_nav_date_lag': '近3月收益受净值日期差异影响',
        'return_6m_soft_due_to_nav_date_lag': '近6月收益受净值日期差异影响',
        'return_1y_soft_due_to_nav_date_lag': '近1年收益受净值日期差异影响',
        'type_mismatch': '基金类型口径差异',
        'name_mismatch': '基金名称口径差异',
    }
    labels = []
    for reason in normalized:
        labels.append(label_map.get(reason, reason))
    return '；'.join(labels)


def build_autostock_validation_status_map(
    validation_rows: pd.DataFrame | None,
    *,
    failed_codes: set[str] | None = None,
    normalize_fcode,
    warning_text_builder,
) -> dict[str, dict]:
    """按基金代码汇总主榜校验状态，供导出和摘要复用。"""
    status_map: dict[str, dict] = {}
    failed_codes = {code for code in (failed_codes or set()) if code}

    if isinstance(validation_rows, pd.DataFrame) and not validation_rows.empty:
        for _, row in validation_rows.iterrows():
            fcode = normalize_fcode(row.get('基金代码') or row.get('fcode'))
            if not fcode:
                continue
            warning_reasons = [
                item for item in str(row.get('告警原因') or '').split('；') if item
            ]
            hard_reasons = [
                item for item in str(row.get('失败原因') or '').split('；') if item
            ]
            if hard_reasons or fcode in failed_codes:
                status = 'hard_block'
            elif warning_reasons:
                status = 'soft_pass_with_warnings'
            else:
                status = 'strict_match'
            status_map[fcode] = {
                'autostock_validation_status': status,
                'autostock_warning_reasons': warning_reasons,
                'autostock_warning_display': warning_text_builder(warning_reasons),
                'autostock_hard_reasons': hard_reasons,
            }

    for fcode in failed_codes:
        status_map.setdefault(
            fcode,
            {
                'autostock_validation_status': 'hard_block',
                'autostock_warning_reasons': [],
                'autostock_warning_display': '',
                'autostock_hard_reasons': [],
            },
        )
    return status_map


def build_autostock_local_signature(row, *, normalize_fcode, normalize_iso_date, safe_float) -> str:
    """为单只基金构造本地校验签名，用于缓存命中与增量复用。"""
    fcode = normalize_fcode(row.get('基金代码'))
    fund_name = str(row.get('基金名称') or '').strip()
    fund_type = str(row.get('基金类型') or '').strip()
    nav_date = normalize_iso_date(row.get('最新净值日期')) or ''
    local_nav = safe_float(row.get('最新单位净值'))
    return_fields = (
        safe_float(row.get('近1周(%)')),
        safe_float(row.get('近1月(%)')),
        safe_float(row.get('近3月(%)')),
        safe_float(row.get('近6月(%)')),
        safe_float(row.get('近1年(%)')),
    )
    normalized_returns = [
        '' if value is None else round(value, 4)
        for value in return_fields
    ]
    local_nav_value = '' if local_nav is None else round(local_nav, 6)
    return '|'.join([
        str(fcode or ''),
        fund_name,
        fund_type,
        nav_date,
        normalize_iso_date(row.get('区间收益基准日')) or '',
        str(row.get('区间收益来源') or ''),
        str(row.get('区间收益来源SHA256') or ''),
        str(local_nav_value),
        *[str(item) for item in normalized_returns],
    ])


def write_autostock_validation_audit_file(
    *,
    fund_type: str,
    timestamp: str,
    payload: dict,
    output_dir: Path,
    json_writer,
) -> Path:
    """写出完整 AutoStock 审计 JSON，不面向用户下载，仅供审计与复核。"""
    audit_dir = Path(output_dir) / '.audit' / 'autostock_validation'
    audit_dir.mkdir(parents=True, exist_ok=True)
    audit_path = audit_dir / f'{fund_type}_autostock_validation_{timestamp}.json'
    json_writer(audit_path, payload)
    return audit_path


def build_autostock_validation_audit_payload(
    *,
    generated_at: datetime,
    fund_type: str,
    candidate_pool_count: int,
    validated_count: int,
    failed_codes: set[str],
    summary: dict,
    failed_df: pd.DataFrame,
    detail_df: pd.DataFrame,
    normalize_fcode,
) -> dict:
    """构造完整 AutoStock 校验审计载荷。"""
    summary_payload = dict(summary or {})
    source_evidence = (
        dict(summary_payload.get('source_evidence') or {})
        if isinstance(summary_payload.get('source_evidence'), dict)
        else {}
    )
    return {
        'generated_at': generated_at.isoformat(),
        'fund_type': fund_type,
        'candidate_pool_count': int(candidate_pool_count),
        'validated_count': int(validated_count),
        'failed_count': int(len(failed_codes)),
        'failed_codes': sorted(
            normalize_fcode(code) for code in failed_codes if normalize_fcode(code)
        ),
        'summary': summary_payload,
        'source_evidence': source_evidence,
        'failed_records': (
            failed_df.to_dict(orient='records')
            if isinstance(failed_df, pd.DataFrame) and not failed_df.empty else []
        ),
        'detail_records': (
            detail_df.to_dict(orient='records')
            if isinstance(detail_df, pd.DataFrame) and not detail_df.empty else []
        ),
    }


def normalize_iso_date(value) -> str | None:
    """规整日期到 YYYY-MM-DD 文本。"""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None

    if isinstance(value, pd.Timestamp):
        return value.strftime('%Y-%m-%d')

    if isinstance(value, datetime):
        return value.strftime('%Y-%m-%d')

    if isinstance(value, date_type):
        return value.strftime('%Y-%m-%d')

    text = str(value).strip()
    if not text or text.lower() == 'nan':
        return None

    try:
        return pd.to_datetime(text).strftime('%Y-%m-%d')
    except Exception:
        return text[:10]


def safe_float(value):
    """安全转换浮点值。"""
    if value is None or pd.isna(value):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def is_type_match(local_type: str, autostock_type: str | None) -> bool:
    """判断本地基金类型与 AutoStock 类型是否语义一致。"""
    if not autostock_type:
        return True
    if not local_type:
        return False

    local_type = str(local_type)
    autostock_type = str(autostock_type)

    if local_type in autostock_type or autostock_type in local_type:
        return True

    local_is_bond = ('债券' in local_type) or ('固收' in local_type)
    autostock_is_bond = ('债券' in autostock_type) or ('固收' in autostock_type)
    return local_is_bond and autostock_is_bond


def compare_return_with_autostock(local_value, autostock_value, config: dict) -> tuple[bool, dict]:
    """按验证器容差口径比较收益率字段。

    收益率字段本身已经是百分比数值，绝对差表示“百分点差”。低收益债基
    容易被相对差放大，因此硬校验应允许百分点绝对差兜底通过。
    """
    local_return = safe_float(local_value)
    autostock_return = safe_float(autostock_value)
    abs_tolerance = float(config['return_absolute_tolerance'])
    relative_tolerance = float(config['return_relative_tolerance_pct'])

    if local_return is None or autostock_return is None:
        return False, {
            'local': local_return,
            'autostock': autostock_return,
            'mode': 'missing',
            'abs_diff': None,
            'diff_pct': None,
            'relative_diff_pct': None,
            'absolute_diff_pct_point': None,
            'absolute_tolerance_pct_point': abs_tolerance,
            'relative_tolerance_pct': relative_tolerance,
        }

    abs_diff = abs(local_return - autostock_return)
    relative_diff_pct = abs_diff / max(abs(autostock_return), 0.01) * 100
    absolute_match = abs_diff <= abs_tolerance
    relative_match = relative_diff_pct <= relative_tolerance
    if abs(autostock_return) < float(config['return_small_base_threshold']):
        return (
            absolute_match,
            {
                'local': local_return,
                'autostock': autostock_return,
                'mode': 'absolute_pct_point',
                'abs_diff': round(abs_diff, 4),
                'diff_pct': round(abs_diff, 4),
                'relative_diff_pct': round(relative_diff_pct, 4),
                'absolute_diff_pct_point': round(abs_diff, 4),
                'absolute_tolerance_pct_point': abs_tolerance,
                'relative_tolerance_pct': relative_tolerance,
                'absolute_match': bool(absolute_match),
                'relative_match': bool(relative_match),
            },
        )

    return (
        bool(relative_match or absolute_match),
        {
            'local': local_return,
            'autostock': autostock_return,
            'mode': 'relative_pct' if relative_match else 'absolute_pct_point_fallback',
            'abs_diff': round(abs_diff, 4),
            'diff_pct': round(relative_diff_pct, 4),
            'relative_diff_pct': round(relative_diff_pct, 4),
            'absolute_diff_pct_point': round(abs_diff, 4),
            'absolute_tolerance_pct_point': abs_tolerance,
            'relative_tolerance_pct': relative_tolerance,
            'absolute_match': bool(absolute_match),
            'relative_match': bool(relative_match),
        },
    )


def compare_candidate_with_autostock(
    local_row: pd.Series,
    autostock_row: dict,
    config: dict,
    *,
    converter_cls,
    normalize_fcode,
    normalize_iso_date_fn,
    safe_float_fn,
    is_type_match_fn,
    compare_return_with_autostock_fn,
    return_field_mappings,
) -> dict:
    """比较单只基金的本地数据与 AutoStock 数据。"""
    converter = converter_cls()
    fcode = normalize_fcode(local_row.get('基金代码'))

    hard_reasons = []
    warning_reasons = []
    details = {
        '基金代码': fcode,
        '基金名称': local_row.get('基金名称'),
        '本地基金类型': str(local_row.get('基金类型') or '').strip() or None,
        'AutoStock基金类型': str(autostock_row.get('type') or '').strip() or None,
        '本地最新净值': safe_float_fn(local_row.get('最新单位净值')),
        'AutoStock最新净值': safe_float_fn(autostock_row.get('netWorth')),
        '本地净值日期': normalize_iso_date_fn(local_row.get('最新净值日期')),
        'AutoStock净值日期': normalize_iso_date_fn(autostock_row.get('netWorthDate')),
        '本地区间收益基准日': normalize_iso_date_fn(local_row.get('区间收益基准日')),
        'AutoStock区间收益基准日': normalize_iso_date_fn(autostock_row.get('netWorthDate')),
    }

    autostock_code = normalize_fcode(autostock_row.get('code'))
    if fcode != autostock_code:
        hard_reasons.append('code_mismatch')

    local_name = str(local_row.get('基金名称') or '').strip()
    autostock_name = str(autostock_row.get('name') or '').strip()
    if local_name and autostock_name:
        name_match = (
            local_name == autostock_name or
            local_name in autostock_name or
            autostock_name in local_name
        )
        if not name_match:
            warning_reasons.append('name_mismatch')

    type_match = is_type_match_fn(str(local_row.get('基金类型') or ''), autostock_row.get('type'))
    details['类型一致'] = bool(type_match)
    if not type_match:
        warning_reasons.append('type_mismatch')

    local_nav_date = normalize_iso_date_fn(local_row.get('最新净值日期'))
    autostock_nav_date = normalize_iso_date_fn(autostock_row.get('netWorthDate'))
    nav_date_diff_days = None
    nav_date_mismatch = False
    soft_nav_date_lag = False
    if not local_nav_date or not autostock_nav_date:
        hard_reasons.append('latest_nav_date_missing')
    elif local_nav_date != autostock_nav_date:
        nav_date_mismatch = True
        try:
            nav_date_diff_days = abs(
                int((pd.to_datetime(local_nav_date) - pd.to_datetime(autostock_nav_date)).days)
            )
        except Exception:
            nav_date_diff_days = None
        details['净值日期差(天)'] = nav_date_diff_days

    local_nav = safe_float_fn(local_row.get('最新单位净值'))
    autostock_nav = safe_float_fn(autostock_row.get('netWorth'))
    nav_match = False
    if local_nav is None or autostock_nav is None:
        hard_reasons.append('latest_nav_missing')
    else:
        nav_abs_diff = abs(local_nav - autostock_nav)
        nav_diff_pct = nav_abs_diff / max(abs(autostock_nav), 1e-8) * 100
        details['净值绝对差'] = round(nav_abs_diff, 6)
        details['净值相对差(%)'] = round(nav_diff_pct, 4)
        nav_match = not (
            nav_diff_pct > float(config['nav_relative_tolerance_pct']) and
            nav_abs_diff > float(config['nav_absolute_tolerance'])
        )
        if not nav_match:
            hard_reasons.append('latest_nav_mismatch')

    if nav_date_mismatch:
        soft_lag_days = int(config.get('nav_date_soft_lag_days') or 1)
        if nav_match and nav_date_diff_days is not None and nav_date_diff_days <= soft_lag_days:
            soft_nav_date_lag = True
            warning_reasons.append('latest_nav_date_soft_lag')
        else:
            hard_reasons.append('latest_nav_date_mismatch')

    return_issue_records = []
    period_date = normalize_iso_date_fn(local_row.get('区间收益基准日'))
    if not period_date:
        hard_reasons.append('period_return_date_missing')
    elif not autostock_nav_date or period_date != autostock_nav_date:
        hard_reasons.append('period_return_date_mismatch')
    for local_col, autostock_col, reason_key in return_field_mappings:
        autostock_value = converter.parse_growth_rate(autostock_row.get(autostock_col))
        matched, compare_detail = compare_return_with_autostock_fn(
            local_row.get(local_col),
            autostock_value,
            config,
        )
        details[f'{reason_key}_local'] = compare_detail.get('local')
        details[f'{reason_key}_autostock'] = compare_detail.get('autostock')
        details[f'{reason_key}_diff'] = compare_detail.get('diff_pct')
        details[f'{reason_key}_abs_diff_pct_point'] = compare_detail.get('absolute_diff_pct_point')
        details[f'{reason_key}_relative_diff_pct'] = compare_detail.get('relative_diff_pct')
        details[f'{reason_key}_diff_mode'] = compare_detail.get('mode')
        if not matched:
            return_issue_records.append({
                'reason_key': reason_key,
                'mode': compare_detail.get('mode'),
            })

    unresolved_return_issue_records = return_issue_records
    # A NAV date change cannot resolve a period-return conflict. Return dates
    # must match independently, and numeric differences keep their original
    # policy even when NAV dates are within the existing soft-lag tolerance.

    if (
        len(unresolved_return_issue_records) == 1 and
        unresolved_return_issue_records[0]['reason_key'] == 'return_1w' and
        unresolved_return_issue_records[0]['mode'] != 'missing'
    ):
        warning_reasons.append('return_1w_soft_mismatch')
    else:
        for issue in unresolved_return_issue_records:
            if issue['mode'] == 'missing':
                hard_reasons.append(f"{issue['reason_key']}_missing")
            else:
                hard_reasons.append(f"{issue['reason_key']}_mismatch")

    details['失败原因'] = '；'.join(hard_reasons)
    details['告警原因'] = '；'.join(warning_reasons)

    return {
        'passed': len(hard_reasons) == 0,
        'hard_reasons': hard_reasons,
        'warning_reasons': warning_reasons,
        'details': details,
    }
