#!/usr/bin/env python3
"""
AutoStock API客户端
用于从api.autostock.cn获取基金数据进行双重验证
"""

import hashlib
import json
import math
import os
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Callable, Deque, Dict, List, Optional, Tuple
from urllib.parse import quote

import requests

from utils.logger import get_logger, setup_logger
from utils.autostock_quota_state import QuotaStateError, parse_state, quota_state_lock, read_locked_state, write_locked_state

try:
    import fcntl
except ImportError:  # pragma: no cover - Windows 环境不支持 fcntl
    fcntl = None

logger = get_logger(__name__)


def _default_autostock_shared_state_path() -> Path:
    """返回 AutoStock 跨进程配额状态文件路径。

    优先级：
    1. 环境变量 ``AUTOSTOCK_RATE_LIMIT_STATE_PATH``（运维/测试覆盖）
    2. 项目根 ``backend/data/autostock_rate_limit_state.json``（与 Fund-ANA
       `config/project.yaml::apis.autostock.rate_limit.shared_state_path` 默认值对齐）
    3. 兜底 ``core/logs/autostock_rate_limit_state.json``（历史路径，仅当
       ``backend/data/`` 不可写时使用，避免单元测试在只读 core/ 下炸掉）
    """
    configured = os.environ.get('AUTOSTOCK_RATE_LIMIT_STATE_PATH')
    if configured:
        return Path(configured)

    # P0-04.2 / V4: 默认与 backend settings 对齐，避免 core 内部 fallback 与
    # 生产主链路形成两个独立的共享状态文件。
    project_root = Path(__file__).resolve().parent.parent.parent
    backend_state = project_root / 'backend' / 'data' / 'autostock_rate_limit_state.json'
    try:
        backend_state.parent.mkdir(parents=True, exist_ok=True)
        return backend_state
    except Exception:
        return Path(__file__).resolve().parent.parent / 'logs' / 'autostock_rate_limit_state.json'


class AutoStockQuotaExceeded(Exception):
    """AutoStock 本地配额保护触发。"""

    def __init__(self, retry_after_seconds: float, max_calls_per_hour: int, reason: str = 'hourly_quota_exhausted'):
        self.retry_after_seconds = max(0.0, float(retry_after_seconds))
        self.max_calls_per_hour = int(max_calls_per_hour)
        self.reason = reason
        super().__init__(
            f"AutoStock配额已耗尽: {self.max_calls_per_hour}次/小时, "
            f"请在 {self.retry_after_seconds:.0f} 秒后重试"
        )


class AutoStockQuotaStateError(RuntimeError):
    """An unreadable quota ledger must not silently become a new empty hour."""


class AutoStockRateLimiter:
    """
    AutoStock API速率限制器
    每次 HTTP 尝试计费；跨进程共享滚动小时配额和服务端冷却。
    线程安全：使用 Lock 保护共享状态
    """

    def __init__(
        self,
        min_interval: float = 2.0,
        max_calls_per_hour: Optional[int] = 100,
        time_func: Optional[Callable[[], float]] = None,
        sleep_func: Optional[Callable[[float], None]] = None,
        shared_state_path: Optional[str] = None,
    ):
        setup_logger(__name__)
        self.min_interval = min_interval
        self.max_calls_per_hour = (
            int(max_calls_per_hour) if max_calls_per_hour is not None and int(max_calls_per_hour) > 0 else 100
        )
        self.last_call_time = 0.0
        self.call_count = 0
        self.blocked_until = 0.0
        self._recent_call_timestamps: Deque[float] = deque()
        self._time_func = time_func or time.time
        self._sleep_func = sleep_func or time.sleep
        self._lock = threading.Lock()
        self._shared_state_path = Path(shared_state_path) if shared_state_path else _default_autostock_shared_state_path()

    def _prune_hourly_window(self, current_time: float) -> None:
        """清理 1 小时窗口外的调用记录。"""
        if self.max_calls_per_hour is None:
            return

        window_seconds = 3600
        while self._recent_call_timestamps and current_time - self._recent_call_timestamps[0] >= window_seconds:
            self._recent_call_timestamps.popleft()

    @staticmethod
    def _load_shared_state(payload: str) -> Dict[str, Any]:
        try:
            return parse_state(payload)
        except QuotaStateError as exc:
            raise AutoStockQuotaStateError(str(exc)) from exc

    def _persist_shared_state(self, handle, state: Dict[str, Any]) -> None:
        try:
            write_locked_state(self._shared_state_path, state, handle)
        except OSError as exc:
            raise AutoStockQuotaStateError(f"AutoStock quota state could not be persisted: {exc}") from exc

    def _read_locked_shared_state(self, handle) -> Dict[str, Any]:
        try:
            return read_locked_state(self._shared_state_path, handle)
        except (QuotaStateError, OSError) as exc:
            raise AutoStockQuotaStateError(str(exc)) from exc

    def _sync_local_window_from_shared_state(self, state: Dict[str, Any]) -> None:
        """将共享状态回写到当前实例，便于调试和单测观察。"""
        self.last_call_time = float(state.get('last_call_time', 0.0))
        self.blocked_until = float(state.get('blocked_until') or 0.0)
        timestamps = state.get('recent_call_timestamps', [])
        self._recent_call_timestamps = deque(float(value) for value in timestamps)

    def _wait_if_needed_in_memory(self) -> None:
        """原有进程内限流实现。"""
        current_time = self._time_func()
        self._check_cooldown(current_time, self.blocked_until)
        self._prune_hourly_window(current_time)

        if (
            self.max_calls_per_hour is not None and
            len(self._recent_call_timestamps) >= self.max_calls_per_hour
        ):
            retry_after = 3600 - (current_time - self._recent_call_timestamps[0])
            raise AutoStockQuotaExceeded(
                retry_after_seconds=retry_after,
                max_calls_per_hour=self.max_calls_per_hour,
            )

        elapsed = current_time - self.last_call_time

        if elapsed < self.min_interval:
            wait_time = self.min_interval - elapsed
            logger.debug(f"速率限制等待 {wait_time:.2f} 秒")
            self._sleep_func(wait_time)

        call_time = self._time_func()
        self.last_call_time = call_time
        self.call_count += 1
        if self.max_calls_per_hour is not None:
            self._recent_call_timestamps.append(call_time)
        logger.debug(f"API调用次数: {self.call_count}")

    def _wait_if_needed_shared(self) -> None:
        """
        使用共享状态文件在多进程间协调 AutoStock 配额。

        说明：
        - web、scheduler 容器和本机脚本共享同一挂载目录；
        - 如果仍只靠实例内内存计数，会出现“每个进程各自认为没超额”的假象；
        - 这里用极小的 JSON 状态文件 + flock，把“最近1小时窗口”和“上次调用时间”拉成全局一致。
        """
        self._shared_state_path.parent.mkdir(parents=True, exist_ok=True)

        with quota_state_lock(self._shared_state_path) as handle:
            state = self._read_locked_shared_state(handle)

            current_time = self._time_func()
            self._check_cooldown(current_time, float(state.get('blocked_until') or 0.0))
            timestamps = [
                ts for ts in state.get('recent_call_timestamps', [])
                if current_time - ts < 3600
            ]
            state['recent_call_timestamps'] = timestamps

            if (
                self.max_calls_per_hour is not None and
                len(timestamps) >= self.max_calls_per_hour
            ):
                self._persist_shared_state(handle, state)
                self._sync_local_window_from_shared_state(state)
                retry_after = 3600 - (current_time - timestamps[0])
                raise AutoStockQuotaExceeded(
                    retry_after_seconds=retry_after,
                    max_calls_per_hour=self.max_calls_per_hour,
                )

            last_call_time = float(state.get('last_call_time', 0.0))
            elapsed = current_time - last_call_time
            if elapsed < self.min_interval:
                wait_time = self.min_interval - elapsed
                logger.debug(f"共享速率限制等待 {wait_time:.2f} 秒")
                self._sleep_func(wait_time)

            call_time = self._time_func()
            state['last_call_time'] = call_time
            if self.max_calls_per_hour is not None:
                timestamps.append(call_time)
                state['recent_call_timestamps'] = timestamps

            self._persist_shared_state(handle, state)
            self._sync_local_window_from_shared_state(state)
            self.call_count += 1
            logger.debug(f"API调用次数: {self.call_count}")

    def wait_if_needed(self):
        """在需要时等待，确保不超过速率限制（线程安全）"""
        with self._lock:
            if fcntl is not None and self._shared_state_path:
                try:
                    self._wait_if_needed_shared()
                except OSError as exc:
                    raise AutoStockQuotaStateError(f"AutoStock quota lock unavailable: {exc}") from exc
                return
            self._wait_if_needed_in_memory()

    def _check_cooldown(self, now: float, blocked_until: float) -> None:
        if blocked_until > now:
            raise AutoStockQuotaExceeded(
                blocked_until - now, self.max_calls_per_hour, reason='upstream_cooldown',
            )

    def record_upstream_cooldown(self, retry_after_seconds: float) -> None:
        """Persist a server throttle so another process does not immediately retry."""
        with self._lock:
            deadline = self._time_func() + max(0.0, float(retry_after_seconds))
            if fcntl is None or not self._shared_state_path:
                self.blocked_until = max(self.blocked_until, deadline)
                return
            self._shared_state_path.parent.mkdir(parents=True, exist_ok=True)
            with quota_state_lock(self._shared_state_path) as handle:
                state = self._read_locked_shared_state(handle)
                state['blocked_until'] = max(float(state.get('blocked_until') or 0.0), deadline)
                self._persist_shared_state(handle, state)
                self._sync_local_window_from_shared_state(state)


class AutoStockClient:
    """AutoStock API客户端"""

    BASE_URL = "https://api.autostock.cn/v1"
    SAFE_FUND_BATCH_SIZE = 50
    FUND_BATCH_TRANSPORT_FAILURE_CIRCUIT_THRESHOLD = 3
    DIRECTORY_PARSE_ERROR_SAMPLE_LIMIT = 20
    DIRECTORY_PARSE_ERROR_CODE_LIMIT = 5000
    RANK_REQUEST_TIMEOUT_FLOOR = 60
    REQUEST_METRIC_FIELDS = (
        'http_attempt_count', 'http_response_count', 'http_retry_count', 'http_elapsed_seconds',
        'request_elapsed_seconds', 'response_body_bytes', 'failed_http_response_count', 'transport_failure_count',
    )
    REQUEST_EVIDENCE_FIELDS = REQUEST_METRIC_FIELDS + (
        'http_status_counts', 'request_count_basis', 'response_body_bytes_basis', 'failure_kind_counts',
        'logical_request_count', 'source_type', 'cache_status', 'cache_age_seconds', 'cache_ttl_seconds',
        'cache_endpoint', 'cache_rejection_reason', 'cache_write_error', 'data_fetched_at_epoch',
        'data_sha256', 'origin_request_metadata', 'origin_request_metadata_sha256', 'cache_integrity_verified',
        'query_mode', 'used_keyword_fallback', 'upstream_total_before_local_filter',
    )
    QUOTA_ERROR_KEYWORDS = (
        '免费', '次数', '频次', '配额', '额度', '限制', '上限',
        'limit', 'quota', 'too many', 'too frequent'
    )
    RETRYABLE_HTTP_STATUS = {429, 500, 502, 503, 504}
    RETRYABLE_BUSINESS_CODES = {412, 429, 500, 503}
    RETRYABLE_BUSINESS_KEYWORDS = (
        '系统繁忙', 'system is busy', '数据量过大', '服务不可用',
        '稍后尝试', '稍后再试', '解析失败', 'too many', 'rate limit'
    )

    def __init__(
        self,
        min_interval: float = 2.0,
        timeout: int = 60,
        max_calls_per_hour: Optional[int] = 100,
        shared_state_path: Optional[str] = None,
        base_url: Optional[str] = None,
        directory_cache_path: Optional[str] = None,
        directory_cache_ttl_seconds: float = 3600,
        enabled: bool = True,
    ):
        self.enabled = enabled is True
        self.rate_limiter = AutoStockRateLimiter(
            min_interval=min_interval,
            max_calls_per_hour=max_calls_per_hour,
            shared_state_path=shared_state_path,
        )
        self.timeout = timeout
        self.max_calls_per_hour = self.rate_limiter.max_calls_per_hour
        resolved_base_url = str(base_url or os.environ.get('AUTOSTOCK_API_BASE_URL') or self.BASE_URL).strip().rstrip('/')
        self.base_url = resolved_base_url or self.BASE_URL
        self.directory_cache_path = Path(directory_cache_path) if directory_cache_path else None
        self.directory_cache_ttl_seconds = max(0.0, float(directory_cache_ttl_seconds))
        self._thread_local = threading.local()
        self._default_headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        self._last_rank_fetch_meta: Dict[str, Any] = {}
        self._last_fund_all_meta: Dict[str, Any] = {}
        self._last_fund_batch_meta: Dict[str, Any] = {}
        self._last_fund_detail_batch_meta: Dict[str, Any] = {}

        quota_text = f", 每小时上限: {self.max_calls_per_hour}次"
        logger.info(f"AutoStock API客户端初始化完成 (基础地址: {self.base_url}, 最小间隔: {min_interval}秒{quota_text})")

    @property
    def session(self) -> requests.Session:
        """获取当前线程的 requests.Session（线程安全）"""
        if not hasattr(self._thread_local, 'session'):
            self._thread_local.session = requests.Session()
            self._thread_local.session.headers.update(self._default_headers)
        return self._thread_local.session

    def _send_http_request(self, method: str, url: str, **kwargs) -> requests.Response:
        """发送原始 HTTP 请求。"""
        timeout = kwargs.pop('timeout', self.timeout)
        if method.upper() == 'GET':
            return self.session.get(url, timeout=timeout, **kwargs)
        if method.upper() == 'POST':
            return self.session.post(url, timeout=timeout, **kwargs)
        raise ValueError(f"不支持的HTTP方法: {method}")

    @classmethod
    def _extract_error_message(cls, payload: Optional[Dict[str, Any]]) -> str:
        """提取响应中的错误信息。"""
        if not isinstance(payload, dict):
            return '未知错误'

        for key in ('message', 'msg', 'error', 'errorMsg', 'error_message', 'detail'):
            value = payload.get(key)
            if value:
                return str(value)
        return '未知错误'

    @classmethod
    def _is_quota_error(
        cls,
        status_code: Optional[int] = None,
        business_code: Optional[int] = None,
        message: str = '',
    ) -> bool:
        """识别配额/限流类错误。"""
        normalized = (message or '').strip().lower()
        keyword_hit = any(keyword.lower() in normalized for keyword in cls.QUOTA_ERROR_KEYWORDS)
        return (
            status_code == 429 or
            business_code == 429 or
            (business_code == 400 and keyword_hit) or
            keyword_hit
        )

    @classmethod
    def _is_retryable_error(
        cls,
        status_code: Optional[int] = None,
        business_code: Optional[int] = None,
        message: str = '',
    ) -> bool:
        """识别可重试错误。"""
        normalized = (message or '').strip().lower()
        return (
            status_code in cls.RETRYABLE_HTTP_STATUS or
            business_code in cls.RETRYABLE_BUSINESS_CODES or
            any(keyword.lower() in normalized for keyword in cls.RETRYABLE_BUSINESS_KEYWORDS)
        )

    @classmethod
    def _normalize_autostock_code(cls, value: Any) -> Optional[str]:
        """标准化基金代码。"""
        if value is None:
            return None

        text = str(value).strip()
        if not text:
            return None
        return text.zfill(6) if text.isdigit() else text

    @classmethod
    def _normalize_requested_codes(cls, codes: List[str]) -> List[str]:
        normalized = []
        seen = set()
        for value in codes:
            code = cls._normalize_autostock_code(value)
            if not code or len(code) != 6 or not code.isascii() or not code.isdigit():
                raise ValueError(f'Invalid fund code: {value!r}')
            if code not in seen:
                normalized.append(code)
                seen.add(code)
        return normalized

    @staticmethod
    def _business_data_failure(row: Dict[str, Any]) -> Optional[str]:
        """The documented DTO flags can report failure inside a successful envelope."""
        for field in ('requestFail', 'resolveFail'):
            value = row.get(field)
            if value in (None, ''):
                continue
            try:
                number = float(value)
            except (TypeError, ValueError):
                return f'{field}_invalid'
            if not math.isfinite(number) or number != 0:
                return field
        return None

    @classmethod
    def _accepted_fund_rows(cls, data: Any, requested_codes: List[str]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """Keep requested, usable records; retain rejected-row evidence without filling values."""
        if not isinstance(data, list):
            return [], {'rejected_record_count': 0, 'payload_error': 'expected_list', 'rejected_records': []}
        requested = set(requested_codes)
        accepted: Dict[str, Dict[str, Any]] = {}
        conflicted = set()
        rejected = []
        duplicate_count = 0
        for row in data:
            code = cls._normalize_autostock_code(row.get('code')) if isinstance(row, dict) else None
            reason = 'invalid_record' if not code or len(code) != 6 or not code.isascii() or not code.isdigit() else None
            if code and code not in requested:
                reason = 'unexpected_code'
            if not reason:
                reason = cls._business_data_failure(row)
            if not reason and code in conflicted:
                reason = 'conflicting_duplicate'
            if not reason and code in accepted:
                duplicate_count += 1
                if accepted[code] == row:
                    continue
                accepted.pop(code)
                conflicted.add(code)
                reason = 'conflicting_duplicate'
            if reason:
                rejected.append({'code': code, 'reason': reason,
                                 **({k: row[k] for k in ('requestFail', 'resolveFail') if k in row} if isinstance(row, dict) else {})})
                continue
            accepted[code] = row
        return list(accepted.values()), {
            'rejected_record_count': len(rejected), 'rejected_records': rejected[:20],
            'duplicate_record_count': duplicate_count, 'payload_error': None,
        }

    @staticmethod
    def _raise_business_data_failure(path: str, reason: str) -> None:
        raise Exception(f'AutoStock业务数据不可用 ({path}): {reason}', {
            'status_code': 200, 'message': reason, 'quota_error': False,
            'retryable': False, 'failure_kind': 'upstream_business_data_failed',
        })

    @staticmethod
    def _utc_iso_from_epoch(value: Optional[float]) -> Optional[str]:
        if value is None:
            return None
        try:
            return time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(float(value)))
        except (TypeError, ValueError, OSError):
            return None

    @classmethod
    def _hash_records(cls, records: Any) -> Optional[str]:
        try:
            raw = json.dumps(
                records,
                ensure_ascii=False,
                sort_keys=True,
                separators=(',', ':'),
                default=str,
            )
        except (TypeError, ValueError):
            return None
        return hashlib.sha256(raw.encode('utf-8')).hexdigest()

    @classmethod
    def _record_code_samples(cls, rows: Any, *, limit: int = 10) -> List[str]:
        samples: List[str] = []
        for row in rows or []:
            code = None
            if isinstance(row, dict):
                code = cls._normalize_autostock_code(
                    row.get('code') or row.get('FCODE') or row.get('fcode') or row.get('基金代码')
                )
            elif isinstance(row, (list, tuple)) and row:
                code = cls._normalize_autostock_code(row[0])
            if code:
                samples.append(code)
            if len(samples) >= limit:
                break
        return samples

    def _build_source_evidence(
        self,
        *,
        task: str,
        source: str,
        source_type: str = 'api',
        endpoint_path: str,
        started_at: Optional[float],
        finished_at: Optional[float],
        target_count: int = 0,
        request_count: int = 0,
        response_count: int = 0,
        accepted_response_count: int = 0,
        failed_response_count: int = 0,
        transport_exception_count: int = 0,
        returned_record_count: int = 0,
        returned_unique_count: int = 0,
        missing_count: int = 0,
        failed_batch_count: int = 0,
        quota_exhausted: bool = False,
        last_error: Optional[str] = None,
        response_records: Any = None,
        sample_rows: Any = None,
        extra: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        evidence: Dict[str, Any] = {
            'schema_version': 1,
            'task': str(task or '').strip(),
            'source': str(source or '').strip(),
            'source_type': str(source_type or 'api').strip() or 'api',
            'base_url': self.base_url,
            'endpoint_path': endpoint_path,
            'request_window': {
                'started_at': self._utc_iso_from_epoch(started_at),
                'finished_at': self._utc_iso_from_epoch(finished_at),
            },
            'target_count': int(target_count or 0),
            'request_count': int(request_count or 0),
            'response_count': int(response_count or 0),
            'accepted_response_count': int(accepted_response_count or 0),
            'failed_response_count': int(failed_response_count or 0),
            'transport_exception_count': int(transport_exception_count or 0),
            'returned_record_count': int(returned_record_count or 0),
            'returned_unique_count': int(returned_unique_count or 0),
            'missing_count': int(missing_count or 0),
            'failed_batch_count': int(failed_batch_count or 0),
            'quota_exhausted': bool(quota_exhausted),
            'last_error': str(last_error)[:300] if last_error else None,
            'response_hash': self._hash_records(response_records),
            'sample_codes': self._record_code_samples(sample_rows if sample_rows is not None else response_records),
        }
        evidence['source_complete'] = bool(
            not evidence['quota_exhausted']
            and evidence['failed_response_count'] == 0
            and evidence['transport_exception_count'] == 0
            and evidence['failed_batch_count'] == 0
            and evidence['missing_count'] == 0
        )
        if isinstance(extra, dict):
            evidence.update(extra)
        return evidence

    def _request_with_meta(
        self,
        url: str,
        method: str = 'GET',
        max_retries: int = 3,
        **kwargs,
    ) -> Tuple[Any, Dict[str, Any]]:
        """发送请求；每次重试独立占用配额，业务行质量另行验证。"""
        if not self.enabled:
            raise RuntimeError('autostock_provider_disabled: independent_source_unavailable')
        if int(max_retries) < 1:
            raise ValueError('max_retries must be at least 1')
        last_error = None
        http_attempt_count = 0
        http_response_count = 0
        request_started_at = time.monotonic()
        http_elapsed_seconds = 0.0
        response_body_bytes = 0
        response_bytes_complete = True
        http_status_counts: Dict[str, int] = {}
        failed_http_response_count = 0
        transport_failure_count = 0

        def request_metrics() -> Dict[str, Any]:
            return {
                'http_attempt_count': http_attempt_count,
                'http_response_count': http_response_count,
                'http_retry_count': max(0, http_attempt_count - 1),
                'http_elapsed_seconds': http_elapsed_seconds,
                'request_elapsed_seconds': time.monotonic() - request_started_at,
                'response_body_bytes': response_body_bytes if response_bytes_complete else None,
                'response_body_bytes_basis': 'decoded_response_content',
                'http_status_counts': dict(http_status_counts),
                'failed_http_response_count': failed_http_response_count,
                'transport_failure_count': transport_failure_count,
            }

        for attempt in range(max_retries):
            try:
                self.rate_limiter.wait_if_needed()
                http_attempt_count += 1
                http_started = time.monotonic()
                try:
                    response = self._send_http_request(method, url, **kwargs)
                finally:
                    http_elapsed_seconds += time.monotonic() - http_started
                http_response_count += 1
                http_status_counts[str(response.status_code)] = http_status_counts.get(str(response.status_code), 0) + 1
                body = getattr(response, 'content', None)
                if isinstance(body, bytes):
                    response_body_bytes += len(body)
                else:
                    response_bytes_complete = False
                payload: Optional[Dict[str, Any]]
                try:
                    payload = response.json()
                except ValueError:
                    payload = None

                error_message = self._extract_error_message(payload)
                business_code = payload.get('code') if isinstance(payload, dict) else None
                if response.status_code != 200 or not isinstance(payload, dict) or business_code not in [0, 200]:
                    failed_http_response_count += 1
                quota_error = self._is_quota_error(
                    status_code=response.status_code,
                    business_code=business_code,
                    message=error_message,
                )
                retryable = self._is_retryable_error(
                    status_code=response.status_code,
                    business_code=business_code,
                    message=error_message,
                )
                # 免费版配额/频率限制命中后继续重试只会额外浪费额度窗口时间，
                # 这里统一按终态失败处理，让上层尽快停止后续批量任务。
                if quota_error:
                    retryable = False
                    retry_after = self._quota_retry_after(response)
                    self.rate_limiter.record_upstream_cooldown(retry_after)
                    raise Exception(
                        f"AutoStock上游限流: {error_message}",
                        {
                            'status_code': response.status_code,
                            'business_code': business_code,
                            'message': error_message,
                            'quota_error': True,
                            'quota_reason': 'upstream_cooldown',
                            'failure_kind': 'upstream_quota',
                            'retryable': False,
                            'retry_after': retry_after,
                        },
                    )

                if response.status_code != 200:
                    if retryable and attempt < max_retries - 1:
                        wait_time = 10 * (attempt + 1)
                        logger.warning(
                            "AutoStock HTTP错误(status=%s): %s，%s秒后重试 (%s/%s)",
                            response.status_code,
                            error_message,
                            wait_time,
                            attempt + 1,
                            max_retries,
                        )
                        time.sleep(wait_time)
                        continue
                    raise Exception(
                        f"API错误: HTTP {response.status_code} - {error_message}",
                        {
                            'status_code': response.status_code,
                            'business_code': business_code,
                            'message': error_message,
                            'quota_error': quota_error,
                            'retryable': retryable,
                            'failure_kind': 'http_status_error',
                        }
                    )

                if not isinstance(payload, dict):
                    raise Exception(
                        "API错误: 响应不是合法JSON对象",
                        {
                            'status_code': response.status_code,
                            'business_code': None,
                            'message': '响应不是合法JSON对象',
                            'quota_error': False,
                            'retryable': False,
                            'failure_kind': 'invalid_json_response',
                        }
                    )

                if business_code not in [0, 200]:
                    if retryable and attempt < max_retries - 1:
                        wait_time = 10 * (attempt + 1)
                        logger.warning(
                            "AutoStock业务错误(code=%s): %s，%s秒后重试 (%s/%s)",
                            business_code,
                            error_message,
                            wait_time,
                            attempt + 1,
                            max_retries,
                        )
                        time.sleep(wait_time)
                        continue
                    raise Exception(
                        f"API错误: {error_message}",
                        {
                            'status_code': response.status_code,
                            'business_code': business_code,
                            'message': error_message,
                            'quota_error': quota_error,
                            'retryable': retryable,
                            'failure_kind': 'upstream_business_error',
                        }
                    )

                logger.debug(f"请求成功: {url}")
                return payload.get('data'), {
                    'status_code': response.status_code,
                    'business_code': business_code,
                    'message': error_message,
                    'quota_error': False,
                    'retryable': False,
                    **request_metrics(),
                }

            except AutoStockQuotaExceeded as exc:
                message = (
                    f"AutoStock本地配额保护触发: 每小时{exc.max_calls_per_hour}次, "
                    f"请在{exc.retry_after_seconds:.0f}秒后重试 ({exc.reason})"
                )
                raise Exception(message, {
                    'status_code': 429,
                    'business_code': 429,
                    'message': message,
                    'quota_error': True,
                    'quota_reason': exc.reason,
                    'failure_kind': 'local_quota',
                    'retryable': False,
                    'retry_after': exc.retry_after_seconds,
                    **request_metrics(),
                }) from exc

            except AutoStockQuotaStateError as exc:
                raise Exception(str(exc), {
                    'status_code': None, 'message': str(exc), 'quota_error': True,
                    'quota_reason': 'quota_state_unavailable', 'retryable': False,
                    'failure_kind': 'quota_state_unavailable',
                    **request_metrics(),
                }) from exc

            except requests.exceptions.Timeout as exc:
                transport_failure_count += 1
                last_error = '请求超时'
                logger.warning(f"请求超时 (尝试 {attempt + 1}/{max_retries}): {url}")
                if attempt < max_retries - 1:
                    time.sleep(10)
                else:
                    raise Exception(last_error, {
                        'message': last_error, 'status_code': None,
                        'quota_error': False, 'retryable': True, 'failure_kind': 'timeout',
                        **request_metrics(),
                    }) from exc

            except requests.exceptions.RequestException as exc:
                transport_failure_count += 1
                last_error = str(exc)
                logger.warning(f"请求异常 (尝试 {attempt + 1}/{max_retries}): {exc}")
                if attempt < max_retries - 1:
                    time.sleep(10)
                else:
                    raise Exception(last_error, {
                        'message': last_error, 'status_code': None,
                        'quota_error': False, 'retryable': True, 'failure_kind': 'request_error',
                        **request_metrics(),
                    }) from exc

            except Exception as exc:
                if len(exc.args) > 1 and isinstance(exc.args[1], dict):
                    exc.args = (exc.args[0], {**exc.args[1],
                        **request_metrics(),
                    })
                    raise
                logger.error(f"未知错误: {exc}")
                raise Exception(str(exc), {
                    **self._extract_exception_meta(exc), 'failure_kind': 'unexpected_request_error',
                    **request_metrics(),
                }) from exc

        raise Exception(
            f"请求失败，已重试 {max_retries} 次",
            {
                'status_code': None,
                'business_code': None,
                'message': last_error or '请求失败，已重试多次',
                'quota_error': False,
                'retryable': False,
                **request_metrics(),
            }
        )

    def _quota_retry_after(self, response: requests.Response) -> float:
        """Honor Retry-After; absent reset metadata uses the documented one-hour window."""
        value = str(response.headers.get('Retry-After') or '').strip()
        try:
            seconds = float(value)
            if math.isfinite(seconds) and seconds >= 0:
                return seconds
        except ValueError:
            pass
        if value:
            try:
                deadline = parsedate_to_datetime(value)
                if deadline.tzinfo is None:
                    deadline = deadline.replace(tzinfo=timezone.utc)
                return max(0.0, deadline.timestamp() - self.rate_limiter._time_func())
            except (ValueError, TypeError, OverflowError):
                pass
        return 3600.0

    def _request(self, url: str, method: str = 'GET',
                 max_retries: int = 3, **kwargs) -> Dict[str, Any]:
        """发送API请求并返回 data 字段。"""
        try:
            data, meta = self._request_with_meta(url, method=method, max_retries=max_retries, **kwargs)
        except Exception as exc:
            self._thread_local.last_request_meta = self._extract_exception_meta(exc)
            raise
        self._thread_local.last_request_meta = meta
        return data

    def _request_observed(self, url: str, **kwargs) -> Tuple[Any, Dict[str, Any]]:
        self._thread_local.last_request_meta = {}
        data = self._request(url, **kwargs)
        return data, dict(self._thread_local.last_request_meta)

    @classmethod
    def _summarize_request_metadata(cls, items: List[Dict[str, Any]]) -> Dict[str, Any]:
        fields = cls.REQUEST_METRIC_FIELDS
        result = {field: sum(item[field] for item in items) if all(item.get(field) is not None for item in items) else None
                  for field in fields}
        status_counts: Dict[str, int] = {}
        for item in items:
            for status, count in (item.get('http_status_counts') or {}).items():
                status_counts[status] = status_counts.get(status, 0) + count
        result['http_status_counts'] = status_counts
        failure_counts: Dict[str, int] = {}
        for item in items:
            kind = item.get('failure_kind')
            if kind:
                failure_counts[str(kind)] = failure_counts.get(str(kind), 0) + 1
        result['failure_kind_counts'] = failure_counts
        result['request_count_basis'] = 'wire_attempts_include_retries; logical_batches_reported_separately'
        result['response_body_bytes_basis'] = 'decoded_response_content'
        return result

    @classmethod
    def _normalize_fund_directory_row(cls, row: Any) -> Optional[Dict[str, Any]]:
        """标准化 `/fund/all` 返回的目录行。"""
        if isinstance(row, dict):
            code = cls._normalize_autostock_code(
                row.get('code') or row.get('FCODE') or row.get('_id')
            )
            name = row.get('name') or row.get('NAME')
            fund_type = row.get('type') or row.get('fundType') or row.get('FTYPE')
            pinyin = row.get('pinyin') or row.get('JP')
        elif isinstance(row, (list, tuple)) and len(row) >= 4:
            code = cls._normalize_autostock_code(row[0])
            pinyin = row[1]
            name = row[2]
            fund_type = row[3]
        else:
            return None

        if not code or not name or not fund_type:
            return None

        return {
            'code': code,
            'name': str(name).strip(),
            'type': str(fund_type).strip(),
            'pinyin': str(pinyin).strip() if pinyin not in (None, '') else None,
        }

    @classmethod
    def _describe_fund_directory_parse_error(cls, row: Any) -> Dict[str, Any]:
        """记录 `/fund/all` 无法标准化的真实行形态，不补造目录字段。"""
        sample: Dict[str, Any] = {
            'row_type': type(row).__name__,
            'truth_policy': 'preserve_missing_semantics',
            'business_value_handling': (
                'Rows missing required directory fields are recorded as source evidence; '
                'the client does not synthesize fund type, bond membership, or valid directory records.'
            ),
        }
        raw_code = None
        raw_name = None
        raw_type = None
        raw_pinyin = None
        reason = 'unsupported_row_type'

        if isinstance(row, dict):
            raw_code = row.get('code') or row.get('FCODE') or row.get('_id')
            raw_name = row.get('name') or row.get('NAME')
            raw_type = row.get('type') or row.get('fundType') or row.get('FTYPE')
            raw_pinyin = row.get('pinyin') or row.get('JP')
            sample['keys'] = sorted(str(key) for key in row.keys())[:20]
        elif isinstance(row, (list, tuple)):
            sample['raw_length'] = len(row)
            sample['row_preview'] = [
                (str(value)[:120] if value is not None else None)
                for value in list(row)[:6]
            ]
            if len(row) >= 4:
                raw_code = row[0]
                raw_pinyin = row[1]
                raw_name = row[2]
                raw_type = row[3]
            else:
                reason = 'row_sequence_too_short'

        code = cls._normalize_autostock_code(raw_code)
        name = str(raw_name).strip() if raw_name not in (None, '') else ''
        fund_type = str(raw_type).strip() if raw_type not in (None, '') else ''
        pinyin = str(raw_pinyin).strip() if raw_pinyin not in (None, '') else ''
        missing_fields = []
        if not code:
            missing_fields.append('code')
        if not name:
            missing_fields.append('name')
        if not fund_type:
            missing_fields.append('type')
        if reason == 'unsupported_row_type' and missing_fields:
            reason = 'required_field_missing:' + ','.join(missing_fields)
        elif reason == 'unsupported_row_type' and isinstance(row, (dict, list, tuple)):
            reason = 'directory_row_not_normalizable'

        sample.update(
            {
                'reason': reason,
                'code': code,
                'name': name[:120] if name else None,
                'fund_type': fund_type[:80] if fund_type else None,
                'pinyin': pinyin[:120] if pinyin else None,
                'missing_required_fields': missing_fields,
            }
        )
        return sample

    @staticmethod
    def _is_bond_directory_type(fund_type: Optional[str]) -> bool:
        """判断 `/fund/all` 目录项是否属于债券基金口径。"""
        if not fund_type:
            return False

        text = str(fund_type).strip()
        return (
            text == '指数型-固收' or
            text.startswith('债券') or
            text.startswith('定开债')
        )

    def get_all_funds(self, keyword: Optional[str] = None) -> List[Any]:
        """获取真实目录；可配置共享快照，命中缓存不会冒充一次 HTTP 请求。"""
        if not self.enabled:
            raise RuntimeError('autostock_provider_disabled: directory_not_requested')
        started_at = time.time()
        normalized_keyword = str(keyword or '').strip()
        endpoint = f"{self.base_url}/fund/all"
        observed: List[Dict[str, Any]] = []
        cache_meta: Dict[str, Any] = {'cache_status': 'disabled'}
        data = None
        last_error = None
        quota_exhausted = False
        used_keyword_fallback = False
        upstream_total = None
        query_mode = 'remote_keyword' if normalized_keyword else 'directory_full'

        def fetch(url):
            try:
                rows, meta = self._request_observed(url)
            except Exception as exc:
                observed.append({**self._extract_exception_meta(exc), 'logical_request_succeeded': False})
                raise
            if not isinstance(rows, list):
                observed.append({**meta, 'logical_request_succeeded': False,
                                 'failure_kind': 'upstream_business_data_failed'})
                self._raise_business_data_failure('/fund/all', 'expected_directory_list')
            observed.append({**meta, 'logical_request_succeeded': True})
            return rows, meta

        try:
            if self.directory_cache_path is not None and self.directory_cache_ttl_seconds > 0:
                from utils.autostock_directory_cache import get_directory_snapshot

                rows, cache_meta = get_directory_snapshot(
                    self.directory_cache_path, endpoint=endpoint, ttl_seconds=self.directory_cache_ttl_seconds,
                    fetch=lambda: fetch(endpoint),
                )
                upstream_total = len(rows)
                data = self._filter_directory_rows_by_keyword(rows, normalized_keyword) if normalized_keyword else rows
                query_mode = 'directory_snapshot_local_filter' if normalized_keyword else 'directory_snapshot_full'
            else:
                url = endpoint + ('?keyWord=' + quote(normalized_keyword, safe='') if normalized_keyword else '')
                try:
                    data, _ = fetch(url)
                except Exception as exc:
                    meta = self._extract_exception_meta(exc)
                    if not normalized_keyword or meta.get('quota_error'):
                        raise
                    used_keyword_fallback = True
                    last_error = meta.get('message') or str(exc)
                    rows, _ = fetch(endpoint)
                    upstream_total = len(rows)
                    data = self._filter_directory_rows_by_keyword(rows, normalized_keyword)
                    query_mode = 'directory_local_filter_fallback'
            return data
        except Exception as exc:
            meta = self._extract_exception_meta(exc)
            last_error = meta.get('message') or str(exc)
            quota_exhausted = bool(meta.get('quota_error'))
            raise
        finally:
            row_count = len(data) if isinstance(data, list) else None
            source_complete = isinstance(data, list)
            metrics = self._summarize_request_metadata(observed)
            terminal_responses = sum(bool(meta.get('logical_request_succeeded')) or (meta.get('http_response_count') or 0) > 0 for meta in observed)
            # A cache hit has zero current HTTP attempts; origin metadata stays separate.
            self._last_fund_all_meta = {
                'source_name': 'fund/all', 'keyword': normalized_keyword or None,
                'raw_total': row_count, 'returned_total': row_count, 'normalized_total': None, 'unique_total': None,
                'duplicate_count': None, 'bond_like_total': None, 'parse_errors': 0 if source_complete else None,
                'source_complete': source_complete, 'query_mode': query_mode, 'used_keyword_fallback': used_keyword_fallback,
                'upstream_total_before_local_filter': upstream_total, 'logical_request_count': len(observed),
                'quota_exhausted': quota_exhausted, 'last_error': last_error, **metrics, **cache_meta,
            }
            self._last_fund_all_meta['source_evidence'] = self._build_source_evidence(
                task='autostock_directory', source='AutoStock /fund/all', endpoint_path='/fund/all',
                source_type='cache' if cache_meta.get('cache_status') == 'hit' else 'api',
                started_at=started_at, finished_at=time.time(), target_count=0,
                request_count=len(observed), response_count=terminal_responses,
                accepted_response_count=int(source_complete and bool(observed)),
                failed_response_count=max(0, terminal_responses - int(source_complete and bool(observed))),
                transport_exception_count=sum(self._is_transport_failure_meta(meta) for meta in observed if not meta.get('logical_request_succeeded')),
                returned_record_count=row_count or 0, returned_unique_count=0,
                failed_batch_count=int(not source_complete), last_error=last_error, response_records=data,
                quota_exhausted=quota_exhausted,
                extra={'keyword': normalized_keyword or None, 'query_mode': query_mode, **metrics, **cache_meta,
                       'logical_request_count': len(observed), 'request_count_basis': 'logical_requests; http_attempt_count includes retries',
                       'source_complete': source_complete, 'used_keyword_fallback': used_keyword_fallback,
                       'upstream_total_before_local_filter': upstream_total},
            )

    def _filter_directory_rows_by_keyword(self, rows: List[Any], keyword: str) -> List[Any]:
        """在保持原始目录行结构的前提下做本地关键字过滤。"""
        normalized_keyword = str(keyword or '').strip().lower()
        if not normalized_keyword:
            return list(rows or [])

        matched_rows: List[Any] = []
        for row in rows or []:
            normalized = self._normalize_fund_directory_row(row)
            if normalized is None:
                continue
            haystack = ' '.join(
                str(normalized.get(field) or '')
                for field in ('code', 'name', 'type', 'pinyin')
            ).lower()
            if normalized_keyword in haystack:
                matched_rows.append(row)
        return matched_rows

    def search_funds_from_directory(self, keyword: str) -> List[Dict[str, Any]]:
        """
        通过 `/fund/all` 目录接口做本地关键字过滤。

        说明：
        - AutoStock `fund/all?keyWord=` 在高峰时段稳定性明显弱于 `fund/all` 全量目录；
        - 目录全量接口更稳定，但不适合高频调用，因此这里只提供低频审计/运维兜底；
        - 该方法仅返回目录级匹配结果，不输出完整性结论。
        """
        normalized_keyword = str(keyword or '').strip()
        if not normalized_keyword:
            return []

        raw_rows = self._filter_directory_rows_by_keyword(self.get_all_funds(), normalized_keyword)
        matched_rows: List[Dict[str, Any]] = []

        for row in raw_rows:
            normalized = self._normalize_fund_directory_row(row)
            if normalized is None:
                continue
            matched_rows.append(normalized)

        logger.info(
            "AutoStock fund/all 本地关键字过滤完成: keyword=%s, matched_total=%s",
            normalized_keyword,
            len(matched_rows),
        )
        return matched_rows

    def get_all_bond_funds_from_directory(self, keyword: Optional[str] = None) -> List[Dict[str, Any]]:
        """通过 `/fund/all` 目录接口获取债券基金全集。"""
        raw_rows = self.get_all_funds(keyword=keyword)
        normalized_rows: List[Dict[str, Any]] = []
        parse_errors = 0
        parse_error_reason_counts: Dict[str, int] = {}
        parse_error_samples: List[Dict[str, Any]] = []
        parse_error_codes: List[str] = []
        seen_parse_error_codes: set[str] = set()
        previous_meta = dict(self._last_fund_all_meta or {})
        previous_evidence = (
            dict(self._last_fund_all_meta.get('source_evidence') or {})
            if isinstance(self._last_fund_all_meta, dict)
            else {}
        )

        for row in raw_rows or []:
            normalized = self._normalize_fund_directory_row(row)
            if normalized is None:
                parse_errors += 1
                parse_error = self._describe_fund_directory_parse_error(row)
                reason = str(parse_error.get('reason') or 'directory_row_not_normalizable')
                parse_error_reason_counts[reason] = parse_error_reason_counts.get(reason, 0) + 1
                parse_error_code = self._normalize_autostock_code(parse_error.get('code'))
                if (
                    parse_error_code
                    and parse_error_code not in seen_parse_error_codes
                    and len(parse_error_codes) < self.DIRECTORY_PARSE_ERROR_CODE_LIMIT
                ):
                    seen_parse_error_codes.add(parse_error_code)
                    parse_error_codes.append(parse_error_code)
                if len(parse_error_samples) < self.DIRECTORY_PARSE_ERROR_SAMPLE_LIMIT:
                    parse_error_samples.append(parse_error)
                continue
            normalized_rows.append(normalized)

        unique_rows = {row['code']: row for row in normalized_rows}
        bond_rows = [
            row for row in unique_rows.values()
            if self._is_bond_directory_type(row.get('type'))
        ]

        duplicate_count = max(0, len(normalized_rows) - len(unique_rows))
        self._last_fund_all_meta = {
            **previous_meta,
            'source_name': 'fund/all',
            'keyword': keyword,
            'raw_total': len(raw_rows or []),
            'normalized_total': len(normalized_rows),
            'unique_total': len(unique_rows),
            'duplicate_count': duplicate_count,
            'bond_like_total': len(bond_rows),
            'parse_errors': parse_errors,
            'parse_error_reason_counts': parse_error_reason_counts,
            'parse_error_codes': parse_error_codes,
            'parse_error_code_count': len(parse_error_codes),
            'parse_error_code_limit': self.DIRECTORY_PARSE_ERROR_CODE_LIMIT,
            'parse_error_code_truncated': bool(
                parse_errors > len(parse_error_codes)
                and len(parse_error_codes) >= self.DIRECTORY_PARSE_ERROR_CODE_LIMIT
            ),
            'parse_error_samples': parse_error_samples,
            'parse_error_sample_limit': self.DIRECTORY_PARSE_ERROR_SAMPLE_LIMIT,
            'parse_error_sample_truncated': bool(parse_errors > len(parse_error_samples)),
            'source_complete': bool(parse_errors == 0 and previous_evidence.get('source_complete', True)),
        }
        self._last_fund_all_meta['source_evidence'] = self._build_source_evidence(
            task='autostock_directory',
            source='AutoStock /fund/all',
            endpoint_path='/fund/all',
            started_at=None,
            finished_at=None,
            target_count=0,
            request_count=previous_evidence.get('request_count', 0),
            response_count=previous_evidence.get('response_count', 0),
            accepted_response_count=previous_evidence.get('accepted_response_count', 0),
            failed_response_count=previous_evidence.get('failed_response_count') or 0,
            transport_exception_count=previous_evidence.get('transport_exception_count') or 0,
            returned_record_count=len(raw_rows or []),
            returned_unique_count=len(unique_rows),
            missing_count=0,
            failed_batch_count=0,
            last_error=previous_evidence.get('last_error'),
            response_records=raw_rows,
            sample_rows=bond_rows,
            extra={
                **{key: previous_evidence[key] for key in self.REQUEST_EVIDENCE_FIELDS if key in previous_evidence},
                'request_window': previous_evidence.get('request_window') or {
                    'started_at': None,
                    'finished_at': None,
                },
                'keyword': keyword,
                'raw_total': len(raw_rows or []),
                'normalized_total': len(normalized_rows),
                'unique_total': len(unique_rows),
                'duplicate_count': duplicate_count,
                'bond_like_total': len(bond_rows),
                'parse_errors': parse_errors,
                'parse_error_count': parse_errors,
                'parse_error_reason_counts': parse_error_reason_counts,
                'parse_error_codes': parse_error_codes,
                'parse_error_code_count': len(parse_error_codes),
                'parse_error_code_limit': self.DIRECTORY_PARSE_ERROR_CODE_LIMIT,
                'parse_error_code_truncated': bool(
                    parse_errors > len(parse_error_codes)
                    and len(parse_error_codes) >= self.DIRECTORY_PARSE_ERROR_CODE_LIMIT
                ),
                'parse_error_samples': parse_error_samples,
                'parse_error_sample_limit': self.DIRECTORY_PARSE_ERROR_SAMPLE_LIMIT,
                'parse_error_sample_truncated': bool(parse_errors > len(parse_error_samples)),
                'parse_error_truth_policy': 'preserve_missing_semantics',
                'parse_error_business_value_handling': (
                    'Rows missing required AutoStock directory fields are not used to synthesize '
                    'fund type, bond membership, or valid directory records.'
                ),
                'source_complete': bool(parse_errors == 0 and previous_evidence.get('source_complete', True)),
            },
        )

        logger.info(
            "AutoStock fund/all 债券目录统计: raw_total=%s, unique_total=%s, bond_like_total=%s, "
            "duplicate_count=%s, parse_errors=%s",
            self._last_fund_all_meta['raw_total'],
            self._last_fund_all_meta['unique_total'],
            self._last_fund_all_meta['bond_like_total'],
            self._last_fund_all_meta['duplicate_count'],
            self._last_fund_all_meta['parse_errors'],
        )
        return bond_rows

    def get_last_fund_all_meta(self) -> Dict[str, Any]:
        """获取最近一次 `/fund/all` 目录抓取元数据。"""
        return dict(self._last_fund_all_meta)

    def get_bond_fund_rank(self, page_index: int = 1, page_size: int = 400, *, max_retries: int = 3) -> Dict[str, Any]:
        """获取债券基金排行。"""
        if int(page_index) < 1 or int(page_size) < 1:
            raise ValueError('rank page index and size must be positive')
        normalized_page_index = int(page_index)
        page_size = int(page_size)
        url = f"{self.base_url}/fund/rank"
        payload = {
            "fundType": ["zq"],
            "pageIndex": normalized_page_index,
            "pageSize": page_size,
            "asc": "0",
            "sort": "1n",
        }

        logger.info(f"获取债券基金排行 (page={normalized_page_index}, size={page_size})")
        request_timeout = self.timeout
        if page_size >= 400:
            request_timeout = max(self.timeout, self.RANK_REQUEST_TIMEOUT_FLOOR)
        try:
            data, request_meta = self._request_observed(url, method='POST', json=payload, timeout=request_timeout, max_retries=max_retries)
        except Exception as exc:
            self._thread_local.last_rank_page_meta = {
                **self._extract_exception_meta(exc), 'page_index': normalized_page_index, 'page_size': page_size,
            }
            raise
        self._thread_local.last_rank_page_meta = {**request_meta, 'page_index': normalized_page_index, 'page_size': page_size}
        failure = self._business_data_failure(data) if isinstance(data, dict) else 'expected_object'
        if not failure and not isinstance(data.get('rank'), list):
            failure = 'expected_rank_list'
        if failure:
            self._thread_local.last_rank_page_meta['business_data_error'] = failure
            self._thread_local.last_rank_page_meta['failure_kind'] = 'upstream_business_data_failed'
            self._raise_business_data_failure('/fund/rank', failure)
        rank_codes = [self._normalize_autostock_code(row.get('code')) for row in data['rank'] if isinstance(row, dict)]
        accepted, quality = self._accepted_fund_rows(data['rank'], rank_codes)
        if quality['rejected_record_count']:
            self._thread_local.last_rank_page_meta['business_data_error'] = quality
            self._thread_local.last_rank_page_meta['failure_kind'] = 'upstream_business_data_failed'
            self._raise_business_data_failure('/fund/rank', str(quality['rejected_records']))
        data = {**data, 'rank': accepted}

        rank_count = len(data.get('rank', []))
        total_records = data.get('totalRecord', 0)
        logger.info(f"本页获取 {rank_count} 只基金，总共 {total_records} 只")

        return data

    def get_last_rank_page_meta(self) -> Dict[str, Any]:
        return dict(getattr(self._thread_local, 'last_rank_page_meta', {}) or {})

    def get_all_bond_funds_from_rank(self, page_size: int = 400, max_retries: int = 3) -> List[Dict[str, Any]]:
        """
        获取所有债券基金（通过分页排行接口）。

        说明：
        - AutoStock `/fund/rank` 的 `pageIndex` 实测为 1 基；
        - 该接口返回的 `totalRecord` 与真实可翻出的唯一债券基金数存在明显偏差，
          因此仅适合做“可见样本”来源，不适合作为完整性全集基准。
        """
        if int(page_size) < 1 or int(max_retries) < 1:
            raise ValueError('rank page size and max_retries must be positive')
        page_size, max_retries = int(page_size), int(max_retries)
        all_funds: List[Dict[str, Any]] = []
        page_index = 1
        declared_total = None
        request_attempts = 0
        fetched_pages = 0
        stop_reason = 'unknown'
        last_error_message = None
        seen_page_signatures = set()
        request_metadata: List[Dict[str, Any]] = []
        self._thread_local.rank_fetch_request_metadata = request_metadata

        logger.info("开始获取所有债券基金...")

        while True:
            retry_count = 0
            while retry_count < max_retries:
                recorded_request = False
                self._thread_local.last_rank_page_meta = {}
                try:
                    request_attempts += 1
                    # Only this layer retries pages; avoid 3 outer * 3 inner attempts.
                    result = self.get_bond_fund_rank(page_index=page_index, page_size=page_size, max_retries=1)
                    request_metadata.append({**self.get_last_rank_page_meta(), 'page_index': page_index})
                    recorded_request = True
                    rank = result.get('rank', [])
                    total_records = result.get('totalRecord', 0)

                    if isinstance(total_records, int) and total_records > 0:
                        if declared_total is None:
                            declared_total = total_records
                        elif total_records != declared_total:
                            logger.warning(
                                f"债券排行totalRecord在分页过程中变化: {declared_total} -> {total_records}，"
                                "将采用较大值进行完整性判定"
                            )
                            declared_total = max(declared_total, total_records)

                    if not rank:
                        logger.info(f"第{page_index}页无数据，停止获取")
                        stop_reason = 'empty_page'
                        self._update_rank_fetch_meta(
                            all_funds=all_funds,
                            declared_total=declared_total,
                            page_size=page_size,
                            request_attempts=request_attempts,
                            fetched_pages=fetched_pages,
                            stop_reason=stop_reason,
                        )
                        return all_funds

                    page_signature = tuple(sorted(str(row.get('code') or '') for row in rank))
                    if page_signature in seen_page_signatures:
                        self._update_rank_fetch_meta(
                            all_funds=all_funds, declared_total=declared_total, page_size=page_size,
                            request_attempts=request_attempts, fetched_pages=fetched_pages,
                            stop_reason='repeated_page', last_error='upstream repeated a previously returned page',
                        )
                        return all_funds
                    seen_page_signatures.add(page_signature)

                    all_funds.extend(rank)
                    fetched_pages += 1
                    logger.info(f"已获取 {len(all_funds)} 只基金（第{page_index}页）")

                    if declared_total is not None and len(all_funds) >= declared_total:
                        stop_reason = 'reached_declared_total'
                        logger.info(f"已获取所有数据，共 {len(all_funds)} 只债券基金")
                        self._update_rank_fetch_meta(
                            all_funds=all_funds,
                            declared_total=declared_total,
                            page_size=page_size,
                            request_attempts=request_attempts,
                            fetched_pages=fetched_pages,
                            stop_reason=stop_reason,
                        )
                        return all_funds

                    page_index += 1
                    break

                except Exception as exc:
                    retry_count += 1
                    meta = exc.args[1] if len(exc.args) > 1 and isinstance(exc.args[1], dict) else {}
                    if not recorded_request:
                        request_metadata.append({**self.get_last_rank_page_meta(), **meta, 'page_index': page_index})
                    error_str = meta.get('message') or str(exc)
                    last_error_message = error_str
                    quota_error = bool(meta.get('quota_error'))
                    retryable_keywords = ['系统繁忙', '500', '412', '数据量过大', '解析失败', '稍后尝试', '服务不可用']
                    is_retryable = bool(meta.get('retryable')) or any(keyword in error_str for keyword in retryable_keywords)

                    if quota_error:
                        logger.warning(
                            "获取第%s页时触发 AutoStock 配额/频率限制: %s，停止继续分页并将结果标记为不完整",
                            page_index,
                            error_str,
                        )
                        stop_reason = 'quota_exhausted'
                        self._update_rank_fetch_meta(
                            all_funds=all_funds,
                            declared_total=declared_total,
                            page_size=page_size,
                            request_attempts=request_attempts,
                            fetched_pages=fetched_pages,
                            stop_reason=stop_reason,
                            last_error=error_str,
                        )
                        return all_funds

                    if is_retryable and retry_count < max_retries:
                        wait_time = 10 * retry_count
                        logger.warning(f"第{page_index}页获取失败（{exc}），{wait_time}秒后重试（{retry_count}/{max_retries}）...")
                        time.sleep(wait_time)
                    elif retry_count >= max_retries:
                        logger.error(f"获取第{page_index}页时发生错误: {exc}，已重试{max_retries}次")
                        break
                    else:
                        logger.error(f"获取第{page_index}页时发生错误: {exc}")
                        self._update_rank_fetch_meta(
                            all_funds=all_funds,
                            declared_total=declared_total,
                            page_size=page_size,
                            request_attempts=request_attempts,
                            fetched_pages=fetched_pages,
                            stop_reason='fatal_error',
                            last_error=error_str,
                        )
                        raise

            if retry_count >= max_retries:
                logger.error(f"第{page_index}页重试{max_retries}次后仍失败，已获取{len(all_funds)}只基金")
                stop_reason = 'retry_exhausted'
                break

        self._update_rank_fetch_meta(
            all_funds=all_funds,
            declared_total=declared_total,
            page_size=page_size,
            request_attempts=request_attempts,
            fetched_pages=fetched_pages,
            stop_reason=stop_reason,
            last_error=last_error_message,
        )
        logger.info(f"共获取 {len(all_funds)} 只债券基金")
        return all_funds

    def _update_rank_fetch_meta(
        self,
        all_funds: List[Dict[str, Any]],
        declared_total: Optional[int],
        page_size: int,
        request_attempts: int,
        fetched_pages: int,
        stop_reason: str,
        last_error: Optional[str] = None,
    ) -> None:
        """更新最近一次债券排行抓取的元数据。"""
        unique_codes = {f.get('code') for f in all_funds if isinstance(f, dict) and f.get('code')}
        fetched_total = len(all_funds)
        fetched_unique_total = len(unique_codes)
        duplicate_count = max(0, fetched_total - fetched_unique_total)
        declared_total_value = declared_total if isinstance(declared_total, int) and declared_total > 0 else None

        source_complete = True
        incomplete_reason = None
        if stop_reason == 'fatal_error':
            source_complete = False
            incomplete_reason = 'fatal_error'
        elif stop_reason == 'repeated_page':
            source_complete = False
            incomplete_reason = 'repeated_page'
        elif stop_reason == 'retry_exhausted':
            source_complete = False
            incomplete_reason = 'retry_exhausted'
        elif stop_reason == 'quota_exhausted':
            source_complete = False
            incomplete_reason = 'quota_exhausted'
        elif declared_total_value is not None and fetched_unique_total < declared_total_value:
            source_complete = False
            incomplete_reason = (
                f"declared_total_unmet(declared={declared_total_value}, unique_fetched={fetched_unique_total})"
            )
        elif declared_total_value is None:
            source_complete = False
            incomplete_reason = 'declared_total_unavailable'

        self._last_rank_fetch_meta = {
            'declared_total': declared_total_value,
            'fetched_total': fetched_total,
            'fetched_unique_total': fetched_unique_total,
            'duplicate_count': duplicate_count,
            'page_size': page_size,
            'request_attempts': request_attempts,
            'fetched_pages': fetched_pages,
            'stop_reason': stop_reason,
            'source_complete': source_complete,
            'incomplete_reason': incomplete_reason,
            'last_error': last_error,
        }
        request_metadata = getattr(self._thread_local, 'rank_fetch_request_metadata', [])
        self._last_rank_fetch_meta.update(self._summarize_request_metadata(request_metadata))
        page_attempts: Dict[int, int] = {}
        if all(item.get('http_attempt_count') is not None for item in request_metadata):
            for item in request_metadata:
                page = item['page_index']
                page_attempts[page] = page_attempts.get(page, 0) + item['http_attempt_count']
            # Pagination retries live outside _request_with_meta; count all attempts at the same page.
            self._last_rank_fetch_meta['http_retry_count'] = sum(max(0, count - 1) for count in page_attempts.values())
        else:
            self._last_rank_fetch_meta['http_retry_count'] = None
        self._last_rank_fetch_meta['logical_page_attempt_count'] = request_attempts

        if not source_complete:
            logger.warning(
                "债券排行分页结果疑似不完整: "
                f"declared_total={declared_total_value}, fetched_unique_total={fetched_unique_total}, "
                f"stop_reason={stop_reason}, incomplete_reason={incomplete_reason}"
            )

    def get_last_rank_fetch_meta(self) -> Dict[str, Any]:
        """获取最近一次债券排行全量抓取的元数据。"""
        return dict(self._last_rank_fetch_meta)

    def _build_batch_meta(
        self,
        requested_codes: List[str],
        returned_codes: set,
        requested_batch_size: int,
        effective_batch_size: int,
        capped: bool,
    ) -> Dict[str, Any]:
        """构建批量接口完整性元数据。"""
        requested_code_set = {
            self._normalize_autostock_code(code)
            for code in requested_codes
            if code
        }
        requested_code_set.discard(None)
        missing_codes = sorted(requested_code_set - returned_codes)
        return {
            'requested_total': len(requested_codes),
            'requested_unique_total': len(requested_code_set),
            'effective_batch_size': effective_batch_size,
            'requested_batch_size': requested_batch_size,
            'capped': capped,
            'returned_unique_total': len(returned_codes),
            'missing_count': len(missing_codes),
            'missing_codes_preview': missing_codes[:20],
        }

    @staticmethod
    def _extract_exception_meta(exc: Exception) -> Dict[str, Any]:
        """从异常中提取结构化元数据。"""
        if len(exc.args) > 1 and isinstance(exc.args[1], dict):
            return dict(exc.args[1])
        return {
            'message': str(exc),
            'quota_error': False,
            'retryable': False,
        }

    @staticmethod
    def _is_transport_failure_meta(meta: Dict[str, Any] | None) -> bool:
        """识别没有收到 HTTP 响应的网络传输失败。"""
        payload = dict(meta or {})
        return bool(payload.get('status_code') is None and not payload.get('quota_error'))

    def get_fund_batch_with_meta(
        self,
        codes: List[str],
        batch_size: int = 50,
        max_workers: int = 1,
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """批量获取基金基础数据，并返回本次调用的完整性元数据。"""
        started_at = time.time()
        requested_batch_size = int(batch_size or self.SAFE_FUND_BATCH_SIZE)
        effective_batch_size = max(1, min(requested_batch_size, self.SAFE_FUND_BATCH_SIZE))
        capped = effective_batch_size != requested_batch_size
        if capped:
            logger.warning(
                "AutoStock fund 批量查询单批请求上限收敛: requested_batch_size=%s -> effective_batch_size=%s",
                batch_size,
                effective_batch_size,
            )

        requested_codes = self._normalize_requested_codes(codes)
        returned_codes = set()
        all_results: List[Dict[str, Any]] = []
        quota_exhausted = False
        last_error = None
        failed_batches: List[Dict[str, Any]] = []
        request_count = 0
        response_count = 0
        accepted_response_count = 0
        failed_response_count = 0
        transport_exception_count = 0
        consecutive_transport_failure_count = 0
        max_consecutive_transport_failure_count = 0
        circuit_breaker_open = False
        circuit_breaker_reason = None
        circuit_breaker_batch_no = None
        batch_response_metadata: List[Dict[str, Any]] = []
        batches = [
            (index + 1, requested_codes[offset:offset + effective_batch_size])
            for index, offset in enumerate(range(0, len(requested_codes), effective_batch_size))
        ]

        def fetch_batch(batch_no: int, batch_codes: List[str]) -> Tuple[int, List[Dict[str, Any]], set, bool, Optional[str], Dict[str, Any]]:
            url = f"{self.base_url}/fund?code={','.join(batch_codes)}"
            logger.info(
                "批量获取基金数据 (%s-%s/%s)",
                (batch_no - 1) * effective_batch_size + 1,
                (batch_no - 1) * effective_batch_size + len(batch_codes),
                len(requested_codes),
            )

            try:
                data, meta = self._request_with_meta(url)
            except Exception as exc:
                meta = self._extract_exception_meta(exc)
                error_message = meta.get('message') or str(exc)
                logger.error(f"批量获取失败: {error_message}")
                return batch_no, [], set(), bool(meta.get('quota_error')), error_message, meta

            result_rows, quality = self._accepted_fund_rows(data, batch_codes)
            meta = {**meta, 'row_quality': quality, 'requested_row_count': len(batch_codes),
                    'returned_row_count': len(result_rows)}
            returned = {
                self._normalize_autostock_code(item.get('code'))
                for item in result_rows
                if isinstance(item, dict) and item.get('code')
            }
            returned.discard(None)

            if result_rows:
                logger.info(f"成功获取 {len(result_rows)} 只基金数据")

            quality_error = None
            if quality.get('payload_error') or quality['rejected_record_count']:
                quality_error = f"upstream_business_data_failed: {quality.get('payload_error') or quality['rejected_records']}"
                meta['failure_kind'] = 'upstream_business_data_failed'
            elif len(result_rows) < len(batch_codes):
                meta['failure_kind'] = 'missing_rows'
            return batch_no, result_rows, returned, bool(meta.get('quota_error')), quality_error, meta

        worker_count = max(1, min(int(max_workers or 1), len(batches) or 1))
        if worker_count <= 1:
            for batch_no, batch_codes in batches:
                request_count += 1
                _, batch_rows, batch_returned_codes, batch_quota_exhausted, error_message, response_meta = fetch_batch(batch_no, batch_codes)
                batch_response_metadata.append(response_meta)
                all_results.extend(batch_rows)
                returned_codes.update(batch_returned_codes)
                quota_exhausted = quota_exhausted or batch_quota_exhausted
                last_error = error_message or last_error
                if error_message:
                    failed_response_count += 1
                    transport_failure = self._is_transport_failure_meta(response_meta)
                    if transport_failure:
                        transport_exception_count += 1
                        consecutive_transport_failure_count += 1
                        max_consecutive_transport_failure_count = max(
                            max_consecutive_transport_failure_count,
                            consecutive_transport_failure_count,
                        )
                    else:
                        consecutive_transport_failure_count = 0
                    failed_batches.append({
                        'batch_no': int(batch_no),
                        'requested_count': len(batch_codes),
                        'requested_codes_preview': batch_codes[:10],
                        'quota_error': bool(response_meta.get('quota_error')),
                        'error': str(error_message)[:300],
                    })
                else:
                    response_count += 1
                    accepted_response_count += 1
                    consecutive_transport_failure_count = 0
                if batch_quota_exhausted:
                    logger.warning("AutoStock fund 批量查询触发配额/频率限制，停止后续批次")
                    break
                if (
                    consecutive_transport_failure_count
                    >= self.FUND_BATCH_TRANSPORT_FAILURE_CIRCUIT_THRESHOLD
                ):
                    circuit_breaker_open = True
                    circuit_breaker_reason = 'consecutive_transport_failures'
                    circuit_breaker_batch_no = int(batch_no)
                    logger.error(
                        "AutoStock fund 批量查询连续传输失败达到熔断阈值，停止后续批次: "
                        "batch_no=%s consecutive_failures=%s threshold=%s",
                        batch_no,
                        consecutive_transport_failure_count,
                        self.FUND_BATCH_TRANSPORT_FAILURE_CIRCUIT_THRESHOLD,
                    )
                    break
        else:
            logger.info(
                "AutoStock fund 批量并发获取: requested_total=%s, batch_total=%s, max_workers=%s",
                len(requested_codes),
                len(batches),
                worker_count,
            )
            with ThreadPoolExecutor(max_workers=worker_count) as executor:
                for window_start in range(0, len(batches), worker_count):
                    current_window = batches[window_start:window_start + worker_count]
                    future_to_batch_no = {
                        executor.submit(fetch_batch, batch_no, batch_codes): batch_no
                        for batch_no, batch_codes in current_window
                    }
                    request_count += len(current_window)
                    window_results: Dict[int, Tuple[List[Dict[str, Any]], set, bool, Optional[str], Dict[str, Any]]] = {}

                    for future in as_completed(future_to_batch_no):
                        batch_no = future_to_batch_no[future]
                        try:
                            _, batch_rows, batch_returned_codes, batch_quota_exhausted, error_message, response_meta = future.result()
                        except Exception as exc:  # pragma: no cover - future 异常兜底
                            meta = self._extract_exception_meta(exc)
                            batch_rows = []
                            batch_returned_codes = set()
                            batch_quota_exhausted = bool(meta.get('quota_error'))
                            error_message = meta.get('message') or str(exc)
                            response_meta = meta
                            logger.error(f"批量获取 future 异常: {error_message}")
                        window_results[batch_no] = (
                            batch_rows,
                            batch_returned_codes,
                            batch_quota_exhausted,
                            error_message,
                            response_meta,
                        )

                    for batch_no, _batch_codes in current_window:
                        batch_rows, batch_returned_codes, batch_quota_exhausted, error_message, response_meta = window_results.get(
                            batch_no,
                            ([], set(), False, None, {}),
                        )
                        batch_response_metadata.append(response_meta)
                        all_results.extend(batch_rows)
                        returned_codes.update(batch_returned_codes)
                        quota_exhausted = quota_exhausted or batch_quota_exhausted
                        last_error = error_message or last_error
                        if error_message:
                            failed_response_count += 1
                            transport_failure = self._is_transport_failure_meta(response_meta)
                            if transport_failure:
                                transport_exception_count += 1
                                consecutive_transport_failure_count += 1
                                max_consecutive_transport_failure_count = max(
                                    max_consecutive_transport_failure_count,
                                    consecutive_transport_failure_count,
                                )
                            else:
                                consecutive_transport_failure_count = 0
                            failed_batches.append({
                                'batch_no': int(batch_no),
                                'requested_count': len(_batch_codes),
                                'requested_codes_preview': _batch_codes[:10],
                                'quota_error': bool(response_meta.get('quota_error')),
                                'error': str(error_message)[:300],
                            })
                        else:
                            response_count += 1
                            accepted_response_count += 1
                            consecutive_transport_failure_count = 0

                    if quota_exhausted:
                        logger.warning("AutoStock fund 批量查询已检测到配额/频率限制，停止提交后续批次")
                        break
                    if (
                        consecutive_transport_failure_count
                        >= self.FUND_BATCH_TRANSPORT_FAILURE_CIRCUIT_THRESHOLD
                    ):
                        circuit_breaker_open = True
                        circuit_breaker_reason = 'consecutive_transport_failures'
                        circuit_breaker_batch_no = int(current_window[-1][0])
                        logger.error(
                            "AutoStock fund 批量查询连续传输失败达到熔断阈值，停止后续窗口: "
                            "batch_no=%s consecutive_failures=%s threshold=%s",
                            circuit_breaker_batch_no,
                            consecutive_transport_failure_count,
                            self.FUND_BATCH_TRANSPORT_FAILURE_CIRCUIT_THRESHOLD,
                        )
                        break

        batch_meta = self._build_batch_meta(
            requested_codes=requested_codes,
            returned_codes=returned_codes,
            requested_batch_size=requested_batch_size,
            effective_batch_size=effective_batch_size,
            capped=capped,
        )
        batch_meta['returned_total'] = len(all_results)
        batch_meta['input_code_count'] = len(codes)
        batch_meta['deduplicated_code_count'] = len(codes) - len(requested_codes)
        wire_metrics = self._summarize_request_metadata(batch_response_metadata)
        batch_meta.update(wire_metrics)
        batch_meta['partial_success_batch_count'] = sum(
            0 < meta.get('returned_row_count', 0) < meta.get('requested_row_count', 0)
            for meta in batch_response_metadata
        )
        batch_meta['rejected_record_count'] = sum(
            (meta.get('row_quality') or {}).get('rejected_record_count', 0) for meta in batch_response_metadata
        )
        batch_meta['rejected_records'] = [
            row for meta in batch_response_metadata for row in (meta.get('row_quality') or {}).get('rejected_records', [])
        ][:20]
        batch_meta['quota_exhausted'] = quota_exhausted
        batch_meta['last_error'] = last_error
        batch_meta['max_workers'] = worker_count
        batch_meta['failed_batch_count'] = len(failed_batches)
        batch_meta['failed_batches'] = failed_batches
        batch_meta['attempted_batch_count'] = int(request_count)
        batch_meta['total_batch_count'] = int(len(batches))
        batch_meta['unattempted_batch_count'] = max(int(len(batches) - request_count), 0)
        batch_meta['unrequested_count'] = max(
            int(len(requested_codes) - min(request_count * effective_batch_size, len(requested_codes))),
            0,
        )
        batch_meta['consecutive_transport_failure_count'] = int(
            consecutive_transport_failure_count
        )
        batch_meta['max_consecutive_transport_failure_count'] = int(
            max_consecutive_transport_failure_count
        )
        batch_meta['transport_failure_circuit_threshold'] = int(
            self.FUND_BATCH_TRANSPORT_FAILURE_CIRCUIT_THRESHOLD
        )
        batch_meta['circuit_breaker_open'] = bool(circuit_breaker_open)
        batch_meta['circuit_breaker_reason'] = circuit_breaker_reason
        batch_meta['circuit_breaker_batch_no'] = circuit_breaker_batch_no
        batch_meta['source_evidence'] = self._build_source_evidence(
            task='autostock_validation',
            source='AutoStock /fund',
            endpoint_path='/fund',
            started_at=started_at,
            finished_at=time.time(),
            target_count=len(requested_codes),
            request_count=request_count,
            response_count=response_count,
            accepted_response_count=accepted_response_count,
            failed_response_count=failed_response_count,
            transport_exception_count=transport_exception_count,
            returned_record_count=len(all_results),
            returned_unique_count=len(returned_codes),
            missing_count=batch_meta.get('missing_count') or 0,
            failed_batch_count=len(failed_batches),
            quota_exhausted=quota_exhausted,
            last_error=last_error,
            response_records=all_results,
            extra={
                **wire_metrics,
                'request_count_basis': 'logical_batches; http_attempt_count includes retries',
                'partial_success_batch_count': batch_meta['partial_success_batch_count'],
                'rejected_record_count': batch_meta['rejected_record_count'],
                'rejected_records': batch_meta['rejected_records'],
                'requested_total': batch_meta.get('requested_total'),
                'requested_unique_total': batch_meta.get('requested_unique_total'),
                'requested_batch_size': requested_batch_size,
                'effective_batch_size': effective_batch_size,
                'capped': capped,
                'max_workers': worker_count,
                'missing_codes_preview': batch_meta.get('missing_codes_preview') or [],
                'failed_batches': failed_batches[:5],
                'attempted_batch_count': batch_meta.get('attempted_batch_count'),
                'total_batch_count': batch_meta.get('total_batch_count'),
                'unattempted_batch_count': batch_meta.get('unattempted_batch_count'),
                'unrequested_count': batch_meta.get('unrequested_count'),
                'max_consecutive_transport_failure_count': batch_meta.get(
                    'max_consecutive_transport_failure_count'
                ),
                'transport_failure_circuit_threshold': batch_meta.get(
                    'transport_failure_circuit_threshold'
                ),
                'circuit_breaker_open': batch_meta.get('circuit_breaker_open'),
                'circuit_breaker_reason': batch_meta.get('circuit_breaker_reason'),
                'circuit_breaker_batch_no': batch_meta.get('circuit_breaker_batch_no'),
            },
        )
        self._last_fund_batch_meta = batch_meta

        if batch_meta['missing_count']:
            logger.warning(
                "AutoStock fund 批量查询存在返回缺失: requested_unique_total=%s returned_unique_total=%s missing_count=%s",
                batch_meta['requested_unique_total'],
                batch_meta['returned_unique_total'],
                batch_meta['missing_count'],
            )

        return all_results, dict(batch_meta)

    def get_fund_batch(self, codes: List[str], batch_size: int = 50) -> List[Dict[str, Any]]:
        """批量获取基金基础数据。"""
        all_results, _meta = self.get_fund_batch_with_meta(codes, batch_size=batch_size)
        return all_results

    def get_last_fund_batch_meta(self) -> Dict[str, Any]:
        """获取最近一次 fund 批量查询元数据。"""
        return dict(self._last_fund_batch_meta)

    def get_last_fund_detail_batch_meta(self) -> Dict[str, Any]:
        """获取最近一次 fund/detail/list 批量查询元数据。"""
        return dict(self._last_fund_detail_batch_meta)

    def get_fund_detail(self, code: str, start_date: Optional[str] = None,
                        end_date: Optional[str] = None) -> Dict[str, Any]:
        """获取单个基金详情。

        ⚠️ P0-04.3 / B15: 该方法会在跨进程限速器里消耗一个 100次/小时 配额槽，
        与批量接口 `/fund/detail/list` 开销完全相同。新代码请优先调用
        ``get_fund_detail_batch([code], ...)``，本方法仅保留作为稀有单码兜底。
        """
        import warnings as _warnings

        _warnings.warn(
            "AutoStockClient.get_fund_detail(single code) 已弃用：请改用 "
            "get_fund_detail_batch([code]) 以复用批量通道并避免浪费 100/hour 配额。",
            DeprecationWarning,
            stacklevel=2,
        )
        logger.warning(
            "AutoStockClient.get_fund_detail 单码调用（code=%s）已弃用，"
            "建议改用 get_fund_detail_batch 批量通道",
            code,
        )

        batch_results = self.get_fund_detail_batch(
            [str(code)],
            start_date=start_date,
            end_date=end_date,
            batch_size=1,
        )
        if not batch_results:
            return {}
        for item in batch_results:
            if isinstance(item, dict):
                item_code = self._normalize_autostock_code(item.get('code'))
                if item_code and item_code == self._normalize_autostock_code(code):
                    return item
        return {}

    def get_fund_detail_batch(self, codes: List[str], start_date: Optional[str] = None,
                              end_date: Optional[str] = None,
                              batch_size: int = 50) -> List[Dict[str, Any]]:
        """批量获取基金详情，并记录完整性元数据。"""
        for value in (start_date, end_date):
            if value is not None and date.fromisoformat(value).isoformat() != value:
                raise ValueError('history dates must use yyyy-MM-dd')
        if start_date and end_date and start_date > end_date:
            raise ValueError('start_date must not exceed end_date')
        requested_batch_size = int(batch_size or self.SAFE_FUND_BATCH_SIZE)
        effective_batch_size = max(1, min(requested_batch_size, self.SAFE_FUND_BATCH_SIZE))
        capped = effective_batch_size != requested_batch_size
        if capped:
            logger.warning(
                "AutoStock fund/detail/list 单批请求上限收敛: requested_batch_size=%s -> effective_batch_size=%s",
                batch_size,
                effective_batch_size,
            )

        all_results: List[Dict[str, Any]] = []
        requested_codes = self._normalize_requested_codes(codes)
        returned_codes = set()
        quota_exhausted = False
        last_error = None
        failed_batches: List[Dict[str, Any]] = []
        rejected_records: List[Dict[str, Any]] = []
        rejected_count = 0
        request_metadata: List[Dict[str, Any]] = []
        partial_success_batch_count = 0

        for i in range(0, len(requested_codes), effective_batch_size):
            batch = requested_codes[i:i + effective_batch_size]
            codes_str = ','.join(batch)
            batch_no = i // effective_batch_size + 1

            url = f"{self.base_url}/fund/detail/list?code={codes_str}"
            if start_date:
                url += f"&startDate={start_date}"
            if end_date:
                url += f"&endDate={end_date}"

            logger.info(f"批量获取基金详情 ({i+1}-{i+len(batch)}/{len(requested_codes)})")

            observed_meta: Dict[str, Any] = {}
            try:
                data, observed_meta = self._request_with_meta(url)
                accepted_rows, quality = self._accepted_fund_rows(data, batch)
                partial_success_batch_count += int(0 < len(accepted_rows) < len(batch))
                rejected_count += quality['rejected_record_count']
                rejected_records.extend(quality['rejected_records'])
                if quality.get('payload_error') or quality['rejected_record_count']:
                    observed_meta['failure_kind'] = 'upstream_business_data_failed'
                    last_error = f"upstream_business_data_failed: {quality.get('payload_error') or quality['rejected_records']}"
                    failed_batches.append({'batch_no': batch_no, 'requested_count': len(batch),
                                           'error': last_error, 'quota_error': False})
                elif len(accepted_rows) < len(batch):
                    observed_meta['failure_kind'] = 'missing_rows'
                if accepted_rows:
                    all_results.extend(accepted_rows)
                    returned_codes.update(
                        self._normalize_autostock_code(item.get('code')) for item in accepted_rows
                        if isinstance(item, dict) and item.get('code')
                    )
                    returned_codes.discard(None)
                    logger.info(f"成功获取 {len(accepted_rows)} 只基金详情")
            except Exception as exc:
                meta = self._extract_exception_meta(exc)
                if not observed_meta:
                    observed_meta = meta
                error_message = meta.get('message') or str(exc)
                quota_exhausted = quota_exhausted or bool(meta.get('quota_error'))
                last_error = error_message or last_error
                failed_batches.append({
                    'batch_no': batch_no,
                    'requested_count': len(batch),
                    'requested_codes_preview': batch[:10],
                    'quota_error': bool(meta.get('quota_error')),
                    'error': error_message,
                })
                logger.error(f"批量获取详情失败: {error_message}")
                if quota_exhausted:
                    logger.warning("AutoStock fund/detail/list 批量查询触发配额/频率限制，停止后续批次")
                    break
            finally:
                request_metadata.append(observed_meta)

        self._last_fund_detail_batch_meta = self._build_batch_meta(
            requested_codes=requested_codes,
            returned_codes=returned_codes,
            requested_batch_size=requested_batch_size,
            effective_batch_size=effective_batch_size,
            capped=capped,
        )
        self._last_fund_detail_batch_meta['returned_total'] = len(all_results)
        self._last_fund_detail_batch_meta['quota_exhausted'] = quota_exhausted
        self._last_fund_detail_batch_meta['last_error'] = last_error
        self._last_fund_detail_batch_meta['failed_batch_count'] = len(failed_batches)
        self._last_fund_detail_batch_meta['failed_batches'] = failed_batches
        self._last_fund_detail_batch_meta['input_code_count'] = len(codes)
        self._last_fund_detail_batch_meta['rejected_record_count'] = rejected_count
        self._last_fund_detail_batch_meta['rejected_records'] = rejected_records[:20]
        self._last_fund_detail_batch_meta['source_complete'] = bool(
            not failed_batches and not self._last_fund_detail_batch_meta['missing_count']
        )
        self._last_fund_detail_batch_meta.update(self._summarize_request_metadata(request_metadata))
        self._last_fund_detail_batch_meta.update({
            'logical_batch_count': (len(requested_codes) + effective_batch_size - 1) // effective_batch_size,
            'attempted_batch_count': len(request_metadata),
            'unattempted_batch_count': (len(requested_codes) + effective_batch_size - 1) // effective_batch_size - len(request_metadata),
            'partial_success_batch_count': partial_success_batch_count,
        })

        if self._last_fund_detail_batch_meta['missing_count']:
            logger.warning(
                "AutoStock fund/detail/list 批量查询存在返回缺失: requested_unique_total=%s returned_unique_total=%s missing_count=%s",
                self._last_fund_detail_batch_meta['requested_unique_total'],
                self._last_fund_detail_batch_meta['returned_unique_total'],
                self._last_fund_detail_batch_meta['missing_count'],
            )

        return all_results

    def get_fund_position(self, code: str) -> Dict[str, Any]:
        """获取基金持仓数据。

        ⚠️ P0-04.3 / B15: AutoStock `/fund/position` 暂无批量版本，保留单码入口，
        但新调用方应确认"每次只拿一只基金持仓"是必要路径而不是循环误用。
        每次调用消耗 1 次/hour 配额（100 上限）。
        """
        code = self._normalize_requested_codes([code])[0]
        url = f"{self.base_url}/fund/position?code={code}"

        logger.info(f"获取基金持仓: {code}")
        data = self._request(url)
        failure = self._business_data_failure(data) if isinstance(data, dict) else 'expected_object'
        if failure:
            self._raise_business_data_failure('/fund/position', failure)
        return data

    def get_call_count(self) -> int:
        """获取API调用次数。"""
        return self.rate_limiter.call_count


if __name__ == '__main__':
    client = AutoStockClient(min_interval=2.0)

    print("=" * 70)
    print("测试1: 获取所有基金")
    all_funds = client.get_all_funds()
    print(f"总共有 {len(all_funds) - 1} 只基金")
    print(f"前5只基金: {all_funds[1:6]}")

    print("\n" + "=" * 70)
    print("测试2: 获取债券基金排行")
    rank_data = client.get_bond_fund_rank(page_index=1, page_size=10)
    print(f"获取到 {len(rank_data.get('rank', []))} 只债券基金")
    for fund in rank_data.get('rank', [])[:5]:
        print(f"  {fund.get('code')} - {fund.get('name')} - {fund.get('dayGrowth')}%")

    print("\n" + "=" * 70)
    print("测试3: 批量获取基金数据")
    test_codes = ['000001', '000002', '000003']
    batch_data = client.get_fund_batch(test_codes)
    print(f"获取到 {len(batch_data)} 只基金数据")
    for fund in batch_data:
        print(f"  {fund.get('code')} - {fund.get('name')}")

    print(f"\n总API调用次数: {client.get_call_count()}")
