"""
天天基金API客户端
整合速率限制、重试机制、错误处理
增强版：包含详细的请求/响应日志、性能监控
"""

import os
import logging
import threading
import time
import uuid
from typing import Dict, Any, Optional

import requests
from api.rate_limiter import RateLimiter
from api.retry_handler import RetryHandler
from api.response_observation import ObservedRankResponse
from utils.project_runtime_config import load_project_config_payload

logger = logging.getLogger(__name__)
DEFAULT_TIANTIAN_BASE_URL = 'http://tiantian-api:3000'

# 尝试导入增强日志
try:
    from utils.enhanced_logger import get_api_logger, get_performance_logger
    HAS_ENHANCED_LOGGER = True
except ImportError:
    HAS_ENHANCED_LOGGER = False

api_logger = None
perf_logger = None


class APIFailureResult(dict):
    """结构化 API 失败结果；布尔值恒为 False，以兼容旧调用方的 `if not result`。"""

    def __init__(self, payload: Dict[str, Any], *, exception: Optional[Exception] = None):
        super().__init__(payload)
        self.exception = exception

    def __bool__(self) -> bool:
        return False


def resolve_tiantian_base_url(config: Optional[Dict[str, Any]] = None) -> str:
    """
    统一解析天天基金 API 地址。

    优先级：
    1) TIANTIAN_API_URL 环境变量
    2) api.tiantian.base_url
    3) api.base_url
    4) services.tiantian_api_url
    5) config/project.yaml -> apis.tiantian_base_url
    6) 项目兼容硬回退地址
    """
    config = config or {}
    api_config = config.get('api', {}) or {}
    tiantian_config = api_config.get('tiantian', {}) or {}

    env_api_url = os.environ.get('TIANTIAN_API_URL')
    project_config = load_project_config_payload()
    project_api_url = str(
        ((project_config.get('apis') or {}).get('tiantian_base_url') or '')
    ).strip()
    configured_api_url = (
        tiantian_config.get('base_url')
        or api_config.get('base_url')
        or (config.get('services') or {}).get('tiantian_api_url')
        or project_api_url
    )
    return (env_api_url or configured_api_url or DEFAULT_TIANTIAN_BASE_URL).rstrip('/')


class APIClient:
    """天天基金API客户端"""

    def __init__(self, config=None):
        """
        初始化API客户端

        Args:
            config: 配置字典，包含API和速率限制配置
        """
        global api_logger, perf_logger
        if HAS_ENHANCED_LOGGER:
            if api_logger is None:
                api_logger = get_api_logger()
            if perf_logger is None:
                perf_logger = get_performance_logger()
        self.config = config or {}

        # API基础配置（优先使用新的tiantian配置路径，回退到旧路径以保持向后兼容）
        api_config = self.config.get('api', {})
        tiantian_config = api_config.get('tiantian', {})

        # base_url 优先级：
        # 1) TIANTIAN_API_URL 环境变量（部署/审计时可显式覆盖）
        # 2) api.tiantian.base_url
        # 3) api.base_url
        # 4) services.tiantian_api_url（兼容旧配置路径）
        # 5) 项目兼容硬回退地址
        env_api_url = os.environ.get('TIANTIAN_API_URL')
        configured_api_url = resolve_tiantian_base_url(
            {
                'api': {
                    'tiantian': tiantian_config,
                    **api_config,
                },
                'services': self.config.get('services', {}),
            }
        )
        self.base_url = configured_api_url

        configured_without_env = (
            tiantian_config.get('base_url')
            or api_config.get('base_url')
            or self.config.get('services', {}).get('tiantian_api_url')
        )
        if env_api_url and configured_without_env and env_api_url.rstrip('/') != str(configured_without_env).rstrip('/'):
            logger.info(
                "检测到 TIANTIAN_API_URL 覆盖配置地址: config=%s, env=%s",
                configured_without_env,
                env_api_url,
            )

        # headers: 优先使用 api.tiantian.headers，回退到 api.headers
        self.headers = tiantian_config.get('headers') or api_config.get('headers', {})

        # base_params: 优先使用 api.tiantian.base_params，回退到 api.base_params
        self.base_params = tiantian_config.get('base_params') or api_config.get('base_params', {})

        # 初始化速率限制器（优先使用tiantian配置）
        rate_limit_config = tiantian_config.get('rate_limit') or api_config.get('rate_limit', {})
        circuit_breaker_config = tiantian_config.get('circuit_breaker') or api_config.get('circuit_breaker', {})

        # 兼容配置字段：将 circuit_breaker.enabled 映射为 RateLimiter 接受的 circuit_breaker_enabled
        # KISS: 最小改动使配置显式生效，避免熔断开关配置无效
        if 'enabled' in circuit_breaker_config and 'circuit_breaker_enabled' not in circuit_breaker_config:
            circuit_breaker_config = {
                **circuit_breaker_config,
                'circuit_breaker_enabled': bool(circuit_breaker_config.get('enabled'))
            }

        combined_config = {**rate_limit_config, **circuit_breaker_config}
        self.rate_limiter = RateLimiter(combined_config)

        # 初始化重试处理器（优先使用tiantian配置）
        retry_config = tiantian_config.get('retry') or api_config.get('retry', {})
        self.retry_handler = RetryHandler(retry_config)

        # v1.9.28修复：使用 threading.local() 为每个线程创建独立 Session
        # requests.Session 内部连接池和 cookie jar 无锁保护，
        # 多线程共享会导致连接损坏和 socket 错误
        self._thread_local = threading.local()

        # 统计信息
        self.stats = {
            'total_requests': 0,
            'successful_requests': 0,
            'failed_requests': 0,
            'attempt_total_requests': 0,
            'attempt_successful_requests': 0,
            'attempt_failed_requests': 0,
            'total_response_time': 0.0,
        }
        self._stats_lock = threading.Lock()

        logger.info(f"API客户端初始化完成: {self.base_url}")

    def _get_session(self) -> requests.Session:
        """获取当前线程专属的 requests.Session（延迟创建，TCP 连接复用）"""
        session = getattr(self._thread_local, 'session', None)
        if session is None:
            session = requests.Session()
            session.headers.update(self.headers)
            self._thread_local.session = session
        return session

    def _update_stats(self, **kwargs):
        """线程安全的统计更新"""
        with self._stats_lock:
            for key, value in kwargs.items():
                self.stats[key] = self.stats.get(key, 0) + value

    @staticmethod
    def _classify_failure_kind(exception: Exception) -> str:
        """归类终态失败类型，便于调用方保留失败语义。"""
        exception_name = type(exception).__name__
        error_text = str(exception).lower()

        if (
            isinstance(exception, requests.exceptions.Timeout)
            or 'Timeout' in exception_name
            or 'timeout' in error_text
        ):
            return 'timeout'
        if isinstance(exception, requests.exceptions.ConnectionError):
            return 'connection_error'
        if isinstance(exception, requests.exceptions.HTTPError):
            return 'http_error'
        if isinstance(exception, (ValueError, requests.exceptions.JSONDecodeError)):
            return 'invalid_json'
        return 'unknown_error'

    @staticmethod
    def _extract_proxy_failure_meta(response: Optional[requests.Response]) -> Dict[str, Any]:
        """尽力解析本地 Node 代理返回的结构化失败体。"""
        if response is None:
            return {}

        try:
            payload = response.json()
        except (ValueError, requests.exceptions.JSONDecodeError, TypeError, AttributeError):
            return {}

        if not isinstance(payload, dict):
            return {}

        proxy_error = payload.get('error') if isinstance(payload.get('error'), dict) else {}
        err_msg = str(
            payload.get('ErrMsg')
            or proxy_error.get('message')
            or ''
        ).strip() or None
        endpoint = str(payload.get('endpoint') or '').strip() or None
        proxy_context = str(proxy_error.get('context') or '').strip() or None
        upstream_business_message = str(proxy_error.get('upstreamBusinessMessage') or '').strip() or None

        meta: Dict[str, Any] = {}
        if isinstance(payload.get('Success'), bool):
            meta['Success'] = payload.get('Success')
        if payload.get('ErrCode') not in (None, ''):
            meta['ErrCode'] = payload.get('ErrCode')
        if err_msg:
            meta['ErrMsg'] = err_msg
        if isinstance(payload.get('retryable'), bool):
            meta['retryable'] = payload.get('retryable')
        if endpoint:
            meta['endpoint'] = endpoint
        if payload.get('attempts') not in (None, ''):
            meta['attempts'] = payload.get('attempts')
        if payload.get('upstreamStatus') not in (None, ''):
            meta['upstream_status'] = payload.get('upstreamStatus')
        if proxy_error.get('type'):
            meta['proxy_error_type'] = proxy_error.get('type')
        if proxy_error.get('code') not in (None, ''):
            meta['proxy_error_code'] = proxy_error.get('code')
        if proxy_context:
            meta['proxy_context'] = proxy_context
        if proxy_error.get('upstreamBusinessCode') not in (None, ''):
            meta['upstream_business_code'] = str(proxy_error.get('upstreamBusinessCode')).strip()
        if upstream_business_message:
            meta['upstream_business_message'] = upstream_business_message

        return meta

    @staticmethod
    def _normalize_failure_kind(default_kind: str, proxy_meta: Dict[str, Any]) -> str:
        """对代理层已归一化的失败做更精确的失败分类。"""
        proxy_error_code = str(
            proxy_meta.get('proxy_error_code')
            or proxy_meta.get('ErrCode')
            or ''
        ).strip()
        if proxy_error_code == 'UPSTREAM_INVALID_JSON':
            return 'invalid_json'
        return default_kind

    def _build_failure_result(
        self,
        *,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]],
        data: Optional[Dict[str, Any]],
        exception: Exception,
    ) -> APIFailureResult:
        """构造结构化失败结果，避免终态失败被压扁为裸 None。"""
        response = getattr(exception, 'response', None)
        status_code = getattr(response, 'status_code', None)
        proxy_meta = self._extract_proxy_failure_meta(response)
        failure_kind = self._normalize_failure_kind(
            self._classify_failure_kind(exception),
            proxy_meta,
        )
        error_message = proxy_meta.get('ErrMsg') or str(exception)
        payload = {
            'ok': False,
            'error': error_message,
            'error_type': type(exception).__name__,
            'failure_kind': failure_kind,
            'method': str(method).upper(),
            'endpoint': proxy_meta.get('endpoint') or endpoint,
            'status_code': status_code,
            'retry_exhausted': True,
        }
        raw_transport_error = str(exception).strip()
        if raw_transport_error and raw_transport_error != error_message:
            payload['transport_error'] = raw_transport_error
        payload.update(proxy_meta)
        if params:
            payload['params'] = dict(params)
        if data:
            payload['data'] = dict(data)
        return APIFailureResult(payload, exception=exception)

    def _make_request(self, method: str, endpoint: str, params: Dict = None,
                      data: Dict = None, request_id: str = None) -> Dict[str, Any]:
        """
        执行HTTP请求（内部方法）

        Args:
            method: HTTP方法 (GET, POST)
            endpoint: API端点
            params: URL参数
            data: POST数据
            request_id: 请求ID（用于日志追踪）

        Returns:
            响应JSON数据

        Raises:
            requests.RequestException: 请求失败
        """
        url = f"{self.base_url}{endpoint}"
        request_id = request_id or str(uuid.uuid4())[:8]
        thread_id = threading.current_thread().ident

        # 合并参数
        request_params = {**self.base_params, **(params or {})}

        # 记录请求开始
        if HAS_ENHANCED_LOGGER:
            api_logger.log_request(method, endpoint, params, request_id)
        else:
            logger.debug(f"[{request_id}] {method} {endpoint} params={params}")

        # 发送请求
        start_time = time.time()
        self._update_stats(attempt_total_requests=1)

        try:
            if method.upper() == 'GET':
                response = self._get_session().get(
                    url,
                    params=request_params,
                    headers=self.headers,
                    timeout=self.retry_handler.timeout
                )
            elif method.upper() == 'POST':
                response = self._get_session().post(
                    url,
                    params=request_params,
                    data=data,
                    headers=self.headers,
                    timeout=self.retry_handler.timeout
                )
            else:
                raise ValueError(f"不支持的HTTP方法: {method}")

            # 检查响应状态
            response.raise_for_status()

            # 记录响应时间
            response_time = time.time() - start_time
            response_time_ms = response_time * 1000
            response_size = len(response.content)

            # 先解析 JSON，成功后再记录到统计和熔断器（避免 JSON 失败时需要撤销）
            try:
                json_data = response.json()
            except (ValueError, requests.exceptions.JSONDecodeError) as e:
                self._update_stats(attempt_failed_requests=1)
                logger.error(f"[{request_id}] JSON解析失败 {method} {endpoint}: {e}")
                raise

            self.rate_limiter.record_success(response_time)

            # 更新统计
            self._update_stats(
                attempt_successful_requests=1,
                total_response_time=response_time,
            )

            # 记录成功响应
            if HAS_ENHANCED_LOGGER:
                api_logger.log_response(method, endpoint, response.status_code,
                                       response_time_ms, response_size, request_id, True)
            else:
                logger.debug(f"[{request_id}] {method} {endpoint} status={response.status_code} "
                           f"elapsed={response_time_ms:.2f}ms size={response_size}B")

            # 慢请求告警（超过2秒）
            if response_time > 2.0:
                logger.warning(f"慢请求告警 [{request_id}] {method} {endpoint} "
                             f"elapsed={response_time_ms:.2f}ms")

            if endpoint == '/fundMNRank' and isinstance(json_data, dict):
                return ObservedRankResponse(
                    json_data, body=response.content, request_id=request_id,
                    http_status=response.status_code,
                    proxy_request_id=getattr(response, 'headers', {}).get('X-Request-ID'),
                )
            return json_data

        except requests.exceptions.Timeout as e:
            elapsed = (time.time() - start_time) * 1000
            error_msg = f"请求超时 (>{self.retry_handler.timeout}s)"

            if HAS_ENHANCED_LOGGER:
                api_logger.log_error(method, endpoint, error_msg, request_id)
            else:
                logger.error(f"[{request_id}] {error_msg}")

            self.rate_limiter.record_failure()
            self._update_stats(attempt_failed_requests=1)
            raise

        except requests.exceptions.ConnectionError as e:
            elapsed = (time.time() - start_time) * 1000
            error_msg = f"连接错误: {str(e)[:100]}"

            if HAS_ENHANCED_LOGGER:
                api_logger.log_error(method, endpoint, error_msg, request_id)
            else:
                logger.error(f"[{request_id}] {error_msg}")

            self.rate_limiter.record_failure()
            self._update_stats(attempt_failed_requests=1)
            raise

        except requests.exceptions.HTTPError as e:
            elapsed = (time.time() - start_time) * 1000
            error_msg = f"HTTP错误: {response.status_code}"

            if HAS_ENHANCED_LOGGER:
                api_logger.log_response(method, endpoint, response.status_code,
                                       elapsed, 0, request_id, False)
                api_logger.log_error(method, endpoint, error_msg, request_id)
            else:
                logger.error(f"[{request_id}] {error_msg}")

            self.rate_limiter.record_failure()
            self._update_stats(attempt_failed_requests=1)
            raise

        except Exception as e:
            elapsed = (time.time() - start_time) * 1000
            error_msg = f"{type(e).__name__}: {str(e)[:100]}"

            if HAS_ENHANCED_LOGGER:
                api_logger.log_error(method, endpoint, error_msg, request_id)
            else:
                logger.error(f"[{request_id}] 未知错误: {error_msg}")

            self.rate_limiter.record_failure()
            self._update_stats(attempt_failed_requests=1)
            raise

    def request(self, method: str, endpoint: str, params: Dict = None, data: Dict = None) -> Dict[str, Any]:
        """
        执行API请求（带速率限制和重试）

        Args:
            method: HTTP方法
            endpoint: API端点
            params: URL参数
            data: POST数据

        Returns:
            响应数据；失败时返回可保留错误语义的结构化结果对象（布尔值为 False）
        """
        self._update_stats(total_requests=1)

        try:
            def _rate_limited_attempt():
                # 每次重试都重新经过限流器，避免退避等待长期占用并发槽，
                # 同时确保重试请求也纳入 QPS 控制。
                with self.rate_limiter.acquire():
                    return self._make_request(method, endpoint, params, data)

            result = self.retry_handler.execute_with_retry(_rate_limited_attempt)
            self._update_stats(successful_requests=1)
            return result

        except Exception as e:
            self._update_stats(failed_requests=1)
            failure_result = self._build_failure_result(
                method=method,
                endpoint=endpoint,
                params=params,
                data=data,
                exception=e,
            )
            logger.error(f"API请求失败 {endpoint}: {failure_result.get('error')}")
            return failure_result

    def get(self, endpoint: str, params: Dict = None) -> Dict[str, Any]:
        """
        GET请求

        Args:
            endpoint: API端点
            params: URL参数

        Returns:
            响应数据
        """
        return self.request('GET', endpoint, params=params)

    def post(self, endpoint: str, params: Dict = None, data: Dict = None) -> Dict[str, Any]:
        """
        POST请求

        Args:
            endpoint: API端点
            params: URL参数
            data: POST数据

        Returns:
            响应数据
        """
        return self.request('POST', endpoint, params=params, data=data)

    def get_stats(self) -> Dict[str, Any]:
        """
        获取客户端统计信息

        Returns:
            统计信息字典
        """
        stats = {**self.stats}

        # 逻辑请求级口径：每次 request() 调用只计一次成功/失败。
        if stats['total_requests'] > 0:
            stats['success_rate'] = (stats['successful_requests'] / stats['total_requests']) * 100
            stats['error_rate'] = (stats['failed_requests'] / stats['total_requests']) * 100
        else:
            stats['success_rate'] = 0
            stats['error_rate'] = 0

        # 尝试级口径：重试中的每次 HTTP 尝试都会计入。
        if stats['attempt_total_requests'] > 0:
            stats['attempt_success_rate'] = (
                stats['attempt_successful_requests'] / stats['attempt_total_requests']
            ) * 100
            stats['attempt_error_rate'] = (
                stats['attempt_failed_requests'] / stats['attempt_total_requests']
            ) * 100
        else:
            stats['attempt_success_rate'] = 0
            stats['attempt_error_rate'] = 0

        stats['retry_overhead'] = max(0, stats['attempt_total_requests'] - stats['total_requests'])

        # 计算平均响应时间
        if stats['successful_requests'] > 0:
            stats['avg_response_time'] = stats['total_response_time'] / stats['successful_requests']
        else:
            stats['avg_response_time'] = 0

        # 添加速率限制器统计
        stats['rate_limiter'] = self.rate_limiter.get_stats()

        return stats

    def print_stats(self):
        """打印统计信息"""
        stats = self.get_stats()

        print("\n" + "="*50)
        print("API客户端统计信息")
        print("="*50)
        print(f"逻辑请求数:       {stats['total_requests']:,}")
        print(f"逻辑成功:         {stats['successful_requests']:,}")
        print(f"逻辑失败:         {stats['failed_requests']:,}")
        print(f"逻辑成功率:       {stats['success_rate']:.2f}%")
        print(f"逻辑失败率:       {stats['error_rate']:.2f}%")
        print(f"尝试总次数:       {stats['attempt_total_requests']:,}")
        print(f"尝试成功:         {stats['attempt_successful_requests']:,}")
        print(f"尝试失败:         {stats['attempt_failed_requests']:,}")
        print(f"尝试成功率:       {stats['attempt_success_rate']:.2f}%")
        print(f"重试额外尝试:     {stats['retry_overhead']:,}")
        print(f"平均响应时间:     {stats['avg_response_time']:.3f}秒")

        if 'rate_limiter' in stats:
            print(f"\n速率限制:")
            print(f"  QPS设置:        {stats['rate_limiter']['qps']}")
            if 'current_qps' in stats['rate_limiter']:
                print(f"  当前QPS:        {stats['rate_limiter']['current_qps']:.2f}")
            print(f"  最大并发:       {stats['rate_limiter']['max_workers']}")

            if stats['rate_limiter'].get('circuit_breaker_open'):
                print(f"\n⚠️  熔断器状态:    打开")
                print(f"  失败次数:       {stats['rate_limiter']['failure_count']}")

        print("="*50 + "\n")

    def reset_stats(self):
        """重置统计信息"""
        self.stats = {
            'total_requests': 0,
            'successful_requests': 0,
            'failed_requests': 0,
            'attempt_total_requests': 0,
            'attempt_successful_requests': 0,
            'attempt_failed_requests': 0,
            'total_response_time': 0.0,
        }
        logger.info("统计信息已重置")
