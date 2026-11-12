"""
重试处理器：分级重试策略和指数退避
"""

import time
import logging
from functools import wraps
import requests

logger = logging.getLogger(__name__)


class RetryHandler:
    """重试处理器"""

    def __init__(self, config=None):
        """
        初始化重试处理器

        Args:
            config: 配置字典，包含:
                - max_attempts: 最大重试次数 (default: 3)
                - backoff_factor: 退避因子 (default: 2)
                - timeout: 请求超时秒数 (default: 10)
                - retry_on_timeout: 超时是否重试 (default: True)
                - retry_on_invalid_json: 非 JSON / 非法 JSON 是否重试 (default: True)
                - retry_on_500: 5xx错误是否重试 (default: True)
                - retry_on_429: 429限流是否重试 (default: True)
                - rate_limit_wait: 429后等待时间 (default: 60)
        """
        config = config or {}

        self.max_attempts = config.get('max_attempts', 3)
        self.backoff_factor = config.get('backoff_factor', 2)
        self.timeout = config.get('timeout', 10)

        self.retry_on_timeout = config.get('retry_on_timeout', True)
        self.retry_on_invalid_json = config.get('retry_on_invalid_json', True)
        self.retry_on_500 = config.get('retry_on_500', True)
        self.retry_on_429 = config.get('retry_on_429', True)
        self.rate_limit_wait = config.get('rate_limit_wait', 60)

        logger.info(f"重试处理器初始化: 最大{self.max_attempts}次, 退避因子{self.backoff_factor}")

    def should_retry(self, exception, attempt):
        """
        判断是否应该重试

        Args:
            exception: 异常对象
            attempt: 当前尝试次数

        Returns:
            (should_retry: bool, wait_time: float)
        """
        # 超过最大重试次数
        if attempt >= self.max_attempts:
            return False, 0

        # 检查异常类型
        exception_name = type(exception).__name__

        # 超时错误
        if (
            'Timeout' in exception_name
            or 'timeout' in str(exception).lower()
            or isinstance(exception, requests.exceptions.Timeout)
        ):
            if self.retry_on_timeout:
                wait_time = self.backoff_factor ** (attempt - 1)
                logger.warning(f"请求超时，{wait_time}秒后重试（第{attempt}/{self.max_attempts}次）")
                return True, wait_time
            return False, 0

        # JSON 解析错误：对上游瞬时空响应 / 非 JSON 响应进行有限重试
        if self.retry_on_invalid_json:
            error_msg = str(exception).lower()
            looks_like_json_decode_error = (
                exception_name == 'JSONDecodeError'
                or isinstance(exception, requests.exceptions.JSONDecodeError)
                or (
                    isinstance(exception, ValueError)
                    and any(
                        marker in error_msg
                        for marker in (
                            'expecting value',
                            'extra data',
                            'unterminated string',
                            'no json object could be decoded',
                        )
                    )
                )
            )
            if looks_like_json_decode_error:
                wait_time = self.backoff_factor ** (attempt - 1)
                logger.warning(f"JSON解析失败，{wait_time}秒后重试（第{attempt}/{self.max_attempts}次）")
                return attempt < self.max_attempts, wait_time

        # 连接错误：区分永久性和临时性DNS错误
        if isinstance(exception, requests.exceptions.ConnectionError):
            error_msg = str(exception).lower()

            # 临时性DNS错误（DNS服务器暂时不可达），应当重试
            if 'temporary failure in name resolution' in error_msg:
                wait_time = self.backoff_factor ** (attempt - 1)
                logger.warning(f"DNS临时解析失败，{wait_time}秒后重试（第{attempt}/{self.max_attempts}次）")
                return attempt < self.max_attempts, wait_time

            # 永久性DNS错误（域名不存在），不重试
            if 'name or service not known' in error_msg or 'nodename nor servname provided' in error_msg:
                logger.error(f"DNS解析失败（域名不存在），不重试: {exception}")
                return False, 0

            wait_time = self.backoff_factor ** (attempt - 1)
            logger.warning(f"连接错误，{wait_time}秒后重试（第{attempt}/{self.max_attempts}次）")
            return attempt < self.max_attempts, wait_time

        # HTTP错误
        response = getattr(exception, 'response', None)
        if response is not None:
            status_code = response.status_code

            # 429 限流
            if status_code == 429 and self.retry_on_429:
                logger.warning(f"触发限流(429)，等待{self.rate_limit_wait}秒后重试")
                return True, self.rate_limit_wait

            # 5xx 服务器错误
            if 500 <= status_code < 600 and self.retry_on_500:
                wait_time = self.backoff_factor ** (attempt - 1)
                logger.warning(f"服务器错误({status_code})，{wait_time}秒后重试（第{attempt}/{self.max_attempts}次）")
                return True, wait_time

            # 4xx 客户端错误（除429外）
            if 400 <= status_code < 500:
                logger.error(f"客户端错误({status_code})，不重试")
                return False, 0

        # 其他错误：默认不重试
        logger.error(f"未知错误({exception_name})，不重试")
        return False, 0

    def execute_with_retry(self, func, *args, **kwargs):
        """
        执行函数，失败时自动重试

        Args:
            func: 要执行的函数
            *args, **kwargs: 函数参数

        Returns:
            函数返回值

        Raises:
            最后一次尝试的异常
        """
        last_exception = None

        for attempt in range(1, self.max_attempts + 1):
            try:
                result = func(*args, **kwargs)
                if attempt > 1:
                    logger.info(f"重试成功（第{attempt}次尝试）")
                return result

            except Exception as e:
                last_exception = e
                should_retry, wait_time = self.should_retry(e, attempt)

                if should_retry:
                    time.sleep(wait_time)
                else:
                    break

        # 所有尝试都失败
        logger.error(f"所有重试失败，共{self.max_attempts}次尝试")
        raise last_exception


def retry_on_failure(max_attempts=3, backoff_factor=2, logger=None):
    """
    装饰器：自动重试失败的函数

    Args:
        max_attempts: 最大尝试次数
        backoff_factor: 退避因子
        logger: 日志记录器

    Usage:
        @retry_on_failure(max_attempts=3)
        def fetch_data():
            return requests.get(url)
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            _logger = logger or logging.getLogger(func.__module__)
            last_exception = None

            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt < max_attempts:
                        wait_time = backoff_factor ** (attempt - 1)
                        _logger.warning(
                            f"{func.__name__} 失败，{wait_time}秒后重试（第{attempt}/{max_attempts}次）: {e}"
                        )
                        time.sleep(wait_time)
                    else:
                        _logger.error(f"{func.__name__} 所有重试失败: {e}")

            raise last_exception

        return wrapper
    return decorator
