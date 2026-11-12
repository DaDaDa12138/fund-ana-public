"""
三级速率限制器
- 级别1: 全局QPS限制
- 级别2: 并发控制
- 级别3: 请求间隔 + 随机延迟
"""

import time
import threading
import random
from collections import deque
import logging

logger = logging.getLogger(__name__)


class TokenBucket:
    """令牌桶算法实现QPS限制"""

    def __init__(self, rate=100, capacity=None):
        """
        初始化令牌桶

        Args:
            rate: 每秒生成令牌数（QPS）
            capacity: 桶容量（默认等于rate）
        """
        self.rate = rate
        self.capacity = capacity or rate
        self.tokens = self.capacity
        self.last_update = time.time()
        self.lock = threading.Lock()

    def acquire(self, tokens=1):
        """
        获取令牌（阻塞直到获取成功）

        Args:
            tokens: 需要的令牌数

        Returns:
            True when tokens acquired
        """
        while True:
            wait_time = self._try_acquire(tokens)
            if wait_time == 0:
                return True
            time.sleep(wait_time)

    def _try_acquire(self, tokens):
        """尝试获取令牌，返回需等待的秒数（0 表示已获取成功）"""
        with self.lock:
            now = time.time()
            elapsed = now - self.last_update

            # 补充令牌
            self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
            self.last_update = now

            # 检查是否有足够令牌
            if self.tokens >= tokens:
                self.tokens -= tokens
                return 0
            # 计算精确等待时间，避免 busy-wait
            deficit = tokens - self.tokens
            return deficit / self.rate


class CircuitBreaker:
    """熔断器：连续失败后自动暂停，含半开状态"""

    # 半开状态需连续成功的次数
    HALF_OPEN_SUCCESS_THRESHOLD = 3

    def __init__(self, failure_threshold=5, timeout_duration=300):
        """
        初始化熔断器

        Args:
            failure_threshold: 连续失败次数阈值
            timeout_duration: 熔断后暂停时间（秒）
        """
        self.failure_threshold = failure_threshold
        self.timeout_duration = timeout_duration
        self.failure_count = 0
        self.last_failure_time = None
        self.is_open = False
        self._half_open = False
        self._half_open_successes = 0
        self.lock = threading.Lock()
        self._recovery_event = threading.Event()
        self._recovery_event.set()  # 初始状态：未熔断，事件已设置

    def record_success(self):
        """记录成功请求"""
        with self.lock:
            if self._half_open:
                self._half_open_successes += 1
                if self._half_open_successes >= self.HALF_OPEN_SUCCESS_THRESHOLD:
                    # 半开状态连续成功足够多次，完全恢复
                    self._half_open = False
                    self._half_open_successes = 0
                    self.failure_count = 0
                    self.is_open = False
                    self._recovery_event.set()
                    logger.info("熔断器完全恢复（半开测试通过）")
            else:
                self.failure_count = 0
                self.is_open = False
                self._recovery_event.set()

    def record_failure(self):
        """记录失败请求"""
        with self.lock:
            if self._half_open:
                # 半开状态下失败，立即重新打开熔断器
                self._half_open = False
                self._half_open_successes = 0
                self.is_open = True
                self.last_failure_time = time.time()
                self._recovery_event.clear()
                logger.warning("熔断器半开测试失败，重新打开熔断")
                return

            self.failure_count += 1
            self.last_failure_time = time.time()

            if self.failure_count >= self.failure_threshold:
                self.is_open = True
                self._recovery_event.clear()
                logger.warning(
                    f"熔断器触发！连续失败{self.failure_count}次，暂停{self.timeout_duration}秒"
                )

    def can_proceed(self):
        """检查是否可以继续请求"""
        with self.lock:
            if not self.is_open:
                return True

            # 检查是否超过暂停时间 → 进入半开状态
            if time.time() - self.last_failure_time >= self.timeout_duration:
                logger.info("熔断器进入半开状态，尝试恢复")
                self._half_open = True
                self._half_open_successes = 0
                self.is_open = False
                return True

            return False

    def wait_if_open(self, max_wait: float | None = None) -> bool:
        """如果熔断器打开，等待直到恢复（Event 驱动，避免 busy-wait）

        Args:
            max_wait: 最长等待秒数（None 表示一直等到恢复）

        Returns:
            True 表示已恢复可继续；False 表示达到最长等待仍未恢复
        """
        # 快速路径：未熔断直接返回
        if self.can_proceed():
            return True

        # 计算剩余超时时间，用 Event.wait() 替代 sleep 轮询
        start = time.time()
        while not self.can_proceed():
            if max_wait is not None:
                remaining = max_wait - (time.time() - start)
                if remaining <= 0:
                    return False
                wait_time = min(remaining, self.timeout_duration)
            else:
                wait_time = self.timeout_duration
            # Event.wait() 在恢复时被 set() 唤醒，或超时后重新检查
            self._recovery_event.wait(timeout=wait_time)
        return True


class AdaptiveRateLimiter:
    """自适应速率调整器：根据响应时间动态调整QPS"""

    def __init__(self, initial_qps=2.5, min_qps=1.0, max_qps=3.0):
        """
        初始化自适应速率调整器

        Args:
            initial_qps: 初始QPS
            min_qps: 最小QPS
            max_qps: 最大QPS
        """
        self.current_qps = initial_qps
        self.min_qps = min_qps
        self.max_qps = max_qps
        self.response_times = deque(maxlen=20)  # 保留最近20次请求的响应时间
        self.lock = threading.Lock()

    def record_response_time(self, response_time):
        """记录响应时间"""
        with self.lock:
            self.response_times.append(response_time)
            self._adjust_qps()

    def _adjust_qps(self):
        """根据平均响应时间调整QPS"""
        if len(self.response_times) < 5:
            return

        avg_response_time = sum(self.response_times) / len(self.response_times)

        # 响应时间过长，降低QPS
        if avg_response_time > 2.0:
            self.current_qps = max(self.min_qps, self.current_qps - 0.2)
            logger.debug(f"响应时间过长({avg_response_time:.2f}s)，降低QPS至{self.current_qps}")

        # 响应时间正常，可以尝试提升QPS
        elif avg_response_time < 0.5 and self.current_qps < self.max_qps:
            self.current_qps = min(self.max_qps, self.current_qps + 0.1)
            logger.debug(f"响应时间良好({avg_response_time:.2f}s)，提升QPS至{self.current_qps}")

    def get_current_qps(self):
        """获取当前QPS"""
        with self.lock:
            return self.current_qps


class _AcquireContext:
    """独立的信号量上下文：每次 acquire() 返回独立实例，避免多线程共享标志位竞态"""

    __slots__ = ('_limiter', 'released')

    def __init__(self, limiter: 'RateLimiter'):
        self._limiter = limiter
        self.released = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not self.released:
            self.released = True
            self._limiter.semaphore.release()

    def record_success(self, response_time):
        self._limiter.record_success(response_time)

    def record_failure(self):
        self._limiter.record_failure()


class RateLimiter:
    """综合速率限制器（三级控制）"""

    def __init__(self, config=None):
        """
        初始化速率限制器

        Args:
            config: 配置字典，包含:
                - qps: 每秒请求数 (default: 2.5)
                - max_workers: 最大并发数 (default: 3)
                - min_interval: 最小请求间隔秒数 (default: 0.5)
                - random_delay: 随机延迟范围 (default: [0.1, 0.3])
                - circuit_breaker_enabled: 是否启用熔断器 (default: True)
                - failure_threshold: 熔断阈值 (default: 5)
                - timeout_duration: 熔断暂停时间 (default: 300)
                - adaptive: 是否启用自适应速率 (default: True)
        """
        config = config or {}

        # 级别1: 全局QPS限制（令牌桶）
        self.qps = config.get('qps', 2.5)
        self.token_bucket = TokenBucket(rate=self.qps)

        # 级别2: 并发控制（信号量）
        self.max_workers = config.get('max_workers', 3)
        self.semaphore = threading.Semaphore(self.max_workers)

        # 级别3: 请求间隔和随机延迟
        self.min_interval = config.get('min_interval', 0.5)
        self.random_delay = config.get('random_delay', [0.1, 0.3])
        self.last_request_time = 0
        self.interval_lock = threading.Lock()

        # 熔断器
        # 支持两种配置键：circuit_breaker_enabled 与 enabled（向后兼容）
        self.circuit_breaker_enabled = (
            config.get('circuit_breaker_enabled', config.get('enabled', True))
        )
        if self.circuit_breaker_enabled:
            self.circuit_breaker = CircuitBreaker(
                failure_threshold=config.get('failure_threshold', 5),
                timeout_duration=config.get('timeout_duration', 300)
            )
            # 可选：限制每次等待的最大时长，避免长时间卡死
            self.circuit_breaker_max_wait = config.get('circuit_breaker_max_wait')

        # 自适应速率调整（已禁用以支持高速采集）
        self.adaptive_enabled = config.get('adaptive', False)  # 默认禁用
        if self.adaptive_enabled:
            self.adaptive_limiter = AdaptiveRateLimiter(
                initial_qps=self.qps,
                min_qps=max(1.0, self.qps * 0.5),  # 最小为配置值的50%
                max_qps=self.qps  # 使用配置的QPS值
            )

        logger.info(f"速率限制器初始化: QPS={self.qps}, 并发={self.max_workers}, 间隔={self.min_interval}s")

    def acquire(self):
        """
        获取请求许可（阻塞）

        Returns:
            上下文管理器，自动释放并发槽
        """
        # 检查熔断器
        if self.circuit_breaker_enabled:
            if hasattr(self, 'circuit_breaker_max_wait') and self.circuit_breaker_max_wait is not None:
                if not self.circuit_breaker.wait_if_open(max_wait=self.circuit_breaker_max_wait):
                    # 超过最大等待，放弃本次获取，交由上层处理
                    raise RuntimeError("circuit breaker open - wait exceeded")
            else:
                self.circuit_breaker.wait_if_open()

        # 获取令牌（QPS限制）
        self.token_bucket.acquire()

        # 获取并发槽
        self.semaphore.acquire()

        # 信号量已获取后的所有操作用 try/except BaseException 保护
        # 覆盖 KeyboardInterrupt/SystemExit，防止信号量泄漏
        try:
            # 创建独立的上下文对象，避免多线程共享 self 上的标志位竞态
            ctx = _AcquireContext(self)

            # 仅在配置了较大间隔时才进行间隔控制
            # 对于高速采集（min_interval <= 0.01），跳过此控制以避免串行化瓶颈
            if self.min_interval > 0.01 or self.random_delay[1] > 0.01:
                with self.interval_lock:
                    now = time.time()
                    elapsed = now - self.last_request_time
                    if elapsed < self.min_interval:
                        time.sleep(self.min_interval - elapsed)

                    # 添加随机延迟（模拟人工操作）
                    if self.random_delay[1] > 0:
                        delay = random.uniform(*self.random_delay)
                        time.sleep(delay)

                    self.last_request_time = time.time()
        except BaseException:
            # 异常时释放信号量，防止泄漏（先释放再标记，确保一致性）
            # 使用 BaseException 覆盖 KeyboardInterrupt/SystemExit
            self.semaphore.release()
            if 'ctx' in locals():
                ctx.released = True
            raise

        return ctx

    def record_success(self, response_time):
        """记录成功请求"""
        if self.circuit_breaker_enabled:
            self.circuit_breaker.record_success()

        if self.adaptive_enabled:
            self.adaptive_limiter.record_response_time(response_time)
            # 更新令牌桶速率
            new_qps = self.adaptive_limiter.get_current_qps()
            if abs(new_qps - self.token_bucket.rate) > 0.1:
                self.token_bucket.rate = new_qps

    def record_failure(self):
        """记录失败请求"""
        if self.circuit_breaker_enabled:
            self.circuit_breaker.record_failure()

    def get_stats(self):
        """获取统计信息"""
        stats = {
            'qps': self.qps,
            'max_workers': self.max_workers,
            'min_interval': self.min_interval,
        }

        if self.adaptive_enabled:
            stats['current_qps'] = self.adaptive_limiter.get_current_qps()

        if self.circuit_breaker_enabled:
            stats['circuit_breaker_open'] = self.circuit_breaker.is_open
            stats['failure_count'] = self.circuit_breaker.failure_count

        return stats
