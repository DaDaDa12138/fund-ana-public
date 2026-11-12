"""
增强版日志系统
提供结构化日志、性能监控、上下文跟踪等功能
"""

import logging
import os
import json
import time
import sys
import threading
import functools
import traceback
from datetime import datetime
from typing import Dict, Any, Optional, Callable

from contextlib import contextmanager

# colorlog是可选依赖
try:
    import colorlog
    HAS_COLORLOG = True
except ImportError:
    HAS_COLORLOG = False


class StructuredLogger:
    """
    结构化日志记录器
    支持上下文信息、性能指标、自动追踪
    """

    # 线程本地存储，用于存储请求上下文
    _context = threading.local()

    def __init__(self, name: str, log_dir: str = 'logs'):
        self.name = name
        # v1.9.64: 统一日志目录解析（自动处理权限降级）
        from utils.log_dir import resolve_log_dir
        self.log_dir = resolve_log_dir(log_dir)
        self.logger = logging.getLogger(name)
        # 使用自有 handler 输出，避免向 root 传播造成重复控制台/文件日志与额外 I/O
        self.logger.propagate = False
        self._setup_handlers()

    def _setup_handlers(self):
        """设置日志处理器"""
        if self.logger.handlers:
            return

        os.makedirs(self.log_dir, exist_ok=True)
        today = datetime.now().strftime('%Y-%m-%d')

        # 设置日志级别
        self.logger.setLevel(logging.DEBUG)

        # 控制台处理器
        console_handler = self._create_console_handler()
        self.logger.addHandler(console_handler)

        # 主日志文件
        main_handler = self._create_file_handler(
            f'{self.log_dir}/collector_{today}.log',
            logging.INFO
        )
        self.logger.addHandler(main_handler)

        # 错误日志文件
        error_handler = self._create_file_handler(
            f'{self.log_dir}/error_{today}.log',
            logging.ERROR
        )
        self.logger.addHandler(error_handler)

        # DEBUG日志文件（详细日志）
        debug_handler = self._create_file_handler(
            f'{self.log_dir}/debug_{today}.log',
            logging.DEBUG
        )
        self.logger.addHandler(debug_handler)

    def _create_console_handler(self):
        """创建控制台处理器"""
        if HAS_COLORLOG:
            handler = colorlog.StreamHandler(stream=sys.stdout)
            formatter = colorlog.ColoredFormatter(
                '%(log_color)s%(asctime)s [%(levelname)s] %(name)s: %(message)s%(reset)s',
                datefmt='%H:%M:%S',
                log_colors={
                    'DEBUG': 'cyan',
                    'INFO': 'green',
                    'WARNING': 'yellow',
                    'ERROR': 'red',
                    'CRITICAL': 'red,bg_white',
                }
            )
        else:
            handler = logging.StreamHandler(stream=sys.stdout)
            formatter = logging.Formatter(
                '%(asctime)s [%(levelname)s] %(name)s: %(message)s',
                datefmt='%H:%M:%S'
            )
        handler.setLevel(logging.INFO)
        handler.setFormatter(formatter)
        return handler

    def _create_file_handler(self, filepath: str, level: int):
        """创建文件处理器（权限安全）"""
        from utils.log_dir import safe_rotating_handler
        handler = safe_rotating_handler(filepath, maxBytes=50 * 1024 * 1024, backupCount=10)
        handler.setLevel(level)
        formatter = logging.Formatter(
            '%(asctime)s [%(levelname)s] %(name)s [%(thread)d] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        handler.setFormatter(formatter)
        return handler

    @classmethod
    def set_context(cls, **kwargs):
        """设置当前线程的上下文信息"""
        if not hasattr(cls._context, 'data'):
            cls._context.data = {}
        cls._context.data.update(kwargs)

    @classmethod
    def get_context(cls) -> Dict[str, Any]:
        """获取当前线程的上下文信息"""
        if not hasattr(cls._context, 'data'):
            cls._context.data = {}
        return cls._context.data.copy()

    @classmethod
    def clear_context(cls):
        """清除当前线程的上下文"""
        cls._context.data = {}

    def _format_message(self, msg: str, extra: Dict = None) -> str:
        """格式化消息，添加上下文信息"""
        context = self.get_context()
        if extra:
            context.update(extra)

        if context:
            context_str = ' '.join(f'{k}={v}' for k, v in context.items())
            return f"{msg} | {context_str}"
        return msg

    def debug(self, msg: str, **extra):
        self.logger.debug(self._format_message(msg, extra))

    def info(self, msg: str, **extra):
        self.logger.info(self._format_message(msg, extra))

    def warning(self, msg: str, **extra):
        self.logger.warning(self._format_message(msg, extra))

    def error(self, msg: str, exc_info: bool = False, **extra):
        if exc_info:
            extra['traceback'] = traceback.format_exc()
        self.logger.error(self._format_message(msg, extra))

    def critical(self, msg: str, exc_info: bool = False, **extra):
        if exc_info:
            extra['traceback'] = traceback.format_exc()
        self.logger.critical(self._format_message(msg, extra))


class PerformanceLogger:
    """
    性能日志记录器
    记录操作耗时、吞吐量等指标
    """

    def __init__(self, log_dir: str = 'logs'):
        from utils.log_dir import resolve_log_dir
        self.log_dir = resolve_log_dir(log_dir)
        self.logger = logging.getLogger('performance')
        self.logger.propagate = False
        self._setup_handler()

        # 性能统计
        self._stats = {}
        self._lock = threading.Lock()

    def _setup_handler(self):
        """设置性能日志处理器"""
        if self.logger.handlers:
            return

        os.makedirs(self.log_dir, exist_ok=True)
        today = datetime.now().strftime('%Y-%m-%d')

        self.logger.setLevel(logging.DEBUG)

        from utils.log_dir import safe_rotating_handler
        handler = safe_rotating_handler(
            f'{self.log_dir}/performance_{today}.log',
            maxBytes=50 * 1024 * 1024,
            backupCount=10,
        )
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
            '%(asctime)s [PERF] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S.%f'
        )
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

    @contextmanager
    def measure(self, operation: str, **context):
        """
        测量操作耗时的上下文管理器

        使用示例:
            with perf_logger.measure('database_query', table='funds'):
                result = db.query(...)
        """
        start_time = time.time()
        start_memory = self._get_memory_usage()

        try:
            yield
            success = True
            error = None
        except Exception as e:
            success = False
            error = str(e)
            raise
        finally:
            elapsed = time.time() - start_time
            end_memory = self._get_memory_usage()
            memory_delta = end_memory - start_memory if start_memory and end_memory else None

            self._record_metric(operation, elapsed, success, error, context, memory_delta)

    def _get_memory_usage(self) -> Optional[float]:
        """获取当前内存使用量（MB）"""
        try:
            import psutil
            process = psutil.Process()
            return process.memory_info().rss / 1024 / 1024
        except (ImportError, OSError, AttributeError):
            return None  # psutil不可用或无法获取内存信息

    def _record_metric(self, operation: str, elapsed: float, success: bool,
                       error: Optional[str], context: Dict, memory_delta: Optional[float]):
        """记录性能指标"""
        metric = {
            'operation': operation,
            'elapsed_ms': round(elapsed * 1000, 2),
            'success': success,
            'timestamp': datetime.now().isoformat(),
            'thread_id': threading.current_thread().ident,
            **context
        }

        if error:
            metric['error'] = error
        if memory_delta is not None:
            metric['memory_delta_mb'] = round(memory_delta, 2)

        # 记录到日志
        self.logger.info(json.dumps(metric, ensure_ascii=False))

        # 更新统计
        with self._lock:
            if operation not in self._stats:
                self._stats[operation] = {
                    'count': 0,
                    'total_time': 0,
                    'min_time': float('inf'),
                    'max_time': 0,
                    'errors': 0
                }

            stats = self._stats[operation]
            stats['count'] += 1
            stats['total_time'] += elapsed
            stats['min_time'] = min(stats['min_time'], elapsed)
            stats['max_time'] = max(stats['max_time'], elapsed)
            if not success:
                stats['errors'] += 1

    def get_stats(self, operation: str = None) -> Dict[str, Any]:
        """获取性能统计"""
        with self._lock:
            if operation:
                stats = self._stats.get(operation, {})
                if stats:
                    return {
                        'count': stats['count'],
                        'avg_time_ms': round((stats['total_time'] / stats['count']) * 1000, 2),
                        'min_time_ms': round(stats['min_time'] * 1000, 2),
                        'max_time_ms': round(stats['max_time'] * 1000, 2),
                        'error_rate': round(stats['errors'] / stats['count'] * 100, 2) if stats['count'] > 0 else 0
                    }
                return {}
            return {k: self.get_stats(k) for k in self._stats.keys()}

    def log_metric(self, metric_name: str, value: float, **tags):
        """
        记录单个性能指标

        使用示例:
            perf_logger.log_metric("task.duration_ms", 1234.5, task_name="daily_nav_update")
        """
        metric = {
            'metric': metric_name,
            'value': value,
            'timestamp': datetime.now().isoformat(),
            'thread_id': threading.current_thread().ident,
            **tags
        }
        self.logger.info(json.dumps(metric, ensure_ascii=False))

        # 更新统计
        with self._lock:
            if metric_name not in self._stats:
                self._stats[metric_name] = {
                    'count': 0,
                    'total_time': 0,
                    'min_time': float('inf'),
                    'max_time': 0,
                    'errors': 0
                }
            stats = self._stats[metric_name]
            stats['count'] += 1
            stats['total_time'] += value / 1000 if 'ms' in metric_name else value


class DatabaseLogger:
    """
    数据库操作日志记录器
    记录SQL执行、连接状态、事务等
    """

    def __init__(self, log_dir: str = 'logs'):
        from utils.log_dir import resolve_log_dir
        self.log_dir = resolve_log_dir(log_dir)
        self.logger = logging.getLogger('database')
        self.logger.propagate = False
        self._setup_handler()

    def _setup_handler(self):
        """设置数据库日志处理器"""
        if self.logger.handlers:
            return

        os.makedirs(self.log_dir, exist_ok=True)
        today = datetime.now().strftime('%Y-%m-%d')

        self.logger.setLevel(logging.DEBUG)

        from utils.log_dir import safe_rotating_handler
        handler = safe_rotating_handler(
            f'{self.log_dir}/database_{today}.log',
            maxBytes=50 * 1024 * 1024,
            backupCount=10,
        )
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
            '%(asctime)s [DB] [%(levelname)s] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

    def log_connection(self, action: str, pool_size: int = None,
                       active_connections: int = None, **extra):
        """记录连接池状态"""
        msg = f"Connection {action}"
        details = []
        if pool_size is not None:
            details.append(f"pool_size={pool_size}")
        if active_connections is not None:
            details.append(f"active={active_connections}")
        for k, v in extra.items():
            details.append(f"{k}={v}")
        if details:
            msg += f" | {' '.join(details)}"
        self.logger.info(msg)

    def log_query(self, operation: str, table: str, rows_affected: int = None,
                  elapsed_ms: float = None, **extra):
        """记录查询操作"""
        msg = f"Query {operation} on {table}"
        details = []
        if rows_affected is not None:
            details.append(f"rows={rows_affected}")
        if elapsed_ms is not None:
            details.append(f"elapsed={elapsed_ms:.2f}ms")
        for k, v in extra.items():
            details.append(f"{k}={v}")
        if details:
            msg += f" | {' '.join(details)}"
        self.logger.debug(msg)

    def log_transaction(self, action: str, session_id: str = None, **extra):
        """记录事务操作"""
        msg = f"Transaction {action}"
        details = []
        if session_id:
            details.append(f"session={session_id}")
        for k, v in extra.items():
            details.append(f"{k}={v}")
        if details:
            msg += f" | {' '.join(details)}"
        self.logger.info(msg)

    def log_error(self, operation: str, error: str, **extra):
        """记录数据库错误"""
        msg = f"Error in {operation}: {error}"
        for k, v in extra.items():
            msg += f" | {k}={v}"
        msg += f" | traceback={traceback.format_exc()}"
        self.logger.error(msg)


class APILogger:
    """
    API请求日志记录器
    记录请求/响应详情、延迟、错误等
    """

    def __init__(self, log_dir: str = 'logs'):
        from utils.log_dir import resolve_log_dir
        self.log_dir = resolve_log_dir(log_dir)
        self.logger = logging.getLogger('api')
        self.logger.propagate = False
        self._setup_handler()

        # 请求统计
        self._stats = {
            'total': 0,
            'success': 0,
            'failed': 0,
            'by_endpoint': {}
        }
        self._lock = threading.Lock()

    def _setup_handler(self):
        """设置API日志处理器"""
        if self.logger.handlers:
            return

        os.makedirs(self.log_dir, exist_ok=True)
        today = datetime.now().strftime('%Y-%m-%d')

        self.logger.setLevel(logging.DEBUG)

        from utils.log_dir import safe_rotating_handler
        handler = safe_rotating_handler(
            f'{self.log_dir}/api_{today}.log',
            maxBytes=50 * 1024 * 1024,
            backupCount=10,
        )
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
            '%(asctime)s [API] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

    def log_request(self, method: str, endpoint: str, params: Dict = None,
                    request_id: str = None):
        """记录请求开始"""
        msg = f">>> {method} {endpoint}"
        if request_id:
            msg += f" [req_id={request_id}]"
        if params:
            # 只记录关键参数，避免日志过长
            key_params = {k: v for k, v in params.items()
                         if k in ['fcode', 'page', 'limit', 'type']}
            if key_params:
                msg += f" params={key_params}"
        self.logger.debug(msg)

    def log_response(self, method: str, endpoint: str, status_code: int,
                     elapsed_ms: float, response_size: int = None,
                     request_id: str = None, success: bool = True):
        """记录响应"""
        level = logging.DEBUG if success else logging.WARNING

        msg = f"<<< {method} {endpoint}"
        if request_id:
            msg += f" [req_id={request_id}]"
        msg += f" status={status_code} elapsed={elapsed_ms:.2f}ms"
        if response_size:
            msg += f" size={response_size}B"

        self.logger.log(level, msg)

        # 更新统计
        with self._lock:
            self._stats['total'] += 1
            if success:
                self._stats['success'] += 1
            else:
                self._stats['failed'] += 1

            if endpoint not in self._stats['by_endpoint']:
                self._stats['by_endpoint'][endpoint] = {
                    'count': 0, 'total_time': 0, 'errors': 0
                }
            ep_stats = self._stats['by_endpoint'][endpoint]
            ep_stats['count'] += 1
            ep_stats['total_time'] += elapsed_ms
            if not success:
                ep_stats['errors'] += 1

    def log_error(self, method: str, endpoint: str, error: str,
                  request_id: str = None, retry_count: int = None):
        """记录请求错误"""
        msg = f"!!! {method} {endpoint} ERROR: {error}"
        if request_id:
            msg += f" [req_id={request_id}]"
        if retry_count is not None:
            msg += f" retry={retry_count}"
        self.logger.error(msg)

    def log_rate_limit(self, qps: float, queue_size: int = None,
                       wait_time: float = None):
        """记录速率限制状态"""
        msg = f"Rate limit: current_qps={qps:.2f}"
        if queue_size is not None:
            msg += f" queue={queue_size}"
        if wait_time is not None:
            msg += f" wait={wait_time:.3f}s"
        self.logger.debug(msg)

    def log_circuit_breaker(self, state: str, failure_count: int = None,
                            recovery_time: float = None):
        """记录熔断器状态"""
        msg = f"Circuit breaker: state={state}"
        if failure_count is not None:
            msg += f" failures={failure_count}"
        if recovery_time is not None:
            msg += f" recovery_in={recovery_time:.1f}s"
        level = logging.WARNING if state == 'OPEN' else logging.INFO
        self.logger.log(level, msg)

    def get_stats(self) -> Dict[str, Any]:
        """获取API统计"""
        with self._lock:
            stats = {
                'total': self._stats['total'],
                'success': self._stats['success'],
                'failed': self._stats['failed'],
                'success_rate': round(
                    self._stats['success'] / self._stats['total'] * 100, 2
                ) if self._stats['total'] > 0 else 0
            }

            # 按端点统计
            stats['by_endpoint'] = {}
            for endpoint, ep_stats in self._stats['by_endpoint'].items():
                stats['by_endpoint'][endpoint] = {
                    'count': ep_stats['count'],
                    'avg_time_ms': round(
                        ep_stats['total_time'] / ep_stats['count'], 2
                    ) if ep_stats['count'] > 0 else 0,
                    'error_rate': round(
                        ep_stats['errors'] / ep_stats['count'] * 100, 2
                    ) if ep_stats['count'] > 0 else 0
                }
            return stats


class TaskLogger:
    """
    任务生命周期日志记录器
    记录任务启动、进度、完成、错误等
    兼容调度器使用的字符串task_id格式
    """

    def __init__(self, log_dir: str = 'logs'):
        from utils.log_dir import resolve_log_dir
        self.log_dir = resolve_log_dir(log_dir)
        self.logger = logging.getLogger('task')
        self.logger.propagate = False
        self._setup_handler()

        # 活跃任务跟踪
        self._active_tasks = {}
        self._lock = threading.Lock()

    def _setup_handler(self):
        """设置任务日志处理器"""
        if self.logger.handlers:
            return

        os.makedirs(self.log_dir, exist_ok=True)
        today = datetime.now().strftime('%Y-%m-%d')

        self.logger.setLevel(logging.DEBUG)

        from utils.log_dir import safe_rotating_handler
        handler = safe_rotating_handler(
            f'{self.log_dir}/task_{today}.log',
            maxBytes=50 * 1024 * 1024,
            backupCount=10,
        )
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
            '%(asctime)s [TASK] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

    def log_start(self, task_name_or_id, task_name: str = None, **params):
        """
        记录任务开始

        支持两种调用方式:
        - log_start(task_id: int, task_name: str, **params) - 旧版兼容
        - log_start(task_name: str, **params) - 新版调度器使用
        """
        # 处理两种调用方式
        if task_name is None:
            # 新版调用: log_start("任务名称", **params)
            task_id = str(task_name_or_id)
            actual_task_name = str(task_name_or_id)
        else:
            # 旧版调用: log_start(task_id, "任务名称", **params)
            task_id = str(task_name_or_id)
            actual_task_name = task_name

        with self._lock:
            self._active_tasks[task_id] = {
                'name': actual_task_name,
                'start_time': time.time(),
                'progress': 0
            }

        msg = f"▶ START [{task_id}] {actual_task_name}"
        if params:
            param_str = ' '.join(f'{k}={v}' for k, v in params.items())
            msg += f" | {param_str}"
        self.logger.info(msg)

    def log_progress(self, task_name_or_id, current: int, total: int,
                     message: str = None, **extra):
        """
        记录任务进度

        支持两种调用方式:
        - log_progress(task_id: int, current, total, ...) - 旧版
        - log_progress(task_name: str, current, total, ...) - 新版
        """
        task_id = str(task_name_or_id)
        progress = round(current / total * 100, 1) if total > 0 else 0

        with self._lock:
            if task_id in self._active_tasks:
                self._active_tasks[task_id]['progress'] = progress

        msg = f"⏳ PROGRESS [{task_id}] {progress}% ({current}/{total})"
        if message:
            msg += f" - {message}"
        for k, v in extra.items():
            msg += f" | {k}={v}"
        self.logger.debug(msg)

    def log_complete(self, task_name_or_id, duration_or_task_name = None,
                     records_processed: int = 0, records_inserted: int = 0,
                     records_updated: int = 0, records_failed: int = 0,
                     duration: float = None, success: bool = True, **extra):
        """
        记录任务完成

        支持多种调用方式:
        - log_complete(task_id, task_name, records_processed, ...) - 旧版
        - log_complete(task_name, duration_ms, success=True, **extra) - 新版调度器
        """
        task_id = str(task_name_or_id)

        # 判断调用方式
        if isinstance(duration_or_task_name, str):
            # 旧版调用: log_complete(task_id, task_name, ...)
            actual_task_name = duration_or_task_name
        else:
            # 新版调用: log_complete(task_name, duration_ms, ...)
            actual_task_name = task_id
            if duration_or_task_name is not None:
                duration = duration_or_task_name / 1000.0  # 转换ms到s

        with self._lock:
            task_info = self._active_tasks.pop(task_id, {})

        if duration is None and task_info:
            duration = time.time() - task_info.get('start_time', time.time())
        elif duration is None:
            duration = 0

        status_icon = "✅" if success else "❌"
        msg = f"{status_icon} COMPLETE [{task_id}] {actual_task_name}"
        msg += f" | duration={duration:.1f}s"
        msg += f" | success={success}"
        if records_processed > 0:
            msg += f" | processed={records_processed}"
        if records_inserted > 0:
            msg += f" inserted={records_inserted}"
        if records_updated > 0:
            msg += f" updated={records_updated}"
        if records_failed > 0:
            msg += f" failed={records_failed}"
        for k, v in extra.items():
            msg += f" | {k}={v}"
        self.logger.info(msg)

    def log_error(self, task_name_or_id, error_or_task_name: str,
                  error: str = None, recoverable: bool = False, **extra):
        """
        记录任务错误

        支持两种调用方式:
        - log_error(task_id, task_name, error, ...) - 旧版
        - log_error(task_name, error, ...) - 新版调度器
        """
        task_id = str(task_name_or_id)

        if error is None:
            # 新版调用: log_error(task_name, error, ...)
            actual_task_name = task_id
            actual_error = error_or_task_name
        else:
            # 旧版调用: log_error(task_id, task_name, error, ...)
            actual_task_name = error_or_task_name
            actual_error = error

        with self._lock:
            task_info = self._active_tasks.get(task_id, {})
            progress = task_info.get('progress', 0)

        msg = f"❌ ERROR [{task_id}] {actual_task_name}"
        msg += f" | progress={progress}%"
        msg += f" | error={actual_error}"
        msg += f" | recoverable={recoverable}"
        for k, v in extra.items():
            msg += f" | {k}={v}"

        # 只在有异常上下文时添加traceback
        current_traceback = traceback.format_exc()
        if current_traceback and 'NoneType' not in current_traceback:
            msg += f"\n{current_traceback}"
        self.logger.error(msg)

    def log_warning(self, task_name_or_id, message: str, **extra):
        """记录任务警告"""
        task_id = str(task_name_or_id)
        msg = f"⚠️ WARNING [{task_id}] {message}"
        for k, v in extra.items():
            msg += f" | {k}={v}"
        self.logger.warning(msg)

    def get_active_tasks(self) -> Dict[str, Dict]:
        """获取活跃任务列表"""
        with self._lock:
            return {
                task_id: {
                    **info,
                    'running_time': time.time() - info['start_time']
                }
                for task_id, info in self._active_tasks.items()
            }


# 全局日志实例
_loggers = {}
_lock = threading.Lock()


def get_structured_logger(name: str) -> StructuredLogger:
    """获取结构化日志记录器"""
    with _lock:
        if name not in _loggers:
            _loggers[name] = StructuredLogger(name)
        return _loggers[name]


def get_performance_logger() -> PerformanceLogger:
    """获取性能日志记录器"""
    with _lock:
        if 'performance' not in _loggers:
            _loggers['performance'] = PerformanceLogger()
        return _loggers['performance']


def get_database_logger() -> DatabaseLogger:
    """获取数据库日志记录器"""
    with _lock:
        if 'database' not in _loggers:
            _loggers['database'] = DatabaseLogger()
        return _loggers['database']


def get_api_logger() -> APILogger:
    """获取API日志记录器"""
    with _lock:
        if 'api' not in _loggers:
            _loggers['api'] = APILogger()
        return _loggers['api']


def get_task_logger() -> TaskLogger:
    """获取任务日志记录器"""
    with _lock:
        if 'task' not in _loggers:
            _loggers['task'] = TaskLogger()
        return _loggers['task']


def log_function_call(logger_name: str = None):
    """
    函数调用装饰器，自动记录函数执行

    使用示例:
        @log_function_call('database')
        def query_funds():
            ...
    """
    def decorator(func: Callable):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            name = logger_name or func.__module__
            logger = get_structured_logger(name)
            perf_logger = get_performance_logger()

            func_name = f"{func.__module__}.{func.__name__}"

            logger.debug(f"Calling {func_name}",
                        args_count=len(args), kwargs_keys=list(kwargs.keys()))

            with perf_logger.measure(func_name):
                result = func(*args, **kwargs)

            return result
        return wrapper
    return decorator
