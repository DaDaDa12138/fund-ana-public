"""
日志系统：彩色控制台输出 + 文件日志
"""

import logging
import os
import sys
import threading
from datetime import datetime

# colorlog是可选依赖，如果不可用则使用标准日志
try:
    import colorlog
    HAS_COLORLOG = True
except ImportError:
    HAS_COLORLOG = False


_setup_lock = threading.RLock()


def _handler_points_to_file(handler: logging.Handler, target_file: str) -> bool:
    """判断 handler 是否绑定到目标日志文件。"""
    base_filename = getattr(handler, 'baseFilename', None)
    if not base_filename:
        return False
    try:
        return os.path.abspath(base_filename) == os.path.abspath(target_file)
    except Exception:
        return False


def _ensure_api_logger_handler(log_dir: str, retention_days: int, log_format: str, level: str):
    """确保 api logger 仅绑定一个当日 API 文件 handler，避免泄漏未挂载句柄。"""
    from utils.log_dir import safe_rotating_handler

    today = datetime.now().strftime('%Y-%m-%d')
    api_log_file = os.path.join(log_dir, f'api_{today}.log')
    api_logger = logging.getLogger('api')
    api_level = getattr(logging, level.upper(), logging.INFO)

    for handler in api_logger.handlers:
        if _handler_points_to_file(handler, api_log_file):
            handler.setLevel(api_level)
            api_logger.setLevel(api_level)
            return

    api_handler = safe_rotating_handler(
        api_log_file,
        maxBytes=10 * 1024 * 1024,
        backupCount=retention_days,
    )
    api_handler.setLevel(api_level)
    api_handler.setFormatter(logging.Formatter(log_format, datefmt='%Y-%m-%d %H:%M:%S'))
    api_logger.setLevel(api_level)
    api_logger.addHandler(api_handler)


def setup_logger(name='bond-fund-tracker', config=None):
    """Initialize runtime handlers once, including concurrent client creation."""
    with _setup_lock:
        return _setup_logger(name, config)


def _setup_logger(name, config):
    """
    配置日志系统

    Args:
        name: 日志记录器名称
        config: 配置字典

    Returns:
        配置好的logger实例
    """
    config = config or {}
    log_config = config.get('logging', {})

    # 获取配置
    level = log_config.get('level', 'INFO')
    log_format = log_config.get('format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    retention_days = log_config.get('retention_days', 30)
    console_enabled = bool(log_config.get('console_enabled', True))
    console_stream_name = str(log_config.get('console_stream', 'stdout')).strip().lower()
    emit_init_message = bool(log_config.get('emit_init_message', True))
    console_stream = sys.stderr if console_stream_name == 'stderr' else sys.stdout

    # v1.9.64: 统一日志目录解析（自动处理权限降级）
    from utils.log_dir import resolve_log_dir
    log_dir = resolve_log_dir('logs')

    # 创建logger
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper()))

    # 避免重复添加handler
    if logger.handlers:
        # 即使当前 logger 已初始化，也确保 api logger 拥有正确文件 handler
        _ensure_api_logger_handler(log_dir, retention_days, log_format, level)
        return logger

    # 1. 彩色控制台输出（如果colorlog可用）或标准控制台输出
    if console_enabled:
        if HAS_COLORLOG:
            console_handler = colorlog.StreamHandler(stream=console_stream)
            console_handler.setLevel(logging.INFO)

            console_format = colorlog.ColoredFormatter(
                '%(log_color)s%(asctime)s - %(name)s - %(levelname)s - %(message)s%(reset)s',
                datefmt='%Y-%m-%d %H:%M:%S',
                log_colors={
                    'DEBUG': 'cyan',
                    'INFO': 'green',
                    'WARNING': 'yellow',
                    'ERROR': 'red',
                    'CRITICAL': 'red,bg_white',
                }
            )
            console_handler.setFormatter(console_format)
        else:
            # 使用标准StreamHandler（无颜色）
            console_handler = logging.StreamHandler(stream=console_stream)
            console_handler.setLevel(logging.INFO)

            console_format = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            console_handler.setFormatter(console_format)

        logger.addHandler(console_handler)

    # 2. 主日志文件（INFO及以上）
    # 注意：文件名含当天日期，跨天后新日志写入新文件
    from utils.log_dir import safe_rotating_handler
    today = datetime.now().strftime('%Y-%m-%d')
    main_log_file = os.path.join(log_dir, f'collector_{today}.log')

    main_handler = safe_rotating_handler(
        main_log_file,
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=retention_days,
    )
    main_handler.setLevel(logging.INFO)
    main_formatter = logging.Formatter(log_format, datefmt='%Y-%m-%d %H:%M:%S')
    main_handler.setFormatter(main_formatter)
    logger.addHandler(main_handler)

    # 3. 错误日志文件（ERROR及以上）
    error_log_file = os.path.join(log_dir, f'error_{today}.log')

    error_handler = safe_rotating_handler(
        error_log_file,
        maxBytes=10 * 1024 * 1024,
        backupCount=retention_days,
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(main_formatter)
    logger.addHandler(error_handler)

    # 4. API日志文件（单独记录API请求）
    _ensure_api_logger_handler(log_dir, retention_days, log_format, level)

    if emit_init_message:
        logger.info(f"日志系统初始化完成: {log_dir}")

    return logger


def get_logger(name=None):
    """
    获取logger实例

    Args:
        name: logger名称，默认使用模块名

    Returns:
        logger实例
    """
    if name is None:
        # 获取调用者的模块名
        import inspect
        frame = inspect.currentframe().f_back
        name = frame.f_globals['__name__']

    return logging.getLogger(name)
