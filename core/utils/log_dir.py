"""
统一日志目录解析器

解决 Docker 容器 root 创建日志文件导致宿主用户无法写入的问题。
所有日志模块（logger.py、enhanced_logger.py、base_analyzer.py）统一通过此模块获取可写日志目录。

策略：
1. 优先使用 LOG_DIR_OVERRIDE 环境变量
2. 测试默认 logs/ 目录是否可写
3. 不可写时降级到 /tmp/bond-fund-logs/
4. 全进程只告警一次
5. safe_rotating_handler 处理已有文件不可写的情况
"""

import logging
import os
import sys
import tempfile
import threading
import re
from logging.handlers import RotatingFileHandler

_lock = threading.Lock()
_resolved_dir: str | None = None
_warned = False


def _looks_like_test_process() -> bool:
    """识别 unittest/pytest 或直接执行 tests/ 下单文件的场景。"""
    if os.environ.get('PYTEST_CURRENT_TEST'):
        return True

    argv = [str(item or '').strip() for item in sys.argv if str(item or '').strip()]
    lowered = [item.lower().replace('\\', '/') for item in argv]

    for item in lowered:
        if item.endswith('/pytest') or item.endswith('/py.test'):
            return True
        if 'pytest' in item or 'unittest' in item:
            return True
        if item.startswith('tests/') or '/tests/' in item:
            return True

    return False


def resolve_log_dir(default: str = 'logs') -> str:
    """
    解析可写的日志目录。进程内缓存结果，只解析一次。

    Args:
        default: 默认日志目录路径（相对或绝对）

    Returns:
        可写的日志目录绝对路径
    """
    global _resolved_dir, _warned

    # 快速路径：已解析过
    if _resolved_dir is not None:
        return _resolved_dir

    with _lock:
        # 双重检查
        if _resolved_dir is not None:
            return _resolved_dir

        # 1. 环境变量优先
        env_override = os.environ.get('LOG_DIR_OVERRIDE')
        if env_override:
            os.makedirs(env_override, exist_ok=True)
            _resolved_dir = os.path.abspath(env_override)
            return _resolved_dir

        # 1.5 测试进程兜底：直接执行 tests/ 下单文件时也要隔离到临时目录
        if _looks_like_test_process():
            test_fallback = os.path.join(tempfile.gettempdir(), 'bond-fund-test-logs')
            os.makedirs(test_fallback, exist_ok=True)
            _resolved_dir = os.path.abspath(test_fallback)
            os.environ.setdefault('LOG_DIR_OVERRIDE', _resolved_dir)
            return _resolved_dir

        # 2. 测试默认目录
        try:
            os.makedirs(default, exist_ok=True)
            # 实际写入测试（目录存在不代表可写）。
            # 使用唯一临时文件，避免固定 .write_test 在并发初始化或异常残留时误判目录不可写。
            fd, test_file = tempfile.mkstemp(prefix='.write_test_', dir=default)
            try:
                with os.fdopen(fd, 'w', encoding='utf-8') as f:
                    f.write('ok')
            finally:
                try:
                    os.remove(test_file)
                except FileNotFoundError:
                    pass
            _resolved_dir = os.path.abspath(default)
            return _resolved_dir
        except (PermissionError, OSError):
            pass

        # 3. 降级到临时目录
        fallback = os.path.join(tempfile.gettempdir(), 'bond-fund-logs')
        os.makedirs(fallback, exist_ok=True)
        _resolved_dir = fallback

        # 全进程只告警一次
        if not _warned:
            _warned = True
            print(
                f"[WARNING] 日志目录 '{os.path.abspath(default)}' 不可写，"
                f"已降级到 '{fallback}'。"
                f"如需修复，请执行: sudo chown -R $(whoami) {os.path.abspath(default)}",
                file=sys.stderr
            )
            # 同步设置环境变量，供后续子进程/延迟初始化的模块使用
            os.environ['LOG_DIR_OVERRIDE'] = fallback

        return _resolved_dir


def safe_rotating_handler(
    filepath: str,
    maxBytes: int = 50 * 1024 * 1024,
    backupCount: int = 10,
    encoding: str = 'utf-8',
) -> logging.Handler:
    """
    创建 RotatingFileHandler，若目标文件不可写则自动降级。

    降级策略：
    1. 尝试打开原始文件
    2. 失败则尝试同目录下加 _new 后缀的替代文件
    3. 仍失败则返回 NullHandler（静默丢弃日志，不阻塞进程启动）
    """
    for attempt_path in [filepath, _alt_path(filepath)]:
        try:
            return RotatingFileHandler(
                attempt_path,
                maxBytes=maxBytes,
                backupCount=backupCount,
                encoding=encoding,
            )
        except (PermissionError, OSError):
            continue

    # 所有路径均失败，返回空处理器
    print(
        f"[WARNING] 日志文件 '{filepath}' 不可写，该日志通道已禁用。"
        f"如需修复: sudo chown -R $(whoami) {os.path.dirname(filepath)}",
        file=sys.stderr,
    )
    return logging.NullHandler()


def _alt_path(filepath: str) -> str:
    """生成替代文件路径：base_new.ext"""
    base, ext = os.path.splitext(filepath)
    return f"{base}_new{ext}"


def reset():
    """重置缓存（仅用于测试）"""
    global _resolved_dir, _warned
    with _lock:
        _resolved_dir = None
        _warned = False


def configure_cli_log_dir(scope: str, prefix: str = 'bond-fund-cli-logs') -> str:
    """
    为本地一次性 CLI/审计脚本分配独立日志目录，避免污染运行态 logs/。

    说明：
    - 若外部已显式设置 LOG_DIR_OVERRIDE，则尊重现有配置；
    - 目录落在系统临时目录下，按 scope 分桶，便于后续审计归因。
    """
    global _resolved_dir

    env_override = os.environ.get('LOG_DIR_OVERRIDE')
    if env_override:
        resolved = os.path.abspath(env_override)
        os.makedirs(resolved, exist_ok=True)
        with _lock:
            _resolved_dir = resolved
        return resolved

    normalized_scope = re.sub(r'[^a-zA-Z0-9._-]+', '-', str(scope or 'default')).strip('-') or 'default'
    cli_log_dir = os.path.join(tempfile.gettempdir(), prefix, normalized_scope)
    resolved = os.path.abspath(cli_log_dir)
    os.makedirs(resolved, exist_ok=True)
    os.environ['LOG_DIR_OVERRIDE'] = resolved
    with _lock:
        _resolved_dir = resolved
    return resolved
