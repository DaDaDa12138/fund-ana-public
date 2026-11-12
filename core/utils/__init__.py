"""
工具模块。

保持包初始化轻量，避免仅导入 ``utils``/``utils.<submodule>`` 时触发
数据库、增强日志等高开销副作用。
"""

from __future__ import annotations

from importlib import import_module

_EXPORT_MAP = {
    # Logger
    'setup_logger': ('utils.logger', 'setup_logger'),
    'get_logger': ('utils.logger', 'get_logger'),
    'resolve_log_dir': ('utils.log_dir', 'resolve_log_dir'),
    'safe_rotating_handler': ('utils.log_dir', 'safe_rotating_handler'),

    # Progress
    'ProgressMonitor': ('utils.progress', 'ProgressMonitor'),
    'BatchProgress': ('utils.progress', 'BatchProgress'),
    'track_progress': ('utils.progress', 'track_progress'),

    # Task Lock
    'TaskLock': ('utils.task_lock', 'TaskLock'),
    'TaskStatusManager': ('utils.task_lock', 'TaskStatusManager'),
    'task_execution_lock': ('utils.task_lock', 'task_execution_lock'),

    # Helpers
    'parse_date': ('utils.helpers', 'parse_date'),
    'safe_float': ('utils.helpers', 'safe_float'),
    'safe_int': ('utils.helpers', 'safe_int'),
    'chunks': ('utils.helpers', 'chunks'),
    'format_number': ('utils.helpers', 'format_number'),
    'format_percentage': ('utils.helpers', 'format_percentage'),
    'is_valid_fcode': ('utils.helpers', 'is_valid_fcode'),
    'extract_bond_subtype': ('utils.helpers', 'extract_bond_subtype'),
    'json_dumps': ('utils.helpers', 'json_dumps'),
    'json_loads': ('utils.helpers', 'json_loads'),
    'calculate_days_between': ('utils.helpers', 'calculate_days_between'),
    'merge_dicts': ('utils.helpers', 'merge_dicts'),
    'build_nav_snapshot_signature': ('utils.helpers', 'build_nav_snapshot_signature'),
}

__all__ = list(_EXPORT_MAP.keys())


def __getattr__(name: str):
    """按需导出旧有公共 API，兼容现有 `from utils import ...` 调用。"""
    module_name, attr_name = _EXPORT_MAP.get(name, (None, None))
    if not module_name:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    module = import_module(module_name)
    value = getattr(module, attr_name)
    globals()[name] = value
    return value


def __dir__():
    return sorted(set(globals()) | set(__all__))
