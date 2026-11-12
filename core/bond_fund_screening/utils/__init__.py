#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
工具模块兼容入口。

当 `bond_fund_screening/` 被放到 sys.path 最前时，`import utils` 会先命中
当前包；而项目真实通用工具位于根级 `utils/`。这里把根级目录合并进
`utils` 包搜索路径，并桥接导出常用符号，避免脚本/测试因导入顺序不同
出现 `ModuleNotFoundError` 或属性缺失。
"""

import importlib.util
import sys
from pathlib import Path

_PACKAGE_DIR = Path(__file__).parent
_PROJECT_ROOT = _PACKAGE_DIR.parent.parent
_ROOT_UTILS_DIR = _PROJECT_ROOT / "utils"

__path__ = [str(_PACKAGE_DIR)]
if _ROOT_UTILS_DIR.exists():
    __path__.append(str(_ROOT_UTILS_DIR))

# 添加项目根目录到路径以便导入 version
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

try:
    from version import __version__
except ImportError:
    __version__ = "1.1.0"  # 回退版本号

if (_ROOT_UTILS_DIR / "__init__.py").exists():
    _root_utils_spec = importlib.util.spec_from_file_location(
        "_root_utils_bridge",
        _ROOT_UTILS_DIR / "__init__.py",
        submodule_search_locations=[str(_ROOT_UTILS_DIR)],
    )
    _root_utils_module = importlib.util.module_from_spec(_root_utils_spec)
    sys.modules[_root_utils_spec.name] = _root_utils_module
    _root_utils_spec.loader.exec_module(_root_utils_module)

    _export_names = getattr(_root_utils_module, "__all__", None)
    if _export_names is None:
        _export_names = [name for name in dir(_root_utils_module) if not name.startswith("_")]

    __all__ = list(dict.fromkeys(["__version__", *_export_names]))
else:
    __all__ = ["__version__"]


def __getattr__(name: str):
    # Preserve the root package's lazy exports. Eagerly resolving TaskLock
    # while database.connection is importing this package creates a cycle.
    if name not in __all__ or name == "__version__":
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(_root_utils_module, name)
    globals()[name] = value
    return value


def __dir__():
    return sorted(set(globals()) | set(__all__))
