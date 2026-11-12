#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
分析器模块

说明：
- 当前仓库同时存在顶层 `analyzers/` 与 `bond_fund_screening/analyzers/`
- 测试或脚本一旦把 `bond_fund_screening` 提前加入 `sys.path`，
  `import analyzers` 就可能解析到本目录
- 因此这里需要显式暴露 `RiskAnalyzer` 兼容入口，避免导入链路断裂
"""

import sys
from pathlib import Path

# 添加项目根目录到路径以便导入version
_project_root = Path(__file__).parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

try:
    from version import __version__
except ImportError:
    __version__ = "1.1.0"  # 回退版本号

from .risk_analyzer import RiskAnalyzer

__all__ = ['RiskAnalyzer', '__version__']
