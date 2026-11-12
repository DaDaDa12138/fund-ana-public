#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
核心筛选引擎模块

包含：
- DataLoader: 数据加载器
- RiskCalculator: 风险指标计算器
- FilteringEngine: 10层筛选引擎
- ScoringEngine: 双评分系统
"""

import sys
from pathlib import Path

# 添加项目根目录到路径以便导入version
_project_root = Path(__file__).parent.parent.parent
# Keep screening modules first while exposing project-owned core subpackages.
# Otherwise this regular package hides the outer namespace when screening/
# precedes the project root on sys.path (for example, scheduler CLI imports).
__path__.append(str(_project_root))
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

try:
    from version import __version__
except ImportError:
    __version__ = "1.1.0"  # 回退版本号

