#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
RiskAnalyzer 兼容桥接层

背景：
- 仓库保留了历史顶层包 `analyzers/risk_analyzer.py`
- 部分测试会把 `bond_fund_screening` 目录提前加入 `sys.path`
- 此时 `import analyzers.risk_analyzer` 会错误命中本目录，导致历史实现丢失

策略：
- 不复制风险分析器实现
- 直接按文件路径加载顶层历史实现，保证单一实现来源
"""

from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
import sys


_LEGACY_MODULE_NAME = '_bond_tracker_legacy_risk_analyzer'
_LEGACY_MODULE_PATH = Path(__file__).resolve().parents[2] / 'analyzers' / 'risk_analyzer.py'


def _load_legacy_module():
    """按文件路径加载顶层 RiskAnalyzer 实现，规避包名冲突。"""
    module = sys.modules.get(_LEGACY_MODULE_NAME)
    if module is not None:
        return module

    if not _LEGACY_MODULE_PATH.exists():
        raise ImportError(f'未找到顶层 RiskAnalyzer 实现: {_LEGACY_MODULE_PATH}')

    spec = spec_from_file_location(_LEGACY_MODULE_NAME, _LEGACY_MODULE_PATH)
    if spec is None or spec.loader is None:
        raise ImportError(f'无法加载顶层 RiskAnalyzer 模块: {_LEGACY_MODULE_PATH}')

    module = module_from_spec(spec)
    sys.modules[_LEGACY_MODULE_NAME] = module
    spec.loader.exec_module(module)
    return module


_LEGACY_MODULE = _load_legacy_module()

RiskAnalyzer = _LEGACY_MODULE.RiskAnalyzer
ProgressMonitor = getattr(_LEGACY_MODULE, 'ProgressMonitor', None)

__all__ = ['RiskAnalyzer', 'ProgressMonitor']
