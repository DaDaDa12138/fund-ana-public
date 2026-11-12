#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
顶层 analyzers.base_analyzer 兼容层。

这里直接把模块对象别名到正式实现，确保：
1. `analyzers.base_analyzer` 与 `bond_fund_screening.analyzers.base_analyzer`
   指向同一个模块对象；
2. `patch('analyzers.base_analyzer.*')` 会准确作用到真实实现；
3. 私有辅助函数也能沿旧导入口径访问。
"""

from __future__ import annotations

import sys

from bond_fund_screening.analyzers import base_analyzer as _impl


sys.modules[__name__] = _impl
