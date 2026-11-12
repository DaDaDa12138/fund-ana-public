#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
债券基金 Profile 模块

当前项目仅保留债券基金筛选所需的 Profile 导出。

使用方法:
    from bond_fund_screening.profiles import get_profile, BOND_PROFILE

    # 获取债券型Profile
    profile = get_profile('bond')
    # 或
    profile = get_profile(FundTypeCode.BOND)
"""

import sys
from pathlib import Path

# 添加父目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from config.fund_type_registry import (
    FundTypeCode,
    FundCategory,
    FundTypeProfile,
    register_profile,
    get_profile,
    get_all_profiles,
    get_available_types,
)

# 导入并注册债券 Profile
from .bond_profile import BOND_PROFILE, BOND_FIXED_PROFILE

# 注册债券 Profile 到注册表
register_profile(BOND_PROFILE)
register_profile(BOND_FIXED_PROFILE)

# 导出
__all__ = [
    # 核心类
    'FundTypeCode',
    'FundCategory',
    'FundTypeProfile',

    # 注册表函数
    'register_profile',
    'get_profile',
    'get_all_profiles',
    'get_available_types',

    # 预定义Profile
    'BOND_PROFILE',
    'BOND_FIXED_PROFILE',
]
