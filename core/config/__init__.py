#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
兼容导出：统一暴露 `bond_fund_screening.config`。

项目里同时存在：
- 历史导入方式：`from config import DB_PATH`
- 子模块导入方式：`from config.api_config import ...`

将 `config/` 明确为常规包后，二者都能稳定工作，不再受 Docker
环境下命名空间目录解析差异影响。
"""

from bond_fund_screening.config import *  # noqa: F401,F403
