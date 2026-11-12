"""
债券型基金列表采集器兼容入口。

保留旧模块路径，统一复用 `collectors.fund_collector`，避免两套实现继续分叉。
"""

from collectors.fund_collector import BondFundCollector

__all__ = ["BondFundCollector"]
