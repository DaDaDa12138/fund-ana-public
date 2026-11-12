"""
采集器模块
"""

from .base_collector import BaseCollector
from .fund_collector import FundCollector, BondFundCollector  # 通用基金采集器
from .nav_collector import NavCollector
from .detail_collector import FundDetailCollector
from .manager_collector import ManagerCollector
from .company_collector import CompanyCollector
# P0-R2新增采集器
from .annual_return_collector import AnnualReturnCollector
from .rank_collector import RankCollector

__all__ = [
    'BaseCollector',
    'FundCollector',      # 新增：通用基金采集器
    'BondFundCollector',  # 向后兼容别名
    'NavCollector',
    'FundDetailCollector',
    'ManagerCollector',
    'CompanyCollector',
    # P0-R2新增
    'AnnualReturnCollector',
    'RankCollector',
]
