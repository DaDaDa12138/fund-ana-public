#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
基金类型注册表 - Fund Type Registry

提供基金类型的统一管理:
1. FundTypeCode: 基金类型代码枚举（项目内部统一口径）
2. FundCategory: 基金大类枚举
3. FundTypeProfile: 基金类型配置数据类
4. 注册表访问接口
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Union


class FundTypeCode(Enum):
    """
    基金类型代码枚举（项目内部统一口径）

    使用方法:
        FundTypeCode.BOND.value  # "31"
        FundTypeCode.STOCK.api_code  # "25"

    说明:
        - QDII/FOF 在项目内部延续历史统一代码 36/37；
        - 调用 Tiantian /fundMNRank 时会在请求层映射为文档要求的 6/15；
        - 数据库与筛选链路仍以 36/37 作为统一类型码。
    """
    # 权益类
    STOCK = "25"          # 股票型
    INDEX = "26"          # 指数型
    ETF = "3"             # ETF (交易所交易基金)
    LOF = "4"             # LOF (上市开放式基金)

    # 固定收益类
    BOND = "31"           # 债券型
    BOND_FIXED = "32"     # 定开债

    # 货币类
    MONEY = "35"          # 货币型

    # 混合/另类
    HYBRID = "27"         # 混合型
    QDII = "36"           # QDII (境外投资基金)
    FOF = "37"            # FOF (基金中基金)

    @property
    def api_code(self) -> str:
        """返回API使用的代码"""
        return self.value

    @property
    def display_name(self) -> str:
        """返回中文显示名称"""
        return FUND_TYPE_NAMES.get(self.value, self.name)

    @classmethod
    def from_string(cls, code: str) -> Optional['FundTypeCode']:
        """从字符串代码获取枚举值"""
        for member in cls:
            if member.value == code:
                return member
        return None

    @classmethod
    def from_alias(cls, alias: str) -> Optional['FundTypeCode']:
        """从别名获取枚举值（支持英文简称）"""
        alias = alias.lower().strip()
        alias_map = {
            'stock': cls.STOCK,
            'equity': cls.STOCK,
            'index': cls.INDEX,
            'etf': cls.ETF,
            'lof': cls.LOF,
            'bond': cls.BOND,
            'fixed': cls.BOND_FIXED,
            'money': cls.MONEY,
            'monetary': cls.MONEY,
            'hybrid': cls.HYBRID,
            'mixed': cls.HYBRID,
            'qdii': cls.QDII,
            'fof': cls.FOF,
        }
        return alias_map.get(alias)


class FundCategory(Enum):
    """
    基金大类枚举

    用于分组相似风险特征的基金类型
    """
    EQUITY = "equity"              # 权益类（高风险高收益）
    FIXED_INCOME = "fixed_income"  # 固定收益类（低风险）
    MONEY_MARKET = "money_market"  # 货币市场（极低风险）
    HYBRID = "hybrid"              # 混合类（中等风险）
    ALTERNATIVE = "alternative"    # 另类（QDII、跨境等）
    MULTI_ASSET = "multi_asset"    # 多资产（FOF等）

    @property
    def display_name(self) -> str:
        """返回中文显示名称"""
        names = {
            'equity': '权益类',
            'fixed_income': '固定收益类',
            'money_market': '货币市场类',
            'hybrid': '混合类',
            'alternative': '另类投资',
            'multi_asset': '多资产类',
        }
        return names.get(self.value, self.value)


# 基金类型中文名称映射
FUND_TYPE_NAMES: Dict[str, str] = {
    "25": "股票型",
    "26": "指数型",
    "27": "混合型",
    "31": "债券型",
    "32": "定开债",
    "35": "货币型",
    "36": "QDII",
    "37": "FOF",
    "3": "ETF",
    "4": "LOF",
}

# 基金类型到大类的映射
FUND_TYPE_CATEGORY_MAP: Dict[FundTypeCode, FundCategory] = {
    FundTypeCode.STOCK: FundCategory.EQUITY,
    FundTypeCode.INDEX: FundCategory.EQUITY,
    FundTypeCode.ETF: FundCategory.EQUITY,
    FundTypeCode.LOF: FundCategory.EQUITY,
    FundTypeCode.BOND: FundCategory.FIXED_INCOME,
    FundTypeCode.BOND_FIXED: FundCategory.FIXED_INCOME,
    FundTypeCode.MONEY: FundCategory.MONEY_MARKET,
    FundTypeCode.HYBRID: FundCategory.HYBRID,
    FundTypeCode.QDII: FundCategory.ALTERNATIVE,
    FundTypeCode.FOF: FundCategory.MULTI_ASSET,
}


@dataclass
class FundTypeProfile:
    """
    基金类型配置Profile

    包含特定基金类型的所有筛选和评分参数
    """
    # 基本信息
    fund_type: FundTypeCode
    category: FundCategory
    display_name: str
    description: str = ""

    # 流动性筛选参数
    max_holding_days: int = 90  # 最大持有期天数
    holding_keywords: List[str] = field(default_factory=list)  # 长持有期关键词

    # 成熟度筛选参数
    min_establish_days: int = 365  # 最小成立天数
    min_scale_baseline: float = 1.0  # 最小规模阈值（亿元）

    # 规模分层阈值（周期年数 -> 规模阈值亿元）
    scale_thresholds: Dict[int, float] = field(default_factory=dict)

    # 风险指标筛选参数
    risk_metric_pass_count: int = 4  # 至少N/6个指标优于中位数
    risk_metric_min_funds: int = 10  # 最少基金数

    # 异常值筛选参数
    max_annual_return: float = 12.0  # 最高年化收益率(%)
    min_annual_return: float = -5.0  # 最低年化收益率(%)
    max_drawdown_threshold: float = 0.08  # 最大回撤阈值（小数）

    # 业绩持续性筛选参数
    consistency_pass_count: int = 2  # 至少2/3个周期收益≥中位数
    min_absolute_return: Dict[int, float] = field(default_factory=dict)  # 周期 -> 最低累计收益率(%)

    # 无风险利率（用于风险指标计算）
    risk_free_rate: float = 0.015  # 1.5%

    # 评分权重配置
    short_term_weights: Dict[str, float] = field(default_factory=dict)
    long_term_weights: Dict[str, float] = field(default_factory=dict)
    combo_weights: Dict[str, float] = field(default_factory=dict)

    # 绝对基准配置
    absolute_baselines: Dict[str, Dict] = field(default_factory=dict)

    def __post_init__(self):
        """初始化后处理"""
        # 确保默认值
        if not self.scale_thresholds:
            self.scale_thresholds = {1: 1.0, 2: 1.0, 3: 1.0, 4: 1.0, 5: 1.0}
        if not self.min_absolute_return:
            self.min_absolute_return = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0}
        if not self.combo_weights:
            self.combo_weights = {'short_term': 0.4, 'long_term': 0.6}


# ============================================================================
# Profile 注册表
# ============================================================================

# 使用value作为key，避免跨模块枚举实例不相等问题
_profile_registry: Dict[str, FundTypeProfile] = {}


def register_profile(profile: FundTypeProfile) -> None:
    """注册基金类型Profile"""
    # 使用fund_type.value作为key，确保跨模块一致性
    key = profile.fund_type.value if hasattr(profile.fund_type, 'value') else str(profile.fund_type)
    _profile_registry[key] = profile


def get_profile(fund_type: Union[str, FundTypeCode]) -> Optional[FundTypeProfile]:
    """
    获取基金类型Profile

    Args:
        fund_type: 基金类型代码或枚举

    Returns:
        对应的Profile，如果未注册则返回None
    """
    # 获取value字符串
    if isinstance(fund_type, str):
        # 尝试从别名获取
        fund_type_enum = FundTypeCode.from_alias(fund_type)
        if fund_type_enum is not None:
            key = fund_type_enum.value
        else:
            # 尝试从代码获取
            fund_type_enum = FundTypeCode.from_string(fund_type)
            if fund_type_enum is not None:
                key = fund_type_enum.value
            else:
                # 直接使用字符串作为key
                key = fund_type
    elif hasattr(fund_type, 'value'):
        key = fund_type.value
    else:
        key = str(fund_type)

    return _profile_registry.get(key)


def get_all_profiles() -> Dict[str, FundTypeProfile]:
    """获取所有已注册的Profile"""
    return _profile_registry.copy()


def get_available_types() -> List[FundTypeCode]:
    """获取所有可用的基金类型"""
    return list(_profile_registry.keys())


def get_types_by_category(category: FundCategory) -> List[FundTypeCode]:
    """按大类获取基金类型列表"""
    return [
        fund_type for fund_type, cat in FUND_TYPE_CATEGORY_MAP.items()
        if cat == category and fund_type in _profile_registry
    ]


def resolve_alias(alias: str) -> Optional[FundTypeCode]:
    """
    解析别名为FundTypeCode

    便捷函数，委托给 FundTypeCode.from_alias

    Args:
        alias: 别名字符串（如 'bond', 'stock', 'hybrid' 等）

    Returns:
        对应的FundTypeCode枚举值，如果无效别名则返回None
    """
    return FundTypeCode.from_alias(alias)


# ============================================================================
# CLI 类型选项辅助
# ============================================================================

CLI_TYPE_CHOICES = [
    'stock', 'index', 'hybrid', 'bond', 'money', 'qdii', 'fof', 'etf', 'lof', 'all'
]

CLI_TYPE_HELP = """
基金类型选项:
  说明: 本项目正式工作流默认只使用 bond；其余类型仅保留给底层采集/维护命令。
  stock  - 股票型 (API代码: 25)
  index  - 指数型 (API代码: 26)
  hybrid - 混合型 (API代码: 27)
  bond   - 债券型 (API代码: 31, 32) [默认]
  money  - 货币型 (API代码: 35)
  qdii   - QDII   (内部统一类型码: 36；rank接口映射为6)
  fof    - FOF    (内部统一类型码: 37；rank接口映射为15)
  etf    - ETF    (API代码: 3)
  lof    - LOF    (API代码: 4)
  all    - 所有类型
"""


def cli_type_to_fund_types(cli_type: str) -> List[FundTypeCode]:
    """
    将CLI类型参数转换为FundTypeCode列表

    Args:
        cli_type: CLI输入的类型字符串

    Returns:
        对应的FundTypeCode列表
    """
    cli_type = cli_type.lower().strip()

    if cli_type == 'all':
        return list(FundTypeCode)

    if cli_type == 'bond':
        # 债券型包含普通债券和定开债
        return [FundTypeCode.BOND, FundTypeCode.BOND_FIXED]

    fund_type = FundTypeCode.from_alias(cli_type)
    if fund_type:
        return [fund_type]

    return []


# ============================================================================
# 测试函数
# ============================================================================

def test_fund_type_registry():
    """测试基金类型注册表"""
    print("=" * 60)
    print("测试 Fund Type Registry 模块")
    print("=" * 60)

    # 测试1: 枚举基本功能
    print("\n【测试1】枚举基本功能")
    print(f"  BOND.value = {FundTypeCode.BOND.value}")
    print(f"  BOND.api_code = {FundTypeCode.BOND.api_code}")
    print(f"  BOND.display_name = {FundTypeCode.BOND.display_name}")

    # 测试2: 从字符串转换
    print("\n【测试2】从字符串转换")
    print(f"  from_string('31') = {FundTypeCode.from_string('31')}")
    print(f"  from_alias('bond') = {FundTypeCode.from_alias('bond')}")
    print(f"  from_alias('equity') = {FundTypeCode.from_alias('equity')}")

    # 测试3: 大类映射
    print("\n【测试3】大类映射")
    for fund_type, category in FUND_TYPE_CATEGORY_MAP.items():
        print(f"  {fund_type.display_name} -> {category.display_name}")

    # 测试4: CLI类型转换
    print("\n【测试4】CLI类型转换")
    for cli_type in ['bond', 'stock', 'all']:
        types = cli_type_to_fund_types(cli_type)
        print(f"  '{cli_type}' -> {[t.display_name for t in types]}")

    print("\n" + "=" * 60)
    print("✅ Fund Type Registry 测试完成")
    print("=" * 60)


if __name__ == "__main__":
    test_fund_type_registry()
