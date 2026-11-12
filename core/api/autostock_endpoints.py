#!/usr/bin/env python3
"""
AutoStock API端点定义
定义所有API端点的URL、参数和元数据
"""

from enum import Enum
import math


class AutoStockEndpoint:
    """AutoStock API端点定义"""

    BASE_URL = "https://api.autostock.cn/v1"

    # ==================== 基金查询端点 ====================

    # 获取所有基金
    FUND_ALL = f"{BASE_URL}/fund/all"

    # 获取基金数据（支持批量，逗号分隔代码）
    FUND = f"{BASE_URL}/fund"

    # 获取基金详情（单个）
    FUND_DETAIL = f"{BASE_URL}/fund/detail"

    # 获取基金详情（批量）
    FUND_DETAIL_LIST = f"{BASE_URL}/fund/detail/list"

    # 获取基金排行
    FUND_RANK = f"{BASE_URL}/fund/rank"

    # 获取基金持仓
    FUND_POSITION = f"{BASE_URL}/fund/position"


class FundType(Enum):
    """基金类型枚举"""

    STOCK = "gp"        # 股票型
    HYBRID = "hh"       # 混合型
    BOND = "zq"         # 债券型
    INDEX = "zs"        # 指数型


class SortType(Enum):
    """排序类型枚举"""

    DAY = "r"           # 日涨幅
    WEEK = "z"          # 周涨幅
    MONTH_1 = "1y"      # 一月涨幅
    THIS_YEAR = "jn"    # 今年涨幅
    YEAR_1 = "1n"       # 近一年涨幅


class FundCompany(Enum):
    """基金公司代码枚举（常用）"""

    HUAXIA = "80000222"         # 华夏
    YIFANGDA = "80000229"       # 易方达
    GUANGFA = "80000248"        # 广发
    NANFANG = "80000226"        # 南方
    JIASHI = "80000235"         # 嘉实
    BOSHI = "80000232"          # 博时
    ZHONGGUO = "80000239"       # 中国
    FUGUO = "80000245"          # 富国
    GONGYIN = "80000255"        # 工银瑞信
    JIANXIN = "80000246"        # 建信


class FieldMapping:
    """
    AutoStock API字段与本地数据库字段映射
    """

    # ==================== 基金基本信息 ====================

    BASIC_FIELDS = {
        # AutoStock字段 -> 本地字段
        'code': 'fcode',
        'name': 'shortname',
        'type': 'ftype',  # 需要转换
    }

    # ==================== 净值数据 ====================

    NAV_FIELDS = {
        'netWorth': 'latest_nav',        # 单位净值
        'expectWorth': None,             # 净值估算（不存储）
        'totalWorth': 'accum_nav',       # 累计净值
    }

    # ==================== 收益率数据 ====================

    RETURN_FIELDS = {
        'dayGrowth': 'daily_growth',         # 日增长
        'lastWeekGrowth': 'return_1w',       # 近1周
        'lastMonthGrowth': 'return_1m',      # 近1月
        'lastThreeMonthsGrowth': 'return_3m',  # 近3月
        'lastSixMonthsGrowth': 'return_6m',    # 近6月
        'lastYearGrowth': 'return_1y',       # 近1年
        'thisYearGrowth': 'return_ytd',      # 今年来
    }

    # ==================== 基金经理和规模 ====================

    MANAGER_FIELDS = {
        'manager': 'manager_names',       # 基金经理
        'fundScale': None,                # 基金规模（格式不同，需特殊处理）
    }

    # ==================== 费率信息 ====================

    FEE_FIELDS = {
        'buyMin': None,           # 最小购买金额
        'buySourceRate': None,    # 原始费率
        'buyRate': None,          # 购买费率
    }

    # ==================== 所有字段映射 ====================

    ALL_FIELDS = {
        **BASIC_FIELDS,
        **NAV_FIELDS,
        **RETURN_FIELDS,
        **MANAGER_FIELDS,
        **FEE_FIELDS
    }

    # ==================== 反向映射（本地->AutoStock） ====================

    @classmethod
    def get_reverse_mapping(cls):
        """获取反向映射（本地字段 -> AutoStock字段）"""
        reverse = {}
        for as_field, local_field in cls.ALL_FIELDS.items():
            if local_field:
                reverse[local_field] = as_field
        return reverse


class DataConverter:
    """数据转换工具类"""

    @staticmethod
    def parse_growth_rate(value: str | float | None) -> float | None:
        """
        解析增长率字符串

        Args:
            value: 增长率字符串，如 "15.3%"

        Returns:
            浮点数，如 15.3
        """
        if value is None or value == '' or value == '--':
            return None

        try:
            # 移除百分号并转换为浮点数
            parsed = float(str(value).strip().removesuffix('%'))
            return parsed if math.isfinite(parsed) else None
        except (ValueError, AttributeError):
            return None

    @staticmethod
    def parse_fund_scale(value: str) -> float:
        """
        解析基金规模字符串

        Args:
            value: 基金规模字符串，如 "100.50亿元"

        Returns:
            浮点数（亿元），如 100.50
        """
        if value is None or value == '' or value == '--':
            return None

        try:
            # 移除"亿元"等后缀
            value = value.replace('亿元', '').replace('亿', '').strip()
            return float(value)
        except (ValueError, AttributeError):
            return None

    @staticmethod
    def convert_fund_type(autostock_type: str) -> str | None:
        """
        转换基金类型编码

        Args:
            autostock_type: AutoStock的基金类型，如 "债券型"

        Returns:
            本地数据库的类型编码，如 "003"
        """
        type_mapping = {
            '债券型': '003',
            '债券型-长债': '003',
            '债券型-短债': '003',
            '债券型-纯债': '003',
            '定开债': '008',
            '定开债券': '008',
        }

        return type_mapping.get(autostock_type)

    @staticmethod
    def convert_to_local_fund_type(fund_type_code: str) -> str | None:
        """
        转换本地基金类型编码到AutoStock类型

        Args:
            fund_type_code: 本地类型编码，如 "003"

        Returns:
            AutoStock的基金类型参数值
        """
        type_mapping = {
            '003': 'zq',  # 债券型
            '008': 'zq',  # 定开债
        }

        return type_mapping.get(fund_type_code)


class APILimits:
    """API限制和配置"""

    # 速率限制
    MAX_CALLS_PER_HOUR = 100        # 每小时最大调用次数
    MIN_INTERVAL_SECONDS = 2        # 最小请求间隔（秒）

    # 批量请求限制
    # 文档未显式说明免费版批量上限，但项目内真实压测显示：
    # /fund 单批超过 50 时会明显出现返回缺失，因此安全上限统一按 50。
    MAX_BATCH_SIZE = 50             # 单批安全上限
    RECOMMENDED_BATCH_SIZE = 50     # 推荐批量大小

    # 超时设置
    DEFAULT_TIMEOUT = 30            # 默认超时时间（秒）

    # 重试设置
    MAX_RETRIES = 3                 # 最大重试次数
    RETRY_WAIT_BASE = 60            # 重试等待基础时间（秒）


class ResponseCode:
    """API响应代码"""

    SUCCESS = 0             # 成功
    PARAM_ERROR = 400       # 参数错误
    UNAUTHORIZED = 401      # 未授权
    FORBIDDEN = 403         # 禁止访问
    NOT_FOUND = 404         # 未找到
    TOO_MANY_REQUESTS = 429 # 请求过多
    SERVER_ERROR = 500      # 服务器错误


# ==================== 示例用法 ====================

if __name__ == '__main__':
    print("=" * 70)
    print("AutoStock API端点定义示例")
    print("=" * 70)

    # 1. 端点URL
    print("\n1. API端点:")
    print(f"   获取所有基金: {AutoStockEndpoint.FUND_ALL}")
    print(f"   获取基金排行: {AutoStockEndpoint.FUND_RANK}")
    print(f"   批量获取详情: {AutoStockEndpoint.FUND_DETAIL_LIST}")

    # 2. 基金类型
    print("\n2. 基金类型:")
    for fund_type in FundType:
        print(f"   {fund_type.name}: {fund_type.value}")

    # 3. 字段映射
    print("\n3. 字段映射示例:")
    print(f"   AutoStock 'code' -> 本地 '{FieldMapping.BASIC_FIELDS['code']}'")
    print(f"   AutoStock 'netWorth' -> 本地 '{FieldMapping.NAV_FIELDS['netWorth']}'")
    print(f"   AutoStock 'lastYearGrowth' -> 本地 '{FieldMapping.RETURN_FIELDS['lastYearGrowth']}'")

    # 4. 数据转换
    print("\n4. 数据转换示例:")
    converter = DataConverter()
    print(f"   '15.3%' -> {converter.parse_growth_rate('15.3%')}")
    print(f"   '100.50亿元' -> {converter.parse_fund_scale('100.50亿元')}")
    print(f"   '债券型-纯债' -> '{converter.convert_fund_type('债券型-纯债')}'")

    # 5. API限制
    print("\n5. API限制:")
    print(f"   每小时最大调用: {APILimits.MAX_CALLS_PER_HOUR}次")
    print(f"   最小请求间隔: {APILimits.MIN_INTERVAL_SECONDS}秒")
    print(f"   批量请求最大: {APILimits.MAX_BATCH_SIZE}个")
    print(f"   推荐批量大小: {APILimits.RECOMMENDED_BATCH_SIZE}个")
