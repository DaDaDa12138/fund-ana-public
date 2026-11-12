"""
API端点配置和常量定义
"""

# API端点
class APIEndpoints:
    """天天基金API端点"""

    # 基金搜索类
    FUND_SEARCH = "/fundSearch"                          # 基金多维度搜索
    FUND_SEARCH_BY_NAME = "/fundSearchInfoByName"        # 按名称搜索
    FUND_SUGGEST_LIST = "/fundSuggestList"               # 所有基金列表

    # 基金排行类
    FUND_RANK = "/fundMNRank"                            # 基金排行（核心）
    FUND_HK_RANK = "/fundMNHKRank"                       # 香港基金排行

    # 基金详情类
    FUND_DETAIL = "/fundMNDetailInformation"             # 基金详情（核心）
    FUND_STOP_WATCH = "/fundMNStopWatch"                 # 基金简介
    FUND_NAV_HISTORY = "/fundMNHisNetList"              # 历史净值（核心）
    FUND_NAV_DIAGRAM = "/fundVPageDiagram"              # 净值走势图
    FUND_VALUATION = "/fundVarietieValuationDetail"     # 实时估值
    FUND_ACC = "/fundVPageAcc"                           # 累计收益走势
    FUND_PERIOD_INCREASE = "/fundMNPeriodIncrease"      # 阶段涨幅
    FUND_RANK_DIAGRAM = "/fundRankDiagram"              # 同类排名走势
    FUND_GRADE = "/fundGradeDetail"                      # 基金评级
    FUND_MANAGER_LIST = "/fundMNMangerList"             # 基金的经理列表

    # 基金经理类
    MANAGER_INFO = "/fundMSNMangerInfo"                  # 经理详情
    MANAGER_ACC = "/fundMSNMangerAcc"                    # 经理业绩走势
    MANAGER_RANK = "/fundMSNMangerPerRank"              # 经理业绩排行
    MANAGER_EVAL = "/fundMSNMangerPerEval"              # 经理业绩评价
    MANAGER_POS_MARK = "/fundMSNMangerPosMark"          # 持仓风格
    MANAGER_POS_CHAR = "/fundMSNMangerPosChar"          # 持仓特点
    MANAGER_PRO_CONTR = "/fundMSNMangerProContr"        # 历史管理基金

    # 基金公司类
    COMPANY_LIST = "/fundCompanyBaseList"                # 公司列表
    COMPANY_API = "/companyApi2"                         # 公司详情（多功能）


# 基金类型代码
class FundType:
    """基金类型编码"""
    ALL = "0"                   # 全部
    ETF = "3"                   # ETF (交易所交易基金)
    LOF = "4"                   # LOF (上市开放式基金)
    STOCK = "25"                # 股票型
    INDEX = "26"                # 指数型
    HYBRID = "27"               # 混合型
    BOND = "31"                 # 债券型（核心）
    BOND_FIXED = "32"           # 定开债
    MONEY = "35"                # 货币型
    QDII = "36"                 # QDII（内部统一类型码；fundMNRank调用时映射为6）
    FOF = "37"                  # FOF（内部统一类型码；fundMNRank调用时映射为15）


# 债券型基金子类型
class BondSubType:
    """债券型基金子类型"""
    LONG_BOND = "债券型-长债"      # 长期债券
    SHORT_BOND = "债券型-中短债"   # 中短期债券
    HYBRID_BOND = "债券型-混合债"  # 混合债券
    CONVERTIBLE = "债券型-可转债"  # 可转换债券


# 排序字段
class SortColumn:
    """排行榜排序字段"""
    # 收益率
    RETURN_1W = "SYL_Z"         # 近1周
    RETURN_1M = "SYL_Y"         # 近1月
    RETURN_3M = "SYL_3Y"        # 近3月
    RETURN_6M = "SYL_6Y"        # 近6月
    RETURN_1Y = "SYL_1N"        # 近1年
    RETURN_2Y = "SYL_2N"        # 近2年
    RETURN_3Y = "SYL_3N"        # 近3年
    RETURN_5Y = "SYL_5N"        # 近5年
    RETURN_YTD = "SYL_JN"       # 今年来
    RETURN_SINCE = "SYL_LN"     # 成立来

    # 净值
    NAV = "HLDWJZ"              # 单位净值
    DAILY_GROWTH = "RZDF"       # 日涨幅

    # 规模
    SCALE = "ENDNAV"            # 基金规模

    # 热销榜
    SALES_MONTH = "SALESRANK"   # 月榜
    SALES_DAY = "SALESRANK_D"   # 日榜
    SALES_WEEK = "SALESRANK_Z"  # 周榜


# 时间范围
class TimeRange:
    """数据时间范围"""
    YEAR_1 = "1n"               # 近1年
    YEAR_2 = "2n"               # 近2年
    YEAR_3 = "3n"               # 近3年
    YEAR_5 = "5n"               # 近5年
    MONTH_1 = "1y"              # 近1月
    MONTH_3 = "3y"              # 近3月
    MONTH_6 = "6y"              # 近6月
    YTD = "jn"                  # 今年来
    SINCE = "ln"                # 成立来


# 公司API操作类型
class CompanyAction:
    """基金公司API操作类型"""
    BASE_INFO = "fundcompanybaseinfo"      # 公司基础信息
    FUND_LIST = "fundlist"                 # 旗下基金列表
    CATEGORY = "categoryoffund"            # 基金分类统计
    ARCHIVES = "companyarchives"           # 公司档案
    SCALE_CHANGE = "companygmbd"           # 规模变动


# 风险等级
class RiskLevel:
    """风险等级"""
    LOW = "1"                   # 低风险
    MID_LOW = "2"               # 中低风险
    MID = "3"                   # 中等风险
    MID_HIGH = "4"              # 中高风险
    HIGH = "5"                  # 高风险


# 评级
class Rating:
    """基金评级"""
    FIVE_STAR = "5"             # 五星
    FOUR_STAR = "4"             # 四星
    THREE_STAR = "3"            # 三星
    TWO_STAR = "2"              # 二星
    ONE_STAR = "1"              # 一星


# 筛选方案
class FilterType:
    """筛选方案代码"""
    GOLD_BULL_AWARD = "022"     # 金牛奖
    FIVE_STAR = "006"           # 五星评级
    QUALITY_BOND = "038"        # 优质债券
