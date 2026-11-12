#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
债券基金筛选系统 - 全局配置
"""

from pathlib import Path
from database.connection import resolve_db_path

# 兼容历史导入：
# 当测试或脚本把 `bond_fund_screening/` 目录置于 sys.path 最前时，
# `import config` 会命中当前模块。为保持 `from config.api_config import ...`
# 这类根级子模块导入仍可用，这里将当前模块声明为可解析根级 `config/` 的包式模块。
_PROJECT_ROOT = Path(__file__).parent.parent
_ROOT_CONFIG_DIR = _PROJECT_ROOT / "config"
if _ROOT_CONFIG_DIR.exists():
    __path__ = [str(_ROOT_CONFIG_DIR)]

# ============================================================================
# 路径配置
# ============================================================================

# 项目根目录
PROJECT_ROOT = _PROJECT_ROOT

# 数据库路径
DB_PATH = Path(resolve_db_path())

def _resolve_output_dir() -> Path:
    """Resolve the legacy path without creating directories or repairing links.

    A copied checkout/image can contain a dangling absolute host symlink.
    Writers use the configured project directory in that case; the workflow's
    explicit ensure_core_output_link still owns link initialization.
    """
    legacy = Path(__file__).parent / "output"
    if not (legacy.is_symlink() and not legacy.exists()):
        return legacy
    from utils.project_runtime_config import load_project_config_payload

    paths = load_project_config_payload().get("paths") or {}
    configured = Path(paths.get("core_output_dir") or "reports/core_output")
    return configured if configured.is_absolute() else _PROJECT_ROOT.parent / configured


# Only resolve here. An analyzer/workflow creates its output directory at run time.
OUTPUT_DIR = _resolve_output_dir()

# ============================================================================
# 数据库表配置
# ============================================================================

# 表名
TABLE_FUNDS = "funds"
TABLE_RETURNS = "fund_returns"
TABLE_NAV_HISTORY = "fund_nav_history"

# 字段名映射（设计文档 -> 数据库实际字段）
FIELD_MAPPING = {
    'fund_code': 'fcode',
    'short_name': 'shortname',
    'fund_type': 'ftype',
    'establish_date': 'estab_date',
    'scale_billion': 'scale_billion',
    'has_holding_period': 'has_holding_period',
    'holding_days': 'holding_days',
    'holding_keywords': 'holding_keywords',
    'manager_name': 'manager_names',
}

# ============================================================================
# 筛选阈值配置
# ============================================================================

# 第2层：流动性筛选
MAX_HOLDING_DAYS = 90  # 最大持有期天数

# 长持有期关键词列表
# [Fallback] 仅在运行时债券 Profile 未命中时生效
LONG_HOLDING_KEYWORDS = [
    '定开', '定期开放', '封闭', '封闭式',
    '半年持有', '6个月持有', '180天持有',
    '一年持有', '12个月持有', '1年持有',
    '两年持有', '24个月持有', '2年持有',
    '三年持有', '36个月持有', '3年持有',
    '6个月定开', '一年定开', '两年定开', '三年定开',
    '锁定期', '封闭期', '持有至到期'
]

# 第3层：基础成熟度筛选
MIN_ESTABLISH_DAYS = 365  # 最少成立天数（1年）
MIN_SCALE_BASELINE = 1.0  # 最小规模阈值（亿元）

# 第5层：规模分层筛选（动态阈值）
# [Fallback] 仅在运行时债券 Profile 未命中时生效
# 优化规模阈值梯度，避免陡峭跳跃
SCALE_THRESHOLDS = {
    1: 2.0,  # 1年期：1.5→2.0亿（提高基础门槛，降低清盘和流动性风险）
    2: 2.0,  # 2年期：1.6→2.0亿
    3: 2.5,  # 3年期：1.7→2.5亿
    4: 3.0,  # 4年期：1.9→3.0亿
    5: 3.0,  # 5年期：2.2→3.0亿
}

# 第6层：风险指标筛选
# 至少4/6个指标优于中位数（67%通过率）
RISK_METRIC_PASS_COUNT = 4
RISK_METRIC_MIN_FUNDS = 10  # 最少基金数（否则降级）

# 第6层补充：风险指标质量门控（默认仅告警，不改变筛选结果）
# mode:
# - warn: 仅记录异常并打印告警（默认，零回归）
# - hard: 异常基金直接淘汰（reason_code=RISK_METRICS_QC_FAIL）
RISK_METRIC_QC = {
    'enabled': True,
    'mode': 'warn',
    'thresholds': {
        # 这些阈值与 risk_calculator 的 cap 协同，用于兜底历史脏数据/异常输入
        'max_abs_sharpe_ratio': 5.0,
        'max_abs_sortino_ratio': 10.0,
        'max_abs_calmar_ratio': 20.0,
        # 波动率/一致性是小数形式（如 0.08 = 8%）
        'max_volatility': 1.5,
        'max_consistency': 1.0,
        # 最大回撤为正数小数（如 0.12 = 12%）
        'max_drawdown': 0.95,
    },
}

# 第7层：异常值筛选
# 注意：长期周期的累积收益阈值 = 年化阈值 × 周期年数（线性缩放）
# [Fallback] 仅在运行时债券 Profile 未命中时生效
MAX_ANNUAL_RETURN = 12  # 最高年化收益率(%) - 含混合二级债基，与Profile保持一致
MIN_ANNUAL_RETURN = -5   # 最低年化收益率(%) (从-10提高到-5)

# 🎯 方案B精准精选: 最大回撤要求
# 修复：统一为小数形式（0.08表示8%）
MAX_DRAWDOWN_THRESHOLD = 0.08  # 最大回撤阈值：≤8%（方案B收紧）

# 第8层：业绩持续性筛选
CONSISTENCY_PASS_COUNT = 2  # 至少2/3个周期收益≥中位数

# 第8层增强：绝对收益阈值（避免极端行情下筛选出负收益基金）
# 格式：{周期年数: 最低累计收益率(%)}
# 适应低利率环境：年化基准从2%降至1.5%
MIN_ABSOLUTE_RETURN = {
    1: 1.5,    # 1年期：≥1.5%（年化1.5%）
    2: 3.0,    # 2年期：≥3.0%（年化1.5%）
    3: 4.5,    # 3年期：≥4.5%（年化1.5%）
    4: 6.0,    # 4年期：≥6.0%（年化1.5%）
    5: 7.5,    # 5年期：≥7.5%（年化1.5%）
}

# 短周期绝对收益阈值（避免线性缩放导致半年期/1年期Layer 8淘汰率过高~80%）
# 短期债基年化1-1.5%收益率更合理
MIN_ABSOLUTE_RETURN_SHORT_TERM = {
    0.02: 0.0,   # 1周: 不设绝对收益门槛（周期太短）
    0.08: 0.05,  # 1月: ≥0.05%
    0.25: 0.3,   # 3月: ≥0.3%（年化~1.2%）
    0.49: 0.5,   # 半年: ≥0.5%（年化~1.0%）
}

# 第9层：多维度交叉验证
TOP_DIMENSION_RATIO = 0.3  # 前30%阈值
MIN_TOP_DIMENSIONS = 3  # 至少3/5个维度前30%

# 第10层：最终精选
# 单周期正式产物默认输出全部达标基金，不再使用固定 Top N 截断。
# 如需做实验性截断，只允许由 CLI 显式传入。
DEFAULT_TOP_N = None

# ============================================================================
# v1.9.60 P1-4: 费率筛选配置
# ============================================================================
# Finominal 2024 研究：费率是唯一具有正向预测力的基金筛选指标
# 低费率基金样本外超额收益+3.0%，高费率基金-14.8%
# 阈值为持续持有费率之和（管理费+托管费+销售服务费，百分比形式）
# v1.9.67 P1-2: 债券基金费率阈值收紧（Kinnel: 固收 ≤ 0.70%，保留适度余量）
FEE_RATE_THRESHOLDS = {
    'bond': 0.80,        # 债券型：1.0% -> 0.80%
    'bond_fixed': 0.80,  # v1.9.62 P1-2: 定开债与普通债券一致
    'stock': 2.0,       # 股票型：持续持有费率 < 2.0%
    'hybrid': 1.5,      # 混合型：持续持有费率 < 1.5%
    'index': 1.5,       # v1.9.62 P1-2: 指数型介于股票和债券之间
}

# ============================================================================
# 多周期交集质量过滤配置
# ============================================================================

# 🔧 新增：多周期合并时的质量过滤阈值
# 用于过滤掉聚合后表现较差的基金
INTERSECTION_QUALITY_FILTERS = {
    'min_avg_score': 25,         # 平均综合评分最低阈值（v1.9.58: 30→25，提高通过率至约15%）
    'min_avg_sharpe': 0,         # 平均夏普比率最低阈值（排除负夏普基金）
    'min_period_return': -3,     # 单周期最低收益率阈值（排除有严重亏损的基金）
    'max_score_std': 15,         # 评分标准差最大阈值（排除极不稳定的基金）
}

# ============================================================================
# 风险指标计算参数
# ============================================================================

# 无风险利率（参考10年期国债收益率）
# v1.9.68 P1-S2: 更新为1.80%，基于2026年2月中国10年期国债收益率中枢
# 2026年2月24日: ~1.81%，近期低点: 1.78%（2月11日），中枢约1.80%
# 应每季度审视并更新
RISK_FREE_RATE = 0.018  # 1.80%

# 最低可接受回报（索提诺比率）
MAR = 0.0  # 0%

# 年交易日数
TRADING_DAYS_PER_YEAR = 252

# P1-06 / B7.4: Calmar 分母下限按风格区分。
# 原硬编码 0.005（0.5%）对"中短债 / 现金增强"这些日常回撤 <0.3% 的低波动债基
# 会把所有样本 cap 到同一分子，掩盖真实区分度。改为按 bond_style_classifier
# 的风格标签读字典；未知或其他风格回退 default=0.005。
CALMAR_MIN_DD_BY_STYLE = {
    "中短债": 0.002,
    "现金增强": 0.002,
    "综合纯债": 0.003,
    "固收指数": 0.003,
    "default": 0.005,
}

# 风险指标列表
RISK_METRICS = [
    'sharpe_ratio',      # 夏普比率
    'sortino_ratio',     # 索提诺比率
    'calmar_ratio',      # 卡玛比率
    'max_drawdown',      # 最大回撤
    'volatility',        # 波动率
    'consistency',       # 收益一致性
]

# ============================================================================
# 评分系统配置
# ============================================================================

# 短期潜力分权重（总计100%）
# 🔧 P0-E1修复: 重新平衡短期权重
# 旧版问题：收益指标占60%（1m=25% + 1w=20% + 3m=15%），风险调整仅40%
# 新版方案：删除return_1w（与return_1m相关性0.85），调整为收益40% + 风险调整60%
SHORT_TERM_WEIGHTS = {
    'return_1m': 0.20,     # 月涨幅：20%（↓5%）
    'return_3m': 0.20,     # 3月收益：20%（↑5%）
    # 'return_1w': 删除（与return_1m高度相关r=0.85）
    'sharpe_ratio': 0.30,  # 夏普比率：30%（↑10%）
    'sortino_ratio': 0.15, # 索提诺比率：15%（新增）
    'volatility': 0.15,    # 波动率：15%（↓5%，负向指标）
}

# 长期价值分权重（总计100%）
# v1.9.61 P1-3: 纳入 total_fee_rate 作为负向评分因子（Finominal 2024 研究）
# v1.9.67 P1-3: 费率权重 5%→8%，从 annual_return 减 3%（Morningstar: 费率预测力 > 所有其他因子）
# v1.9.68 P2-S3: 费率权重 8%→12%（Morningstar 2025 MPS 30%权重，逐步对齐行业标杆）
LONG_TERM_WEIGHTS = {
    'annual_return': 0.25,           # 年化收益：27% -> 25%（-2%给费率）
    'consistency': 0.23,             # 稳定性：25% -> 23%（-2%给费率）
    'max_drawdown': 0.15,            # 最大回撤：15%
    'max_drawdown_duration': 0.05,   # 最大回撤持续期：5%
    'sortino_ratio': 0.10,           # 索提诺比率：10%
    'calmar_ratio': 0.10,            # 卡玛比率：10%
    'total_fee_rate': 0.12,          # 费率因子：8% -> 12%
}

# 组合评分权重
SCORE_COMBO_WEIGHTS = {
    'short_term': 0.40,  # 短期潜力分：40%
    'long_term': 0.60,   # 长期价值分：60%
}

# ============================================================================
# P0-E3修复：混合归一化配置（相对排名90% + 绝对基准10%）
# ============================================================================

# 归一化权重配置
NORMALIZATION_WEIGHTS = {
    'relative': 0.90,  # 相对排名权重：90%
    'absolute': 0.10,  # 绝对基准权重：10%
}

# 绝对基准配置（定义各指标的"优秀"和"不合格"阈值）
# 格式：{指标名: {'good': 优秀值, 'bad': 不合格值, 'direction': 'positive'/'negative'}}
# 🔧 优化：基于行业数据重新校准（2013-2024债券型基金指数：年化5.05%，夏普1.24，回撤-5%）
ABSOLUTE_BASELINES = {
    # 收益率指标（正向，越高越好）- 基于行业中位数校准
    'return_1m': {'good': 0.8, 'bad': -0.3, 'direction': 'positive'},    # 月收益：0.8%优秀，-0.3%不合格
    'return_3m': {'good': 2.0, 'bad': -0.5, 'direction': 'positive'},    # 3月收益：2.0%优秀，-0.5%不合格
    'return_1y': {'good': 5.0, 'bad': 1.5, 'direction': 'positive'},     # v1.9.68 P2-S4: 年收益：6.0%→5.0%优秀，2%→1.5%不合格（适配低利率环境）
    'annual_return': {'good': 5.0, 'bad': 1.5, 'direction': 'positive'}, # v1.9.68 P2-S4: 年化收益：同上
    'main_return': {'good': 5.0, 'bad': 1.5, 'direction': 'positive'},   # v1.9.68 P2-S4: 主收益：同上

    # 风险调整指标（正向，越高越好）- 基于债券基金特性校准
    'sharpe_ratio': {'good': 0.8, 'bad': 0.3, 'direction': 'positive'},   # 夏普：0.8优秀，0.3不合格（债券中位数0.6-0.8）
    'sortino_ratio': {'good': 1.2, 'bad': 0.5, 'direction': 'positive'},  # 索提诺：1.2优秀，0.5不合格
    'calmar_ratio': {'good': 1.0, 'bad': 0.3, 'direction': 'positive'},   # 卡玛：1.0优秀，0.3不合格

    # 风险指标（负向，越低越好）- 统一为小数形式
    'volatility': {'good': 0.025, 'bad': 0.08, 'direction': 'negative'},   # 波动率：2.5%优秀，8%不合格
    'max_drawdown': {'good': 0.03, 'bad': 0.08, 'direction': 'negative'},  # 最大回撤：3%优秀，8%不合格
    'max_drawdown_duration': {'good': 30, 'bad': 250, 'direction': 'negative'},  # v1.9.67 P1-5: bad 365→250；单位为水下观测数，参数政策保持
    'consistency': {'good': 0.02, 'bad': 0.06, 'direction': 'negative'},   # 一致性：2%优秀，6%不合格

    # Step 7-6: 一致性V2指标（正向，越高越好）
    'positive_month_ratio': {'good': 0.65, 'bad': 0.45, 'direction': 'positive'},  # 正收益月占比：65%优秀，45%不合格

    # v1.9.61 P1-3: 费率因子（负向，越低越好）
    # v1.9.67 P1-4: good 0.30→0.25（优秀债基费率~0.20-0.25%），bad 1.00→0.80（对齐筛选阈值）
    'total_fee_rate': {'good': 0.25, 'bad': 0.80, 'direction': 'negative'},  # 持续持有费率：≤0.25%满分，≥0.80%零分
}

# ============================================================================
# 多维度排名维度
# ============================================================================

RANKING_DIMENSIONS = [
    'combo_score',        # 组合评分
    'sharpe_ratio',       # 夏普比率
    'max_drawdown',       # 最大回撤（负向）
    'consistency',        # 收益稳定性
    'annual_return',      # 年化收益
]

# ============================================================================
# 周期配置
# ============================================================================

# 长期周期参数 (年期)
PERIOD_CONFIGS = {
    1: {'days': 365, 'name': '1年期', 'output_name': '1年期'},
    2: {'days': 730, 'name': '2年期', 'output_name': '2年期'},
    3: {'days': 1095, 'name': '3年期', 'output_name': '3年期'},
    4: {'days': 1460, 'name': '4年期', 'output_name': '4年期'},
    5: {'days': 1825, 'name': '5年期', 'output_name': '5年期'},
}

# 短期周期参数 (天期/月期)
SHORT_TERM_PERIOD_CONFIGS = {
    '1week': {'days': 7, 'name': '最近一周', 'output_name': '一周期'},
    '1month': {'days': 30, 'name': '最近一月', 'output_name': '一月期'},
    '3month': {'days': 90, 'name': '最近3月', 'output_name': '三月期'},
    '6month': {'days': 180, 'name': '最近半年', 'output_name': '半年期'},
}

# 合并短期周期到主配置 (使用小数key避免与年期冲突)
PERIOD_CONFIGS.update({
    0.02: SHORT_TERM_PERIOD_CONFIGS['1week'],   # 7/365 ≈ 0.02年
    0.08: SHORT_TERM_PERIOD_CONFIGS['1month'],  # 30/365 ≈ 0.08年
    0.25: SHORT_TERM_PERIOD_CONFIGS['3month'],  # 90/365 ≈ 0.25年
    0.49: SHORT_TERM_PERIOD_CONFIGS['6month'],  # 180/365 ≈ 0.49年
})

# ============================================================================
# 短期投资筛选阈值配置
# ============================================================================

# 短期持有期阈值 (天数)
MAX_HOLDING_DAYS_SHORT_TERM = {
    0.02: 7,    # 一周期: ≤7天
    0.08: 30,   # 一月期: ≤30天
    0.25: 90,   # 三月期: ≤90天
    0.49: 180,  # 半年期: ≤180天
}

# 短期规模阈值 (亿元) - 提高门槛远离清盘线（中国公募基金清盘线通常为5000万）
SCALE_THRESHOLDS_SHORT_TERM = {
    0.02: 0.5,  # 一周: 0.3→0.5亿（远离清盘线）
    0.08: 0.8,  # 一月: 0.5→0.8亿
    0.25: 1.0,  # 三月: 0.5→1.0亿
    0.49: 1.5,  # 半年: 1.0→1.5亿（与1年期持平）
}

# 短期风险指标通过数 (n/6个指标需优于中位数)
RISK_METRIC_PASS_COUNT_SHORT_TERM = {
    0.02: 3,  # 一周: 3/6
    0.08: 3,  # 一月: 3/6
    0.25: 3,  # 三月: 3/6
    0.49: 4,  # 半年: 4/6 (接近长期要求)
}

# 短期异常值阈值 (收益率范围 %)
MAX_PERIOD_RETURN_SHORT_TERM = {
    0.02: {'days': 7, 'min': -2, 'max': 3},      # 周: -2% ~ +3%
    0.08: {'days': 30, 'min': -5, 'max': 8},     # 月: -5% ~ +8%
    0.25: {'days': 90, 'min': -8, 'max': 12},    # 季: -8% ~ +12%
    0.49: {'days': 180, 'min': -10, 'max': 15},  # 半年: -10% ~ +15%
}

# ============================================================================
# 短期投资评分权重配置
# ============================================================================

# 激进型权重 (适用于一周/一月 - 超短期投资)
# 🔧 策略优化：7d/30d周期的Sharpe比率统计意义不足（Lo 2002需≥56月），
# 降低sharpe_ratio权重(15%→5%)，将权重转移到波动率(20%→30%)
SHORT_TERM_WEIGHTS_AGGRESSIVE = {
    'return_1w': 0.30,      # 周涨幅: 30%
    'return_1m': 0.25,      # 月涨幅: 25%
    'return_3m': 0.10,      # 3月收益: 10%
    'sharpe_ratio': 0.05,   # 夏普比率: 5% (↓10%，短周期统计意义不足)
    'volatility': 0.30,     # 波动率: 30% (↑10%，短周期可直接观测，负向指标)
}

LONG_TERM_WEIGHTS_AGGRESSIVE = {
    'annual_return': 0.33,           # 年化收益: 35% -> 33%（-2%给费率）
    'consistency': 0.13,             # 稳定性: 15% -> 13%（-2%给费率）
    'max_drawdown': 0.20,            # 最大回撤: 20%
    'max_drawdown_duration': 0.05,   # 最大回撤持续期: 5%
    'sortino_ratio': 0.07,           # 索提诺比率: 7%
    'calmar_ratio': 0.10,            # 卡玛比率: 10%
    'total_fee_rate': 0.12,          # v1.9.68 P2-S3: 费率因子 8% -> 12%
}

SCORE_COMBO_WEIGHTS_AGGRESSIVE = {
    'short_term': 0.70,  # 短期潜力分: 70% (↑30%)
    'long_term': 0.30,   # 长期价值分: 30% (↓30%)
}

# 平衡型权重 (适用于三月/半年 - 中短期投资)
SHORT_TERM_WEIGHTS_BALANCED = {
    'return_1w': 0.15,      # 周涨幅: 15% (↓5%)
    'return_1m': 0.25,      # 月涨幅: 25% (↑5%)
    'return_3m': 0.25,      # 3月收益: 25% (↑10%)
    'sharpe_ratio': 0.20,   # 夏普比率: 20%
    'volatility': 0.15,     # 波动率: 15% (↓5%)
}

LONG_TERM_WEIGHTS_BALANCED = {
    'annual_return': 0.28,           # 年化收益: 30% -> 28%（-2%给费率）
    'consistency': 0.18,             # 稳定性: 20% -> 18%（-2%给费率）
    'max_drawdown': 0.20,            # 最大回撤: 20%
    'max_drawdown_duration': 0.05,   # 最大回撤持续期: 5%
    'sortino_ratio': 0.07,           # 索提诺比率: 7%
    'calmar_ratio': 0.10,            # 卡玛比率: 10%
    'total_fee_rate': 0.12,          # v1.9.68 P2-S3: 费率因子 8% -> 12%
}

SCORE_COMBO_WEIGHTS_BALANCED = {
    'short_term': 0.55,  # 短期潜力分: 55% (↑15%)
    'long_term': 0.45,   # 长期价值分: 45% (↓15%)
}

# 权重配置映射 (period_days -> weight_profile)
# 使用整数天数作为 key，避免浮点数精度问题导致匹配失败
WEIGHT_PROFILES = {
    7: 'aggressive',     # 一周 -> 激进型
    30: 'aggressive',    # 一月 -> 激进型
    90: 'balanced',      # 三月 -> 平衡型
    180: 'balanced',     # 半年 -> 平衡型
    # 长期（>=365天）默认使用 'standard'
}

# ============================================================================
# 并发配置
# ============================================================================

# 风险指标计算线程池大小
RISK_CALC_THREADS = 10

# 数据库连接池大小
DB_POOL_SIZE = 10

# ============================================================================
# Excel输出配置
# ============================================================================

# 工作表名称
EXCEL_SHEET_NAMES = {
    'results': '筛选结果',
    'overview': '筛选概览',
    'details': '详细数据',
    'reasons': '筛除原因',
    'statistics': '统计分析',
}

# Excel格式配置
EXCEL_FORMATS = {
    'header': {
        'bold': True,
        'bg_color': '#4F81BD',
        'font_color': 'white',
        'align': 'center',
        'valign': 'vcenter',
        'border': 1,
    },
    'number': {
        'num_format': '0.00',
    },
    'percent': {
        'num_format': '0.00%',
    },
    'integer': {
        'num_format': '0',
    },
}

# 列名中英文映射
COLUMN_NAME_MAPPING = {
    # 基础信息
    'fcode': '基金代码',
    'shortname': '基金名称',
    'ftype': '基金类型',
    'company_name': '基金公司',
    'estab_date': '成立日期',
    'establish_days': '成立天数',
    'manager_names': '基金经理',
    'rating': '评级',

    # 持有期信息
    'has_holding_period': '有持有期',
    'holding_days': '持有期天数',
    'holding_keywords': '持有期关键词',

    # 收益指标
    'main_return': '收益率(%)',
    'annual_return': '年化收益率(%)',
    'avg_return': '平均收益率(%)',
    'min_return': '最低收益率(%)',
    'max_return': '最高收益率(%)',
    'return_std': '收益率标准差(%)',
    'return_1w': '周涨幅(%)',
    'return_1m': '月涨幅(%)',
    'return_3m': '近3月(%)',
    'return_6m': '近6月(%)',
    'return_1y': '1年收益率(%)',
    'return_2y': '2年收益率(%)',
    'return_3y': '3年收益率(%)',
    'return_4y': '4年收益率(%)',
    'return_5y': '5年收益率(%)',
    'return_ytd': '今年以来(%)',
    'return_since': '成立以来(%)',

    # 规模指标
    'scale_billion': '基金规模(亿元)',

    # 净值信息
    'latest_nav': '最新净值',
    'latest_nav_date': '净值日期',
    'period_return_date': '区间收益基准日',
    'period_return_observed_at': '区间收益获取时间',
    'period_return_source': '区间收益来源',
    'period_return_source_sha256': '区间收益来源SHA256',

    # 风险指标
    'sharpe_ratio': '夏普比率',
    'sortino_ratio': '索提诺比率',
    'calmar_ratio': '卡玛比率',
    'max_drawdown': '最大回撤(%)',
    'max_drawdown_duration': '最长回撤观测数',  # v1.9.63: 新增
    'volatility': '波动率(%)',
    'consistency': '收益一致性',
    'positive_month_ratio': '正收益月占比',
    'pass_periods': '通过周期数',

    # 评分指标
    'short_term_score': '短期潜力分',
    'long_term_score': '长期价值分',
    'combo_score': '综合评分',
    'avg_combo_score': '平均综合评分',
    'min_combo_score': '最低综合评分',
    'max_combo_score': '最高综合评分',
    'score_std': '评分标准差',
    'score_1y': '1年评分',
    'score_2y': '2年评分',
    'score_3y': '3年评分',
    'score_4y': '4年评分',
    'score_5y': '5年评分',

    # 多周期分析字段
    'period_count': '出现周期数',
    'periods': '出现周期',
    'avg_sharpe': '平均夏普比率',
    'period_score': '周期数得分',
    'avg_score_normalized': '平均评分(标准化)',
    'stability_score': '稳定性得分',
    'comprehensive_score': '综合稳定性得分',
    'base_fund_name': '母基金名称',
    'share_class_count': '关联份额数',
    'share_class_names': '关联份额名称',
    'share_class_codes': '关联份额代码',

    # 排名字段
    'rank_combo': '综合排名',
    'rank_sharpe': '夏普排名',
    'rank_drawdown': '回撤排名',
    'rank_return': '收益排名',
    'rank_combo_score': '综合评分排名',
    'rank_sharpe_ratio': '夏普比率排名',
    'rank_max_drawdown': '最大回撤排名',
    'rank_consistency': '一致性排名',
    'rank_annual_return': '年化收益排名',

    # Step 3: 分桶统计字段
    'bucket_key': '子类型分组',
    'bucket_size': '组内基金数',
    'bucket_rank': '组内排名',
    'bucket_percentile': '组内百分位(%)',
    'is_fallback': '降级标记',

    # Step 4: 回撤阈值可追溯字段
    'threshold_used': '回撤阈值',
    'threshold_source': '阈值来源',
    'drawdown_mode': '回撤模式',
    'shadow_fail_2pct': '2%阈值淘汰',
    'shadow_fail_5pct': '5%阈值淘汰',

    # Step 4: 持续性窗口可追溯字段
    'windows_used': '使用窗口',
    'main_return_mapped_from': '主收益映射',

    # Step 5: 数据新鲜度字段
    'nav_latest_date': '最新净值日期(NAV)',
    'nav_age_days': '净值过期天数',
    'is_fresh': '数据新鲜',
    'freshness_reason': '新鲜度原因',

    # Step 5: 影子过期标记
    'shadow_age_fail_7d': '7天过期标记',
    'shadow_age_fail_14d': '14天过期标记',
    'shadow_age_fail_30d': '30天过期标记',
}

# 基金类型映射（将数字编码转换为中文名称）
FUND_TYPE_MAPPING = {
    '001': '股票型',
    '002': '混合型',
    '003': '债券型',
    '004': '货币型',
    '005': '指数型',
    '006': 'QDII',
    '007': 'FOF',
    '008': '理财型',
    # 数字类型（向后兼容）
    1: '股票型',
    2: '混合型',
    3: '债券型',
    4: '货币型',
    5: '指数型',
    6: 'QDII',
    7: 'FOF',
    8: '理财型',
}

# 百分比类型字段（需要×100显示）- 存储为小数形式如 0.0457
# 修复：max_drawdown已改回小数形式（如0.0837表示8.37%），需要×100显示
PERCENTAGE_FIELDS = [
    'volatility', 'consistency', 'max_drawdown',  # 都是小数形式，需×100显示
    'positive_month_ratio',  # Step 7-6: 0~1 小数，×100 显示为百分比
]

# 已经是百分比值的字段（不需要×100）- 存储为百分比如 30.26 表示 30.26%
PERCENTAGE_VALUE_FIELDS = [
    'main_return', 'annual_return',
    'avg_return', 'min_return', 'max_return',
    'return_1y', 'return_2y', 'return_3y', 'return_4y', 'return_5y',
    'return_std',  # 收益率标准差在CSV中已经是百分比值(5.26表示5.26%)
]

# ============================================================================
# 日志配置
# ============================================================================

# 日志级别
LOG_LEVEL = "INFO"

# 日志格式
LOG_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"

# 日志时间格式
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

# ============================================================================
# 调试配置
# ============================================================================

# 是否开启调试模式
DEBUG = False

# 是否保存中间结果
SAVE_INTERMEDIATE = True

# 是否打印详细日志
VERBOSE = True

# ============================================================================
# 每日筛选任务配置
# ============================================================================

# 每日自动筛选任务配置
DAILY_SCREENING_CONFIG = {
    # 长期分析周期（年期）
    'long_term_periods': [1, 2, 3, 4, 5],

    # 短期分析周期
    'short_term_periods': ['1week', '1month', '3month', '6month'],

    # 单周期正式分析默认输出全部达标基金；如需限量，仅允许显式实验参数覆盖。
    'top_n': None,

    # 累积跟踪文件路径（固定文件名，不带时间戳）
    'accumulation_tracker_file': OUTPUT_DIR / '优质基金累积跟踪.xlsx',

    # 输出目录
    'output_dir': OUTPUT_DIR,

    # 长期优质基金文件名模板（带时间戳）
    'long_term_output_template': '长期优质基金_{timestamp}.xlsx',

    # 短期优质基金文件名模板（带时间戳）
    'short_term_output_template': '近期优质基金_{timestamp}.xlsx',

    # 最终优质基金文件名模板（带时间戳）
    'final_output_template': '当日最终优质基金_{timestamp}.xlsx',

    # 详细报告文件名模板（带时间戳）
    'long_term_detail_template': '长期优质基金_详细报告_{timestamp}.xlsx',
    'short_term_detail_template': '近期优质基金_详细报告_{timestamp}.xlsx',
}

# 最终筛选（daily/stock/hybrid final_screening）策略配置
# 说明：
# - mainlist_anchor_limit: 仅在 fixed_top_n / score_band_topn_anchor 模式下作为主榜锚点数量
# - watchlist_limit: 单边观察池导出数量上限
# - short_score_weight/long_score_weight: 近期/长期分权重
# - hard_intersection_bonus: 硬交集奖励分（同时出现在近期+长期 TopN）
# - soft_intersection_bonus: 软交集奖励分（双方有分但至少一侧非TopN）
FINAL_SCREENING_POLICY = {
    'mainlist_anchor_limit': None,
    'watchlist_limit': 20,
    'short_score_weight': 0.35,
    'long_score_weight': 0.50,
    'hard_intersection_bonus': 15.0,
    'soft_intersection_bonus': 8.0,
    'mainlist_selection_mode': 'all_eligible',
    'mainlist_score_band_delta': 0.0,
    'bond_subtype_edge_governance_enabled': False,
    'bond_subtype_edge_warn_share_pct': 70.0,
    'bond_subtype_edge_max_replacements': 0,
    'bond_subtype_edge_score_gap_tolerance': 0.0,
    'drawdown_duration_analysis_window_days': 365,
    'drawdown_duration_nav_buffer_days': 30,
}

# 按基金类型覆盖最终筛选策略。
# 仅在存在明确验证结论时才为单一类型做覆盖，避免误伤其他链路。
FINAL_SCREENING_POLICY_BY_FUND_TYPE = {
    'bond': {
        # 审计结论：债券链路当前更稳妥的主榜口径是 hard=4 / soft=0。
        # 仅保留轻度硬交集优先，不再用软奖励抬升质量偏弱的邻近交集。
        'hard_intersection_bonus': 4.0,
        'soft_intersection_bonus': 0.0,
        # 当前项目目标是直观看到全部达标债基，不再对正式主榜做 TopN 截断。
        'mainlist_selection_mode': 'all_eligible',
        'mainlist_score_band_delta': 0.0,
        # 仅在主榜对子类型明显失衡时，允许用极少量 strict-match 非长债临界样本
        # 替换末位长债样本，避免把正式主榜完全锁死在单一长债风格。
        'bond_subtype_edge_governance_enabled': True,
        'bond_subtype_edge_warn_share_pct': 70.0,
        'bond_subtype_edge_max_replacements': 2,
        'bond_subtype_edge_score_gap_tolerance': 0.8,
    },
}

# ============================================================================
# 增强版四维评分系统配置
# ============================================================================

# 三种风险偏好模式的维度权重
ENHANCED_RISK_PROFILES = {
    'conservative': {
        'return': 0.20,      # 收益能力
        'risk': 0.40,        # 风险控制（保守型更重视）
        'stability': 0.25,   # 稳定性
        'liquidity': 0.15,   # 流动性
        'name_cn': '保守型',
        'description': '适合风险厌恶型投资者，更重视风险控制',
    },
    'balanced': {
        'return': 0.25,      # 收益能力
        'risk': 0.35,        # 风险控制
        'stability': 0.25,   # 稳定性
        'liquidity': 0.15,   # 流动性
        'name_cn': '平衡型',
        'description': '适合普通投资者，收益与风险均衡',
    },
    'aggressive': {
        'return': 0.35,      # 收益能力（进取型更重视）
        'risk': 0.25,        # 风险控制
        'stability': 0.25,   # 稳定性
        'liquidity': 0.15,   # 流动性
        'name_cn': '进取型',
        'description': '适合追求收益型投资者，更重视收益能力',
    },
}

# 收益能力分指标权重
ENHANCED_RETURN_WEIGHTS = {
    'annual_return': 0.40,      # 年化收益率
    'excess_return': 0.30,      # 超额收益（年化收益-无风险利率）
    'positive_ratio': 0.30,     # 正收益比率
}

# 风险控制分指标权重
ENHANCED_RISK_WEIGHTS = {
    'sharpe_ratio': 0.35,       # 夏普比率
    'sortino_ratio': 0.25,      # 索提诺比率
    'max_drawdown': 0.25,       # 最大回撤（负向）
    'var_95': 0.15,             # VaR 95%（负向）
}

# 稳定性分指标权重
ENHANCED_STABILITY_WEIGHTS = {
    'consistency': 0.40,        # 收益一致性（负向）
    'calmar_ratio': 0.30,       # 卡玛比率
    'multi_period': 0.30,       # 多周期一致性
}

# 流动性分指标权重
ENHANCED_LIQUIDITY_WEIGHTS = {
    'holding_period': 0.50,     # 持有期限制
    'scale': 0.30,              # 基金规模
    'maturity': 0.20,           # 成立时间
}

# 评分等级阈值
ENHANCED_GRADE_THRESHOLDS = {
    'A': {'min': 80, 'description': '优秀，强烈推荐'},
    'B': {'min': 65, 'description': '良好，推荐关注'},
    'C': {'min': 50, 'description': '中等，谨慎考虑'},
    'D': {'min': 35, 'description': '较差，不建议'},
    'E': {'min': 0, 'description': '差，避免'},
}

# 持有期评分分档
HOLDING_PERIOD_SCORES = {
    0: 100,      # 无持有期限制
    7: 90,       # ≤7天
    30: 75,      # ≤30天
    90: 50,      # ≤90天
    180: 25,     # ≤180天
    365: 10,     # >180天
}

# 规模评分分档（倒U型，2-10亿最优）
SCALE_SCORES = {
    0.5: 30,     # <0.5亿（规模过小）
    2: 70,       # 0.5-2亿（规模偏小）
    10: 100,     # 2-10亿（最优区间）
    50: 80,      # 10-50亿（规模偏大）
    100: 60,     # 50-100亿（规模较大）
    float('inf'): 40,  # >100亿（规模过大）
}

# 成熟度评分分档
MATURITY_SCORES = {
    365: 30,     # <1年（成立时间过短）
    730: 60,     # 1-2年
    1095: 80,    # 2-3年
    1825: 100,   # 3-5年（最优）
    float('inf'): 90,  # >5年（超长期略降）
}

# 默认风险偏好模式
DEFAULT_RISK_PROFILE = 'balanced'

# ============================================================================
# Step 3: 分桶统计配置（同类比较）
# ============================================================================

# 是否启用分桶统计（按 ftype 分组计算中位数）
BUCKETING_ENABLED = True

# 分桶键字段（funds 表中的字段名）
# 注意：sub_type 全部为空，只能使用 ftype
BUCKET_KEY_FIELD = 'ftype'

# 分桶最小样本数（低于此数时降级为全局比较）
BUCKET_MIN_SAMPLES = 20

# 样本不足时是否降级为全局比较
# True: 使用全局中位数作为比较基准
# False: 直接通过（不做相对比较）
BUCKET_FALLBACK_TO_GLOBAL = True

# 第10层最终排名模式
# 'global': 全局排名取 TopN
# 'bucket': 按 bucket 分配名额后取 Top
# 当前项目目标覆盖全部债券子类型（含一级债增强/二级债增强/转债增强），
# 因此默认使用 bucket 模式，减少低波长债对最终 Top 池的结构性挤占。
FINAL_RANKING_MODE = 'bucket'

# ============================================================================
# Step 4: 回撤阈值与持续性窗口配置（可追溯化）
# ============================================================================

# 短周期模式判定阈值（天数）
# period_days <= 此值时视为短周期模式
SHORT_TERM_MODE_THRESHOLD_DAYS = 180

# 回撤阈值配置（按周期分级）
# 短周期使用更严格的回撤控制，长周期保持默认
DRAWDOWN_THRESHOLDS = {
    'default': MAX_DRAWDOWN_THRESHOLD,      # 默认阈值：8%（长周期）
    'short_term': {
        7: 0.015,    # 1周: ≤1.5%
        30: 0.025,   # 1月: ≤2.5%
        90: 0.04,    # 3月: ≤4%
        180: 0.05,   # 半年: ≤5%
    },
}

# 影子评估阈值列表（仅用于评估，不影响实际筛选结果）
# 用于回答"如果阈值更严格会怎样"的问题
SHADOW_DRAWDOWN_THRESHOLDS = [0.02, 0.05]  # 2%, 5%

# 第8层持续性筛选使用的收益字段（显式配置）
# 保持现状：main_return + return_1y + return_6m（作为 fallback）
CONSISTENCY_RETURN_COLS = ['main_return', 'return_1y', 'return_6m']

# v1.9.57: 按周期自适应的持续性窗口映射（修复窗口重复问题）
# 规则：避免 main_return 与其映射源字段重复（如1年期 main_return=return_1y）
# 每个窗口中的字段应代表不同的时间维度
CONSISTENCY_RETURN_COLS_BY_PERIOD = {
    7:    ['main_return', 'return_1m', 'return_3m'],      # 超短期：周→月→季（去掉return_1w，与main_return重复）
    30:   ['main_return', 'return_3m', 'return_6m'],      # 短期：月→季→半年（去掉return_1m）
    90:   ['main_return', 'return_6m', 'return_1y'],      # 中短期：季→半年→年（去掉return_3m）
    180:  ['main_return', 'return_1y', 'return_3y'],      # 中期：半年→年→3年（去掉return_6m）
    365:  ['main_return', 'return_6m', 'return_3m'],      # 年度：1年→6月→3月（去掉return_1y）
    730:  ['main_return', 'return_1y', 'return_6m'],      # v1.9.62 P0-2: 2年→1年→6月（新增，覆盖1年维度）
    1095: ['main_return', 'return_1y', 'return_6m'],      # v1.9.62 P0-2: 3年→1年→6月（修复：原为6月→3月，缺少1年维度）
    9999: ['main_return', 'return_1y', 'return_6m'],      # 超长期：≥4年→1年→6月（return_3y≠main_return）
}


def get_consistency_return_cols(period_days: int) -> list:
    """根据周期天数获取适配的持续性窗口列"""
    for threshold in sorted(CONSISTENCY_RETURN_COLS_BY_PERIOD.keys()):
        if period_days <= threshold:
            return CONSISTENCY_RETURN_COLS_BY_PERIOD[threshold]
    return CONSISTENCY_RETURN_COLS  # fallback

# main_return 映射规则说明（供追溯参考）
# period_days <= 7:   main_return = return_1w
# period_days <= 30:  main_return = return_1m
# period_days <= 90:  main_return = return_3m
# period_days <= 180: main_return = return_6m
# period_days <= 365: main_return = return_1y
# period_days <= 730: main_return = return_2y
# ... 以此类推

# ============================================================================
# Step 5: 数据新鲜度与缺口检测配置（可追溯化）
# ============================================================================

# 新鲜度配置（控制净值数据时效性检查）
# 默认策略为 'soft'：仅标记不淘汰，保持筛选结果不变
FRESHNESS_CONFIG = {
    # 最大净值过期天数：超过此值的基金将被标记为"数据过期"
    # 默认7天（排除周末后约5个交易日）
    'max_nav_age_days': 7,

    # 是否跳过周末计算：True 时周六日不计入过期天数
    'skip_weekends': True,

    # 最大连续断档天数：净值序列中超过此值的空洞视为异常断档
    # 默认14天（排除节假日后约10个交易日）
    'max_gap_days': 14,

    # 新鲜度检查策略：
    # - 'soft': 仅标记（添加 is_fresh, freshness_reason 等列），不直接淘汰
    # - 'hard': 直接淘汰不新鲜的基金（reason_code = NAV_TOO_OLD 等）
    # 默认 'soft' 以保持向后兼容
    'strategy': 'soft',

    # 影子评估阈值列表（仅用于统计，不影响实际筛选结果）
    # 用于回答"不同新鲜度阈值下有多少基金会被淘汰"的问题
    'shadow_max_nav_age_days': [7, 14, 30],
}

# 新鲜度相关的 reason_code 定义（供筛选逻辑使用）
FRESHNESS_REASON_CODES = {
    'NAV_TOO_OLD': '净值数据过期',
    'NAV_DATA_GAP': '净值序列断档',
    'NO_NAV_HISTORY': '无净值历史记录',
    'INSUFFICIENT_NAV_COVERAGE': '净值覆盖率不足',
}

# ============================================================================
# Step 7-2: 影子评分 A/B 对照配置（不改变默认评分结果）
# ============================================================================

# 影子评分配置：在不改变主评分（90%相对+10%绝对）的前提下，
# 额外计算若干组不同 absolute_weight 的评分，用于回测 A/B 对照。
SCORING_SHADOW_CONFIG = {
    # 是否启用影子评分（False 时不生成任何 shadow 列）
    'enabled': True,

    # 影子对照组的绝对基准权重候选值
    # 基线仍严格使用 NORMALIZATION_WEIGHTS（0.90/0.10），此处仅为额外对照
    'absolute_weight_candidates': [0.20, 0.30],

    # 列名格式模板：{label} 会被替换为 abs20 / abs30 等
    # 生成列名示例：shadow_short_term_score_abs20, shadow_combo_score_abs30
    'label_fmt': 'abs{pct}',
}

# ============================================================================
# Step 7-4: 评分策略选择配置（可配置的生产策略）
# ============================================================================

# 评分策略配置：控制生产路径中 TopK 排序使用的 score_key。
# 默认仍使用 baseline combo_score（向后兼容）。
# 当 A/B 回测确认某个 shadow 策略更优时，可在 per_fund_type 中指定。
#
# 优先级：CLI override > per_fund_type > default_score_key
SCORING_POLICY = {
    # 默认评分列（未配置 per_fund_type 时使用）
    'default_score_key': 'combo_score',

    # 按 fund_type + period_years 指定不同 score_key
    # 格式：{ "fund_type": { "period_years_str": "score_key" } }
    # 示例（取消注释即可启用）：
    # 'per_fund_type': {
    #     'bond': {'3': 'shadow_combo_score_abs20', '5': 'combo_score'},
    #     'stock': {'3': 'combo_score'},
    #     'hybrid': {'3': 'shadow_combo_score_abs30'},
    # }
    'per_fund_type': {},
}

# ============================================================================
# Step 7-5: 运行时灰度开关 + 自我保护回退
# ============================================================================

# 运行时开关：控制 SCORING_POLICY 是否在生产路径中生效。
# 默认 enabled=False，确保零回归（selected_combo_score == combo_score）。
# 启用方式：
#   1) 直接改 enabled=True
#   2) 设置环境变量 SCORING_POLICY_ENABLED=true（优先于 enabled 字段）
#   3) 设置环境变量 SCORING_SCORE_KEY_OVERRIDE=shadow_combo_score_abs20（直接指定）
SCORING_POLICY_RUNTIME = {
    'enabled': False,                          # 默认关闭
    'env_toggle': 'SCORING_POLICY_ENABLED',    # 环境变量开关名
    'override_env': 'SCORING_SCORE_KEY_OVERRIDE',  # 环境变量直接指定 score_key
}

# 自我保护阈值：当选中的 score_key 不满足以下条件时，自动 fallback 到 combo_score。
SCORING_POLICY_GUARDS = {
    'min_coverage': 0.80,      # 非空覆盖率 < 80% -> fallback
    'min_rows': 200,           # 样本行数 < 200 -> fallback
    'min_std_nonzero': True,   # std=0（全部相同分数）-> fallback
}

# ============================================================================
# Step 7-6: 一致性V2 shadow 评分配置
# ============================================================================

# 一致性口径替换版 shadow 评分：在 long_term_score 中用 positive_month_ratio
# 替代原 consistency（负向月度标准差）的 25% 权重，生成 shadow_combo_score_cstv2。
# baseline combo_score 不受影响。
CONSISTENCY_V2_CONFIG = {
    'enabled': True,           # 是否生成 shadow_combo_score_cstv2 列
    'label': 'cstv2',          # shadow 列后缀标签
}

# ============================================================================
# Step 7-8: 运行时推荐策略注入（Recommended Runtime Policy Injection）
# ============================================================================

# 允许将 run_ab_matrix 生成的推荐 score_key 以运行时方式注入筛选流程。
# 不改变默认 SCORING_POLICY，仅在 enabled=True 时按 fund_type 覆盖 score_key。
# 优先级低于 CLI override 和 SCORING_POLICY_RUNTIME override_env，
# 但高于静态 SCORING_POLICY。
SCORING_POLICY_RECOMMENDED = {
    'enabled': False,          # 默认关闭，保证零回归
    'per_fund_type': {
        # 允许留空，表示不用推荐
        # 填入 run_ab_matrix RecommendedPolicy 片段中的推荐值即可
        # "bond": "shadow_combo_score_cstv2",
        # "stock": "shadow_combo_score_abs30",
        # "hybrid": "combo_score",
    },
    'source': 'run_ab_matrix',  # 仅用于追溯：标记推荐来源
}

# ============================================================================
# Step 7-16: 受控自动回滚策略（Auto Rollback Only — 永不自动 Promote）
# ============================================================================

# 当 daily_policy_monitor 检测到 verdict=rollback 且满足红线条件时，
# 系统可自动执行 rollback patch。Promote 永远需要人工触发。
# 默认严格关闭（零回归），需同时开启 enabled 并配置 token 才能生效。
AUTO_ROLLBACK_POLICY = {
    'enabled': False,                   # 默认 False：零回归
    'min_verdict': 'rollback',          # 只有 verdict 达到 rollback 才允许自动回滚
    'allow_on_rollback_suggested': False,  # 默认 False：更保守
    'cooldown_hours': 24,               # 冷却期：上次成功 rollback 后 24h 内不再自动回滚
    'require_token': True,              # 强制要求 token
    'token_env': 'POLICY_PATCH_TOKEN',  # 从环境变量读取 token（不写入日志）
    'max_rollbacks_per_day': 1,         # 每自然日最多 1 次成功自动回滚
    'dry_run_first': True,              # 自动回滚前先 dry-run 验证（失败则不执行）
    'min_runs_in_window': 3,            # 监控数据不足（runs_count < 3）则不自动回滚
}

# ============================================================================
# Step 8-5: 可投性 Guardrails 配置
# ============================================================================
# 不改变筛选逻辑，仅在 pick 级别打标（flag）+ 前端提示。
# verdict: ok=所有检查通过, warn=存在警告性风险, avoid=存在严重风险
INVESTABILITY_CONFIG = {
    # 净值新鲜度阈值（天数，排除周末后）
    'stale_nav_days': 10,

    # 最低数据覆盖率（0-1）
    'min_coverage_ratio': 0.80,

    # 极端回撤硬限（按 fund_type）
    'extreme_drawdown': {
        'bond': -0.15,          # 债券型：超过 -15% 视为极端
        'bond_fixed': -0.12,    # v1.9.62 P1-3: 定开债风控更严格
        'stock': -0.50,         # 股票型：超过 -50% 视为极端
        'hybrid': -0.35,        # 混合型：超过 -35% 视为极端
        'index': -0.45,         # v1.9.62 P1-3: 指数型略低于主动股票型
    },

    # 最小可投规模（亿元，按 fund_type）
    'min_scale': {
        'bond': 0.5,
        'bond_fixed': 0.5,     # v1.9.62 P1-3
        'stock': 1.0,
        'hybrid': 0.8,
        'index': 0.5,          # v1.9.62 P1-3
    },
}


# ============================================================================
# v1.9.68 P2-S5: 权重配置验证函数（模块加载时自动执行）
# ============================================================================

def _validate_weights() -> None:
    """启动时验证所有权重配置总和为 1.0，防止权重配置错误导致评分偏差。"""
    weight_dicts = {
        'SHORT_TERM_WEIGHTS': SHORT_TERM_WEIGHTS,
        'LONG_TERM_WEIGHTS': LONG_TERM_WEIGHTS,
        'SCORE_COMBO_WEIGHTS': SCORE_COMBO_WEIGHTS,
        'SHORT_TERM_WEIGHTS_AGGRESSIVE': SHORT_TERM_WEIGHTS_AGGRESSIVE,
        'LONG_TERM_WEIGHTS_AGGRESSIVE': LONG_TERM_WEIGHTS_AGGRESSIVE,
        'SCORE_COMBO_WEIGHTS_AGGRESSIVE': SCORE_COMBO_WEIGHTS_AGGRESSIVE,
        'SHORT_TERM_WEIGHTS_BALANCED': SHORT_TERM_WEIGHTS_BALANCED,
        'LONG_TERM_WEIGHTS_BALANCED': LONG_TERM_WEIGHTS_BALANCED,
        'SCORE_COMBO_WEIGHTS_BALANCED': SCORE_COMBO_WEIGHTS_BALANCED,
    }
    for name, d in weight_dicts.items():
        total = sum(d.values())
        assert abs(total - 1.0) < 1e-6, (
            f"{name} 权重总和={total:.6f}，期望=1.0。"
            f"请检查权重配置，确保各项之和恰好为1.0"
        )


_validate_weights()  # 模块加载时自动执行，任何权重错误立即报错
