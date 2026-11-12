"""
数据库模型定义 - SQLAlchemy ORM
"""

from sqlalchemy import (
    Column, String, Integer, Float, Boolean, Date, DateTime, Text,
    Index, UniqueConstraint, create_engine
)
from sqlalchemy.orm import declarative_base
from sqlalchemy.sql import func
from sqlalchemy.types import TypeDecorator
from datetime import datetime

from .screening_pick_payloads import decode_snapshot, encode_snapshot


class PickSnapshotText(TypeDecorator):
    """Keep the TEXT schema while compacting SQLite storage transparently."""
    impl = Text
    cache_ok = True

    def process_bind_param(self, value, dialect):
        return encode_snapshot(value) if dialect.name == "sqlite" else decode_snapshot(value)

    def process_result_value(self, value, dialect):
        return decode_snapshot(value)


Base = declarative_base()


class Fund(Base):
    """基金基础信息表"""
    __tablename__ = 'funds'

    fcode = Column(String(10), primary_key=True, comment='基金代码')
    shortname = Column(String(100), nullable=False, comment='基金简称')
    fullname = Column(String(200), comment='基金全称')
    ftype = Column(String(50), comment='基金类型')
    sub_type = Column(String(30), comment='子类型')

    # 通用基金类型支持 - 新增字段
    type_code = Column(String(10), comment='API类型代码(25股票/26指数/27混合/31债券/32定开债/35货币/36QDII/37FOF)')
    type_category = Column(String(30), comment='基金大类(equity/fixed_income/money_market/hybrid/alternative/multi_asset)')
    risk_level = Column(Integer, comment='风险等级1-5')
    rating = Column(String(10), comment='评级')
    estab_date = Column(Date, comment='成立日期')
    scale_billion = Column(Float, comment='基金规模(亿元)')

    # 持有期信息
    has_holding_period = Column(Integer, default=0, comment='是否有持有期限制 0/1')
    holding_days = Column(Integer, comment='持有期天数')
    holding_keywords = Column(String(50), comment='持有期关键词')
    # P1-01 / B13: 从 /fundMNDetailInformation 返回的权威结构化字段落库。
    # 用途：供 supplement_holding_period 的 L1 `detail_struct` 路径直接取值，
    # 不再仅依赖名称关键词 (L2) 或策略文本 (L3)。
    cycle = Column(String(50), comment='封闭期 / 持有期描述（CYCLE）')
    preserve_period = Column(String(50), comment='定开 / 保本周期天数或年数（PRSVPERIOD）')
    preserve_date = Column(String(50), comment='保本到期日（PRSVDATE）')
    preserve_type = Column(String(50), comment='保本类型（PRSVTYPE）')
    buy_time = Column(String(50), comment='买入次日起算持有天数（BUYTIME）')
    feature_code = Column(String(20), comment='FEATURE 原始特色代码')
    base_fund_type_code = Column(String(10), comment='BFUNDTYPE 基金基础分类代码')
    rating_shanghai = Column(String(10), comment='RLEVEL_SZ 上海证券评级')
    holding_period_source = Column(String(20), comment='持有期识别来源（detail_struct|name|strategy_text|share_family|no_signal_reviewed）')
    # P1-09.2 / B14: 外部候选池标注（展示维度，不参与评分）。
    # 典型值：'tiantian_038'（天天基金"优质债券"筛选方案）。
    # 多个来源用 '+' 分隔，例如 'tiantian_038+tiantian_022'。
    external_curation_flag = Column(String(40), comment='外部候选池标注（天天基金 CLTYPE=038 等）')
    # P1-09.3 / B14: 三机构评级聚合（来自 /fundGradeDetail），仅补齐 funds.rating 空洞。
    rating_multi_source = Column(String(40), comment='招商证券/上海证券/济安金信 最近一期聚合评级')
    # P1-10 / B15: 对无法由当前真实 API 复证 data_source 的历史残留样本打治理标签。
    # 注意：该字段不替代 data_source，不参与评分，只用于显式区分
    # “真实来源已知” 与 “API 未复证的历史残留”。
    data_source_governance_flag = Column(String(60), comment='数据来源治理标签（如 api_unreconciled_historical_residual）')

    company_id = Column(String(20), comment='基金公司ID')
    company_name = Column(String(100), comment='基金公司名称')
    manager_ids = Column(Text, comment='基金经理ID列表(JSON)')
    manager_names = Column(Text, comment='基金经理姓名')

    min_purchase = Column(Float, comment='起购金额')
    mgr_fee_rate = Column(Float, comment='管理费率')
    trust_fee_rate = Column(Float, comment='托管费率')
    sales_fee_rate = Column(Float, comment='销售服务费率')

    benchmark = Column(Text, comment='业绩比较基准')
    investment_target = Column(Text, comment='投资目标')
    investment_strategy = Column(Text, comment='投资策略')

    # P0-R2新增字段: 从fundMNDetailInformation API采集的有价值字段
    custodian_bank = Column(String(100), comment='托管银行')
    net_asset = Column(Float, comment='净资产（亿元）')
    feature_tags = Column(String(200), comment='特色标签（如：低风险/稳定收益）')
    scale_report_date = Column(Date, comment='规模报告期')

    is_buyable = Column(Boolean, nullable=True, comment='是否可购买；未观察到明确标志时为NULL')
    purchase_status = Column(String(20), comment='申购状态')
    redemption_status = Column(String(20), comment='赎回状态')

    data_source = Column(String(50), comment='数据来源')
    first_collected_at = Column(DateTime, default=datetime.now, comment='首次采集时间')
    last_updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, comment='最后更新时间')
    detail_updated_at = Column(DateTime, comment='基金详情最近成功采集时间')
    manager_list_updated_at = Column(DateTime, comment='基金经理名单最近确认并成功保存时间')
    manager_list_attempted_at = Column(DateTime, comment='基金经理名单最近请求尝试开始时间，不代表成功')
    manager_list_last_status = Column(String(50), comment='基金经理名单最近尝试状态，历史未知保持NULL')

    __table_args__ = (
        Index('idx_ftype', 'ftype'),
        Index('idx_type_code', 'type_code'),
        Index('idx_type_category', 'type_category'),
        Index('idx_sub_type', 'sub_type'),
        Index('idx_company', 'company_id'),
        Index('idx_rating', 'rating'),
        Index('idx_estab_date', 'estab_date'),
        Index('idx_fund_scale', 'scale_billion'),
    )

    def __repr__(self):
        return f"<Fund(fcode='{self.fcode}', name='{self.shortname}', type='{self.ftype}')>"


class FundNavHistory(Base):
    """基金净值历史表 - 核心表，预计875万条记录"""
    __tablename__ = 'fund_nav_history'

    id = Column(Integer, primary_key=True, autoincrement=True)
    fcode = Column(String(10), nullable=False, comment='基金代码')
    nav_date = Column(Date, nullable=False, comment='净值日期')
    unit_nav = Column(Float, nullable=False, comment='单位净值')
    accum_nav = Column(Float, comment='累计净值')
    daily_growth = Column(Float, comment='日增长率%')

    __table_args__ = (
        UniqueConstraint('fcode', 'nav_date', name='uk_nav_fcode_date'),
        Index('idx_nav_date', 'nav_date'),
        # idx_nav_fcode / idx_nav_fcode_date 已移除：
        # - (fcode, nav_date) 唯一约束已覆盖当前运行路径的主要访问模式
        # - 遗留按 fcode 单列计数路径已迁出当前链路，后续仅保留审计证据
    )

    def __repr__(self):
        return f"<NavHistory(fcode='{self.fcode}', date='{self.nav_date}', nav={self.unit_nav})>"


class FundReturns(Base):
    """基金收益指标表"""
    __tablename__ = 'fund_returns'

    fcode = Column(String(10), primary_key=True, comment='基金代码')

    latest_nav = Column(Float, comment='最新净值')
    latest_nav_date = Column(Date, comment='最新净值日期')
    daily_growth = Column(Float, comment='日涨幅%')

    return_1w = Column(Float, comment='近1周%')
    return_1m = Column(Float, comment='近1月%')
    return_3m = Column(Float, comment='近3月%')
    return_6m = Column(Float, comment='近6月%')
    return_1y = Column(Float, comment='近1年%')
    return_2y = Column(Float, comment='近2年%')
    return_3y = Column(Float, comment='近3年%')
    return_5y = Column(Float, comment='近5年%')
    return_ytd = Column(Float, comment='今年来%')
    return_since = Column(Float, comment='成立来%')

    # NULL on legacy rows: neither NAV dates nor row update times prove the
    # observation date of an old period return.
    period_return_date = Column(Date, comment='区间收益API业务基准日')
    period_return_observed_at = Column(DateTime, comment='区间收益API获取时间')
    period_return_source = Column(String(50), comment='区间收益API来源')
    period_return_source_sha256 = Column(String(64), comment='本次基金响应SHA256')
    period_return_source_json = Column(Text, comment='最新一次已接受基金响应，非历史明细')
    period_return_attempted_at = Column(DateTime, comment='周期收益补采最近尝试')
    period_return_last_status = Column(String(50), comment='周期收益补采真实终态')
    period_return_rejection_json = Column(Text, comment='隔离的上游占位响应及NAV身份，不覆盖已接受收益')
    period_return_rejection_sha256 = Column(String(64), comment='上游隔离证据SHA256')

    last_updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, comment='更新时间')

    __table_args__ = (
        Index('idx_returns_1y', 'return_1y'),
        Index('idx_returns_3y', 'return_3y'),
        Index('idx_returns_latest_nav_date', 'latest_nav_date'),
    )

    def __repr__(self):
        return f"<Returns(fcode='{self.fcode}', 1y={self.return_1y}%, 3y={self.return_3y}%)>"


class FundScale(Base):
    """基金规模表
    TODO: 未启用，规模数据直接存储在 Fund.scale_billion 字段。
    保留此模型以支持未来按报告期追踪规模变化的需求。
    """
    __tablename__ = 'fund_scale'

    id = Column(Integer, primary_key=True, autoincrement=True)
    fcode = Column(String(10), nullable=False, comment='基金代码')
    report_date = Column(Date, nullable=False, comment='报告期')

    total_asset = Column(Float, comment='总资产（亿元）')
    net_asset = Column(Float, comment='净资产（亿元）')
    total_share = Column(Float, comment='总份额（亿份）')

    __table_args__ = (
        UniqueConstraint('fcode', 'report_date', name='uk_scale_fcode_report_date'),
        Index('idx_scale_fcode', 'fcode'),
        Index('idx_scale_report_date', 'report_date'),
    )

    def __repr__(self):
        return f"<Scale(fcode='{self.fcode}', date='{self.report_date}', asset={self.net_asset}亿)>"


class FundRiskMetrics(Base):
    """基金风险指标表"""
    __tablename__ = 'fund_risk_metrics'

    id = Column(Integer, primary_key=True, autoincrement=True)
    fcode = Column(String(10), nullable=False, comment='基金代码')
    period = Column(String(10), nullable=False, comment='计算周期: 1y/3y/5y')

    # 8个核心风险指标
    volatility = Column(Float, comment='波动率（年化）')
    max_drawdown = Column(Float, comment='最大回撤%')
    max_drawdown_duration = Column(Integer, comment='同一风险财富序列最长连续水下观测数，含期末未恢复段；不是自然天数')
    annual_return = Column(Float, comment='年化收益率%')
    sharpe_ratio = Column(Float, comment='夏普比率')
    sortino_ratio = Column(Float, comment='索提诺比率')
    raw_sharpe_ratio = Column(Float, comment='原始夏普比率（未做展示封顶）')
    raw_sortino_ratio = Column(Float, comment='原始索提诺比率（未做展示封顶）')
    sharpe_ratio_cap_hit = Column(Boolean, comment='夏普比率是否触发展示封顶')
    sortino_ratio_cap_hit = Column(Boolean, comment='索提诺比率是否触发展示封顶')
    calmar_ratio = Column(Float, comment='卡玛比率')
    var_95 = Column(Float, comment='VaR 95%')
    var_99 = Column(Float, comment='VaR 99%')
    positive_ratio = Column(Float, comment='正收益比率')

    data_points = Column(Integer, comment='数据点数量')
    start_date = Column(Date, comment='计算起始日期')
    end_date = Column(Date, comment='计算结束日期')
    nav_snapshot_signature = Column(String(64), comment='对应 fund_returns 快照关键字段签名')
    formula_contract_json = Column(Text, comment='风险策略公式版本、实际参数和输入来源；旧行保持NULL')
    undefined_metric_reasons_json = Column(Text, comment='有效窗口中未定义风险比率的原因；旧记录未知时保留NULL')
    calculated_at = Column(DateTime, default=datetime.now, comment='计算时间')

    __table_args__ = (
        UniqueConstraint('fcode', 'period', name='uk_risk_fcode_period'),
        Index('idx_risk_fcode', 'fcode'),
        Index('idx_risk_period_end_date_fcode', 'period', 'end_date', 'fcode'),
        Index('idx_risk_period_fcode_end_date', 'period', 'fcode', 'end_date'),
        Index('idx_risk_sharpe', 'sharpe_ratio'),
        Index('idx_risk_max_drawdown', 'max_drawdown'),
    )

    def __repr__(self):
        return f"<RiskMetrics(fcode='{self.fcode}', period='{self.period}', sharpe={self.sharpe_ratio})>"


class FundGrade(Base):
    """P1-09.3 / B14: 三机构基金评级历史表。

    对应 tiantian ``/fundGradeDetail`` 返回的 ``RDATE / ZSPJ / SZPJ3 / JAPJ``
    分别是 (评级日期, 招商证券评级, 上海证券评级, 济安金信评级)；按 (fcode, rdate)
    唯一，保留历史。``funds.rating_multi_source`` 字段只缓存最近一期的聚合值。
    """

    __tablename__ = 'fund_grades'

    id = Column(Integer, primary_key=True, autoincrement=True)
    fcode = Column(String(10), nullable=False, comment='基金代码')
    rdate = Column(Date, nullable=False, comment='评级日期')
    zspj_zs = Column(String(10), comment='招商证券评级（1-5）')
    szpj3 = Column(String(10), comment='上海证券评级（1-5）')
    japj = Column(String(10), comment='济安金信评级（1-5）')
    htpj = Column(String(10), comment='海通证券评级（1-5，可选）')
    created_at = Column(DateTime, default=datetime.now, comment='入库时间')

    __table_args__ = (
        UniqueConstraint('fcode', 'rdate', name='uk_fund_grade_fcode_rdate'),
        Index('idx_fund_grade_fcode', 'fcode'),
        Index('idx_fund_grade_rdate', 'rdate'),
    )

    def __repr__(self):
        return f"<FundGrade(fcode='{self.fcode}', rdate='{self.rdate}', zs={self.zspj_zs}, sh={self.szpj3}, ja={self.japj})>"


class FundManager(Base):
    """基金经理表"""
    __tablename__ = 'fund_managers'

    mgr_id = Column(String(20), primary_key=True, comment='经理ID')
    name = Column(String(50), nullable=False, comment='姓名')
    resume = Column(Text, comment='简历')

    total_days = Column(Integer, comment='从业天数')
    manage_scale = Column(Float, comment='管理规模（亿元）')
    fund_count = Column(Integer, comment='在管基金数')

    annual_return = Column(Float, comment='年化回报%')
    max_profit = Column(Float, comment='最大盈利%')
    max_drawdown = Column(Float, comment='最大回撤%')

    score_overall = Column(Float, comment='综合评分')
    score_experience = Column(Float, comment='经验值评分')
    score_return = Column(Float, comment='收益率评分')

    company_id = Column(String(20), comment='所属公司ID')
    company_name = Column(String(100), comment='所属公司名称')
    photo_url = Column(String(500), comment='头像URL')

    last_updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, comment='更新时间')

    __table_args__ = (
        Index('idx_manager_company', 'company_id'),
        Index('idx_manager_score', 'score_overall'),
    )

    def __repr__(self):
        return f"<Manager(id='{self.mgr_id}', name='{self.name}', funds={self.fund_count})>"


class FundCompany(Base):
    """基金公司表"""
    __tablename__ = 'fund_companies'

    company_id = Column(String(20), primary_key=True, comment='公司ID')
    company_name = Column(String(100), nullable=False, comment='公司名称')
    full_name = Column(String(200), comment='法定名称')

    legal_rep = Column(String(50), comment='法人代表')
    ceo = Column(String(50), comment='总经理')
    establish_date = Column(Date, comment='成立日期')
    registered_capital = Column(String(50), comment='注册资本')

    total_scale = Column(Float, comment='管理规模（亿元）')
    fund_count = Column(Integer, comment='基金总数')
    manager_count = Column(Integer, comment='基金经理人数')

    address = Column(Text, comment='办公地址')
    hotline = Column(String(50), comment='客服电话')

    last_updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, comment='更新时间')

    __table_args__ = (
        Index('idx_company_total_scale', 'total_scale'),
    )

    def __repr__(self):
        return f"<Company(id='{self.company_id}', name='{self.company_name}', funds={self.fund_count})>"


class CollectionLog(Base):
    """采集日志表"""
    __tablename__ = 'collection_log'

    id = Column(Integer, primary_key=True, autoincrement=True)
    task_type = Column(String(50), comment='任务类型')
    fcode = Column(String(10), comment='基金代码（如适用）')
    status = Column(String(20), comment='状态: success/failed/pending')
    message = Column(Text, comment='错误信息或备注')
    retry_count = Column(Integer, default=0, comment='重试次数')
    created_at = Column(DateTime, default=datetime.now, comment='创建时间')

    __table_args__ = (
        Index('idx_log_task_type', 'task_type'),
        Index('idx_log_status', 'status'),
        Index('idx_log_fcode', 'fcode'),
        Index('idx_log_created_at', 'created_at'),
    )

    def __repr__(self):
        return f"<Log(id={self.id}, task='{self.task_type}', status='{self.status}')>"


class TaskStatus(Base):
    """任务状态表（用于防止任务冲突）"""
    __tablename__ = 'task_status'

    task_name = Column(String(50), primary_key=True, comment='任务名称')
    status = Column(String(20), comment='状态: running/completed/failed')
    started_at = Column(DateTime, comment='开始时间')
    completed_at = Column(DateTime, comment='完成时间')
    progress = Column(Integer, default=0, comment='进度百分比')
    error_message = Column(Text, comment='错误信息')

    def __repr__(self):
        return f"<TaskStatus(task='{self.task_name}', status='{self.status}', progress={self.progress}%)>"


class TaskHistory(Base):
    """任务执行历史记录表"""
    __tablename__ = 'task_history'

    id = Column(Integer, primary_key=True, autoincrement=True, comment='记录ID')
    task_name = Column(String(50), nullable=False, comment='任务名称')
    status = Column(String(20), nullable=False, comment='执行状态: success/failed')
    started_at = Column(DateTime, nullable=False, comment='开始时间')
    completed_at = Column(DateTime, comment='完成时间')
    duration = Column(Integer, comment='执行耗时(秒)')

    # 执行结果统计
    records_processed = Column(Integer, comment='处理记录数')
    records_inserted = Column(Integer, comment='插入记录数')
    records_updated = Column(Integer, comment='更新记录数')
    records_failed = Column(Integer, comment='失败记录数')

    # 错误信息
    error_message = Column(Text, comment='错误信息')
    error_traceback = Column(Text, comment='错误堆栈')

    # 额外数据（JSON格式）
    extra_data = Column(Text, comment='额外元数据(JSON)')

    __table_args__ = (
        Index('idx_task_history_name', 'task_name'),
        Index('idx_task_history_started', 'started_at'),
        Index('idx_task_history_status', 'status'),
    )

    def __repr__(self):
        return f"<TaskHistory(id={self.id}, task='{self.task_name}', status='{self.status}')>"


class FundAnnualReturns(Base):
    """基金年度涨幅历史表 - P0-R2新增"""
    __tablename__ = 'fund_annual_returns'

    id = Column(Integer, primary_key=True, autoincrement=True)
    fcode = Column(String(10), nullable=False, comment='基金代码')
    year = Column(Integer, nullable=False, comment='年份')
    annual_return = Column(Float, comment='年度涨幅%')
    rank_in_category = Column(Integer, comment='同类排名')
    category_count = Column(Integer, comment='同类总数')
    percentile = Column(Float, comment='排名百分位（rank/total）')
    created_at = Column(DateTime, default=datetime.now, comment='采集时间')

    __table_args__ = (
        UniqueConstraint('fcode', 'year', name='uk_annual_fcode_year'),
        Index('idx_annual_fcode', 'fcode'),
        Index('idx_annual_year', 'year'),
        Index('idx_annual_return', 'annual_return'),
    )

    def __repr__(self):
        return f"<AnnualReturn(fcode='{self.fcode}', year={self.year}, return={self.annual_return}%)>"


class FundRankHistory(Base):
    """基金同类排名历史表 - P0-R2新增"""
    __tablename__ = 'fund_rank_history'

    id = Column(Integer, primary_key=True, autoincrement=True)
    fcode = Column(String(10), nullable=False, comment='基金代码')
    rank_date = Column(Date, nullable=False, comment='排名日期')
    period = Column(String(10), comment='排名周期（1m/3m/6m/1y/3y）')
    rank_position = Column(Integer, comment='当前排名')
    category_total = Column(Integer, comment='同类总数')
    percentile = Column(Float, comment='排名百分位（rank/total）')
    created_at = Column(DateTime, default=datetime.now, comment='采集时间')

    __table_args__ = (
        UniqueConstraint('fcode', 'rank_date', 'period', name='uk_rank_fcode_date_period'),
        Index('idx_rank_fcode', 'fcode'),
        Index('idx_rank_date', 'rank_date'),
        Index('idx_rank_percentile', 'percentile'),
    )

    def __repr__(self):
        return f"<RankHistory(fcode='{self.fcode}', date='{self.rank_date}', rank={self.rank_position}/{self.category_total})>"


class User(Base):
    """用户表 - API认证系统"""
    __tablename__ = 'users'

    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String(50), unique=True, nullable=False, comment='用户名')
    password_hash = Column(String(128), nullable=False, comment='密码哈希')
    role = Column(String(20), default='user', comment='角色: admin/user/readonly')
    is_active = Column(Boolean, default=True, comment='是否激活')

    created_at = Column(DateTime, default=datetime.now, comment='创建时间')
    last_login_at = Column(DateTime, comment='最后登录时间')

    __table_args__ = (
        Index('idx_user_username', 'username'),
    )

    def __repr__(self):
        return f"<User(id={self.id}, username='{self.username}', role='{self.role}')>"


class ScreeningRun(Base):
    """筛选运行记录表 - 记录每次筛选的元数据"""
    __tablename__ = 'screening_runs'

    id = Column(Integer, primary_key=True, autoincrement=True, comment='运行ID')
    run_id = Column(String(36), unique=True, nullable=False, comment='UUID运行标识')

    # 时间信息
    started_at = Column(DateTime, nullable=False, comment='开始时间')
    finished_at = Column(DateTime, comment='结束时间')
    status = Column(String(20), default='running', comment='状态: running/success/failed')

    # 筛选参数
    period_years = Column(Float, comment='分析周期（年）')
    fund_type = Column(String(30), comment='基金类型(bond/stock/hybrid)')
    profile_name = Column(String(50), comment='使用的Profile名称')

    # 数据新鲜度
    data_cutoff_date = Column(Date, comment='数据截止日期')
    nav_latest_date = Column(Date, comment='最新净值日期')

    # 统计信息
    fund_count_in = Column(Integer, comment='输入基金数')
    fund_count_pass = Column(Integer, comment='通过筛选数')
    fund_count_fail = Column(Integer, comment='未通过数')

    # 配置快照
    config_hash = Column(String(64), comment='配置哈希(SHA256)')
    config_snapshot_json = Column(Text, comment='配置快照(JSON)')

    # 版本信息
    code_version = Column(String(50), comment='代码版本号')

    # 失败信息
    fail_reason = Column(Text, comment='失败原因')

    __table_args__ = (
        Index('idx_screening_run_id', 'run_id'),
        Index('idx_screening_started', 'started_at'),
        Index('idx_screening_status', 'status'),
        Index('idx_screening_period', 'period_years'),
    )

    def __repr__(self):
        return f"<ScreeningRun(id={self.id}, period={self.period_years}y, status='{self.status}')>"


class ScreeningDecision(Base):
    """筛选决策记录表 - 记录每只基金在每层的筛选结果"""
    __tablename__ = 'screening_decisions'

    id = Column(Integer, primary_key=True, autoincrement=True, comment='记录ID')
    run_id = Column(String(36), nullable=False, comment='关联的运行ID')
    fcode = Column(String(10), nullable=False, comment='基金代码')

    # 筛选层信息
    step_id = Column(Integer, nullable=False, comment='筛选层ID(1-10)')
    step_name = Column(String(50), comment='筛选层名称')

    # 决策结果
    passed = Column(Boolean, nullable=False, comment='是否通过')
    reason_code = Column(String(50), comment='原因代码')
    reason_detail = Column(String(500), comment='原因详情(含阈值与实际值)')

    # 指标快照（可选，避免后续指标更新导致解释失真）
    metrics_json = Column(Text, comment='该层用到的关键指标快照(JSON)')

    created_at = Column(DateTime, default=datetime.now, comment='决策记录写入时间')

    __table_args__ = (
        Index('idx_decision_run_id', 'run_id'),
        Index('idx_decision_fcode', 'fcode'),
        Index('idx_decision_step', 'step_id'),
        Index('idx_decision_passed', 'passed'),
        Index('idx_decision_reason', 'reason_code'),
        Index('idx_decision_run_fcode', 'run_id', 'fcode'),
        Index('idx_decision_run_passed_step', 'run_id', 'passed', 'step_id'),
        Index('idx_decision_created_at', 'created_at'),
    )

    def __repr__(self):
        return f"<ScreeningDecision(run={self.run_id[:8]}, fcode='{self.fcode}', step={self.step_id}, passed={self.passed})>"


# 筛选原因代码枚举（供参考）
SCREENING_REASON_CODES = {
    # 第1层：基础资格
    'NO_NAV_DATA': '无净值数据',
    'NO_RETURN_DATA': '无收益数据',

    # 第1层：数据新鲜度（Step 5）
    'NAV_TOO_OLD': '净值数据过期',
    'NO_NAV_HISTORY': '无净值历史记录',

    # 第2层：流动性
    'HOLDING_PERIOD_TOO_LONG': '持有期过长',
    'LONG_HOLDING_KEYWORD': '含长持有期关键词',

    # 第3层：基础成熟度
    'AGE_TOO_YOUNG': '成立时间不足',
    'SCALE_TOO_SMALL': '规模过小',

    # 第4层：周期适配
    'PERIOD_MISMATCH': '成立时间不满足周期要求',

    # 第5层：规模分层
    'SCALE_BELOW_THRESHOLD': '规模低于周期阈值',

    # 第6层：风险指标
    'INSUFFICIENT_NAV_POINTS': '净值数据点不足',
    'RISK_METRICS_FAIL': '风险指标未达标',
    'RISK_METRICS_UNAVAILABLE': '风险指标不可用',
    'RISK_METRICS_QC_FAIL': '风险指标质量门控未通过',
    'NAV_DATA_GAP': '净值数据连续性不足',

    # 第7层：异常值
    'RETURN_TOO_HIGH': '收益率异常偏高',
    'RETURN_TOO_LOW': '收益率异常偏低',
    'DRAWDOWN_TOO_HIGH': '最大回撤过高',

    # 第8层：业绩持续性
    'CONSISTENCY_FAIL': '业绩持续性不足',
    'ABSOLUTE_RETURN_FAIL': '绝对收益未达标',

    # 第9层：多维度交叉
    'CROSS_VALIDATION_FAIL': '多维度交叉验证未通过',

    # 第10层：最终精选
    'RANK_CUTOFF': '排名未进入TopN',

    # 通过
    'PASSED': '通过',
}


class ScreeningPick(Base):
    """筛选精选基金表 - 记录每次筛选的TopK结果，供前端展示"""
    __tablename__ = 'screening_picks'

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(String(36), nullable=False, comment='关联的运行ID')
    fcode = Column(String(10), nullable=False, comment='基金代码')
    rank = Column(Integer, nullable=False, comment='排名(1-based)')

    # 筛选维度
    fund_type = Column(String(30), comment='基金类型(bond/stock/hybrid)')
    period_years = Column(Float, comment='分析周期（年）')

    # 基金基础信息快照
    shortname = Column(String(100), comment='基金简称')
    ftype = Column(String(50), comment='基金分类')
    scale_billion = Column(Float, comment='规模(亿)')
    main_return = Column(Float, comment='主收益率(%)')

    # 评分快照
    combo_score = Column(Float, comment='综合评分')
    short_term_score = Column(Float, comment='短期潜力分')
    long_term_score = Column(Float, comment='长期价值分')
    selected_combo_score = Column(Float, comment='策略选中评分')
    selected_score_key = Column(String(50), comment='策略选中的score_key')

    # 风险指标快照
    sharpe_ratio = Column(Float, comment='夏普比率')
    max_drawdown = Column(Float, comment='最大回撤')
    volatility = Column(Float, comment='波动率')

    # Step 8-5: 筛选时刻快照（JSON）
    snapshot_json = Column(PickSnapshotText, comment='筛选时刻完整快照(JSON): freshness/risk_metrics/returns/coverage/investability')

    # 时间
    picked_at = Column(DateTime, default=datetime.now, comment='入选时间')

    __table_args__ = (
        Index('idx_pick_run_id', 'run_id'),
        Index('idx_pick_fcode', 'fcode'),
        Index('idx_pick_fund_type_period', 'fund_type', 'period_years'),
        Index('idx_pick_fund_type_period_fcode_id', 'fund_type', 'period_years', 'fcode', 'id'),
        Index('idx_pick_picked_at', 'picked_at'),
    )


class ScreeningPickMetricSnapshot(Base):
    """筛选精选基金的轻量风险指标快照，避免报表反复扫描 picks JSON。"""
    __tablename__ = 'screening_pick_metric_snapshots'

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(String(36), nullable=False, comment='关联的运行ID')
    fcode = Column(String(10), nullable=False, comment='基金代码')
    fund_type = Column(String(30), comment='基金类型(bond/stock/hybrid)')
    period_years = Column(Float, comment='分析周期（年）')
    pick_id = Column(Integer, nullable=False, comment='screening_picks.id')
    rank = Column(Integer, comment='排名(1-based)')
    picked_at = Column(DateTime, comment='入选时间')

    sharpe_ratio = Column(Float, comment='夏普比率')
    raw_sharpe_ratio = Column(Float, comment='原始夏普比率（未做展示封顶）')
    sortino_ratio = Column(Float, comment='索提诺比率')
    raw_sortino_ratio = Column(Float, comment='原始索提诺比率（未做展示封顶）')
    calmar_ratio = Column(Float, comment='卡玛比率')
    max_drawdown = Column(Float, comment='最大回撤')
    max_drawdown_duration = Column(Float, comment='最长连续水下观测数（历史入选快照）')
    volatility = Column(Float, comment='波动率')

    stats_source = Column(String(40), comment='刷新来源')
    refreshed_at = Column(DateTime, default=datetime.now, comment='刷新时间')
    created_at = Column(DateTime, default=datetime.now, comment='创建时间')

    __table_args__ = (
        UniqueConstraint('pick_id', name='uk_pick_metric_pick_id'),
        Index('idx_pick_metric_run_id', 'run_id'),
        Index('idx_pick_metric_fund_period_fcode', 'fund_type', 'period_years', 'fcode'),
        Index('idx_pick_metric_fund_period_pick', 'fund_type', 'period_years', 'pick_id'),
        Index('idx_pick_metric_refreshed', 'refreshed_at'),
        Index('idx_pick_metric_picked_at', 'picked_at'),
    )


class ScreeningDecisionReasonSummary(Base):
    """筛选决策原因聚合摘要，避免诊断路径反复聚合 decisions 明细。"""
    __tablename__ = 'screening_decision_reason_summary'

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(String(36), nullable=False, comment='关联的筛选运行ID')
    fund_type = Column(String(30), comment='基金类型')
    period_years = Column(Float, comment='分析周期（年）')
    status = Column(String(20), comment='筛选运行状态')
    step_id = Column(Integer, nullable=False, comment='筛选层ID')
    step_name = Column(String(50), comment='筛选层名称')
    passed = Column(Boolean, nullable=False, comment='是否通过')
    reason_code = Column(String(50), nullable=False, default='', comment='原因代码')
    decision_count = Column(Integer, nullable=False, default=0, comment='该原因决策行数')
    first_decision_at = Column(DateTime, comment='该组最早决策时间')
    latest_decision_at = Column(DateTime, comment='该组最新决策时间')
    stats_source = Column(String(40), comment='刷新来源')
    refreshed_at = Column(DateTime, default=datetime.now, comment='刷新时间')
    created_at = Column(DateTime, default=datetime.now, comment='创建时间')

    __table_args__ = (
        UniqueConstraint(
            'run_id',
            'step_id',
            'passed',
            'reason_code',
            name='uk_decision_reason_summary_key',
        ),
        Index('idx_decision_reason_summary_run', 'run_id'),
        Index('idx_decision_reason_summary_reason', 'reason_code'),
        Index('idx_decision_reason_summary_fund_period', 'fund_type', 'period_years'),
        Index('idx_decision_reason_summary_latest', 'latest_decision_at'),
        Index('idx_decision_reason_summary_refreshed', 'refreshed_at'),
    )


class ScreeningRunLatestActivity(Base):
    """筛选 run 最新明细活动摘要，用于替代明细表 latest/count 探测。"""
    __tablename__ = 'screening_run_latest_activity'

    run_id = Column(String(36), primary_key=True, comment='关联的筛选运行ID')
    fund_type = Column(String(30), comment='基金类型')
    period_years = Column(Float, comment='分析周期（年）')
    status = Column(String(20), comment='筛选运行状态')
    started_at = Column(DateTime, comment='开始时间')
    finished_at = Column(DateTime, comment='结束时间')
    decision_count = Column(Integer, nullable=False, default=0, comment='screening_decisions 行数')
    pick_count = Column(Integer, nullable=False, default=0, comment='screening_picks 行数')
    first_decision_at = Column(DateTime, comment='最早 decision 写入时间')
    latest_decision_at = Column(DateTime, comment='最新 decision 写入时间')
    first_pick_at = Column(DateTime, comment='最早 pick 写入时间')
    latest_pick_at = Column(DateTime, comment='最新 pick 写入时间')
    latest_detail_at = Column(DateTime, comment='decision/pick 最新明细时间')
    stats_source = Column(String(40), comment='刷新来源')
    refreshed_at = Column(DateTime, default=datetime.now, comment='刷新时间')
    created_at = Column(DateTime, default=datetime.now, comment='创建时间')

    __table_args__ = (
        Index('idx_run_latest_activity_latest', 'latest_detail_at'),
        Index('idx_run_latest_activity_status', 'status'),
        Index('idx_run_latest_activity_fund_period', 'fund_type', 'period_years'),
        Index('idx_run_latest_activity_refreshed', 'refreshed_at'),
    )


class ScreeningRunStorageStats(Base):
    """筛选 run 的明细行数物化统计，用于避免反复扫描千万级明细表。"""
    __tablename__ = 'screening_run_storage_stats'

    run_id = Column(String(36), primary_key=True, comment='关联的筛选运行ID')
    fund_type = Column(String(30), comment='基金类型')
    period_years = Column(Float, comment='分析周期（年）')
    status = Column(String(20), comment='筛选运行状态')
    started_at = Column(DateTime, comment='开始时间')
    finished_at = Column(DateTime, comment='结束时间')
    decision_count = Column(Integer, nullable=False, default=0, comment='screening_decisions 行数')
    pick_count = Column(Integer, nullable=False, default=0, comment='screening_picks 行数')
    stats_source = Column(String(40), comment='统计来源')
    refreshed_at = Column(DateTime, default=datetime.now, comment='刷新时间')
    created_at = Column(DateTime, default=datetime.now, comment='创建时间')

    __table_args__ = (
        Index('idx_run_storage_started', 'started_at'),
        Index('idx_run_storage_status', 'status'),
        Index('idx_run_storage_fund_period', 'fund_type', 'period_years'),
        Index('idx_run_storage_refreshed', 'refreshed_at'),
    )
