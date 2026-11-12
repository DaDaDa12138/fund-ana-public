"""
数据库初始化和迁移脚本
"""

import logging
import os
from database.models import Base
from database.connection import DEFAULT_DB_PATH, get_db_manager, resolve_db_path

logger = logging.getLogger(__name__)


def _resolve_db_file_path(db_path=DEFAULT_DB_PATH):
    """统一解析迁移脚本使用的数据库文件路径。"""
    resolved_db_path = resolve_db_path(db_path)
    if resolved_db_path == ':memory:':
        return resolved_db_path, None
    return resolved_db_path, resolved_db_path


def init_database(db_path=DEFAULT_DB_PATH, drop_existing=False):
    """
    初始化数据库，创建所有表

    Args:
        db_path: 数据库路径
        drop_existing: 是否删除已存在的表

    Returns:
        True if successful
    """
    try:
        resolved_db_path, _ = _resolve_db_file_path(db_path)
        db_manager = get_db_manager(db_path=resolved_db_path)

        if drop_existing:
            logger.warning("删除所有现有表...")
            Base.metadata.drop_all(db_manager.engine)

        # 创建所有表
        logger.info("开始创建数据库表...")
        Base.metadata.create_all(db_manager.engine)
        run_all_migrations(resolved_db_path)

        # 获取创建的表列表
        tables = Base.metadata.tables.keys()
        logger.info(f"成功创建 {len(tables)} 个表:")
        for table in tables:
            logger.info(f"  - {table}")

        # 显示数据库信息
        db_size = db_manager.get_db_size()
        logger.info(f"数据库文件大小: {db_size} MB")
        logger.info(f"数据库路径: {db_manager.db_path}")

        return True

    except Exception as e:
        logger.error(f"数据库初始化失败: {e}")
        return False


def migrate_add_screening_indexes(db_path=DEFAULT_DB_PATH):
    """
    迁移：为筛选追踪大表补建关键复合索引（幂等）

    当前 screening_decisions 的高频查询模式主要是：
    - WHERE run_id=? AND step_id=? AND passed=?
    - WHERE run_id=? AND passed=?

    当前 screening_picks 的高频查询模式主要是：
    - WHERE fund_type='bond' AND period_years≈1y GROUP BY fcode MAX(id)

    仅有单列索引时，SQLite 在大表上可能错误选择 passed 索引，
    导致对低选择性布尔列做大范围扫描；screening_picks 仅有
    (fund_type, period_years) 时会回表读取宽行，latest pick 查询成本偏高。
    """
    import sqlite3

    resolved_db_path, db_file_path = _resolve_db_file_path(db_path)
    if db_file_path is None or not os.path.exists(db_file_path):
        logger.warning(f"数据库不存在: {resolved_db_path}，跳过筛选索引迁移")
        return False

    index_statements = [
        (
            'screening_decisions',
            'idx_decision_run_passed_step',
            'CREATE INDEX IF NOT EXISTS idx_decision_run_passed_step '
            'ON screening_decisions(run_id, passed, step_id)'
        ),
        (
            'screening_picks',
            'idx_pick_fund_type_period_fcode_id',
            'CREATE INDEX IF NOT EXISTS idx_pick_fund_type_period_fcode_id '
            'ON screening_picks(fund_type, period_years, fcode, id)'
        ),
    ]

    try:
        conn = sqlite3.connect(resolved_db_path)
        cursor = conn.cursor()

        created = 0
        skipped_missing_tables: set[str] = set()
        for table_name, index_name, sql in index_statements:
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (table_name,),
            )
            if not cursor.fetchone():
                skipped_missing_tables.add(table_name)
                continue
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='index' AND name=?",
                (index_name,)
            )
            if cursor.fetchone():
                continue
            logger.info("创建索引 %s...", index_name)
            cursor.execute(sql)
            created += 1

        if created > 0:
            conn.commit()
            logger.info("筛选索引迁移完成，新增 %d 个索引", created)
        else:
            logger.info("筛选索引已存在，无需迁移")
        if skipped_missing_tables:
            logger.info(
                "以下筛选表不存在，已跳过对应索引: %s",
                ", ".join(sorted(skipped_missing_tables)),
            )

        conn.close()
        return True

    except Exception as e:
        logger.error(f"筛选索引迁移失败: {e}")
        return False


def migrate_add_risk_metric_indexes(db_path=DEFAULT_DB_PATH):
    """
    迁移：为风险指标最新批次查询补建复合索引（幂等）

    高频读取模式主要是：
    - WHERE period='1y' AND end_date=?
    - WHERE period='1y' AND end_date=(SELECT MAX(end_date) ...)
    - WHERE period='1y' AND fcode=? ORDER BY end_date DESC LIMIT 1

    该索引只优化读取路径，不改变任何风险指标值、筛选阈值或发布语义。
    """
    import sqlite3

    resolved_db_path, db_file_path = _resolve_db_file_path(db_path)
    if db_file_path is None or not os.path.exists(db_file_path):
        logger.warning(f"数据库不存在: {resolved_db_path}，跳过风险指标索引迁移")
        return False

    index_statements = [
        (
            "idx_risk_period_end_date_fcode",
            "CREATE INDEX IF NOT EXISTS idx_risk_period_end_date_fcode "
            "ON fund_risk_metrics(period, end_date, fcode)",
        ),
        (
            "idx_risk_period_fcode_end_date",
            "CREATE INDEX IF NOT EXISTS idx_risk_period_fcode_end_date "
            "ON fund_risk_metrics(period, fcode, end_date)",
        ),
    ]

    try:
        conn = sqlite3.connect(resolved_db_path)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='fund_risk_metrics'"
        )
        if not cursor.fetchone():
            logger.info("fund_risk_metrics 表不存在，跳过风险指标索引迁移")
            conn.close()
            return True

        created = 0
        for index_name, sql in index_statements:
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='index' AND name=?",
                (index_name,),
            )
            if cursor.fetchone():
                continue
            logger.info("创建索引 %s...", index_name)
            cursor.execute(sql)
            created += 1
        if created > 0:
            conn.commit()
            logger.info("风险指标索引迁移完成，新增 %d 个索引", created)
        else:
            logger.info("风险指标索引已存在，无需迁移")
        conn.close()
        return True

    except Exception as e:
        logger.error(f"风险指标索引迁移失败: {e}")
        return False


def migrate_add_type_code_fields(db_path=DEFAULT_DB_PATH):
    """
    迁移：添加 type_code 和 type_category 字段

    该迁移用于支持通用基金分析系统扩展。

    Args:
        db_path: 数据库路径

    Returns:
        True if successful
    """
    import sqlite3

    resolved_db_path, db_file_path = _resolve_db_file_path(db_path)
    if db_file_path is None or not os.path.exists(db_file_path):
        logger.warning(f"数据库不存在: {resolved_db_path}，跳过迁移")
        return False

    try:
        conn = sqlite3.connect(resolved_db_path)
        cursor = conn.cursor()

        # 检查字段是否已存在
        cursor.execute("PRAGMA table_info(funds)")
        columns = [col[1] for col in cursor.fetchall()]

        migrations_applied = 0

        # 添加 type_code 字段
        if 'type_code' not in columns:
            logger.info("添加 type_code 字段...")
            cursor.execute("ALTER TABLE funds ADD COLUMN type_code TEXT")
            migrations_applied += 1

        # 添加 type_category 字段
        if 'type_category' not in columns:
            logger.info("添加 type_category 字段...")
            cursor.execute("ALTER TABLE funds ADD COLUMN type_category TEXT")
            migrations_applied += 1

        if migrations_applied > 0:
            conn.commit()
            logger.info(f"迁移完成，应用了 {migrations_applied} 个变更")
        else:
            logger.info("无需迁移，字段已存在")

        # 历史数据修正：按 ftype 文本回填或纠正 type_code/type_category。
        # 注意：fundMNDetailInformation 返回的 FTYPE 是细分类文本（如“债券型-混合二级”），
        # 不能据此把已经识别为 32 的基金回退成 31。
        logger.info("回填并校正债券基金的 type_code/type_category...")
        cursor.execute("""
            UPDATE funds
            SET
                type_code = CASE
                    WHEN ftype = '008' OR ftype LIKE '定开债%' THEN '32'
                    WHEN (ftype = '003' OR ftype = '指数型-固收' OR ftype LIKE '债券%')
                      AND (type_code IS NULL OR type_code NOT IN ('31', '32'))
                    THEN '31'
                    ELSE type_code
                END,
                type_category = CASE
                    WHEN ftype = '003'
                      OR ftype = '008'
                      OR ftype = '指数型-固收'
                      OR ftype LIKE '债券%'
                      OR ftype LIKE '定开债%'
                    THEN 'fixed_income'
                    ELSE type_category
                END
            WHERE ftype IS NOT NULL
              AND (
                    ftype = '003'
                 OR ftype = '008'
                 OR ftype = '指数型-固收'
                 OR ftype LIKE '债券%'
                 OR ftype LIKE '定开债%'
              )
              AND (
                    type_category IS NULL
                 OR type_category != 'fixed_income'
                 OR ((ftype = '008' OR ftype LIKE '定开债%') AND type_code != '32')
                 OR (
                        (ftype = '003' OR ftype = '指数型-固收' OR ftype LIKE '债券%')
                    AND (type_code IS NULL OR type_code NOT IN ('31', '32'))
                    )
              )
        """)
        updated = cursor.rowcount

        logger.info("回填并校正 FOF 基金的 type_code/type_category...")
        cursor.execute("""
            UPDATE funds
            SET
                type_code = '37',
                type_category = 'multi_asset'
            WHERE ftype LIKE 'FOF%'
              AND (
                    type_code IS NULL
                 OR type_code != '37'
                 OR type_category IS NULL
                 OR type_category != 'multi_asset'
              )
        """)
        updated += cursor.rowcount

        logger.info("回填并校正指数基金的 type_code/type_category...")
        cursor.execute("""
            UPDATE funds
            SET
                type_code = '26',
                type_category = 'equity'
            WHERE (ftype = '指数型' OR (ftype LIKE '指数型%' AND ftype != '指数型-固收'))
              AND (
                    type_code IS NULL
                 OR type_code != '26'
                 OR type_category IS NULL
                 OR type_category != 'equity'
              )
        """)
        updated += cursor.rowcount

        conn.commit()
        logger.info(f"校正了 {updated} 条记录")

        # 创建索引（如果不存在）
        try:
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_type_code ON funds(type_code)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_type_category ON funds(type_category)")
            conn.commit()
        except Exception as e:
            logger.warning(f"创建索引时出错: {e}")

        conn.close()
        return True

    except Exception as e:
        logger.error(f"迁移失败: {e}")
        return False


def migrate_ensure_all_tables(db_path=DEFAULT_DB_PATH):
    """
    迁移：确保所有 ORM 模型对应的表都存在

    Base.metadata.create_all() 是幂等操作：
    - 已存在的表不会被修改
    - 缺失的表会被创建（如 screening_runs, screening_decisions）

    Args:
        db_path: 数据库路径

    Returns:
        True if successful
    """
    resolved_db_path, db_file_path = _resolve_db_file_path(db_path)
    if db_file_path is None or not os.path.exists(db_file_path):
        logger.warning(f"数据库不存在: {resolved_db_path}，跳过表补建")
        return False

    try:
        db_manager = get_db_manager(db_path=resolved_db_path)

        # 获取迁移前的表列表
        from sqlalchemy import inspect
        inspector = inspect(db_manager.engine)
        existing_tables = set(inspector.get_table_names())
        all_tables = set(Base.metadata.tables.keys())
        missing_tables = all_tables - existing_tables

        if not missing_tables:
            logger.info("所有表已存在，无需补建")
            return True

        logger.info(f"发现 {len(missing_tables)} 个缺失表: {missing_tables}")
        Base.metadata.create_all(db_manager.engine)
        logger.info(f"成功补建缺失表: {missing_tables}")
        return True

    except Exception as e:
        logger.error(f"表补建失败: {e}")
        return False


def migrate_add_columns(db_path=DEFAULT_DB_PATH):
    """
    迁移：为已有表添加新增列（幂等）

    当 ORM 模型新增列但表已存在时，create_all() 不会自动 ALTER TABLE。
    此函数检测并补建缺失列。
    """
    import sqlite3

    resolved_db_path, db_file_path = _resolve_db_file_path(db_path)
    if db_file_path is None or not os.path.exists(db_file_path):
        logger.warning(f"数据库不存在: {resolved_db_path}，跳过列迁移")
        return False

    # 定义需要检查的列: (表名, 列名, 列定义)
    column_migrations = [
        ('fund_returns', 'period_return_date', 'DATE'),
        ('fund_returns', 'period_return_observed_at', 'DATETIME'),
        ('fund_returns', 'period_return_source', 'VARCHAR(50)'),
        ('fund_returns', 'period_return_source_sha256', 'VARCHAR(64)'),
        ('fund_returns', 'period_return_source_json', 'TEXT'),
        ('fund_returns', 'period_return_attempted_at', 'DATETIME'),
        ('fund_returns', 'period_return_last_status', 'VARCHAR(50)'),
        ('fund_returns', 'period_return_rejection_json', 'TEXT'),
        ('fund_returns', 'period_return_rejection_sha256', 'VARCHAR(64)'),
        ('screening_picks', 'snapshot_json', 'TEXT'),
        ('fund_risk_metrics', 'raw_sharpe_ratio', 'REAL'),
        ('fund_risk_metrics', 'raw_sortino_ratio', 'REAL'),
        ('fund_risk_metrics', 'sharpe_ratio_cap_hit', 'INTEGER'),
        ('fund_risk_metrics', 'sortino_ratio_cap_hit', 'INTEGER'),
        ('fund_risk_metrics', 'nav_snapshot_signature', 'TEXT'),
        ('fund_risk_metrics', 'undefined_metric_reasons_json', 'TEXT'),
        ('fund_risk_metrics', 'formula_contract_json', 'TEXT'),
        ('fund_risk_metrics', 'max_drawdown_duration', 'INTEGER'),
        ('screening_decisions', 'created_at', 'DATETIME'),
        # P1-01 / B13: /fundMNDetailInformation 权威字段落库
        ('funds', 'cycle', 'VARCHAR(50)'),
        ('funds', 'preserve_period', 'VARCHAR(50)'),
        ('funds', 'preserve_date', 'VARCHAR(50)'),
        ('funds', 'preserve_type', 'VARCHAR(50)'),
        ('funds', 'buy_time', 'VARCHAR(50)'),
        ('funds', 'feature_code', 'VARCHAR(20)'),
        ('funds', 'base_fund_type_code', 'VARCHAR(10)'),
        ('funds', 'rating_shanghai', 'VARCHAR(10)'),
        ('funds', 'holding_period_source', 'VARCHAR(20)'),
        # P1-09 / B14: 外部候选标注 + 三机构评级聚合
        ('funds', 'external_curation_flag', 'VARCHAR(40)'),
        ('funds', 'rating_multi_source', 'VARCHAR(40)'),
        # P1-10 / B15: data_source 未复证历史残留的显式治理标签
        ('funds', 'data_source_governance_flag', 'VARCHAR(60)'),
        # 自动周维护使用独立采集时间，避免 funds.last_updated_at 被基金列表刷新覆盖。
        ('funds', 'detail_updated_at', 'DATETIME'),
        ('funds', 'manager_list_updated_at', 'DATETIME'),
        ('funds', 'manager_list_attempted_at', 'DATETIME'),
        ('funds', 'manager_list_last_status', 'VARCHAR(50)'),
    ]

    try:
        conn = sqlite3.connect(resolved_db_path)
        cursor = conn.cursor()
        applied = 0

        for table, column, col_type in column_migrations:
            # 检查表是否存在
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (table,)
            )
            if not cursor.fetchone():
                logger.info(f"  表 {table} 不存在，跳过列 {column}")
                continue

            # 检查列是否已存在
            cursor.execute(f"PRAGMA table_info({table})")
            columns = [col[1] for col in cursor.fetchall()]
            if column in columns:
                continue

            logger.info(f"  添加列 {table}.{column} ({col_type})...")
            cursor.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")
            applied += 1

        if applied > 0:
            conn.commit()
            logger.info(f"列迁移完成，添加了 {applied} 个新列")
        else:
            logger.info("所有新增列已存在，无需迁移")

        conn.close()
        return True

    except Exception as e:
        logger.error(f"列迁移失败: {e}")
        return False


def migrate_drop_redundant_nav_fcode_index(db_path=DEFAULT_DB_PATH):
    """
    迁移：移除 fund_nav_history 上已退出当前运行链路的 idx_nav_fcode（幂等）

    注意：
    - 这是结构变更，只应在已完成查询审计后执行。
    - 若索引已不存在，视为成功。
    """
    import sqlite3

    resolved_db_path, db_file_path = _resolve_db_file_path(db_path)
    if db_file_path is None or not os.path.exists(db_file_path):
        logger.warning(f"数据库不存在: {resolved_db_path}，跳过 idx_nav_fcode 迁移")
        return False

    try:
        conn = sqlite3.connect(resolved_db_path)
        cursor = conn.cursor()
        cursor.execute("PRAGMA index_list('fund_nav_history')")
        index_rows = cursor.fetchall()
        index_names = {str(row[1] or '').strip() for row in index_rows if len(row) > 1}

        if 'idx_nav_fcode' not in index_names:
            logger.info("idx_nav_fcode 已不存在，无需迁移")
            conn.close()
            return True

        logger.info("删除冗余索引 idx_nav_fcode ...")
        cursor.execute("DROP INDEX IF EXISTS idx_nav_fcode")
        conn.commit()
        conn.close()
        logger.info("idx_nav_fcode 删除完成")
        return True

    except Exception as e:
        logger.error(f"删除 idx_nav_fcode 失败: {e}")
        return False


def migrate_add_screening_run_storage_stats(db_path=DEFAULT_DB_PATH):
    """迁移：创建筛选 run 存储统计物化表（幂等）。"""
    resolved_db_path, db_file_path = _resolve_db_file_path(db_path)
    if db_file_path is None or not os.path.exists(db_file_path):
        logger.warning(f"数据库不存在: {resolved_db_path}，跳过筛选 run 存储统计表迁移")
        return False

    try:
        db_manager = get_db_manager(db_path=resolved_db_path)
        from database.models import ScreeningRunStorageStats

        ScreeningRunStorageStats.__table__.create(bind=db_manager.engine, checkfirst=True)
        logger.info("screening_run_storage_stats 表已就绪")
        return True
    except Exception as e:
        logger.error(f"创建 screening_run_storage_stats 失败: {e}")
        return False


def migrate_add_screening_pick_metric_snapshots(db_path=DEFAULT_DB_PATH):
    """迁移：创建筛选 pick 风险指标轻量快照表（幂等）。"""
    resolved_db_path, db_file_path = _resolve_db_file_path(db_path)
    if db_file_path is None or not os.path.exists(db_file_path):
        logger.warning(f"数据库不存在: {resolved_db_path}，跳过筛选 pick 指标快照表迁移")
        return False

    try:
        db_manager = get_db_manager(db_path=resolved_db_path)
        from database.models import ScreeningPickMetricSnapshot

        ScreeningPickMetricSnapshot.__table__.create(bind=db_manager.engine, checkfirst=True)
        logger.info("screening_pick_metric_snapshots 表已就绪")
        return True
    except Exception as e:
        logger.error(f"创建 screening_pick_metric_snapshots 失败: {e}")
        return False


def migrate_add_screening_decision_summaries(db_path=DEFAULT_DB_PATH):
    """迁移：创建筛选 decision 原因摘要与 run 最新活动摘要表（幂等）。"""
    import sqlite3

    resolved_db_path, db_file_path = _resolve_db_file_path(db_path)
    if db_file_path is None or not os.path.exists(db_file_path):
        logger.warning(f"数据库不存在: {resolved_db_path}，跳过筛选 decision 摘要表迁移")
        return False

    try:
        db_manager = get_db_manager(db_path=resolved_db_path)
        from database.models import (
            ScreeningDecisionReasonSummary,
            ScreeningRunLatestActivity,
        )

        ScreeningDecisionReasonSummary.__table__.create(bind=db_manager.engine, checkfirst=True)
        ScreeningRunLatestActivity.__table__.create(bind=db_manager.engine, checkfirst=True)

        conn = sqlite3.connect(resolved_db_path)
        try:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='screening_decisions'"
            )
            if cursor.fetchone():
                cursor.execute(
                    "CREATE INDEX IF NOT EXISTS idx_decision_created_at "
                    "ON screening_decisions(created_at)"
                )
                conn.commit()
        finally:
            conn.close()

        logger.info("screening_decision_reason_summary / screening_run_latest_activity 表已就绪")
        return True
    except Exception as e:
        logger.error(f"创建筛选 decision 摘要表失败: {e}")
        return False


def migrate_add_decision_payload_storage(db_path=DEFAULT_DB_PATH):
    """Create exact-byte metric objects/references without changing decision columns."""
    from database.screening_decision_payloads import ensure_storage

    resolved_db_path, db_file_path = _resolve_db_file_path(db_path)
    if db_file_path is None or not os.path.exists(db_file_path):
        return False
    try:
        manager = get_db_manager(db_path=resolved_db_path)
        with manager.engine.begin() as connection:
            ensure_storage(connection.connection.driver_connection)
        return True
    except Exception:
        logger.exception("决策指标内容存储迁移失败")
        return False


def run_all_migrations(db_path=DEFAULT_DB_PATH):
    """
    运行所有待执行的迁移

    Args:
        db_path: 数据库路径
    """
    logger.info("="*60)
    logger.info("开始数据库迁移...")
    logger.info("目标数据库: %s", resolve_db_path(db_path))
    logger.info("="*60)

    # 子步骤以 False 报告错误时同样停止；不能把部分迁移记为成功启动。
    for migration in (
        migrate_ensure_all_tables,
        migrate_add_type_code_fields,
        migrate_add_columns,
        migrate_add_screening_indexes,
        migrate_add_risk_metric_indexes,
        migrate_drop_redundant_nav_fcode_index,
        migrate_add_screening_run_storage_stats,
        migrate_add_screening_pick_metric_snapshots,
        migrate_add_screening_decision_summaries,
        migrate_add_decision_payload_storage,
    ):
        if migration(db_path) is not True:
            raise RuntimeError(f"数据库迁移未完成: {migration.__name__}")

    logger.info("所有迁移完成")


def check_database_exists(db_path=DEFAULT_DB_PATH):
    """
    检查数据库是否已初始化

    Args:
        db_path: 数据库路径

    Returns:
        True if database exists and has tables
    """
    from sqlalchemy import inspect

    resolved_db_path, db_file_path = _resolve_db_file_path(db_path)
    if db_file_path is None or not os.path.exists(db_file_path):
        return False

    try:
        db_manager = get_db_manager(db_path=resolved_db_path)
        inspector = inspect(db_manager.engine)
        tables = inspector.get_table_names()
        return len(tables) > 0
    except Exception:
        return False


def get_database_stats(db_path=DEFAULT_DB_PATH):
    """
    获取数据库统计信息

    Args:
        db_path: 数据库路径

    Returns:
        字典包含统计信息
    """
    from database.models import (
        Fund, FundNavHistory, FundReturns, FundScale,
        FundRiskMetrics, FundManager, FundCompany, CollectionLog
    )

    resolved_db_path, _ = _resolve_db_file_path(db_path)
    db_manager = get_db_manager(db_path=resolved_db_path)

    stats = {}

    with db_manager.get_session() as session:
        stats['funds'] = session.query(Fund).count()
        stats['nav_history'] = session.query(FundNavHistory).count()
        stats['returns'] = session.query(FundReturns).count()
        stats['scale'] = session.query(FundScale).count()
        stats['risk_metrics'] = session.query(FundRiskMetrics).count()
        stats['managers'] = session.query(FundManager).count()
        stats['companies'] = session.query(FundCompany).count()
        stats['logs'] = session.query(CollectionLog).count()

    stats['db_size_mb'] = db_manager.get_db_size()

    return stats


def print_database_stats(db_path=DEFAULT_DB_PATH):
    """
    打印数据库统计信息

    Args:
        db_path: 数据库路径
    """
    try:
        stats = get_database_stats(db_path)

        print("\n" + "="*50)
        print("数据库统计信息")
        print("="*50)
        print(f"基金总数:           {stats['funds']:,}")
        print(f"净值历史记录:       {stats['nav_history']:,}")
        print(f"收益指标记录:       {stats['returns']:,}")
        print(f"规模记录:           {stats['scale']:,}")
        print(f"风险指标记录:       {stats['risk_metrics']:,}")
        print(f"基金经理数:         {stats['managers']:,}")
        print(f"基金公司数:         {stats['companies']:,}")
        print(f"采集日志数:         {stats['logs']:,}")
        print(f"数据库大小:         {stats['db_size_mb']} MB")
        print("="*50 + "\n")

    except Exception as e:
        logger.error(f"获取统计信息失败: {e}")


if __name__ == '__main__':
    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # 初始化数据库
    success = init_database(db_path=DEFAULT_DB_PATH, drop_existing=False)

    if success:
        print("\n✅ 数据库初始化成功！")
        print_database_stats()
    else:
        print("\n❌ 数据库初始化失败！")
