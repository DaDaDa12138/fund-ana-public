"""
数据库连接管理
增强版：包含详细的连接状态、会话生命周期、错误追踪日志
SQLite锁库修复版：添加全局写锁、重试机制、增强的busy_timeout
"""

import os
import time
import threading
import traceback
from pathlib import Path
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.pool import NullPool
from sqlalchemy.exc import OperationalError
from contextlib import contextmanager
import logging
import yaml
from utils.project_runtime_config import load_runtime_config

# 标准日志
logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DB_PATH = '../backend/data/bond_funds.db'
_LEGACY_DEFAULT_DB_ALIASES = {
    '',
    '.',
    DEFAULT_DB_PATH.replace('\\', '/'),
    'database/bond_funds.db',
    './database/bond_funds.db',
    'backend/data/bond_funds.db',
    './backend/data/bond_funds.db',
    'core/database/bond_funds.db',
    './core/database/bond_funds.db',
}

# 尝试导入增强日志
try:
    from utils.enhanced_logger import get_database_logger, get_performance_logger
    HAS_ENHANCED_LOGGER = True
except ImportError:
    HAS_ENHANCED_LOGGER = False

# Importing path/config/calculation helpers must not open log files.
db_logger = None
perf_logger = None


# 连接统计
class ConnectionStats:
    """数据库连接统计"""

    def __init__(self):
        self._lock = threading.Lock()
        self.reset()

    def reset(self):
        with self._lock:
            self.total_connections = 0
            self.active_sessions = 0
            self.total_commits = 0
            self.total_rollbacks = 0
            self.total_errors = 0
            self.slow_queries = 0  # 超过1秒的查询

    def record_connection(self):
        with self._lock:
            self.total_connections += 1

    def record_session_start(self):
        with self._lock:
            self.active_sessions += 1

    def record_session_end(self):
        with self._lock:
            self.active_sessions = max(0, self.active_sessions - 1)

    def record_commit(self):
        with self._lock:
            self.total_commits += 1

    def record_rollback(self):
        with self._lock:
            self.total_rollbacks += 1

    def record_error(self):
        with self._lock:
            self.total_errors += 1

    def record_slow_query(self):
        with self._lock:
            self.slow_queries += 1

    def get_stats(self):
        with self._lock:
            return {
                'total_connections': self.total_connections,
                'active_sessions': self.active_sessions,
                'total_commits': self.total_commits,
                'total_rollbacks': self.total_rollbacks,
                'total_errors': self.total_errors,
                'slow_queries': self.slow_queries
            }


# 全局连接统计实例
_connection_stats = ConnectionStats()

# 全局写锁：确保同一时间只有一个写操作
# SQLite在WAL模式下支持并发读，但写操作仍需串行
_global_write_lock = threading.RLock()

# 锁等待统计
class LockStats:
    """写锁等待统计"""
    def __init__(self):
        self._lock = threading.Lock()
        self.total_waits = 0
        self.total_wait_time = 0.0
        self.current_holder = None
        self.current_holder_task = None
        self.acquire_time = None

    def record_wait(self, wait_time: float):
        with self._lock:
            self.total_waits += 1
            self.total_wait_time += wait_time

    def set_holder(self, thread_id, task_name=None):
        with self._lock:
            self.current_holder = thread_id
            self.current_holder_task = task_name
            self.acquire_time = time.time()

    def clear_holder(self):
        with self._lock:
            self.current_holder = None
            self.current_holder_task = None
            self.acquire_time = None

    def get_holder_info(self) -> dict:
        with self._lock:
            if self.current_holder:
                hold_time = time.time() - self.acquire_time if self.acquire_time else 0
                return {
                    'thread_id': self.current_holder,
                    'task_name': self.current_holder_task,
                    'hold_time_sec': round(hold_time, 2)
                }
            return {}

    def get_stats(self) -> dict:
        with self._lock:
            return {
                'total_waits': self.total_waits,
                'total_wait_time_sec': round(self.total_wait_time, 2),
                'avg_wait_time_sec': round(self.total_wait_time / self.total_waits, 3) if self.total_waits > 0 else 0
            }

_lock_stats = LockStats()


def _get_env_float(name: str, default: float) -> float:
    """读取浮点环境变量，异常时回退默认值。"""
    value = os.environ.get(name)
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _load_configured_default_db_path() -> str:
    """读取项目级运行配置中的数据库路径默认值，失败时回退内置默认值。"""
    config_path = PROJECT_ROOT.parent / 'config' / 'project.yaml'
    if not config_path.exists():
        return DEFAULT_DB_PATH

    try:
        config = load_runtime_config(config_path)
    except Exception:
        return DEFAULT_DB_PATH

    if not isinstance(config, dict):
        return DEFAULT_DB_PATH

    db_path = config.get('database', {}).get('path')
    if isinstance(db_path, str) and db_path.strip():
        return db_path.strip()

    return DEFAULT_DB_PATH


def _is_default_db_alias(db_path: str) -> bool:
    """识别旧入口常用的默认库别名，避免维护工具误指向 core/database。"""
    text = str(db_path or '').strip().replace('\\', '/')
    if text in _LEGACY_DEFAULT_DB_ALIASES:
        return True
    try:
        candidate = Path(text)
    except (TypeError, ValueError):
        return False
    if candidate.is_absolute():
        return candidate.resolve() in {
            (PROJECT_ROOT / DEFAULT_DB_PATH).resolve(),
            (PROJECT_ROOT / 'database/bond_funds.db').resolve(),
            (PROJECT_ROOT.parent / 'backend/data/bond_funds.db').resolve(),
            (PROJECT_ROOT.parent / 'core/database/bond_funds.db').resolve(),
        }
    normalized = text.lstrip('./')
    return normalized in {item.lstrip('./') for item in _LEGACY_DEFAULT_DB_ALIASES}


def resolve_db_path(db_path: str = DEFAULT_DB_PATH) -> str:
    """统一解析数据库路径，兼容环境变量、配置文件、相对路径和内存数据库。"""
    env_db_path = os.environ.get('DATABASE_PATH')
    if env_db_path:
        candidate = env_db_path.strip()
    elif _is_default_db_alias(db_path):
        candidate = _load_configured_default_db_path()
    else:
        candidate = str(db_path).strip()

    if candidate == ':memory:':
        return candidate

    path_obj = Path(candidate)
    if not path_obj.is_absolute():
        path_obj = (PROJECT_ROOT / path_obj).resolve()

    return str(path_obj)


def _resolve_db_path(db_path: str) -> str:
    """兼容旧测试/内部调用的别名。"""
    return resolve_db_path(db_path)


# 减少高并发任务下锁日志风暴，默认仅记录明显异常的锁等待。
LOCK_WAIT_LOG_THRESHOLD_SEC = _get_env_float('DB_LOCK_WAIT_LOG_THRESHOLD_SEC', 5.0)
LOCK_ACQUIRED_LOG_THRESHOLD_SEC = _get_env_float('DB_LOCK_ACQUIRED_LOG_THRESHOLD_SEC', 1.0)


class DatabaseManager:
    """数据库管理器"""

    def __init__(self, db_path=DEFAULT_DB_PATH, wal_mode=True):
        """
        初始化数据库管理器

        Args:
            db_path: 数据库文件路径
            wal_mode: 是否启用WAL模式（提升并发性能）
        """
        global db_logger, perf_logger
        if HAS_ENHANCED_LOGGER:
            if db_logger is None:
                db_logger = get_database_logger()
            if perf_logger is None:
                perf_logger = get_performance_logger()
        self.db_path = resolve_db_path(db_path)
        self.wal_mode = wal_mode

        # 确保数据库目录存在
        if self.db_path != ':memory:':
            db_dir = os.path.dirname(self.db_path)
            if db_dir:
                os.makedirs(db_dir, exist_ok=True)

        # 创建引擎
        self.engine = self._create_engine()

        # 创建会话工厂
        session_factory = sessionmaker(bind=self.engine)
        self.Session = scoped_session(session_factory)

        logger.info(f"数据库初始化完成: {db_path}")

    def _create_engine(self):
        """创建数据库引擎"""
        # SQLite连接字符串
        db_url = 'sqlite:///:memory:' if self.db_path == ':memory:' else f'sqlite:///{self.db_path}'

        logger.info(f"创建数据库引擎: {db_url}")
        if HAS_ENHANCED_LOGGER:
            db_logger.log_connection('ENGINE_CREATE', db_path=self.db_path,
                                    pool_type='NullPool', wal_mode=self.wal_mode)

        # 创建引擎
        # 使用NullPool：每次操作创建新连接，避免多线程共享连接导致的问题
        # 这对于SQLite高并发写入场景更稳定
        engine = create_engine(
            db_url,
            echo=False,  # 是否打印SQL语句
            poolclass=NullPool,  # 每次操作创建新连接，避免并发问题
            connect_args={
                'check_same_thread': False,  # 允许多线程访问
                'timeout': 120  # 连接超时120秒（SQLite的busy_timeout，增加到2分钟）
            }
        )

        # 监听连接事件
        @event.listens_for(engine, "connect")
        def on_connect(dbapi_conn, connection_record):
            _connection_stats.record_connection()
            thread_id = threading.current_thread().ident

            if HAS_ENHANCED_LOGGER:
                db_logger.log_connection('CONNECT', thread_id=thread_id,
                                        total_connections=_connection_stats.total_connections)
            else:
                logger.debug(f"数据库连接创建 thread={thread_id} total={_connection_stats.total_connections}")

            # 如果启用WAL模式，设置PRAGMA
            if self.wal_mode:
                cursor = dbapi_conn.cursor()
                try:
                    # WAL模式：允许并发读写
                    cursor.execute("PRAGMA journal_mode=WAL")
                    wal_result = cursor.fetchone()
                    if wal_result is None or str(wal_result[0]).lower() != 'wal':
                        logger.warning(f"WAL模式设置可能失败，返回值: {wal_result}（可能是只读文件系统或权限不足）")
                    # NORMAL模式：平衡安全性和性能
                    cursor.execute("PRAGMA synchronous=NORMAL")
                    # 增大缓存：提升性能
                    cursor.execute("PRAGMA cache_size=10000")
                    # 内存临时存储：加速排序和临时表
                    cursor.execute("PRAGMA temp_store=MEMORY")
                    # 增加busy_timeout到120秒：等待2分钟而非立即失败
                    # 这是解决"database is locked"的关键配置
                    cursor.execute("PRAGMA busy_timeout=120000")
                    # WAL自动检查点：每1000页写入
                    cursor.execute("PRAGMA wal_autocheckpoint=1000")
                    # 启用外键约束
                    cursor.execute("PRAGMA foreign_keys=ON")
                    logger.debug("已启用WAL模式和性能优化（busy_timeout=120s）")
                except Exception as e:
                    logger.error(f"设置SQLite PRAGMA失败: {e}")
                    if HAS_ENHANCED_LOGGER:
                        db_logger.log_error('PRAGMA_SET', str(e))
                finally:
                    cursor.close()

        # 监听连接关闭事件
        @event.listens_for(engine, "close")
        def on_close(dbapi_conn, connection_record):
            thread_id = threading.current_thread().ident
            if HAS_ENHANCED_LOGGER:
                db_logger.log_connection('CLOSE', thread_id=thread_id)
            else:
                logger.debug(f"数据库连接关闭 thread={thread_id}")

        return engine

    @contextmanager
    def get_session(self, task_name: str = None, readonly: bool = False):
        """
        获取数据库会话（上下文管理器）

        增强版：支持写锁互斥和重试机制

        Args:
            task_name: 任务名称（用于日志和锁追踪）
            readonly: 是否只读模式（只读不需要获取写锁）

        使用示例:
            with db_manager.get_session() as session:
                session.query(Fund).all()

            # 只读查询（不获取写锁，允许并发）
            with db_manager.get_session(readonly=True) as session:
                session.query(Fund).all()
        """
        session = self.Session()
        session_id = id(session)
        thread_id = threading.current_thread().ident
        start_time = time.time()
        lock_acquired = False
        lock_wait_time = 0

        _connection_stats.record_session_start()

        # 非只读模式需要获取全局写锁
        if not readonly:
            lock_start = time.time()
            holder_info = _lock_stats.get_holder_info()
            if holder_info:
                hold_time = holder_info.get('hold_time_sec') or 0
                if hold_time >= LOCK_WAIT_LOG_THRESHOLD_SEC:
                    logger.info(f"[LOCK_WAIT] session={session_id} task={task_name} "
                               f"waiting for lock held by thread={holder_info.get('thread_id')} "
                               f"task={holder_info.get('task_name')} hold_time={hold_time}s")

            _global_write_lock.acquire()
            # 紧跟 acquire() 设置标志，最小化 KeyboardInterrupt 窗口
            lock_acquired = True

        try:
            # 锁���获取后的日志/统计操作（纳入 try 保护，防止异常导致锁泄漏）
            if not readonly:
                lock_wait_time = time.time() - lock_start
                if lock_wait_time > 0:
                    _lock_stats.record_wait(lock_wait_time)
                if lock_wait_time >= LOCK_ACQUIRED_LOG_THRESHOLD_SEC:
                    logger.info(f"[LOCK_ACQUIRED] session={session_id} task={task_name} "
                               f"wait_time={lock_wait_time:.3f}s")
                _lock_stats.set_holder(thread_id, task_name)

            if HAS_ENHANCED_LOGGER:
                db_logger.log_transaction('BEGIN', session_id=str(session_id),
                                         thread_id=thread_id,
                                         active_sessions=_connection_stats.active_sessions,
                                         task_name=task_name,
                                         readonly=readonly,
                                         lock_wait_ms=round(lock_wait_time * 1000, 2))
            else:
                logger.debug(f"会话开始 session={session_id} thread={thread_id} task={task_name} readonly={readonly}")

            yield session

            # 只读模式跳过 commit，避免不必要的数据库交互和写锁竞争
            if readonly:
                elapsed = time.time() - start_time
                if elapsed > 1.0:
                    _connection_stats.record_slow_query()
                if HAS_ENHANCED_LOGGER:
                    db_logger.log_transaction('READONLY_CLOSE', session_id=str(session_id),
                                             elapsed_ms=round(elapsed * 1000, 2))
            else:
                # 提交事务（带重试）
                commit_start = time.time()
                self._commit_with_retry(session, session_id, task_name)
                commit_time = (time.time() - commit_start) * 1000

                _connection_stats.record_commit()

                elapsed = time.time() - start_time
                if elapsed > 1.0:
                    _connection_stats.record_slow_query()

                if HAS_ENHANCED_LOGGER:
                    db_logger.log_transaction('COMMIT', session_id=str(session_id),
                                             elapsed_ms=round(elapsed * 1000, 2),
                                             commit_ms=round(commit_time, 2),
                                             task_name=task_name)
                else:
                    logger.debug(f"会话提交 session={session_id} elapsed={elapsed:.3f}s task={task_name}")

        except Exception as e:
            # 回滚事务
            _connection_stats.record_rollback()
            _connection_stats.record_error()

            elapsed = time.time() - start_time
            error_type = type(e).__name__
            error_msg = str(e)

            # 特别标记锁相关错误
            is_lock_error = 'locked' in error_msg.lower() or 'busy' in error_msg.lower()

            if HAS_ENHANCED_LOGGER:
                db_logger.log_error('SESSION', f"{error_type}: {error_msg}",
                                   session_id=str(session_id),
                                   elapsed_ms=round(elapsed * 1000, 2),
                                   task_name=task_name,
                                   is_lock_error=is_lock_error)
                db_logger.log_transaction('ROLLBACK', session_id=str(session_id),
                                         error=error_type,
                                         task_name=task_name)
            else:
                if is_lock_error:
                    logger.error(f"[DATABASE_LOCKED] session={session_id} task={task_name} "
                                f"error={error_type}: {error_msg}")
                else:
                    logger.error(f"数据库操作失败: {e}")
                logger.error(f"回滚事务 session={session_id} error={error_type} task={task_name}")
                logger.debug(f"错误详情: {traceback.format_exc()}")

            # rollback/close 各自独立 try-except，确保锁总能在 finally 中释放
            try:
                session.rollback()
            except Exception as rb_err:
                logger.warning(f"rollback 失败 session={session_id}: {rb_err}")
            raise

        finally:
            _connection_stats.record_session_end()
            try:
                session.close()
                # scoped_session 需要 remove() 清理线程本地注册表，防止内存泄漏
                self.Session.remove()
            except Exception as close_err:
                logger.warning(f"session.close() 失败 session={session_id}: {close_err}")

            # 释放写锁（BaseException 保护，确保 KeyboardInterrupt/SystemExit 下也能释放）
            if lock_acquired:
                try:
                    _lock_stats.clear_holder()
                    _global_write_lock.release()
                except BaseException as lock_err:
                    logger.warning(f"写锁释放异常 session={session_id}: {lock_err}")

            if HAS_ENHANCED_LOGGER:
                db_logger.log_transaction('CLOSE', session_id=str(session_id),
                                         active_sessions=_connection_stats.active_sessions,
                                         task_name=task_name)

    def _commit_with_retry(self, session, session_id, task_name, max_retries=5, base_delay=2.0):
        """
        带重试的提交操作

        Args:
            session: 数据库会话
            session_id: 会话ID
            task_name: 任务名称
            max_retries: 最大重试次数
            base_delay: 基础延迟时间（秒）
        """
        last_error = None
        for attempt in range(1, max_retries + 1):
            try:
                session.commit()
                return
            except OperationalError as e:
                last_error = e
                error_msg = str(e).lower()

                # 只对锁相关错误重试
                if 'locked' in error_msg or 'busy' in error_msg:
                    if attempt < max_retries:
                        delay = min(base_delay * (2 ** (attempt - 1)), 10.0)  # 指数退避，单次上限10秒
                        logger.warning(f"[COMMIT_RETRY] session={session_id} task={task_name} "
                                      f"attempt={attempt}/{max_retries} delay={delay}s error={e}")
                        time.sleep(delay)
                        continue
                    else:
                        logger.error(f"[COMMIT_FAILED] session={session_id} task={task_name} "
                                    f"max_retries={max_retries} exhausted, error={e}")
                raise

        # 注：循环内所有路径要么 return（commit成功）要么 raise（非锁错误或重试耗尽），
        # 此处逻辑上不可达，保留注释说明控制流

    def execute_raw_sql(self, sql, params=None):
        """
        执行原始SQL语句

        Args:
            sql: SQL语句
            params: 参数

        Returns:
            结果集
        """
        with self.get_session(task_name='raw_sql') as session:
            from sqlalchemy import text
            stmt = text(sql) if isinstance(sql, str) else sql
            result = session.execute(stmt, params or {})
            # 在session关闭前获取结果
            try:
                return result.fetchall()
            except Exception:
                return result

    def bulk_insert(self, model_class, data_list, batch_size=1000):
        """
        批量插入数据（每批次独立事务，失败只回滚当前批次）

        Args:
            model_class: 模型类
            data_list: 数据列表（字典列表）
            batch_size: 每批大小

        Returns:
            插入的记录数（若 < len(data_list) 则表示部分批次失败）
        """
        total_inserted = 0
        total_batches = (len(data_list) + batch_size - 1) // batch_size
        start_time = time.time()
        table_name = model_class.__tablename__ if hasattr(model_class, '__tablename__') else str(model_class)
        failed_batches = []  # 记录失败批次索引

        logger.info(f"开始批量插入 table={table_name} total={len(data_list)} batch_size={batch_size}")

        if HAS_ENHANCED_LOGGER:
            db_logger.log_query('BULK_INSERT_START', table_name,
                               total_rows=len(data_list), batch_size=batch_size,
                               total_batches=total_batches)

        for batch_idx, i in enumerate(range(0, len(data_list), batch_size)):
            batch = data_list[i:i + batch_size]
            batch_start = time.time()

            try:
                with self.get_session() as session:
                    session.bulk_insert_mappings(model_class, batch)
                total_inserted += len(batch)
                batch_time = (time.time() - batch_start) * 1000

                # 每10批记录一次进度
                if (batch_idx + 1) % 10 == 0 or batch_idx == total_batches - 1:
                    progress = round((batch_idx + 1) / total_batches * 100, 1)
                    elapsed = time.time() - start_time
                    rate = total_inserted / elapsed if elapsed > 0 else 0

                    if HAS_ENHANCED_LOGGER:
                        db_logger.log_query('BULK_INSERT_PROGRESS', table_name,
                                           rows_affected=total_inserted,
                                           progress=progress,
                                           rate=round(rate, 1),
                                           batch_ms=round(batch_time, 2))
                    else:
                        logger.debug(f"已插入 {total_inserted}/{len(data_list)} 条记录 "
                                   f"({progress}%) rate={rate:.1f}/s")

            except Exception as e:
                failed_batches.append(batch_idx)
                logger.error(f"批量插入失败 batch={batch_idx} error={e}（已提交 {total_inserted} 条，继续后续批次）")
                if HAS_ENHANCED_LOGGER:
                    db_logger.log_error('BULK_INSERT', str(e),
                                       table=table_name, batch=batch_idx)

        elapsed = time.time() - start_time
        rate = total_inserted / elapsed if elapsed > 0 else 0

        if failed_batches:
            logger.warning(f"批量插入完成（部分失败）: {total_inserted}/{len(data_list)} 条记录, "
                          f"失败批次={failed_batches}, 耗时 {elapsed:.2f}s, 速率 {rate:.1f}/s")
        else:
            logger.info(f"批量插入完成: {total_inserted} 条记录, 耗时 {elapsed:.2f}s, "
                       f"速率 {rate:.1f}/s")

        if HAS_ENHANCED_LOGGER:
            db_logger.log_query('BULK_INSERT_COMPLETE', table_name,
                               rows_affected=total_inserted,
                               elapsed_ms=round(elapsed * 1000, 2),
                               rate=round(rate, 1))

        return total_inserted

    def vacuum(self):
        """整理数据库碎片"""
        logger.info("开始整理数据库碎片...")
        self.execute_raw_sql("VACUUM")
        logger.info("数据库碎片整理完成")

    def get_db_size(self):
        """获取数据库文件大小（MB）"""
        if os.path.exists(self.db_path):
            size_bytes = os.path.getsize(self.db_path)
            size_mb = size_bytes / (1024 * 1024)
            return round(size_mb, 2)
        return 0

    def close(self):
        """关闭数据库连接"""
        stats = self.get_connection_stats()
        logger.info(f"关闭数据库连接 stats={stats}")

        if HAS_ENHANCED_LOGGER:
            db_logger.log_connection('DISPOSE', **stats)

        self.Session.remove()
        self.engine.dispose()
        logger.info("数据库连接已关闭")

    def get_connection_stats(self) -> dict:
        """获取连接统计信息"""
        stats = _connection_stats.get_stats()
        stats['lock_stats'] = _lock_stats.get_stats()
        stats['current_lock_holder'] = _lock_stats.get_holder_info()
        return stats

    def log_stats_summary(self):
        """记录统计摘要到日志"""
        stats = self.get_connection_stats()
        db_size = self.get_db_size()

        summary = (f"数据库统计: "
                  f"total_connections={stats['total_connections']} "
                  f"active_sessions={stats['active_sessions']} "
                  f"commits={stats['total_commits']} "
                  f"rollbacks={stats['total_rollbacks']} "
                  f"errors={stats['total_errors']} "
                  f"slow_queries={stats['slow_queries']} "
                  f"db_size={db_size}MB")

        logger.info(summary)

        if HAS_ENHANCED_LOGGER:
            db_logger.log_connection('STATS_SUMMARY',
                                    db_size_mb=db_size,
                                    **stats)

        return stats


# 全局数据库管理器实例缓存（按解析后的数据库路径区分）
_db_manager_instances = {}
_db_manager_lock = threading.Lock()


def get_db_manager(db_path=DEFAULT_DB_PATH, wal_mode=True):
    """
    获取全局数据库管理器实例（单例模式，线程安全双重检查锁定）

    Args:
        db_path: 数据库路径
        wal_mode: 是否启用WAL模式

    Returns:
        DatabaseManager实例
    """
    resolved_path = resolve_db_path(db_path)

    with _db_manager_lock:
        manager = _db_manager_instances.get(resolved_path)
        if manager is None:
            manager = DatabaseManager(db_path=resolved_path, wal_mode=wal_mode)
            _db_manager_instances[resolved_path] = manager
        elif manager.wal_mode != wal_mode:
            logger.warning(
                "get_db_manager 检测到同一路径使用不同 wal_mode，复用已存在实例: path=%s existing=%s requested=%s",
                resolved_path,
                manager.wal_mode,
                wal_mode,
            )

    return manager
