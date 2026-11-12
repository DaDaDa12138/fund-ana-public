"""
任务互斥锁：防止任务冲突和重复执行
增强版：支持任务组互斥、依赖检查、详细日志
"""

import fcntl
import os
import time
import logging
import threading
from contextlib import contextmanager
from datetime import datetime, timedelta
from database.models import TaskStatus
from database.connection import get_db_manager

logger = logging.getLogger(__name__)

# 全局任务组锁：确保同一组内的任务不能并发执行
# 例如：数据采集任务和筛选任务不能同时写数据库
_task_group_locks = {}
_task_group_locks_lock = threading.Lock()

# 任务组定义：同一组内的任务互斥
TASK_GROUPS = {
    'db_write': [
        'daily_nav_update',
        'daily_risk_update',
        'daily_screening_workflow',
        'weekly_detail_update',
        'weekly_holding_period_update',
        'weekly_high_speed_detail',
        'weekly_manager_update',
        'weekly_company_update',
        'daily_supplement_missing_nav',
        'monthly_supplement_estab',
    ],
}

# 任务依赖定义：key任务必须在value任务完成后才能执行
TASK_DEPENDENCIES = {
    'daily_screening_workflow': ['daily_nav_update'],
    'daily_risk_update': ['daily_nav_update'],
    'daily_policy_monitor': ['daily_nav_update'],
}


def get_task_group(task_name: str) -> str:
    """获取任务所属的组"""
    for group_name, tasks in TASK_GROUPS.items():
        if task_name in tasks:
            return group_name
    return None


def get_group_lock(group_name: str) -> threading.RLock:
    """获取任务组的锁"""
    with _task_group_locks_lock:
        if group_name not in _task_group_locks:
            _task_group_locks[group_name] = threading.RLock()
        return _task_group_locks[group_name]


class TaskLock:
    """文件锁：确保同一时间只有一个任务实例运行"""

    def __init__(self, task_name, lock_dir='/tmp'):
        """
        初始化任务锁

        Args:
            task_name: 任务名称
            lock_dir: 锁文件目录
        """
        self.task_name = task_name
        self.lock_file = os.path.join(lock_dir, f'bond_fund_{task_name}.lock')
        self.lock_fd = None

    def acquire(self, timeout=10):
        """
        获取锁

        Args:
            timeout: 超时时间（秒），0表示不等待

        Returns:
            True if acquired, False if timeout

        Raises:
            BlockingIOError: 如果timeout=0且锁被占用
        """
        start_time = time.time()

        while True:
            try:
                self.lock_fd = open(self.lock_file, 'w')
                fcntl.flock(self.lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)

                # 写入锁信息（flock成功后，write/flush失败需释放锁）
                try:
                    self.lock_fd.write(f"Task: {self.task_name}\n")
                    self.lock_fd.write(f"PID: {os.getpid()}\n")
                    self.lock_fd.write(f"Time: {datetime.now()}\n")
                    self.lock_fd.flush()
                except OSError:
                    # 磁盘满等异常：释放文件锁和fd，防止永久锁定
                    try:
                        fcntl.flock(self.lock_fd, fcntl.LOCK_UN)
                    except Exception:
                        pass
                    self.lock_fd.close()
                    self.lock_fd = None
                    logger.error(f"写入锁文件失败: {self.task_name}")
                    return False

                logger.info(f"任务锁已获取: {self.task_name}")
                return True

            except BlockingIOError:
                # 关闭已打开的文件描述符，防止循环中重复open导致fd泄漏
                if self.lock_fd:
                    self.lock_fd.close()
                    self.lock_fd = None

                if timeout == 0:
                    logger.warning(f"任务锁已被占用: {self.task_name}")
                    raise

                elapsed = time.time() - start_time
                if elapsed >= timeout:
                    logger.warning(f"任务锁获取超时({timeout}秒): {self.task_name}")
                    return False

                time.sleep(1)

            except Exception as e:
                # 关闭已打开的文件描述符
                if self.lock_fd:
                    self.lock_fd.close()
                    self.lock_fd = None
                logger.error(f"获取任务锁失败: {e}")
                return False

    def release(self):
        """释放锁"""
        if self.lock_fd:
            try:
                fcntl.flock(self.lock_fd, fcntl.LOCK_UN)
                self.lock_fd.close()
                os.remove(self.lock_file)
                logger.info(f"任务锁已释放: {self.task_name}")
            except Exception as e:
                logger.error(f"释放任务锁失败: {e}")

    def __enter__(self):
        """上下文管理器入口"""
        if not self.acquire():
            raise RuntimeError(f"无法获取任务锁: {self.task_name}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器退出"""
        self.release()


class TaskStatusManager:
    """任务状态管理器：在数据库中记录任务状态"""

    def __init__(self, db_manager=None):
        """
        初始化任务状态管理器

        Args:
            db_manager: 数据库管理器实例
        """
        self.db_manager = db_manager or get_db_manager()

    def is_task_running(self, task_name):
        """
        检查任务是否正在运行

        Args:
            task_name: 任务名称

        Returns:
            True if running
        """
        with self.db_manager.get_session() as session:
            task = session.query(TaskStatus).filter_by(task_name=task_name).first()
            if task and task.status == 'running':
                # 检查是否超时（超过12小时认为异常）
                if task.started_at:
                    elapsed = (datetime.now() - task.started_at).total_seconds()
                    if elapsed > 43200:  # 12小时
                        logger.warning(f"任务{task_name}运行超时，可能已异常终止")
                        return False
                return True
            return False

    def start_task(self, task_name):
        """
        标记任务开始

        Args:
            task_name: 任务名称

        Returns:
            True if started successfully
        """
        with self.db_manager.get_session() as session:
            # 原子操作：检查 + 更新在同一个 session 内，消除 TOCTOU 竞态
            task = session.query(TaskStatus).filter_by(task_name=task_name).first()

            if task and task.status == 'running':
                logger.warning(f"任务{task_name}已在运行中")
                return False

            if task:
                task.status = 'running'
                task.started_at = datetime.now()
                task.completed_at = None
                task.progress = 0
                task.error_message = None
            else:
                task = TaskStatus(
                    task_name=task_name,
                    status='running',
                    started_at=datetime.now(),
                    progress=0
                )
                session.add(task)

            logger.info(f"任务已启动: {task_name}")
            return True

    def update_progress(self, task_name, progress, message=None):
        """
        更新任务进度

        Args:
            task_name: 任务名称
            progress: 进度百分比（0-100）
            message: 可选的消息
        """
        with self.db_manager.get_session() as session:
            task = session.query(TaskStatus).filter_by(task_name=task_name).first()
            if task:
                task.progress = min(100, max(0, progress))
                if message:
                    task.error_message = message

    def complete_task(self, task_name, success=True, message=None, status_override=None):
        """
        标记任务完成

        Args:
            task_name: 任务名称
            success: 是否成功
            message: 可选的消息
            status_override: 状态覆盖（如 skipped）
        """
        with self.db_manager.get_session() as session:
            task = session.query(TaskStatus).filter_by(task_name=task_name).first()

            if task:
                task.status = status_override or ('completed' if success else 'failed')
                task.completed_at = datetime.now()
                task.progress = 100 if (success or status_override == 'skipped') else task.progress
                if message:
                    task.error_message = message

                status_text = "跳过" if status_override == 'skipped' else ("成功" if success else "失败")
                logger.info(f"任务{status_text}: {task_name}")

    def get_task_status(self, task_name):
        """
        获取任务状态

        Args:
            task_name: 任务名称

        Returns:
            TaskStatus对象或None
        """
        with self.db_manager.get_session() as session:
            return session.query(TaskStatus).filter_by(task_name=task_name).first()

    @contextmanager
    def task_context(self, task_name):
        """
        任务上下文管理器

        Usage:
            with task_manager.task_context('daily_update'):
                # 执行任务
                pass
        """
        # 开始任务
        if not self.start_task(task_name):
            raise RuntimeError(f"无法启动任务: {task_name}")

        try:
            yield self
            # 任务成功
            self.complete_task(task_name, success=True)

        except Exception as e:
            # 任务失败
            self.complete_task(task_name, success=False, message=str(e))
            raise

    def cleanup_stale_locks(self, timeout_seconds=7200):
        """
        清理超时的 running 状态（容器重启或异常退出后使用）

        Args:
            timeout_seconds: 超时阈值（默认2小时）

        Returns:
            清理的任务数量
        """
        with self.db_manager.get_session() as session:
            cutoff_time = datetime.now() - timedelta(seconds=timeout_seconds)
            stale_tasks = session.query(TaskStatus).filter(
                TaskStatus.status == 'running',
                TaskStatus.started_at < cutoff_time
            ).all()

            cleaned = 0
            for task in stale_tasks:
                elapsed = (datetime.now() - task.started_at).total_seconds()
                task.status = 'failed'
                task.completed_at = datetime.now()
                task.error_message = (
                    f"Stale lock 清理（运行 {int(elapsed/60)} 分钟，"
                    f"超过阈值 {int(timeout_seconds/60)} 分钟）"
                )
                cleaned += 1
                logger.info(f"清理 stale lock: {task.task_name} "
                           f"(运行 {int(elapsed/60)} 分钟)")

            if cleaned:
                logger.info(f"共清理 {cleaned} 个 stale lock")
            return cleaned


@contextmanager
def task_execution_lock(task_name, timeout=10, db_manager=None):
    """
    综合任务执行锁（文件锁 + 数据库状态）

    Usage:
        with task_execution_lock('daily_update'):
            # 执行任务
            pass

    Args:
        task_name: 任务名称
        timeout: 文件锁超时时间
        db_manager: 数据库管理器
    """
    file_lock = TaskLock(task_name)
    status_manager = TaskStatusManager(db_manager)

    # 1. 检查数据库状态
    if status_manager.is_task_running(task_name):
        logger.warning(f"任务{task_name}已在运行，跳过本次执行")
        yield None
        return

    # 2. 获取文件锁
    if not file_lock.acquire(timeout=timeout):
        logger.warning(f"无法获取任务锁: {task_name}")
        yield None
        return

    try:
        # 3. 更新数据库状态
        if not status_manager.start_task(task_name):
            yield None
            return

        # 4. 执行任务
        yield status_manager

        # 5. 标记成功
        status_manager.complete_task(task_name, success=True)

    except Exception as e:
        # 标记失败
        status_manager.complete_task(task_name, success=False, message=str(e))
        raise

    finally:
        # 释放文件锁
        file_lock.release()


@contextmanager
def db_write_task_lock(task_name, timeout=300, db_manager=None, check_dependencies=True):
    """
    数据库写任务互斥锁（增强版）

    功能：
    1. 文件锁：防止同一任务多实例
    2. 任务组锁：防止db_write组内任务并发
    3. 依赖检查：确保前置任务已完成
    4. 数据库状态：记录任务状态

    Usage:
        with db_write_task_lock('daily_screening_workflow') as ctx:
            if ctx is None:
                return  # 无法获取锁或依赖未满足
            # 执行任务
            ctx.update_progress(50)

    Args:
        task_name: 任务名称
        timeout: 锁等待超时时间（秒）
        db_manager: 数据库管理器
        check_dependencies: 是否检查任务依赖
    """
    file_lock = TaskLock(task_name)
    status_manager = TaskStatusManager(db_manager)
    group_name = get_task_group(task_name)
    group_lock = get_group_lock(group_name) if group_name else None
    group_lock_acquired = False
    file_lock_acquired = False

    start_time = time.time()
    thread_id = threading.current_thread().ident

    logger.info(f"[TASK_LOCK] task={task_name} thread={thread_id} "
               f"group={group_name} attempting to acquire locks...")

    # 1. 检查任务依赖
    if check_dependencies:
        dependencies = TASK_DEPENDENCIES.get(task_name, [])
        for dep_task in dependencies:
            if not _check_dependency_satisfied(dep_task, db_manager):
                logger.warning(f"[TASK_LOCK] task={task_name} dependency not satisfied: {dep_task}")
                yield None
                return

    # 2. 检查数据库状态（是否已在运行）
    if status_manager.is_task_running(task_name):
        logger.warning(f"[TASK_LOCK] task={task_name} already running, skip")
        yield None
        return

    # 3. 获取任务组锁（如果有）
    if group_lock:
        lock_wait_start = time.time()
        logger.info(f"[TASK_LOCK] task={task_name} waiting for group lock: {group_name}")

        acquired = group_lock.acquire(timeout=timeout)
        lock_wait_time = time.time() - lock_wait_start

        if not acquired:
            logger.error(f"[TASK_LOCK] task={task_name} failed to acquire group lock "
                        f"after {timeout}s, group={group_name}")
            yield None
            return

        group_lock_acquired = True
        if lock_wait_time > 1:
            logger.info(f"[TASK_LOCK] task={task_name} acquired group lock "
                       f"after {lock_wait_time:.2f}s")

    # 4. 获取文件锁
    remaining_timeout = max(10, timeout - (time.time() - start_time))
    if not file_lock.acquire(timeout=remaining_timeout):
        logger.error(f"[TASK_LOCK] task={task_name} failed to acquire file lock")
        if group_lock_acquired:
            group_lock.release()
        yield None
        return
    file_lock_acquired = True

    try:
        # 5. 更新数据库状态
        if not status_manager.start_task(task_name):
            logger.error(f"[TASK_LOCK] task={task_name} failed to update db status")
            yield None
            return

        total_wait = time.time() - start_time
        logger.info(f"[TASK_LOCK] task={task_name} all locks acquired, "
                   f"total_wait={total_wait:.2f}s, starting execution...")

        # 6. 执行任务
        yield status_manager

        # 7. 标记成功
        status_manager.complete_task(task_name, success=True)
        logger.info(f"[TASK_LOCK] task={task_name} completed successfully")

    except Exception as e:
        # 标记失败（包含锁相关错误信息）
        error_msg = str(e)
        is_lock_error = 'locked' in error_msg.lower() or 'busy' in error_msg.lower()
        if is_lock_error:
            error_msg = f"[DATABASE_LOCKED] {error_msg}"
        status_manager.complete_task(task_name, success=False, message=error_msg)
        logger.error(f"[TASK_LOCK] task={task_name} failed: {error_msg}")
        raise

    finally:
        # 释放所有锁（顺序：文件锁 -> 组锁）
        if file_lock_acquired:
            file_lock.release()
        if group_lock_acquired:
            group_lock.release()
            logger.debug(f"[TASK_LOCK] task={task_name} released group lock: {group_name}")


def _check_dependency_satisfied(dep_task: str, db_manager=None) -> bool:
    """
    检查依赖任务是否已完成（今天内）

    Args:
        dep_task: 依赖的任务名称
        db_manager: 数据库管理器

    Returns:
        True if dependency is satisfied
    """
    try:
        dm = db_manager or get_db_manager()
        with dm.get_session(readonly=True) as session:
            task = session.query(TaskStatus).filter_by(task_name=dep_task).first()
            if not task:
                return False

            # 检查是否在最近24小时内完成（跨天边界安全）
            if task.status in ('completed', 'skipped') and task.completed_at:
                cutoff = datetime.now() - timedelta(hours=24)
                if task.completed_at >= cutoff:
                    return True

            return False
    except Exception as e:
        logger.warning(f"[DEPENDENCY_CHECK] failed to check {dep_task}: {e}")
        return False


def check_task_dependencies(task_name: str, db_manager=None) -> dict:
    """
    检查任务的所有依赖状态

    Args:
        task_name: 任务名称
        db_manager: 数据库管理器

    Returns:
        {
            'satisfied': True/False,
            'dependencies': {
                'dep_task_1': {'satisfied': True, 'completed_at': '...'},
                'dep_task_2': {'satisfied': False, 'reason': '...'},
            }
        }
    """
    dependencies = TASK_DEPENDENCIES.get(task_name, [])
    if not dependencies:
        return {'satisfied': True, 'dependencies': {}}

    result = {'satisfied': True, 'dependencies': {}}

    for dep_task in dependencies:
        satisfied = _check_dependency_satisfied(dep_task, db_manager)
        result['dependencies'][dep_task] = {'satisfied': satisfied}
        if not satisfied:
            result['satisfied'] = False

    return result
