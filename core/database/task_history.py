#!/usr/bin/env python3
"""
任务执行历史管理模块
提供任务执行记录的增删改查功能
增强版：包含详细的任务生命周期日志、统计分析日志
"""

import json
import traceback
import threading
import time
from datetime import datetime
from typing import Dict, List, Optional, Any
from sqlalchemy.orm import Session
from database.models import TaskHistory
from utils.logger import get_logger, setup_logger

logger = get_logger(__name__)

# 尝试导入增强日志
try:
    from utils.enhanced_logger import get_task_logger, get_database_logger
    HAS_ENHANCED_LOGGER = True
except ImportError:
    HAS_ENHANCED_LOGGER = False

task_logger = None
db_logger = None

# 全局锁：防止并发创建任务记录时ID冲突
_task_creation_lock = threading.Lock()


class TaskHistoryManager:
    """任务历史记录管理器"""

    def __init__(self, session: Session):
        """
        初始化管理器

        Args:
            session: SQLAlchemy会话对象
        """
        global task_logger, db_logger
        setup_logger(__name__)
        if HAS_ENHANCED_LOGGER:
            if task_logger is None:
                task_logger = get_task_logger()
            if db_logger is None:
                db_logger = get_database_logger()
        self.session = session

    def start_task(self, task_name: str) -> Dict[str, Any]:
        """
        记录任务开始，创建数据库记录（增强版日志）
        使用全局锁防止并发创建时ID冲突

        Args:
            task_name: 任务名称

        Returns:
            任务开始时的状态信息，包含id
        """
        started_at = datetime.now()
        thread_id = threading.current_thread().ident
        start_time = time.time()

        # 注：log_start 需要task_id，在创建记录后调用

        # 使用全局锁保护任务记录创建，防止并发ID冲突
        with _task_creation_lock:
            lock_wait_time = time.time() - start_time

            # 创建任务记录
            history = TaskHistory(
                task_name=task_name,
                status='running',
                started_at=started_at
            )

            try:
                self.session.add(history)
                self.session.flush()  # 先flush确保生成ID
                self.session.commit()

                task_id = history.id
                elapsed = (time.time() - start_time) * 1000

                if HAS_ENHANCED_LOGGER:
                    db_logger.log_query('INSERT', 'task_history',
                                       task_id=task_id,
                                       task_name=task_name,
                                       elapsed_ms=round(elapsed, 2),
                                       lock_wait_ms=round(lock_wait_time * 1000, 2))
                    task_logger.log_start(task_id, task_name, thread_id=thread_id)
                    task_logger.log_progress(task_id, 0, 100,
                                            message=f'{task_name} STARTED')
                else:
                    logger.info(f"[HISTORY] 任务开始 id={task_id} name={task_name} "
                               f"elapsed={elapsed:.2f}ms")

                return {
                    'id': task_id,
                    'task_name': task_name,
                    'started_at': started_at
                }
            except Exception as e:
                elapsed = (time.time() - start_time) * 1000
                error_type = type(e).__name__

                if HAS_ENHANCED_LOGGER:
                    db_logger.log_error('INSERT_FAILED', str(e),
                                       table='task_history',
                                       task_name=task_name,
                                       elapsed_ms=round(elapsed, 2))
                else:
                    logger.error(f"[HISTORY] 创建任务记录失败: {error_type}: {e}")

                self.session.rollback()
                raise

    def complete_task(self, task_info: Dict, success: bool = True,
                     records_processed: int = 0, records_inserted: int = 0,
                     records_updated: int = 0, records_failed: int = 0,
                     error_message: str = None, extra_data: Dict = None,
                     status_override: Optional[str] = None):
        """
        记录任务完成，更新数据库记录（增强版日志）

        Args:
            task_info: start_task()返回的任务信息
            success: 是否成功（当 status_override 非空时忽略）
            records_processed: 处理记录数
            records_inserted: 插入记录数
            records_updated: 更新记录数
            records_failed: 失败记录数
            error_message: 错误信息
            extra_data: 额外元数据
            status_override: 状态覆盖（如 'skipped'），非空时直接使用此值作为 status
        """
        completed_at = datetime.now()
        duration = int((completed_at - task_info['started_at']).total_seconds())
        start_time = time.time()
        task_id = task_info['id']
        task_name = task_info['task_name']

        final_status = status_override or ('success' if success else 'failed')
        log_success = final_status != 'failed'

        if HAS_ENHANCED_LOGGER:
            task_logger.log_progress(task_name, 100, 100,
                                    task_id=task_id,
                                    phase='COMPLETING',
                                    success=log_success)

        try:
            # 获取现有记录
            query_start = time.time()
            history = self.session.query(TaskHistory).filter(
                TaskHistory.id == task_id
            ).first()
            query_elapsed = (time.time() - query_start) * 1000

            if not history:
                if HAS_ENHANCED_LOGGER:
                    db_logger.log_error('UPDATE_FAILED', f"未找到任务记录 ID={task_id}",
                                       table='task_history',
                                       task_id=task_id)
                else:
                    logger.warning(f"[HISTORY] 未找到任务记录 ID={task_id}")
                return None

            # 更新记录（status_override 优先，如 'skipped'）
            history.status = final_status
            history.completed_at = completed_at
            history.duration = duration
            history.records_processed = records_processed
            history.records_inserted = records_inserted
            history.records_updated = records_updated
            history.records_failed = records_failed
            history.error_message = error_message
            history.error_traceback = traceback.format_exc() if error_message else None
            history.extra_data = json.dumps(extra_data, ensure_ascii=False) if extra_data else None

            # 先flush确保数据写入
            self.session.flush()

            # 执行commit
            commit_start = time.time()
            self.session.commit()
            commit_elapsed = (time.time() - commit_start) * 1000

            # 刷新对象，确保从数据库重新读取
            self.session.refresh(history)

            # 验证更新是否成功
            if history.status == 'running':
                if HAS_ENHANCED_LOGGER:
                    db_logger.log_error('UPDATE_VERIFY_FAILED',
                                       f"任务状态未更新成功，仍为running",
                                       table='task_history',
                                       task_id=task_id)

                logger.error(f"⚠️ 任务 {task_name} (ID={task_id}) 更新失败：状态仍为running")
                # 尝试再次更新
                history.status = final_status
                self.session.commit()
                self.session.refresh(history)

                if history.status == 'running':
                    raise Exception(f"无法更新任务状态，ID={task_id}")

            total_elapsed = (time.time() - start_time) * 1000

            if HAS_ENHANCED_LOGGER:
                db_logger.log_query('UPDATE', 'task_history',
                                   task_id=task_id,
                                   task_name=task_name,
                                   new_status=history.status,
                                   duration_sec=duration,
                                   elapsed_ms=round(total_elapsed, 2),
                                   query_ms=round(query_elapsed, 2),
                                   commit_ms=round(commit_elapsed, 2))

                task_logger.log_complete(task_name, duration * 1000,
                                        success=log_success,
                                        task_id=task_id,
                                        records_processed=records_processed,
                                        records_inserted=records_inserted,
                                        records_updated=records_updated,
                                        records_failed=records_failed)
            else:
                status_icon = "✅" if final_status == 'success' else ("⏭️" if final_status == 'skipped' else "❌")
                logger.info(f"[HISTORY] {status_icon} 任务完成 id={task_id} name={task_name} "
                           f"duration={duration}s processed={records_processed} "
                           f"status={history.status} elapsed={total_elapsed:.2f}ms")

            return history.id

        except Exception as e:
            total_elapsed = (time.time() - start_time) * 1000
            error_type = type(e).__name__

            if HAS_ENHANCED_LOGGER:
                db_logger.log_error('UPDATE_EXCEPTION', str(e),
                                   table='task_history',
                                   task_id=task_id,
                                   task_name=task_name,
                                   elapsed_ms=round(total_elapsed, 2),
                                   error_type=error_type)
            else:
                logger.error(f"[HISTORY] 更新任务历史失败: {error_type}: {e}")

            self.session.rollback()
            return None

    def merge_extra_data(self, task_info: Dict, extra_data_patch: Dict) -> Optional[int]:
        """
        合并任务记录 extra_data，不改变任务状态。

        适用于任务仍处于 running 状态时，增量写入失败快照等轻量审计信息，
        避免等待重试期间进程中断后丢失最近一次真实错误上下文。

        Args:
            task_info: start_task() 返回的任务信息
            extra_data_patch: 需要合并写入的额外元数据

        Returns:
            成功时返回任务 ID，失败返回 None
        """
        if not isinstance(extra_data_patch, dict) or not extra_data_patch:
            return None

        task_id = task_info['id']
        task_name = task_info['task_name']
        start_time = time.time()

        try:
            history = self.session.query(TaskHistory).filter(
                TaskHistory.id == task_id
            ).first()

            if not history:
                logger.warning(f"[HISTORY] 未找到任务记录 ID={task_id}")
                return None

            existing_extra_data = {}
            if history.extra_data:
                try:
                    parsed_extra_data = json.loads(history.extra_data)
                except (TypeError, ValueError, json.JSONDecodeError):
                    parsed_extra_data = {}
                if isinstance(parsed_extra_data, dict):
                    existing_extra_data = parsed_extra_data

            existing_extra_data.update(extra_data_patch)
            history.extra_data = json.dumps(existing_extra_data, ensure_ascii=False)

            self.session.flush()
            self.session.commit()
            self.session.refresh(history)

            elapsed = (time.time() - start_time) * 1000
            logger.info(
                f"[HISTORY] 合并额外元数据 id={task_id} name={task_name} "
                f"elapsed={elapsed:.2f}ms keys={list(extra_data_patch.keys())}"
            )
            return history.id

        except Exception as e:
            logger.error(f"[HISTORY] 合并任务额外元数据失败: {type(e).__name__}: {e}")
            self.session.rollback()
            return None

    def get_recent_history(self, task_name: Optional[str] = None,
                          limit: int = 50) -> List[TaskHistory]:
        """
        获取最近的执行历史

        Args:
            task_name: 任务名称筛选（可选）
            limit: 返回记录数

        Returns:
            任务历史记录列表
        """
        query = self.session.query(TaskHistory)

        if task_name:
            query = query.filter(TaskHistory.task_name == task_name)

        query = query.order_by(TaskHistory.started_at.desc()).limit(limit)

        return query.all()

    def get_task_stats(self, task_name: str, days: int = 7) -> Dict[str, Any]:
        """
        获取任务统计信息（增强版日志）

        Args:
            task_name: 任务名称
            days: 统计天数

        Returns:
            统计信息字典
        """
        from sqlalchemy import func
        from datetime import timedelta

        start_time = time.time()
        cutoff_date = datetime.now() - timedelta(days=days)

        # 统计查询
        total = self.session.query(func.count(TaskHistory.id)).filter(
            TaskHistory.task_name == task_name,
            TaskHistory.started_at >= cutoff_date
        ).scalar()

        success_count = self.session.query(func.count(TaskHistory.id)).filter(
            TaskHistory.task_name == task_name,
            TaskHistory.status == 'success',
            TaskHistory.started_at >= cutoff_date
        ).scalar()

        skipped_count = self.session.query(func.count(TaskHistory.id)).filter(
            TaskHistory.task_name == task_name,
            TaskHistory.status == 'skipped',
            TaskHistory.started_at >= cutoff_date
        ).scalar()

        avg_duration = self.session.query(func.avg(TaskHistory.duration)).filter(
            TaskHistory.task_name == task_name,
            TaskHistory.status == 'success',
            TaskHistory.started_at >= cutoff_date
        ).scalar()

        elapsed = (time.time() - start_time) * 1000

        effective_executions = max((total or 0) - (skipped_count or 0), 0)

        stats = {
            'total_executions': total or 0,
            'success_count': success_count or 0,
            'skipped_count': skipped_count or 0,
            'failed_count': max(effective_executions - (success_count or 0), 0),
            'effective_executions': effective_executions,
            'success_rate': round((success_count / effective_executions * 100), 2) if effective_executions else 0,
            'avg_duration': int(avg_duration) if avg_duration else 0
        }

        if HAS_ENHANCED_LOGGER:
            db_logger.log_query('STATS', 'task_history',
                               task_name=task_name,
                               days=days,
                               elapsed_ms=round(elapsed, 2),
                               **stats)
        else:
            logger.debug(f"[HISTORY] 统计查询 task={task_name} days={days} "
                        f"total={stats['total_executions']} success_rate={stats['success_rate']}%")

        return stats

    def cleanup_old_history(self, days: int = 90) -> int:
        """
        清理旧的历史记录（增强版日志）

        Args:
            days: 保留天数

        Returns:
            删除的记录数
        """
        from datetime import timedelta

        start_time = time.time()
        cutoff_date = datetime.now() - timedelta(days=days)

        if HAS_ENHANCED_LOGGER:
            db_logger.log_query('DELETE_START', 'task_history',
                               retention_days=days,
                               cutoff_date=cutoff_date.isoformat())

        try:
            # 先统计要删除的记录数
            count_to_delete = self.session.query(TaskHistory).filter(
                TaskHistory.started_at < cutoff_date
            ).count()

            if count_to_delete == 0:
                if HAS_ENHANCED_LOGGER:
                    db_logger.log_query('DELETE_SKIP', 'task_history',
                                       retention_days=days,
                                       message="无需清理的记录")
                else:
                    logger.info(f"[HISTORY] 无需清理的记录（{days}天前无数据）")
                return 0

            # 执行删除
            deleted = self.session.query(TaskHistory).filter(
                TaskHistory.started_at < cutoff_date
            ).delete()

            self.session.commit()
            elapsed = (time.time() - start_time) * 1000

            if HAS_ENHANCED_LOGGER:
                db_logger.log_query('DELETE', 'task_history',
                                   rows_affected=deleted,
                                   retention_days=days,
                                   elapsed_ms=round(elapsed, 2))
            else:
                logger.info(f"[HISTORY] 清理完成 deleted={deleted} days={days} "
                           f"elapsed={elapsed:.2f}ms")

            return deleted

        except Exception as e:
            elapsed = (time.time() - start_time) * 1000
            error_type = type(e).__name__

            if HAS_ENHANCED_LOGGER:
                db_logger.log_error('DELETE_FAILED', str(e),
                                   table='task_history',
                                   retention_days=days,
                                   elapsed_ms=round(elapsed, 2),
                                   error_type=error_type)
            else:
                logger.error(f"[HISTORY] 清理历史记录失败: {error_type}: {e}")

            self.session.rollback()
            return 0
