#!/usr/bin/env python3
"""
任务统计数据报告工具
为采集脚本提供统一的任务历史更新接口
"""

import os
from typing import Optional, Dict, Any


def update_task_history(
    success_count: int,
    total_count: int,
    failed_count: Optional[int] = None,
    inserted_count: Optional[int] = None,
    extra_data: Optional[Dict[str, Any]] = None,
    status_override: Optional[str] = None,
):
    """
    更新任务历史记录的统计信息

    此函数会检测环境变量TASK_HISTORY_ID，如果存在则更新任务历史记录。
    如果不存在（手动执行），则不做任何操作。

    Args:
        success_count: 成功处理数
        total_count: 总处理数
        failed_count: 失败数（可选，默认为 total_count - success_count）
        inserted_count: 新增记录数（可选，默认为0）
        extra_data: 额外统计信息（可选，会写入 task_history.extra_data）
        status_override: 已校验的业务终态；省略时保留其他采集器的旧契约
    """
    task_history_id = os.environ.get('TASK_HISTORY_ID')

    if not task_history_id:
        # 手动执行，不需要更新任务历史
        return

    try:
        from database import DatabaseManager
        from database.task_history import TaskHistoryManager
        from database.models import TaskHistory
        # 安全转换task_history_id
        try:
            history_id = int(task_history_id)
        except ValueError:
            print(f"⚠️  TASK_HISTORY_ID 不是有效整数: {task_history_id}")
            return

        # 计算失败数
        if failed_count is None:
            failed_count = total_count - success_count

        # 默认插入数为0
        if inserted_count is None:
            inserted_count = 0

        db_manager = DatabaseManager()
        with db_manager.get_session() as session:
            # 查询任务记录获取started_at
            task_record = session.query(TaskHistory).filter_by(id=history_id).first()
            if not task_record:
                print(f"⚠️  未找到任务历史记录 (ID: {task_history_id})")
                return

            history_manager = TaskHistoryManager(session)

            # 构造task_info（需要id和started_at）
            task_info = {
                'id': history_id,
                'started_at': task_record.started_at,
                'task_name': task_record.task_name
            }

            # 更新任务历史
            history_manager.complete_task(
                task_info=task_info,
                success=status_override not in {'degraded', 'failed', 'skipped'},
                records_processed=total_count,
                records_updated=success_count,
                records_failed=failed_count,
                records_inserted=inserted_count,
                extra_data=extra_data,
                status_override=status_override,
            )

            print(f"✅ 已更新任务历史记录 (ID: {task_history_id})")

    except Exception as e:
        print(f"⚠️  更新任务历史记录失败: {e}")


def get_task_info():
    """
    获取任务信息

    Returns:
        dict: 包含task_history_id和task_name的字典，如果不是任务执行则返回None
    """
    task_history_id = os.environ.get('TASK_HISTORY_ID')
    task_name = os.environ.get('TASK_NAME', '手动执行')

    if task_history_id:
        try:
            return {
                'task_history_id': int(task_history_id),
                'task_name': task_name
            }
        except ValueError:
            return None
    return None
