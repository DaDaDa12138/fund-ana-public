#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
screening_runs 维护工具

用途：
1. 清理异常中断后遗留的 running 状态（僵尸 run）
2. 统一在调度器启动/巡检阶段执行，避免监控口径被污染
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Dict, Optional

from database.connection import get_db_manager
from database.models import ScreeningRun

logger = logging.getLogger(__name__)


def cleanup_stale_screening_runs(
    db_manager=None,
    stale_hours: float = 2.0,
    fail_reason: Optional[str] = None,
) -> Dict[str, int]:
    """
    清理超时未结束的 screening_runs 记录（running -> failed）。

    Args:
        db_manager: 可选 DatabaseManager，默认使用全局实例
        stale_hours: 判定为僵尸的最小时长（小时）
        fail_reason: 可选失败原因，未提供时使用默认文案

    Returns:
        {'cleaned': int, 'inspected_running': int}
    """
    manager = db_manager or get_db_manager()
    reason = fail_reason or (
        f"调度器巡检自动清理：运行超过{stale_hours:g}小时仍未结束（疑似异常中断）"
    )
    cutoff = datetime.now() - timedelta(hours=stale_hours)

    with manager.get_session(task_name='cleanup_stale_screening_runs') as session:
        running_rows = session.query(ScreeningRun).filter(
            ScreeningRun.status == 'running'
        ).all()

        stale_rows = [
            row for row in running_rows
            if row.started_at is not None and row.started_at < cutoff
        ]

        if not stale_rows:
            return {
                'cleaned': 0,
                'inspected_running': len(running_rows),
            }

        now = datetime.now()
        for row in stale_rows:
            row.status = 'failed'
            row.finished_at = now
            if not row.fail_reason:
                row.fail_reason = reason

        logger.warning(
            "已清理 %d 条 screening_runs 僵尸记录（running -> failed，阈值 %.2f 小时）",
            len(stale_rows),
            stale_hours,
        )
        return {
            'cleaned': len(stale_rows),
            'inspected_running': len(running_rows),
        }

