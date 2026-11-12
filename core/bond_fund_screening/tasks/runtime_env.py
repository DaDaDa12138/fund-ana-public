#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
筛选任务运行时环境辅助。

目标：
1. 为高频筛选入口统一补齐 trigger 元数据；
2. 默认将高频任务切到 picked_only，避免 screening_decisions 继续无上限膨胀；
3. 保留显式环境变量覆写能力，便于人工排障时按需切回 all。
"""

from __future__ import annotations

import os
import re
from datetime import datetime
from typing import MutableMapping, Optional


REPORT_DATE_ENV = 'FUND_ANA_REPORT_DATE'


def screening_report_date(
    env: Optional[MutableMapping[str, str]] = None,
    *,
    now: datetime | None = None,
) -> str:
    """Return the workflow business date used in screening artifact names.

    A screening run may cross midnight while its inputs still belong to the
    workflow batch date. The orchestrator supplies ``FUND_ANA_REPORT_DATE``
    for that case; direct invocations continue to use the local calendar date.
    """
    target = env if env is not None else os.environ
    value = str(target.get(REPORT_DATE_ENV) or '').strip()
    if re.fullmatch(r'20\d{6}', value):
        return value
    return (now or datetime.now()).strftime('%Y%m%d')


def apply_screening_runtime_defaults(
    env: Optional[MutableMapping[str, str]] = None,
    *,
    trigger_source: str,
    task_id: str,
    task_name: str,
    decision_persist_mode: str = 'picked_only',
    override: bool = False,
) -> MutableMapping[str, str]:
    """
    为筛选任务补齐运行时环境变量。

    Args:
        env: 目标环境变量映射；默认直接写入 os.environ
        trigger_source: 触发来源
        task_id: 触发任务ID
        task_name: 触发任务名称
        decision_persist_mode: 决策持久化模式
        override: True 时强制覆盖，False 时仅填充缺失值

    Returns:
        已写入默认值的环境变量映射
    """
    target = env if env is not None else os.environ
    values = {
        'SCREENING_DECISION_PERSIST_MODE': str(decision_persist_mode),
        'SCREENING_TRIGGER_SOURCE': str(trigger_source),
        'SCREENING_TRIGGER_TASK_ID': str(task_id),
        'SCREENING_TRIGGER_TASK_NAME': str(task_name),
    }

    for key, value in values.items():
        if override:
            target[key] = value
        else:
            target.setdefault(key, value)

    return target


def _normalize_period_token(
    *,
    period_years: Optional[int] = None,
    period_days: Optional[int] = None,
) -> str:
    """将周期参数转换为稳定 token，便于命名 task_id/task_name。"""
    if period_days is not None:
        day_value = int(period_days)
        day_mapping = {
            7: '1week',
            30: '1month',
            90: '3month',
            180: '6month',
        }
        return day_mapping.get(day_value, f'{day_value}day')

    if period_years is not None:
        year_value = int(period_years)
        return f'{year_value}year'

    return 'unknown_period'


def apply_period_analyzer_runtime_defaults(
    env: Optional[MutableMapping[str, str]] = None,
    *,
    fund_type: str,
    period_years: Optional[int] = None,
    period_days: Optional[int] = None,
    decision_persist_mode: str = 'picked_only',
    override: bool = False,
) -> MutableMapping[str, str]:
    """
    为单周期 analyzer 入口补齐默认运行时上下文。

    说明：
    - 该入口既会被人工 CLI 直接调用，也会被 Web/API 子进程调用；
    - 默认只在上下文缺失时补齐，避免覆盖 Web/API 已显式注入的 trigger 元数据。
    """
    normalized_fund_type = str(fund_type or 'bond').strip().lower() or 'bond'
    period_token = _normalize_period_token(
        period_years=period_years,
        period_days=period_days,
    )
    task_token = f'direct_{normalized_fund_type}_{period_token}_analyzer'
    return apply_screening_runtime_defaults(
        env,
        trigger_source=f'direct_{normalized_fund_type}_period_script',
        task_id=task_token,
        task_name=task_token,
        decision_persist_mode=decision_persist_mode,
        override=override,
    )
