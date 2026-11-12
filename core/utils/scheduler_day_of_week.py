#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
调度 day_of_week 语义辅助函数。

统一处理项目配置中的周几语义：
- 数字 0-6 采用项目内语义：0=周日, 6=周六
- 命名值 sun..sat 采用 Cron 原生语义
- daily 配置中的范围/列表会被规范化为 Cron 可识别表达式
"""

from __future__ import annotations

from typing import Optional


PROJECT_WEEKDAY_TO_CRON = {
    0: 'sun',
    1: 'mon',
    2: 'tue',
    3: 'wed',
    4: 'thu',
    5: 'fri',
    6: 'sat',
    '0': 'sun',
    '1': 'mon',
    '2': 'tue',
    '3': 'wed',
    '4': 'thu',
    '5': 'fri',
    '6': 'sat',
}

CRON_WEEKDAY_LABELS = {
    'sun': '日',
    'mon': '一',
    'tue': '二',
    'wed': '三',
    'thu': '四',
    'fri': '五',
    'sat': '六',
}


def _normalize_weekday_piece_to_cron(value) -> Optional[str]:
    """把单个周几片段规范化为 Cron 命名值。"""
    if isinstance(value, int):
        return PROJECT_WEEKDAY_TO_CRON.get(value)

    normalized = str(value or '').strip().lower()
    if not normalized:
        return None
    if normalized in PROJECT_WEEKDAY_TO_CRON:
        return PROJECT_WEEKDAY_TO_CRON[normalized]
    if normalized in CRON_WEEKDAY_LABELS:
        return normalized
    return None


def normalize_daily_day_of_week_to_cron(value) -> Optional[str]:
    """
    将 daily 调度配置中的 day_of_week 规范化为 Cron 语义。

    兼容以下输入：
    - 1 / '1'    -> mon
    - '1-5'      -> mon-fri
    - '1,3,5'    -> mon,wed,fri
    - 'mon-fri'  -> mon-fri
    """
    if value is None:
        return None

    if isinstance(value, int):
        return _normalize_weekday_piece_to_cron(value)

    normalized = str(value or '').strip().lower()
    if not normalized:
        return None

    if ',' in normalized:
        parts = []
        for part in normalized.split(','):
            cron_value = _normalize_weekday_piece_to_cron(part)
            if cron_value is None:
                return None
            parts.append(cron_value)
        return ','.join(parts)

    if '-' in normalized:
        start, end = [part.strip() for part in normalized.split('-', 1)]
        cron_start = _normalize_weekday_piece_to_cron(start)
        cron_end = _normalize_weekday_piece_to_cron(end)
        if cron_start is None or cron_end is None:
            return None
        return f'{cron_start}-{cron_end}'

    return _normalize_weekday_piece_to_cron(normalized)


def format_day_of_week_window(value) -> str:
    """把 day_of_week 配置格式化为中文展示。"""
    normalized = normalize_daily_day_of_week_to_cron(value)
    if not normalized:
        return ''

    if ',' in normalized:
        labels = [
            CRON_WEEKDAY_LABELS.get(part.strip(), part.strip())
            for part in normalized.split(',')
            if part.strip()
        ]
        if labels:
            return f"每周{'/'.join(labels)}"
        return ''

    if '-' in normalized:
        start, end = [part.strip() for part in normalized.split('-', 1)]
        start_label = CRON_WEEKDAY_LABELS.get(start, start)
        end_label = CRON_WEEKDAY_LABELS.get(end, end)
        return f'每周{start_label}到{end_label}'

    return f"每周{CRON_WEEKDAY_LABELS.get(normalized, normalized)}"
