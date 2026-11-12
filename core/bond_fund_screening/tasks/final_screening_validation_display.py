"""Publish actual validation provenance while preserving historical exports."""
from __future__ import annotations

import pandas as pd


def apply_validation_display(frame: pd.DataFrame, statuses: dict, normalize_code) -> pd.DataFrame:
    current = frame.copy()
    if '基金代码' not in current:
        return current
    is_cross_endpoint = any(item.get('validation_source') == 'eastmoney_cross_endpoint'
                            for item in statuses.values())
    prefix = '数据校验' if is_cross_endpoint else 'AutoStock校验'
    def item(code):
        return statuses.get(normalize_code(code), {})
    def status(code):
        value = item(code)
        return value.get('validation_status', value.get('autostock_validation_status', 'not_checked'))
    display = {'strict_match': '通过', 'soft_pass_with_warnings': '通过（含软告警）', 'hard_block': '拦截'}
    current[prefix + '状态'] = current['基金代码'].map(lambda code: display.get(status(code), '未校验'))
    current[prefix + '备注'] = current['基金代码'].map(lambda code: item(code).get(
        'validation_warning_display', item(code).get('autostock_warning_display', '')))
    if is_cross_endpoint:
        current['数据校验原始状态'] = current['基金代码'].map(status)
        current['数据校验来源'] = current['基金代码'].map(lambda code: item(code).get('validation_source'))
        current['数据校验契约版本'] = current['基金代码'].map(lambda code: item(code).get('validation_contract'))
        current = current.drop(columns=[name for name in current if str(name).startswith('AutoStock')])
    return current
