"""Names and read compatibility for NAV drawdown observation counts."""
from __future__ import annotations

from typing import Any

ANNUAL_DRAWDOWN_OBSERVATIONS = '1年-最长回撤观测数'
DRAWDOWN_OBSERVATION_DEFINITION = (
    '同一风险财富序列连续严格低于此前峰值的最长观测次数；达到或超过峰值时结束，'
    '含期末尚未恢复区间。缺失日期不累计，不是自然天数或承诺恢复天数。'
)
LEGACY_DRAWDOWN_COLUMNS = {
    '1年-最大回撤持续期(天)': ANNUAL_DRAWDOWN_OBSERVATIONS,
    '最新1年-最大回撤持续期(天)': '最新1年-最长回撤观测数',
    '最大回撤持续期(天)': '最长回撤观测数',
    '最新最大回撤持续期(天)': '最新最长回撤观测数',
}


def normalize_drawdown_duration_record(row: dict[str, Any]) -> dict[str, Any]:
    """Read legacy labels without overriding a canonical value, including NULL."""
    result = dict(row)
    for old, new in LEGACY_DRAWDOWN_COLUMNS.items():
        if old in result:
            if new not in result:
                result[new] = result[old]
            del result[old]
    return result


def normalize_drawdown_duration_columns(frame):
    """Return a canonical copy; never modify an immutable source artifact."""
    result = frame.copy()
    for old, new in LEGACY_DRAWDOWN_COLUMNS.items():
        if old in result.columns:
            if new in result.columns:
                result = result.drop(columns=[old])
            else:
                result = result.rename(columns={old: new})
    return result
