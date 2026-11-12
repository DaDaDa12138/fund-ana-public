from __future__ import annotations

from typing import Any

import pandas as pd
from core.utils.drawdown_duration import DRAWDOWN_OBSERVATION_DEFINITION, LEGACY_DRAWDOWN_COLUMNS

DRAWDOWN_OBSERVATION_FIELDS = frozenset({
    *LEGACY_DRAWDOWN_COLUMNS, *LEGACY_DRAWDOWN_COLUMNS.values(),
    'max_drawdown_duration', 'latest_max_drawdown_duration',
})


DECIMAL_RATIO_PERCENT_FIELDS = frozenset(
    {
        "1年-波动率",
        "最新波动率",
        "1年-最大回撤(%)",
        "最新最大回撤(%)",
        "1年-VaR95(%)",
        "1年-VaR99(%)",
        "最新VaR95(%)",
        "最新VaR99(%)",
        "volatility",
        "max_drawdown",
        "var_95",
        "var_99",
    }
)

_FIELD_DISPLAY_LABELS = {
    "1年-波动率": "1年波动率",
    "最新波动率": "最新波动率",
    "1年-最大回撤(%)": "1年最大回撤",
    "最新最大回撤(%)": "最新最大回撤",
    "1年-VaR95(%)": "1年VaR95",
    "1年-VaR99(%)": "1年VaR99",
    "最新VaR95(%)": "最新VaR95",
    "最新VaR99(%)": "最新VaR99",
    "volatility": "波动率",
    "max_drawdown": "最大回撤",
    "var_95": "VaR95",
    "var_99": "VaR99",
}


def normalize_metric_field_name(field: object) -> str:
    return str(field or "").strip()


def is_decimal_ratio_percent_field(field: object) -> bool:
    return normalize_metric_field_name(field) in DECIMAL_RATIO_PERCENT_FIELDS


def parse_metric_number(value: object) -> float | None:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    text = str(value).strip()
    if not text:
        return None
    text = text.replace(",", "")
    if text.endswith("%"):
        text = text[:-1].strip()
    try:
        number = float(text)
    except (TypeError, ValueError):
        return None
    return number


def decimal_ratio_to_display_percent(value: object) -> float | None:
    number = parse_metric_number(value)
    if number is None:
        return None
    return number * 100.0


def normalize_metric_number_text(value: object, *, digits: int = 2) -> str:
    number = parse_metric_number(value)
    if number is None:
        return "" if value is None else str(value).strip()
    normalized = f"{number:.{digits}f}"
    if digits > 0:
        normalized = normalized.rstrip("0").rstrip(".")
    return normalized if normalized != "-0" else "0"


def format_metric_display_text(field: object, value: object, *, digits: int | None = None, suffix: bool = False) -> str:
    if is_decimal_ratio_percent_field(field):
        raw_text = "" if value is None else str(value).strip()
        if raw_text.endswith("%"):
            return raw_text
        display_value = decimal_ratio_to_display_percent(value)
        if display_value is None:
            return "" if value is None else str(value).strip()
        text = normalize_metric_number_text(display_value, digits=2 if digits is None else digits)
        return f"{text}%" if suffix and text else text
    text = normalize_metric_number_text(value, digits=2 if digits is None else digits)
    return text


def apply_metric_display_units_to_frame(
    df: pd.DataFrame,
    *,
    fields: set[str] | None = None,
    digits: int = 2,
) -> pd.DataFrame:
    if not isinstance(df, pd.DataFrame):
        return pd.DataFrame()
    result = df.copy()
    target_fields = fields or set(DECIMAL_RATIO_PERCENT_FIELDS)
    for column in result.columns:
        if normalize_metric_field_name(column) not in target_fields:
            continue
        result[column] = result[column].map(
            lambda value: format_metric_display_text(column, value, digits=digits, suffix=False)
        )
    return result


def metric_excel_number_format(field: object) -> str | None:
    if is_decimal_ratio_percent_field(field):
        return "0.00%"
    if normalize_metric_field_name(field) in DRAWDOWN_OBSERVATION_FIELDS:
        return "0"
    return None


def metric_unit_metadata() -> dict[str, dict[str, Any]]:
    result = {
        field: {
            "raw_unit": "decimal_ratio",
            "display_unit": "percent_point",
            "display_multiplier": 100,
            "display_suffix": "%",
            "display_digits": 2,
            "label": _FIELD_DISPLAY_LABELS.get(field, field),
        }
        for field in sorted(DECIMAL_RATIO_PERCENT_FIELDS)
    }
    result.update({
        field: {
            'raw_unit': 'nav_observations', 'display_unit': 'nav_observations',
            'display_multiplier': 1, 'display_suffix': '', 'display_digits': 0,
            'label': LEGACY_DRAWDOWN_COLUMNS.get(field, field),
            'definition': DRAWDOWN_OBSERVATION_DEFINITION,
            'includes_unrecovered': True,
        }
        for field in sorted(DRAWDOWN_OBSERVATION_FIELDS)
    })
    return result
