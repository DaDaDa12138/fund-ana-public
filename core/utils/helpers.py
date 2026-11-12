"""
辅助函数和工具
"""

import hashlib
import json
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from datetime import datetime, date
from typing import Any, List, Dict


def parse_date(date_str):
    """
    解析日期字符串

    Args:
        date_str: 日期字符串（支持多种格式）

    Returns:
        date对象或None
    """
    if not date_str:
        return None

    if isinstance(date_str, date):
        return date_str

    # 尝试多种格式
    formats = [
        '%Y-%m-%d',
        '%Y/%m/%d',
        '%Y%m%d',
    ]

    for fmt in formats:
        try:
            return datetime.strptime(str(date_str), fmt).date()
        except ValueError:
            continue

    return None


def safe_float(value, default=None):
    """
    安全转换为浮点数

    Args:
        value: 值
        default: 默认值

    Returns:
        浮点数或默认值
    """
    if value is None or value == '':
        return default

    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def safe_int(value, default=None):
    """
    安全转换为整数

    Args:
        value: 值
        default: 默认值

    Returns:
        整数或默认值
    """
    if value is None or value == '':
        return default

    try:
        return int(value)
    except (ValueError, TypeError):
        return default


def chunks(lst, n):
    """
    将列表分割成指定大小的块

    Args:
        lst: 列表
        n: 每块大小

    Yields:
        子列表
    """
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def format_number(num, decimals=2):
    """
    格式化数字（添加千分位）

    Args:
        num: 数字
        decimals: 小数位数

    Returns:
        格式化后的字符串
    """
    if num is None:
        return 'N/A'

    try:
        if decimals == 0:
            return f"{int(num):,}"
        else:
            return f"{float(num):,.{decimals}f}"
    except (ValueError, TypeError):
        return str(num)


def format_percentage(value, decimals=2):
    """
    格式化百分比

    Args:
        value: 数值（如5.23表示5.23%）
        decimals: 小数位数

    Returns:
        格式化后的字符串
    """
    if value is None:
        return 'N/A'

    try:
        return f"{float(value):.{decimals}f}%"
    except (ValueError, TypeError):
        return str(value)


def is_valid_fcode(fcode):
    """
    验证基金代码格式

    Args:
        fcode: 基金代码

    Returns:
        True if valid
    """
    if not fcode:
        return False

    fcode = str(fcode).strip()
    return len(fcode) == 6 and fcode.isdigit()


def extract_bond_subtype(ftype):
    """
    从基金类型字符串中提取债券子类型

    Args:
        ftype: 基金类型字符串（如"债券型-长债"）

    Returns:
        子类型（如"长债"）或None
    """
    if not ftype or '债券型' not in ftype:
        return None

    if '-' in ftype:
        return ftype.split('-')[1].strip()

    return None


def json_dumps(obj):
    """
    JSON序列化（支持日期）

    Args:
        obj: 对象

    Returns:
        JSON字符串
    """
    def default(o):
        if isinstance(o, (date, datetime)):
            return o.isoformat()
        raise TypeError(f"Object of type {type(o)} is not JSON serializable")

    return json.dumps(obj, ensure_ascii=False, default=default)


def json_loads(s):
    """
    JSON反序列化

    Args:
        s: JSON字符串

    Returns:
        Python对象
    """
    if not s:
        return None

    try:
        return json.loads(s)
    except json.JSONDecodeError:
        return None


def calculate_days_between(start_date, end_date):
    """
    计算两个日期之间的天数

    Args:
        start_date: 开始日期
        end_date: 结束日期

    Returns:
        天数
    """
    if not start_date or not end_date:
        return 0

    if isinstance(start_date, str):
        start_date = parse_date(start_date)
    if isinstance(end_date, str):
        end_date = parse_date(end_date)

    return (end_date - start_date).days


def merge_dicts(*dicts):
    """
    合并多个字典

    Args:
        *dicts: 字典列表

    Returns:
        合并后的字典
    """
    result = {}
    for d in dicts:
        if d:
            result.update(d)
    return result


def _normalize_signature_date(value):
    parsed = parse_date(value)
    if parsed:
        return parsed.isoformat()
    return str(value or '').strip()


def _normalize_signature_number(value, decimals=10):
    if value is None or value == '':
        return ''

    try:
        quantizer = Decimal('1').scaleb(-decimals)
        normalized = Decimal(str(value)).quantize(quantizer, rounding=ROUND_HALF_UP)
        return format(normalized, 'f')
    except (InvalidOperation, ValueError, TypeError):
        return str(value).strip()


def build_nav_snapshot_signature(snapshot):
    """
    基于 fund_returns 快照关键字段生成稳定签名。
    """
    payload = snapshot or {}
    fields = [
        ('latest_nav_date', _normalize_signature_date(payload.get('latest_nav_date'))),
        ('latest_nav', _normalize_signature_number(payload.get('latest_nav'))),
        ('daily_growth', _normalize_signature_number(payload.get('daily_growth'))),
        ('return_1w', _normalize_signature_number(payload.get('return_1w'))),
        ('return_1m', _normalize_signature_number(payload.get('return_1m'))),
        ('return_3m', _normalize_signature_number(payload.get('return_3m'))),
        ('return_6m', _normalize_signature_number(payload.get('return_6m'))),
        ('return_1y', _normalize_signature_number(payload.get('return_1y'))),
        ('return_2y', _normalize_signature_number(payload.get('return_2y'))),
        ('return_3y', _normalize_signature_number(payload.get('return_3y'))),
        ('return_5y', _normalize_signature_number(payload.get('return_5y'))),
        ('return_ytd', _normalize_signature_number(payload.get('return_ytd'))),
        ('return_since', _normalize_signature_number(payload.get('return_since'))),
    ]
    if 'period_return_date' in payload:
        fields.append(('period_return_date', _normalize_signature_date(payload.get('period_return_date'))))
    canonical = '|'.join(f'{key}={value}' for key, value in fields)
    return hashlib.sha1(canonical.encode('utf-8')).hexdigest()[:24]
