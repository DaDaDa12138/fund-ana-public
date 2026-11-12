from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Callable


CN_NUMERAL_MAP = {
    "零": 0,
    "一": 1,
    "二": 2,
    "两": 2,
    "三": 3,
    "四": 4,
    "五": 5,
    "六": 6,
    "七": 7,
    "八": 8,
    "九": 9,
}

CN_UNIT_MAP = {
    "十": 10,
    "百": 100,
}


@dataclass(slots=True)
class HoldingMatch:
    days: int
    keyword: str
    source: str


def cn_to_int(text: str) -> int | None:
    value = str(text or "").strip()
    if not value:
        return None
    if value.isdigit():
        return int(value)

    total = 0
    current = 0
    for char in value:
        if char in CN_NUMERAL_MAP:
            current = CN_NUMERAL_MAP[char]
        elif char in CN_UNIT_MAP:
            unit = CN_UNIT_MAP[char]
            if current == 0:
                current = 1
            total += current * unit
            current = 0
        else:
            return None
    return total + current


def normalize_detail_assisted_keyword(keyword: str) -> str:
    value = str(keyword or "").strip()
    if not value:
        return value
    return re.sub(r"(?P<term>(?:\d+|[一二两三四五六七八九十百]+)(?:个?月|年))([A-Z])$", r"\g<term>", value)


def _safe_int(value: Any) -> int | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(float(text))
    except (TypeError, ValueError):
        return None


def _normalized_name_texts(*texts: Any) -> list[str]:
    values: list[str] = []
    for text in texts:
        value = str(text or "").strip()
        if not value or value in values:
            continue
        values.append(value)
    return values


PatternBuilder = Callable[[re.Match[str]], HoldingMatch]


EXACT_PATTERNS: list[tuple[re.Pattern[str], PatternBuilder]] = [
    (
        re.compile(r"双月持有期"),
        lambda m: HoldingMatch(days=60, keyword=m.group(0), source="alias_bimonth_hold"),
    ),
    (
        re.compile(r"季季(?:享|乐)持有期"),
        lambda m: HoldingMatch(days=90, keyword=m.group(0), source="alias_quarter_hold"),
    ),
    (
        re.compile(r"双季享持有期"),
        lambda m: HoldingMatch(days=180, keyword=m.group(0), source="alias_half_year_hold"),
    ),
    (
        re.compile(r"(?P<num>\d+|[一二两三四五六七八九十百]+)天(?:持有期?|滚动持有|滚动|定开|定期|开放|封闭)"),
        lambda m: HoldingMatch(days=cn_to_int(m.group("num")) or 0, keyword=m.group(0), source="day_rule"),
    ),
    (
        re.compile(r"持有(?P<num>\d+|[一二两三四五六七八九十百]+)天"),
        lambda m: HoldingMatch(days=cn_to_int(m.group("num")) or 0, keyword=m.group(0), source="day_rule"),
    ),
    (
        re.compile(r"(?P<num>\d+|[一二两三四五六七八九十百]+)周(?:持有期?|滚动持有|滚动|定开|定期|开放)"),
        lambda m: HoldingMatch(days=(cn_to_int(m.group("num")) or 0) * 7, keyword=m.group(0), source="week_rule"),
    ),
    (
        re.compile(r"(?P<num>\d+|[一二两三四五六七八九十百]+)个?月(?:持有期?|滚动持有|滚动|定开|定期|开放|封闭)"),
        lambda m: HoldingMatch(days=(cn_to_int(m.group("num")) or 0) * 30, keyword=m.group(0), source="month_rule"),
    ),
    (
        re.compile(r"持有(?P<num>\d+|[一二两三四五六七八九十百]+)个?月"),
        lambda m: HoldingMatch(days=(cn_to_int(m.group("num")) or 0) * 30, keyword=m.group(0), source="month_rule"),
    ),
    (
        re.compile(r"(?P<num>\d+|[一二两三四五六七八九十百]+)年(?:持有期?|滚动持有|滚动|定开|定期|开放|封闭)"),
        lambda m: HoldingMatch(days=(cn_to_int(m.group("num")) or 0) * 365, keyword=m.group(0), source="year_rule"),
    ),
    (
        re.compile(r"持有(?P<num>\d+|[一二两三四五六七八九十百]+)年"),
        lambda m: HoldingMatch(days=(cn_to_int(m.group("num")) or 0) * 365, keyword=m.group(0), source="year_rule"),
    ),
    (
        re.compile(r"半年(?:持有期?|滚动持有|滚动|定开|定期|开放|封闭)"),
        lambda m: HoldingMatch(days=180, keyword=m.group(0), source="half_year_rule"),
    ),
    (
        re.compile(r"一年(?:持有期?|滚动持有|滚动|定开|定期|开放)"),
        lambda m: HoldingMatch(days=365, keyword=m.group(0), source="year_rule"),
    ),
]


DETAIL_CLOSED_SIGNAL_PATTERNS: list[re.Pattern[str]] = [
    re.compile(r"以封闭期为周期进行投资运作"),
    re.compile(r"封闭期与开放期采取不同的投资策略"),
    re.compile(r"在封闭期内,?本基金采用买入并持有"),
    re.compile(r"开放期的流动性风险"),
    re.compile(r"每个封闭期将适当的采取期限配置策略"),
    re.compile(r"基金剩余封闭期限"),
    re.compile(r"剩余封闭期限进行适当的匹配"),
    re.compile(r"所投资产到期日(?:\(|（)或回售日(?:\)|）)不得晚于封闭运作期到期日"),
    re.compile(r"投资于剩余期限(?:\(|（)或回售期限(?:\)|）)不超过基金剩余封闭期"),
    re.compile(r"对所投资固定收益品种的剩余期限与基金的剩余封闭期进行期限匹配"),
    re.compile(r"封闭运作期到期日"),
    re.compile(r"封闭期投资策略"),
    re.compile(r"本基金封闭期的投资组合久期与封闭期剩余期限进行适当匹配"),
]


DETAIL_ASSISTED_NAME_PATTERNS: list[tuple[re.Pattern[str], PatternBuilder]] = [
    (
        re.compile(r"(?P<num>\d+|[一二两三四五六七八九十百]+)个?月期"),
        lambda m: HoldingMatch(days=(cn_to_int(m.group("num")) or 0) * 30, keyword=normalize_detail_assisted_keyword(m.group(0)), source="detail_confirmed_month_term"),
    ),
    (
        re.compile(r"(?P<num>\d+|[一二两三四五六七八九十百]+)年期"),
        lambda m: HoldingMatch(days=(cn_to_int(m.group("num")) or 0) * 365, keyword=normalize_detail_assisted_keyword(m.group(0)), source="detail_confirmed_year_term"),
    ),
    (
        re.compile(r"(?P<num>\d+|[一二两三四五六七八九十百]+)个?月理财"),
        lambda m: HoldingMatch(days=(cn_to_int(m.group("num")) or 0) * 30, keyword=normalize_detail_assisted_keyword(m.group(0)), source="detail_confirmed_month_term"),
    ),
    (
        re.compile(r"(?P<num>\d+|[一二两三四五六七八九十百]+)年理财"),
        lambda m: HoldingMatch(days=(cn_to_int(m.group("num")) or 0) * 365, keyword=normalize_detail_assisted_keyword(m.group(0)), source="detail_confirmed_year_term"),
    ),
    (
        re.compile(r"(?P<num>\d+|[一二两三四五六七八九十百]+)个?月(?:债券|纯债|理财|发起|[A-Z]|$)"),
        lambda m: HoldingMatch(days=(cn_to_int(m.group("num")) or 0) * 30, keyword=normalize_detail_assisted_keyword(m.group(0)), source="detail_confirmed_month_term"),
    ),
    (
        re.compile(r"(?P<num>\d+|[一二两三四五六七八九十百]+)年(?:债券|纯债|理财|发起|中高等级债|[A-Z]|$)"),
        lambda m: HoldingMatch(days=(cn_to_int(m.group("num")) or 0) * 365, keyword=normalize_detail_assisted_keyword(m.group(0)), source="detail_confirmed_year_term"),
    ),
]


DETAIL_GENERIC_PATTERNS: list[tuple[re.Pattern[str], PatternBuilder]] = [
    (
        re.compile(r"以封闭期为周期进行投资运作"),
        lambda m: HoldingMatch(days=999, keyword=m.group(0), source="detail_generic_closed_cycle"),
    ),
    (
        re.compile(r"封闭期与开放期采取不同的投资策略"),
        lambda m: HoldingMatch(days=999, keyword=m.group(0), source="detail_generic_closed_cycle"),
    ),
    (
        re.compile(r"(?:由于)?本基金采取定期开放运作方式"),
        lambda m: HoldingMatch(days=999, keyword="定期开放运作方式", source="detail_generic_periodic_open"),
    ),
    (
        re.compile(r"在每个封闭期将适当的采取期限配置策略"),
        lambda m: HoldingMatch(days=999, keyword=m.group(0), source="detail_generic_periodic_open"),
    ),
    (
        re.compile(r"本基金封闭期的投资组合久期与封闭期剩余期限进行适当匹配"),
        lambda m: HoldingMatch(days=999, keyword=m.group(0), source="detail_generic_closed_term_match"),
    ),
]


GENERIC_PATTERNS: list[tuple[re.Pattern[str], int, str]] = [
    (re.compile(r"周周购"), 7, "week_generic"),
    (re.compile(r"封闭式"), 999, "closed_generic"),
    (re.compile(r"定期开放"), 999, "open_period_generic"),
    (re.compile(r"定开"), 999, "fixed_open_generic"),
]

WEAK_GENERIC_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"滚动持有"), "rolling_generic"),
    (re.compile(r"锁定期"), "lock_generic"),
]

WEAK_GENERIC_KEYWORDS = {
    "滚动持有",
    "锁定期",
}


def is_weak_generic_keyword(value: Any) -> bool:
    return str(value or "").strip() in WEAK_GENERIC_KEYWORDS


def parse_holding_match(
    shortname: Any = "",
    fullname: Any = "",
    investment_target: Any = "",
    investment_strategy: Any = "",
) -> HoldingMatch | None:
    name_texts = _normalized_name_texts(shortname, fullname)
    detail_text = " ".join(
        str(text or "").strip()
        for text in (investment_target, investment_strategy)
        if str(text or "").strip()
    )
    if not name_texts and not detail_text:
        return None

    concrete_matches: list[HoldingMatch] = []
    for pattern, builder in EXACT_PATTERNS:
        for name_text in name_texts:
            for match in pattern.finditer(name_text):
                payload = builder(match)
                if payload.days > 0:
                    concrete_matches.append(payload)

    if concrete_matches:
        return max(concrete_matches, key=lambda payload: payload.days)

    detail_closed_signal = any(pattern.search(detail_text) for pattern in DETAIL_CLOSED_SIGNAL_PATTERNS)
    if detail_closed_signal:
        for pattern, builder in DETAIL_ASSISTED_NAME_PATTERNS:
            for name_text in name_texts:
                match = pattern.search(name_text)
                if not match:
                    continue
                payload = builder(match)
                if payload.days > 0:
                    return payload
        for pattern, builder in DETAIL_GENERIC_PATTERNS:
            match = pattern.search(detail_text)
            if not match:
                continue
            payload = builder(match)
            if payload.days > 0:
                return payload

    for pattern, days, source in GENERIC_PATTERNS:
        for name_text in name_texts:
            match = pattern.search(name_text)
            if match:
                return HoldingMatch(days=days, keyword=match.group(0), source=source)

    for pattern, _source in WEAK_GENERIC_PATTERNS:
        for name_text in name_texts:
            match = pattern.search(name_text)
            if match:
                return None
    return None


def needs_holding_update(
    *,
    current_has_holding_period: Any,
    current_days: Any,
    current_keyword: Any,
    parsed: HoldingMatch | None,
) -> bool:
    normalized_keyword = str(current_keyword or "").strip()
    if parsed is None:
        return bool((_safe_int(current_has_holding_period) or 0) == 1 and (_safe_int(current_days) == 999) and is_weak_generic_keyword(normalized_keyword))

    current_has = _safe_int(current_has_holding_period) or 0
    normalized_days = _safe_int(current_days)

    if current_has != 1:
        return True
    if normalized_days is None:
        return True
    if normalized_days != parsed.days:
        return normalized_days == 999 and parsed.days != 999
    return not normalized_keyword and bool(parsed.keyword)


def build_holding_update_fields(
    *,
    current_has_holding_period: Any,
    current_days: Any,
    current_keyword: Any,
    parsed: HoldingMatch | None,
) -> dict[str, Any] | None:
    if parsed is None:
        if not needs_holding_update(
            current_has_holding_period=current_has_holding_period,
            current_days=current_days,
            current_keyword=current_keyword,
            parsed=parsed,
        ):
            return None
        return {
            "has_holding_period": 0,
            "holding_days": None,
            "holding_keywords": None,
        }

    if not needs_holding_update(
        current_has_holding_period=current_has_holding_period,
        current_days=current_days,
        current_keyword=current_keyword,
        parsed=parsed,
    ):
        return None
    assert parsed is not None
    return {
        "has_holding_period": 1,
        "holding_days": parsed.days,
        "holding_keywords": parsed.keyword,
    }


# ============================================================================
# P1-01 / B13: 4 级持有期识别辅助（L1 detail_struct → L2 name → L3 strategy_text → L4 share_family）
# ============================================================================

_PERIOD_UNIT_DAYS = {
    "年": 365,
    "y": 365,
    "Y": 365,
    "月": 30,
    "m": 30,
    "M": 30,
    "周": 7,
    "w": 7,
    "W": 7,
    "日": 1,
    "天": 1,
    "d": 1,
    "D": 1,
}


def _extract_days_from_detail_value(raw: Any) -> int | None:
    """从 ``CYCLE / PRSVPERIOD / BUYTIME`` 原始字符串提取天数。

    支持：
    - ``"180"`` / ``"365"`` → 直接当天数
    - ``"6M"`` / ``"6个月"`` / ``"6月"`` → 6 * 30
    - ``"1Y"`` / ``"1年"`` → 365
    - ``"180天"`` / ``"180D"`` → 180
    - 空值、``"--"``、``"0"``、非常规字符串 → None
    """
    import re

    text = str(raw or "").strip()
    if not text or text in {"--", "---", "0"}:
        return None

    # 纯整数
    if text.isdigit():
        days = int(text)
        return days if days > 0 else None

    # 带单位：数字 + 单位字符
    match = re.search(r"(\d+(?:\.\d+)?)\s*(年|月|周|日|天|y|Y|m|M|w|W|d|D|个月)", text)
    if match:
        number_text = match.group(1)
        unit_text = match.group(2)
        if unit_text == "个月":
            unit_days = 30
        else:
            unit_days = _PERIOD_UNIT_DAYS.get(unit_text)
        if unit_days is None:
            return None
        try:
            value = float(number_text)
        except ValueError:
            return None
        days = int(round(value * unit_days))
        return days if days > 0 else None

    return None


def parse_holding_detail_struct(
    *,
    cycle: Any = "",
    preserve_period: Any = "",
    buy_time: Any = "",
) -> HoldingMatch | None:
    """L1 detail_struct：从 ``/fundMNDetailInformation`` 的权威字段提取持有期。

    规则：取三个字段里解析出的**最大**天数（保守）。若三个都无法解析返回 None，
    让上层 fallback 到 L2 name / L3 strategy_text。
    """
    candidates: list[tuple[int, str, str]] = []
    for field_name, raw in (("CYCLE", cycle), ("PRSVPERIOD", preserve_period), ("BUYTIME", buy_time)):
        days = _extract_days_from_detail_value(raw)
        if days and days > 0:
            candidates.append((days, field_name, str(raw)))
    if not candidates:
        return None
    # 长持有期优先
    candidates.sort(key=lambda item: item[0], reverse=True)
    top_days, field_name, raw_text = candidates[0]
    keyword = f"struct:{field_name}={raw_text.strip()}"
    return HoldingMatch(days=top_days, keyword=keyword, source="detail_struct")


def parse_holding_with_priority(
    *,
    shortname: Any = "",
    fullname: Any = "",
    investment_target: Any = "",
    investment_strategy: Any = "",
    cycle: Any = "",
    preserve_period: Any = "",
    buy_time: Any = "",
) -> tuple[HoldingMatch | None, str]:
    """按 L1 → (L2+L3) 的优先级返回 (match, source_layer)。

    source_layer 取值：``"detail_struct" | "name" | "strategy_text" | "none"``。
    L4（份额族传播）由 ``supplement_holding_period._apply_share_family_propagation``
    在整表扫描阶段决定，不在本函数内。

    P1-01 V6 补强：若 L1 与 L2/L3 同时命中但天数不一致，取**最长**（保守策略
    防止短的 L1 压住更长的名称/策略识别，避免误让基金越过 holding_days 门槛）。
    source_layer 记录胜出一层。
    """
    struct_match = parse_holding_detail_struct(
        cycle=cycle,
        preserve_period=preserve_period,
        buy_time=buy_time,
    )
    name_match = parse_holding_match(
        shortname=shortname,
        fullname=fullname,
        investment_target=investment_target,
        investment_strategy=investment_strategy,
    )

    if struct_match is None and name_match is None:
        return None, "none"

    if struct_match is not None and name_match is not None:
        if name_match.days == 999 and "generic" in name_match.source:
            return struct_match, "detail_struct"
        if int(name_match.days) > int(struct_match.days):
            src = str(getattr(name_match, "source", "") or "")
            return name_match, ("strategy_text" if src.startswith("detail") else "name")
        return struct_match, "detail_struct"

    if struct_match is not None:
        return struct_match, "detail_struct"

    assert name_match is not None
    src = str(getattr(name_match, "source", "") or "")
    return name_match, ("strategy_text" if src.startswith("detail") else "name")


__all__ = [
    "HoldingMatch",
    "build_holding_update_fields",
    "cn_to_int",
    "is_weak_generic_keyword",
    "needs_holding_update",
    "normalize_detail_assisted_keyword",
    "parse_holding_match",
    "parse_holding_detail_struct",
    "parse_holding_with_priority",
]
