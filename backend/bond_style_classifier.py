from __future__ import annotations

import re
from typing import Any


RATE_TOKENS = ("国开", "国债", "政金债", "政策性金融债", "农发", "地方政府债", "国债指数")
CREDIT_TOKENS = ("信用债", "中高等级", "产业债", "城投", "信用利差", "投资级信用债")
CASH_TOKENS = ("存款", "活期", "同业存单", "货币市场")
AGGREGATE_TOKENS = ("综合", "全债", "中债", "综合全价", "总值指数")
CONVERTIBLE_TOKENS = ("可转债", "转债", "可转换债券", "可交换债")
EQUITY_TOKENS = ("股票", "权益", "沪深300", "创业板", "存托凭证", "权证")
SHORT_DURATION_TOKENS = ("中短债", "短债", "1-3年", "短久期")

_PURE_BOND_COMPONENT = re.compile(
    r"(?:可)?分离交易(?:的)?(?:可转换债券|可转债)(?:的)?纯债(?:券)?部分"
    r"(?!\s*(?:以外|之外))"
)
_EXPOSURE_INSTRUMENT = r"(?:可转换债券|可交换债券?|可转债|转债|股票|权证|存托凭证|权益类证券|权益类资产)"
_EXPLICIT_NON_INVESTMENT_LIST = re.compile(
    r"(?:不投资于?|不得投资于?|禁止投资于?)\s*"
    + _EXPOSURE_INSTRUMENT
    + r"(?:\s*[（(]\s*除外\s*[）)])?"
    + r"(?:\s*[、,，]\s*"
    + _EXPOSURE_INSTRUMENT
    + r"(?:\s*[（(]\s*除外\s*[）)])?)*"
    + r"(?:等(?:权益类)?(?:资产|证券|金融工具)?)?"
    # Do not consume a noun prefix in ``股票指数期货`` / ``股票型基金`` or
    # an exclusion of everything *except* the named assets (``股票以外``).
    # A comma followed by another exposure noun belongs to the same list;
    # it cannot be used to accept a shorter, partially understood list.
    + r"(?=\s*(?:$|[。；;.!！？?]|[，,](?!\s*"
    + _EXPOSURE_INSTRUMENT
    + r")))"
)


def _text_with_positive_exposure_mentions(value: Any) -> str:
    """Ignore explicit prohibitions and the stripped pure bond component only.

    Remove the matched list, never the whole sentence: a later affirmative
    investment strategy or a positive benchmark in another field must survive.
    Rating clauses such as ``信用债(不含可转换债券)`` are not global prohibitions.
    """
    text = str(value or "").strip()
    text = _PURE_BOND_COMPONENT.sub("", text)

    def remove_unambiguous_prohibition(match: re.Match[str]) -> str:
        prefix = text[:match.start()].rstrip()
        if prefix.endswith(("并非", "不是", "并不是")):
            return match.group(0)
        return ""

    return _EXPLICIT_NON_INVESTMENT_LIST.sub(remove_unambiguous_prohibition, text)


def _join_text(*values: Any) -> str:
    return "\n".join(str(value or "").strip() for value in values if str(value or "").strip())


def _contains_any(text: str, tokens: tuple[str, ...]) -> bool:
    return any(token in text for token in tokens)


def classify_benchmark_family(benchmark: Any) -> str:
    text = str(benchmark or "").strip()
    if not text:
        return "未知"
    if _contains_any(text, CONVERTIBLE_TOKENS):
        return "可转债"
    if _contains_any(text, RATE_TOKENS):
        return "利率债"
    if _contains_any(text, CREDIT_TOKENS):
        return "信用债"
    if _contains_any(text, CASH_TOKENS):
        return "现金增强"
    if _contains_any(text, AGGREGATE_TOKENS):
        return "综合债"
    return "其他"


def classify_bond_style(
    *,
    ftype: Any,
    sub_type: Any,
    benchmark: Any,
    investment_target: Any,
    investment_strategy: Any,
) -> dict[str, Any]:
    ftype_text = str(ftype or "").strip()
    sub_type_text = str(sub_type or "").strip()
    full_text = _join_text(*(
        _text_with_positive_exposure_mentions(value)
        for value in (benchmark, investment_target, investment_strategy)
    ))
    benchmark_family = classify_benchmark_family(benchmark)

    has_convertible = _contains_any(full_text, CONVERTIBLE_TOKENS)
    has_equity = _contains_any(full_text, EQUITY_TOKENS)
    has_equity_or_convertible = has_convertible or has_equity

    if "混合二级" in ftype_text or "混合二级" in sub_type_text:
        style_tag = "二级债增强"
    elif "混合一级" in ftype_text or "混合一级" in sub_type_text:
        style_tag = "一级债增强"
    elif has_convertible:
        style_tag = "转债增强"
    elif "中短债" in ftype_text or "中短债" in sub_type_text or _contains_any(full_text, SHORT_DURATION_TOKENS):
        style_tag = "中短债"
    elif "指数型-固收" in ftype_text or ("指数" in ftype_text and "固收" in ftype_text):
        style_tag = "固收指数"
    elif benchmark_family == "利率债":
        style_tag = "利率债"
    elif benchmark_family == "信用债":
        style_tag = "信用债"
    elif benchmark_family == "现金增强":
        style_tag = "现金增强"
    else:
        style_tag = "综合纯债"

    exposure_labels: list[str] = []
    if has_equity:
        exposure_labels.append("权益暴露")
    if has_convertible:
        exposure_labels.append("可转债暴露")

    is_enhanced_style = style_tag in {"一级债增强", "二级债增强", "转债增强"}
    is_pure_bond_style = style_tag in {"利率债", "信用债", "综合纯债", "中短债", "固收指数", "现金增强"}

    return {
        "benchmark_family": benchmark_family,
        "style_tag": style_tag,
        "is_enhanced_style": is_enhanced_style,
        "is_pure_bond_style": is_pure_bond_style and not has_equity_or_convertible,
        "has_equity_convertible_exposure": has_equity_or_convertible,
        "exposure_labels": exposure_labels,
    }
