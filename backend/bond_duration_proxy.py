"""债券基金久期代理桶（P1-02.1 / B6）。

工作目标：
- 不依赖结构化久期字段（tiantian / autostock 都没有 modified_duration / effective_duration），
  只用 ``funds.benchmark`` + `bond_style_classifier` 的风格标签，把每只债基映射到
  ``duration_bucket ∈ {short, short_mid, mid, long, convertible, unknown}`` 和
  ``proxy_duration_years``（代理基准久期中枢，单位"年"）。
- 展示维度不参与评分主链路，由 ``report_builder`` 调用嵌入 universe 展示字段。

映射规则（按 benchmark 文本 + style_tag 共同判定）：
| benchmark 关键词 / style_tag                | bucket      | proxy_years |
|--------------------------------------------|-------------|-------------|
| "中短融" / "0-3 年" / "短融" / style=中短债 | short       | 1.5         |
| "中短债" / "1-3 年"                        | short_mid   | 2.2         |
| "综合" / "中债综合" / style=综合纯债        | mid         | 4.5         |
| "长债" / "5-10 年" / "国债 5" / style=利率债 | long        | 7.0         |
| "可转债" / style=转债增强                  | convertible | 1.0         |
| 其它                                       | unknown     | None        |

规则保守：无法识别的条目返回 (unknown, None)，由 report_builder 展示 "—"。
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Optional

logger = logging.getLogger(__name__)


@dataclass(slots=True, frozen=True)
class DurationProxy:
    bucket: str
    proxy_years: Optional[float]
    keyword: Optional[str]  # 命中的 benchmark 子串，便于审计


# 短久期（≤1.5Y）：货币增强 / 中短融 0-3Y
_SHORT_TOKENS = (
    ("中短融", 1.5),
    ("0-3年", 1.5),
    ("0-3 年", 1.5),
    ("短融", 1.5),
    ("1年以下", 1.0),
    ("短期", 1.5),
)

# 短中久期（~2.2Y）
_SHORT_MID_TOKENS = (
    ("中短债", 2.2),
    ("1-3年", 2.2),
    ("1-3 年", 2.2),
    ("3年国开", 2.5),
    ("3年国债", 2.5),
)

# 中久期（~4.5Y）
_MID_TOKENS = (
    ("中债综合", 4.5),
    ("中债-综合", 4.5),
    ("综合财富", 4.8),
    ("综合债", 4.5),
    ("总值指数", 4.0),
)

# 长久期（~7Y）
_LONG_TOKENS = (
    ("5-10年", 7.0),
    ("5-10 年", 7.0),
    ("长债", 7.0),
    ("国债 5", 5.5),
    ("5年国债", 5.5),
    ("10年国债", 9.0),
    ("国开10", 9.0),
    ("长期", 7.0),
)

# 可转债（equity-like，不适用常规久期概念）
_CONVERTIBLE_TOKENS = (
    ("可转债", 1.0),
    ("转债指数", 1.0),
    ("可交换", 1.0),
)


def _match_first(text: str, tokens: tuple[tuple[str, float], ...]) -> Optional[tuple[str, float]]:
    for token, years in tokens:
        if token and token in text:
            return token, years
    return None


def classify_duration_proxy(
    *,
    benchmark: Any = "",
    style_tag: Any = "",
) -> DurationProxy:
    """组合 benchmark + style_tag 做保守映射。

    优先级：benchmark 关键词 → style_tag 兜底。
    """
    benchmark_text = str(benchmark or "").strip()
    style_text = str(style_tag or "").strip()

    # 可转债优先：benchmark 或 style 任一命中都归 convertible（equity-like）
    conv_hit = _match_first(benchmark_text, _CONVERTIBLE_TOKENS) if benchmark_text else None
    if conv_hit or style_text in ("转债增强", "可转债"):
        keyword = conv_hit[0] if conv_hit else f"style:{style_text}"
        years = conv_hit[1] if conv_hit else 1.0
        return DurationProxy(bucket="convertible", proxy_years=years, keyword=keyword)

    for tokens, bucket in (
        (_SHORT_TOKENS, "short"),
        (_SHORT_MID_TOKENS, "short_mid"),
        (_MID_TOKENS, "mid"),
        (_LONG_TOKENS, "long"),
    ):
        if not benchmark_text:
            break
        hit = _match_first(benchmark_text, tokens)
        if hit:
            return DurationProxy(bucket=bucket, proxy_years=hit[1], keyword=hit[0])

    # benchmark 无识别：style_tag 兜底
    style_fallback = {
        "中短债": DurationProxy(bucket="short_mid", proxy_years=2.2, keyword=f"style:{style_text}"),
        "现金增强": DurationProxy(bucket="short", proxy_years=1.0, keyword=f"style:{style_text}"),
        "综合纯债": DurationProxy(bucket="mid", proxy_years=4.5, keyword=f"style:{style_text}"),
        "固收指数": DurationProxy(bucket="mid", proxy_years=4.0, keyword=f"style:{style_text}"),
        "利率债": DurationProxy(bucket="long", proxy_years=7.0, keyword=f"style:{style_text}"),
        "信用债": DurationProxy(bucket="mid", proxy_years=3.5, keyword=f"style:{style_text}"),
    }
    if style_text in style_fallback:
        return style_fallback[style_text]

    return DurationProxy(bucket="unknown", proxy_years=None, keyword=None)


__all__ = ["DurationProxy", "classify_duration_proxy"]
