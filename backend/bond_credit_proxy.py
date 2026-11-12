"""债券基金信用代理桶（P1-02.2 / B6）。

工作目标：
- 从 ``funds.investment_target`` + ``funds.investment_strategy`` 两段文本提取信用偏好
  关键词，映射到 ``credit_bucket ∈ {high, mid, mid_low, unknown}``，供展示层使用。
- 不动评分主链路，也不对 bucket 做数值化（未来若接入外部数据再改）。

关键词设计（优先级：高 → 中低 → 中；保守策略）：
| credit_bucket | 触发关键词（全文文本含即命中）                                        |
|---------------|------------------------------------------------------------|
| high          | ``国债 / 政策性金融债 / 政金债 / 国开 / 农发 / 进出口 / AAA / 同业存单``          |
| mid_low       | ``高收益 / 次级 / 下沉评级 / AA- 以下 / 转股 / 可回售``                       |
| mid           | ``AA+ / AA级 / 中高等级 / 信用利差 / 投资级信用债``                           |
| unknown       | 其它 / 文本为空                                                  |

特别规则：
- ``mid_low`` 优先级最高——保守标注"可能下沉"的产品，不被 high 覆盖。
- ``high + mid`` 同时命中时取 ``high``（保守向上）。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional


@dataclass(slots=True, frozen=True)
class CreditProxy:
    bucket: str
    keyword: Optional[str]  # 命中的关键词，便于审计


# mid_low（风险信号）—— 首查，一旦命中直接标 mid_low
_MID_LOW_TOKENS = (
    "高收益",
    "次级",
    "下沉评级",
    "下沉信用",
    "AA-",
    "AA−",
    "可回售",
    "可转股",
    "夹层",
)

# high（高评级 / 无信用风险暴露）
_HIGH_TOKENS = (
    "国债",
    "政策性金融债",
    "政金债",
    "国开债",
    "国开行",
    "农发",
    "进出口",
    "央行票据",
    "同业存单",
    "AAA",
    "3A",
    "高等级信用债",
)

# mid（中等评级信用）
_MID_TOKENS = (
    "AA+",
    "AA级",
    "AA ",
    "中高等级",
    "投资级信用债",
    "信用利差",
    "产业债",
    "城投",
)


def _find_first(text: str, tokens: tuple[str, ...]) -> Optional[str]:
    for token in tokens:
        if token and token in text:
            return token
    return None


def classify_credit_proxy(
    *,
    investment_target: Any = "",
    investment_strategy: Any = "",
) -> CreditProxy:
    """保守映射：mid_low 优先命中 → high → mid → unknown。"""

    text_parts = [
        str(investment_target or "").strip(),
        str(investment_strategy or "").strip(),
    ]
    text = "\n".join(p for p in text_parts if p)
    if not text:
        return CreditProxy(bucket="unknown", keyword=None)

    low_hit = _find_first(text, _MID_LOW_TOKENS)
    if low_hit:
        return CreditProxy(bucket="mid_low", keyword=low_hit)

    high_hit = _find_first(text, _HIGH_TOKENS)
    if high_hit:
        return CreditProxy(bucket="high", keyword=high_hit)

    mid_hit = _find_first(text, _MID_TOKENS)
    if mid_hit:
        return CreditProxy(bucket="mid", keyword=mid_hit)

    return CreditProxy(bucket="unknown", keyword=None)


__all__ = ["CreditProxy", "classify_credit_proxy"]
