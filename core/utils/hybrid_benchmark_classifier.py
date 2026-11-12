#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
混合型 benchmark 语义分类辅助。
"""

from __future__ import annotations

import re
from collections import Counter
from typing import Dict, List, Optional


HYBRID_BENCHMARK_LABEL_FIRST_RE = re.compile(
    r"(?:(?<=^)|(?<=\+))\s*([^+]+?)\s*[*×xX]\s*(\d+(?:\.\d+)?)%\s*(?=(?:\+|$))"
)
HYBRID_BENCHMARK_WEIGHT_FIRST_RE = re.compile(
    r"(?:(?<=^)|(?<=\+))\s*(\d+(?:\.\d+)?)%\s*[*×xX]\s*([^+]+?)\s*(?=(?:\+|$))"
)
HYBRID_BENCHMARK_LABEL_FIRST_DIRECT_RE = re.compile(
    r"(?:(?<=^)|(?<=\+))\s*([^+%*×xX]+?)\s*(\d+(?:\.\d+)?)%\s*(?=(?:\+|$))"
)
HYBRID_BENCHMARK_WEIGHT_FIRST_DIRECT_RE = re.compile(
    r"(?:(?<=^)|(?<=\+))\s*(\d+(?:\.\d+)?)%\s*([^+%*×xX]+?)\s*(?=(?:\+|$))"
)

COMMODITY_MARKERS = (
    "上海黄金交易所",
    "上海期货交易所",
    "大连商品交易所",
    "黄金现货",
    "白银期货",
    "有色金属期货",
    "豆粕期货",
    "Au99.99",
    "AU99.99",
    "Au9999",
    "SHAU",
    "商品期货",
    "贵金属",
)

BOND_KEYWORDS = (
    "中债",
    "债券",
    "国债",
    "政金债",
    "信用债",
    "可转债",
    "纯债",
    "综合债",
    "全债",
    "短债",
    "中短债",
    "利率债",
    "金融债",
    "债基",
    "固收",
)

STOCK_KEYWORDS = (
    "沪深",
    "股票",
    "权益",
    "创业板",
    "科创",
    "富时",
    "新华富时",
    "申万",
    "申银万国",
    "战略新兴产业",
    "北证50",
    "恒生",
    "纳斯达克",
    "标普",
    "MSCI",
    "道琼斯",
    "罗素",
    "港股",
    "红利",
    "华证",
    "万得专精特新",
    "A股",
    "天相280",
    "天相中盘",
    "天相小盘",
    "中信成长风格",
    "中信周期风格",
    "中信稳定风格",
    "中信消费风格",
    "中信金融风格",
    "中信医药",
    "巨潮200",
    "巨潮500",
    "小市值",
    "消费服务领先",
)

STOCK_INDEX_PREFIXES = (
    "中证",
    "上证",
    "深证",
    "国证",
)

CASH_KEYWORDS = (
    "活期存款",
    "定期存款",
    "银行存款",
    "存款利率",
    "现金",
)

BALANCED_NAME_KEYWORDS = (
    "平衡",
    "均衡",
)

FLEXIBLE_NAME_KEYWORDS = (
    "灵活",
    "配置",
)

HIGH_CONFIDENCE_HYBRID_ACTIONS = {
    "high_confidence_hybrid_stock_candidate",
    "high_confidence_hybrid_bond_candidate",
    "high_confidence_hybrid_balanced_candidate",
}


def _normalize_text(value: Optional[str]) -> str:
    return str(value or "").strip()


def _contains_any(text: str, markers) -> bool:
    return any(marker in text for marker in markers)


def _classify_benchmark_label_bucket(label: Optional[str]) -> str:
    normalized = _normalize_text(label)
    if not normalized:
        return "other"

    if _contains_any(normalized, COMMODITY_MARKERS):
        return "commodity"
    if _contains_any(normalized, CASH_KEYWORDS):
        return "cash"
    if _contains_any(normalized, BOND_KEYWORDS):
        return "bond"
    if _contains_any(normalized, STOCK_KEYWORDS):
        return "stock"
    if _contains_any(normalized, STOCK_INDEX_PREFIXES):
        return "stock"
    return "other"


def extract_hybrid_benchmark_segments(benchmark: Optional[str]) -> List[dict]:
    """从 benchmark 中提取权重段并做资产桶分类。"""
    normalized = _normalize_text(benchmark)
    # 部分 benchmark 会在权重后追加“(经汇率调整)/(使用估值汇率折算)”说明，
    # 这类说明不影响权重结构，先在解析阶段去掉以避免漏掉主段。
    normalized = re.sub(r"%\s*[（(][^）)]*[）)]", "%", normalized)
    raw_segments: List[dict] = []

    for match in HYBRID_BENCHMARK_LABEL_FIRST_RE.finditer(normalized):
        label, weight_text = match.groups()
        raw_segments.append(
            {
                "start": match.start(),
                "label": _normalize_text(label),
                "weight_text": weight_text,
            }
        )

    for match in HYBRID_BENCHMARK_WEIGHT_FIRST_RE.finditer(normalized):
        weight_text, label = match.groups()
        raw_segments.append(
            {
                "start": match.start(),
                "label": _normalize_text(label),
                "weight_text": weight_text,
            }
        )

    for match in HYBRID_BENCHMARK_LABEL_FIRST_DIRECT_RE.finditer(normalized):
        label, weight_text = match.groups()
        raw_segments.append(
            {
                "start": match.start(),
                "label": _normalize_text(label),
                "weight_text": weight_text,
            }
        )

    for match in HYBRID_BENCHMARK_WEIGHT_FIRST_DIRECT_RE.finditer(normalized):
        weight_text, label = match.groups()
        raw_segments.append(
            {
                "start": match.start(),
                "label": _normalize_text(label),
                "weight_text": weight_text,
            }
        )

    segments: List[dict] = []
    seen = set()
    for item in sorted(raw_segments, key=lambda current: (current["start"], current["label"])):
        try:
            weight = float(item["weight_text"])
        except (TypeError, ValueError):
            continue
        dedupe_key = (item["start"], item["label"], round(weight, 4))
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        bucket = _classify_benchmark_label_bucket(item["label"])
        segments.append(
            {
                "label": item["label"],
                "weight": round(weight, 4),
                "bucket": bucket,
            }
        )

    return segments


def classify_hybrid_unclassified_candidate(
    shortname: Optional[str],
    fullname: Optional[str],
    benchmark: Optional[str],
) -> dict:
    """
    基于 benchmark 权重段，对混合型-未分类样本做只读语义归因。

    规则保持保守：
    - 只在股/债权重结构足够清晰时给出高置信偏股/偏债/平衡候选；
    - 其余仍落入人工复核或解析不足。
    """
    benchmark_text = _normalize_text(benchmark)
    name_text = " ".join(
        part for part in (_normalize_text(shortname), _normalize_text(fullname)) if part
    )
    segments = extract_hybrid_benchmark_segments(benchmark_text)

    weights = Counter()
    for segment in segments:
        weights[segment["bucket"]] += float(segment["weight"] or 0.0)

    parsed_weight_total = round(sum(float(segment["weight"] or 0.0) for segment in segments), 4)
    stock_weight = round(weights.get("stock", 0.0), 4)
    bond_weight = round(weights.get("bond", 0.0), 4)
    cash_weight = round(weights.get("cash", 0.0), 4)
    commodity_weight = round(weights.get("commodity", 0.0), 4)
    other_weight = round(weights.get("other", 0.0), 4)
    stock_bond_weight_total = round(stock_weight + bond_weight, 4)
    dominance_gap = round(abs(stock_weight - bond_weight), 4)

    name_hints = []
    if _contains_any(name_text, BALANCED_NAME_KEYWORDS):
        name_hints.append("balanced_name_hint")
    if _contains_any(name_text, FLEXIBLE_NAME_KEYWORDS):
        name_hints.append("flexible_name_hint")

    dominant_asset = "unknown"
    proposed_action = "benchmark_parse_insufficient"
    proposed_semantic_ftype = None
    confidence = "low"
    reason = "benchmark 未解析出足够的股债权重结构，暂不适合做运行时细分。"

    if parsed_weight_total <= 0:
        reason = "benchmark 未解析出任何 `指数*权重%` 结构。"
    elif parsed_weight_total < 70:
        reason = f"可解析权重仅 {parsed_weight_total}%，不足以支撑稳定语义细分。"
    elif stock_bond_weight_total < 50:
        reason = f"股债权重合计仅 {stock_bond_weight_total}%，核心资产配置结构仍不清晰。"
    elif other_weight > 20:
        reason = f"未识别 benchmark 权重达到 {other_weight}%，当前解析规则覆盖不足。"
    elif commodity_weight > 5:
        proposed_action = "flexible_or_balanced_manual_review"
        confidence = "manual"
        dominant_asset = "manual_review"
        reason = f"benchmark 仍含 {commodity_weight}% 商品暴露，建议人工复核后再决定是否细分。"
    elif stock_weight >= 65 and bond_weight <= 35:
        proposed_action = "high_confidence_hybrid_stock_candidate"
        proposed_semantic_ftype = "混合型-偏股"
        confidence = "high"
        dominant_asset = "stock"
        reason = f"股票权重 {stock_weight}% 显著高于债券权重 {bond_weight}%，可作为高置信偏股候选。"
    elif bond_weight >= 65 and stock_weight <= 35:
        proposed_action = "high_confidence_hybrid_bond_candidate"
        proposed_semantic_ftype = "混合型-偏债"
        confidence = "high"
        dominant_asset = "bond"
        reason = f"债券权重 {bond_weight}% 显著高于股票权重 {stock_weight}%，可作为高置信偏债候选。"
    elif (
        40 <= stock_weight <= 60
        and 40 <= bond_weight <= 60
        and dominance_gap <= 20
        and "flexible_name_hint" not in name_hints
    ):
        proposed_action = "high_confidence_hybrid_balanced_candidate"
        proposed_semantic_ftype = "混合型-平衡"
        confidence = "high"
        dominant_asset = "balanced"
        reason = f"股票权重 {stock_weight}% 与债券权重 {bond_weight}% 接近，符合高置信平衡型候选。"
    else:
        proposed_action = "flexible_or_balanced_manual_review"
        confidence = "manual"
        dominant_asset = "manual_review"
        if "flexible_name_hint" in name_hints:
            proposed_semantic_ftype = "混合型-灵活"
            reason = (
                f"benchmark 已可解析，但股债结构为 {stock_weight}%/{bond_weight}%，"
                "更接近灵活配置人工复核场景。"
            )
        elif "balanced_name_hint" in name_hints:
            proposed_semantic_ftype = "混合型-平衡"
            reason = (
                f"benchmark 已可解析，但股债结构为 {stock_weight}%/{bond_weight}%，"
                "建议按平衡/灵活方向人工复核。"
            )
        else:
            reason = (
                f"benchmark 已可解析，但股债结构为 {stock_weight}%/{bond_weight}%，"
                "尚不足以高置信细分，建议人工复核。"
            )

    return {
        "benchmark_segments": segments,
        "segment_count": len(segments),
        "parsed_weight_total": parsed_weight_total,
        "stock_bond_weight_total": stock_bond_weight_total,
        "asset_weights": {
            "stock": stock_weight,
            "bond": bond_weight,
            "cash": cash_weight,
            "commodity": commodity_weight,
            "other": other_weight,
        },
        "dominance_gap": dominance_gap,
        "dominant_asset": dominant_asset,
        "name_hints": name_hints,
        "confidence": confidence,
        "proposed_semantic_ftype": proposed_semantic_ftype,
        "proposed_action": proposed_action,
        "reason": reason,
    }


def infer_high_confidence_hybrid_semantic_ftype(
    shortname: Optional[str],
    fullname: Optional[str],
    benchmark: Optional[str],
) -> Optional[str]:
    """仅返回高置信可自动应用的 hybrid 语义子类。"""
    classification = classify_hybrid_unclassified_candidate(
        shortname=shortname,
        fullname=fullname,
        benchmark=benchmark,
    )
    if classification.get("proposed_action") not in HIGH_CONFIDENCE_HYBRID_ACTIONS:
        return None
    return classification.get("proposed_semantic_ftype")
