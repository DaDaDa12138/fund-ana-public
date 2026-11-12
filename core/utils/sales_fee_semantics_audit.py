#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
销售服务费语义审计。

目标：
1. 基于真实数据库统计 `sales_fee_rate` 的 NULL/0/正值分布；
2. 通过“同一家族不同份额/币种 sibling”对比，识别高优先复核样本；
3. 为后续统一详情补采窗口提供只读证据，不直接改库。
"""

from __future__ import annotations

import json
import re
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from utils.fund_code_alias_audit import (
    DEFAULT_AUDIT_DIR,
    PROJECT_ROOT,
    load_latest_json_artifact,
    open_readonly_db,
)
from utils.fund_type_filters import QDII_RUNTIME_FAMILY_SUFFIX_PATTERNS
from database.connection import resolve_db_path


SALES_FEE_SEMANTICS_AUDIT_JSON_PATTERN = "sales_fee_semantics_audit_*.json"
GENERAL_FAMILY_SUFFIX_PATTERNS: Tuple[str, ...] = (
    r"\(LOF\)$",
    r"\(LOF\)A$",
    r"\(LOF\)C$",
    r"\(LOF\)D$",
    r"\(LOF\)E$",
    r"\(LOF\)F$",
) + tuple(QDII_RUNTIME_FAMILY_SUFFIX_PATTERNS)
CURRENCY_PATTERNS: Tuple[Tuple[str, re.Pattern[str]], ...] = (
    ("人民币", re.compile(r"(?:人民币份额|人民币\(QDII\)|\(人民币份额\)|\(人民币\)|人民币)$")),
    ("美元现汇", re.compile(r"(?:美元现汇份额|美元现汇\(QDII\)|\(美元现汇份额\)|\(美元现汇\)|美元现汇)$")),
    ("美元现钞", re.compile(r"(?:美元现钞份额|美元现钞\(QDII\)|\(美元现钞份额\)|\(美元现钞\)|美元现钞)$")),
    ("美元", re.compile(r"美元$")),
)
SALES_FEE_AUDIT_SQL = """
SELECT
    fcode,
    shortname,
    fullname,
    ftype,
    type_code,
    type_category,
    mgr_fee_rate,
    trust_fee_rate,
    sales_fee_rate,
    benchmark
FROM funds
ORDER BY type_category, shortname, fcode
"""


def _strip_trailing_structural_tags(value: str) -> str:
    """剥离末尾结构标签与份额尾缀，便于识别 share/currency。"""
    normalized = str(value or "").strip()
    changed = True
    while changed and normalized:
        changed = False
        updated = re.sub(r"\((?:QDII-FOF-LOF|QDII-FOF|QDII|LOF)\)$", "", normalized).strip()
        if updated != normalized:
            normalized = updated
            changed = True
    return normalized


def classify_sales_fee_status(value: Any) -> str:
    """将 sales_fee_rate 映射到可审计状态。"""
    if value is None:
        return "null"
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return "invalid"
    if numeric < 0:
        return "invalid"
    if numeric == 0:
        return "zero"
    return "positive"


def detect_share_class(shortname: Optional[str]) -> str:
    """识别份额字母尾缀，仅用于审计分组。"""
    normalized = _strip_trailing_structural_tags(str(shortname or "").strip())
    for label in ("A", "C", "D", "E", "F", "I", "Y"):
        if re.search(rf"{label}类$", normalized):
            return label
        if re.search(rf"(?<![A-Za-z]){label}$", normalized):
            return label
    return "unsuffixed"


def detect_currency_bucket(shortname: Optional[str]) -> str:
    """识别币种尾缀，仅用于审计分组。"""
    normalized = _strip_trailing_structural_tags(str(shortname or "").strip())
    changed = True
    while changed and normalized:
        changed = False
        updated = re.sub(r"(?:A类|C类|D类|E类|F类|I类|Y类|A|C|D|E|F|I|Y)$", "", normalized).strip()
        if updated != normalized:
            normalized = updated
            changed = True
        updated = _strip_trailing_structural_tags(normalized)
        if updated != normalized:
            normalized = updated
            changed = True
    for label, pattern in CURRENCY_PATTERNS:
        if pattern.search(normalized):
            return label
    return "default"


def normalize_sales_fee_family_name(shortname: Optional[str]) -> str:
    """归一化基金家族名，便于 sibling 对比。"""
    normalized = str(shortname or "").strip()
    changed = True
    while changed and normalized:
        changed = False
        for pattern in GENERAL_FAMILY_SUFFIX_PATTERNS:
            updated = re.sub(pattern, "", normalized)
            if updated != normalized:
                normalized = updated.strip()
                changed = True
    return re.sub(r"\s+", "", normalized)


def classify_family_sales_fee_pattern(status_counter: Counter[str]) -> str:
    """将家族内部的 sales_fee 分布归类。"""
    has_null = status_counter.get("null", 0) > 0
    has_zero = status_counter.get("zero", 0) > 0
    has_positive = status_counter.get("positive", 0) > 0
    has_invalid = status_counter.get("invalid", 0) > 0

    if has_invalid:
        return "invalid_present"
    if has_null and has_zero and has_positive:
        return "positive_zero_null_mix"
    if has_null and has_positive:
        return "positive_null_mix"
    if has_null and has_zero:
        return "zero_null_mix"
    if has_positive and has_zero:
        return "positive_zero_mix"
    if has_null:
        return "null_only"
    if has_positive:
        return "positive_only"
    if has_zero:
        return "zero_only"
    return "empty"


def _build_family_summaries(rows: Sequence[dict]) -> Dict[str, dict]:
    family_rows: Dict[str, List[dict]] = defaultdict(list)
    for row in rows:
        family_rows[row["family_name"]].append(row)

    summaries: Dict[str, dict] = {}
    for family_name, family_items in family_rows.items():
        status_counter = Counter(item["sales_fee_status"] for item in family_items)
        pattern = classify_family_sales_fee_pattern(status_counter)
        summaries[family_name] = {
            "family_name": family_name,
            "row_count": len(family_items),
            "status_counts": dict(status_counter),
            "pattern": pattern,
            "type_categories": sorted({str(item.get("type_category") or "(null)") for item in family_items}),
            "share_classes": sorted({item.get("share_class") or "unsuffixed" for item in family_items}),
            "currency_buckets": sorted({item.get("currency_bucket") or "default" for item in family_items}),
            "rows": family_items,
        }
    return summaries


def build_sales_fee_semantics_payload(rows: Sequence[dict]) -> dict:
    """基于 rows 构建销售服务费语义审计 payload。"""
    normalized_rows: List[dict] = []
    type_category_stats: Dict[str, Counter[str]] = defaultdict(Counter)
    share_class_stats: Dict[str, Counter[str]] = defaultdict(Counter)
    currency_stats: Dict[str, Counter[str]] = defaultdict(Counter)

    for raw_row in rows:
        row = dict(raw_row)
        sales_fee_status = classify_sales_fee_status(row.get("sales_fee_rate"))
        type_category = str(row.get("type_category") or "(null)")
        share_class = detect_share_class(row.get("shortname"))
        currency_bucket = detect_currency_bucket(row.get("shortname"))
        family_name = normalize_sales_fee_family_name(row.get("shortname"))
        has_core_fee_pair = row.get("mgr_fee_rate") is not None and row.get("trust_fee_rate") is not None

        normalized_row = {
            **row,
            "type_category": type_category,
            "sales_fee_status": sales_fee_status,
            "share_class": share_class,
            "currency_bucket": currency_bucket,
            "family_name": family_name,
            "has_core_fee_pair": has_core_fee_pair,
        }
        normalized_rows.append(normalized_row)
        type_category_stats[type_category][sales_fee_status] += 1
        share_class_stats[share_class][sales_fee_status] += 1
        currency_stats[currency_bucket][sales_fee_status] += 1

    family_summaries = _build_family_summaries(normalized_rows)
    family_pattern_counts = Counter(summary["pattern"] for summary in family_summaries.values())

    missing_rows = [row for row in normalized_rows if row["sales_fee_status"] == "null"]
    missing_rows_with_core_fee_pair = [row for row in missing_rows if row["has_core_fee_pair"]]
    missing_review_rows = []
    missing_review_counter = Counter()
    top_review_families = []

    for family_name, summary in family_summaries.items():
        pattern = summary["pattern"]
        rows_in_family = summary["rows"]
        null_rows = [row for row in rows_in_family if row["sales_fee_status"] == "null"]
        if not null_rows:
            continue

        if pattern in {"positive_zero_null_mix", "positive_null_mix", "zero_null_mix"}:
            missing_review_counter[pattern] += len(null_rows)
            for row in null_rows:
                missing_review_rows.append({
                    "fcode": row["fcode"],
                    "shortname": row["shortname"],
                    "type_category": row["type_category"],
                    "share_class": row["share_class"],
                    "currency_bucket": row["currency_bucket"],
                    "mgr_fee_rate": row.get("mgr_fee_rate"),
                    "trust_fee_rate": row.get("trust_fee_rate"),
                    "family_name": family_name,
                    "family_pattern": pattern,
                    "family_status_counts": summary["status_counts"],
                })

        if pattern in {"positive_zero_null_mix", "positive_null_mix", "zero_null_mix"}:
            top_review_families.append({
                "family_name": family_name,
                "pattern": pattern,
                "row_count": summary["row_count"],
                "null_count": summary["status_counts"].get("null", 0),
                "zero_count": summary["status_counts"].get("zero", 0),
                "positive_count": summary["status_counts"].get("positive", 0),
                "type_categories": summary["type_categories"],
                "sample_rows": [
                    {
                        "fcode": item["fcode"],
                        "shortname": item["shortname"],
                        "sales_fee_rate": item.get("sales_fee_rate"),
                    }
                    for item in rows_in_family[:6]
                ],
            })

    top_review_families.sort(
        key=lambda item: (
            item["null_count"],
            item["positive_count"],
            item["zero_count"],
            item["row_count"],
            item["family_name"],
        ),
        reverse=True,
    )
    missing_review_rows.sort(
        key=lambda item: (
            item["family_pattern"],
            item["type_category"],
            item["share_class"],
            item["fcode"],
        )
    )

    summary = {
        "total_rows": len(normalized_rows),
        "null_rows": sum(1 for row in normalized_rows if row["sales_fee_status"] == "null"),
        "zero_rows": sum(1 for row in normalized_rows if row["sales_fee_status"] == "zero"),
        "positive_rows": sum(1 for row in normalized_rows if row["sales_fee_status"] == "positive"),
        "invalid_rows": sum(1 for row in normalized_rows if row["sales_fee_status"] == "invalid"),
        "null_rows_with_core_fee_pair": len(missing_rows_with_core_fee_pair),
        "null_rows_requiring_family_review": len(missing_review_rows),
        "family_count": len(family_summaries),
    }

    findings = []
    if summary["null_rows_requiring_family_review"] > 0:
        findings.append(
            "存在 sales_fee_rate 为 NULL、但同一家族 sibling 已出现 0 或正费率的样本，"
            "这类记录更像上游返回不稳定或详情链漏采，不应简单视为业务允许为空。"
        )
    if summary["null_rows_with_core_fee_pair"] > 0:
        findings.append(
            "部分基金已经具备 mgr_fee_rate + trust_fee_rate，但 sales_fee_rate 仍为 NULL，"
            "会直接把 DataLoader 的 total_fee_rate 置为 NaN，进而影响费率阈值筛选。"
        )

    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "summary": summary,
        "findings": findings,
        "type_category_stats": {key: dict(counter) for key, counter in sorted(type_category_stats.items())},
        "share_class_stats": {key: dict(counter) for key, counter in sorted(share_class_stats.items())},
        "currency_bucket_stats": {key: dict(counter) for key, counter in sorted(currency_stats.items())},
        "family_pattern_counts": dict(family_pattern_counts),
        "top_review_families": top_review_families[:20],
        "sample_missing_review_rows": missing_review_rows[:30],
        "review_rows": missing_review_rows,
    }


def build_sales_fee_semantics_audit_payload(
    db_path: Optional[str] = None,
) -> dict:
    """从只读数据库构建销售服务费语义审计。"""
    resolved_db_path = resolve_db_path(db_path or "database/bond_funds.db")
    with open_readonly_db(resolved_db_path) as conn:
        rows = [dict(row) for row in conn.execute(SALES_FEE_AUDIT_SQL).fetchall()]
    payload = build_sales_fee_semantics_payload(rows)
    payload["db_path"] = str(Path(resolved_db_path).resolve())
    payload["row_source"] = "funds"
    payload["consumers"] = [
        "bond_fund_screening/core/data_loader.py -> total_fee_rate",
        "bond_fund_screening/core/filtering_engine.py -> fee_threshold",
        "bond_fund_screening/core/scoring_engine.py -> total_fee_rate scoring",
        "web/api/database.py + web/static/index.html -> 基金详情展示",
    ]
    return payload


def load_latest_sales_fee_semantics_payload(
    output_dir: str = DEFAULT_AUDIT_DIR,
) -> Tuple[Path, dict]:
    """读取最新 sales_fee 语义审计结果。"""
    return load_latest_json_artifact(
        SALES_FEE_SEMANTICS_AUDIT_JSON_PATTERN,
        output_dir=output_dir,
    )


def render_sales_fee_semantics_markdown(payload: dict) -> str:
    """渲染销售服务费语义审计 Markdown。"""
    summary = payload.get("summary") or {}
    lines = [
        "# 销售服务费语义审计",
        "",
        f"- 生成时间: `{payload.get('generated_at', '')}`",
        f"- 数据源: `{payload.get('row_source', '')}`",
        f"- 数据库: `{payload.get('db_path', '')}`",
        "",
        "## 总览",
        "",
        f"- total_rows: `{summary.get('total_rows', 0)}`",
        f"- null_rows: `{summary.get('null_rows', 0)}`",
        f"- zero_rows: `{summary.get('zero_rows', 0)}`",
        f"- positive_rows: `{summary.get('positive_rows', 0)}`",
        f"- invalid_rows: `{summary.get('invalid_rows', 0)}`",
        f"- null_rows_with_core_fee_pair: `{summary.get('null_rows_with_core_fee_pair', 0)}`",
        f"- null_rows_requiring_family_review: `{summary.get('null_rows_requiring_family_review', 0)}`",
        f"- family_count: `{summary.get('family_count', 0)}`",
        "",
        "## 影响链路",
        "",
    ]
    for item in payload.get("consumers") or []:
        lines.append(f"- `{item}`")

    lines.extend([
        "",
        "## 发现",
        "",
    ])
    findings = payload.get("findings") or []
    if findings:
        for item in findings:
            lines.append(f"- {item}")
    else:
        lines.append("- 无异常发现")

    lines.extend([
        "",
        "## 家族模式分布",
        "",
    ])
    for key, value in sorted((payload.get("family_pattern_counts") or {}).items()):
        lines.append(f"- `{key}`: `{value}`")

    lines.extend([
        "",
        "## 类型分布",
        "",
        "| type_category | null | zero | positive | invalid |",
        "| --- | ---: | ---: | ---: | ---: |",
    ])
    for key, counter in sorted((payload.get("type_category_stats") or {}).items()):
        lines.append(
            f"| {key} | {counter.get('null', 0)} | {counter.get('zero', 0)} | {counter.get('positive', 0)} | {counter.get('invalid', 0)} |"
        )

    lines.extend([
        "",
        "## 份额字母分布",
        "",
        "| share_class | null | zero | positive | invalid |",
        "| --- | ---: | ---: | ---: | ---: |",
    ])
    for key, counter in sorted((payload.get("share_class_stats") or {}).items()):
        lines.append(
            f"| {key} | {counter.get('null', 0)} | {counter.get('zero', 0)} | {counter.get('positive', 0)} | {counter.get('invalid', 0)} |"
        )

    lines.extend([
        "",
        "## 高优先家族",
        "",
    ])
    review_families = payload.get("top_review_families") or []
    if not review_families:
        lines.append("- 无")
    else:
        for item in review_families:
            lines.extend([
                f"- 家族: `{item.get('family_name')}`",
                f"  - pattern: `{item.get('pattern')}`",
                f"  - null/zero/positive: `{item.get('null_count', 0)}/{item.get('zero_count', 0)}/{item.get('positive_count', 0)}`",
                f"  - type_categories: `{', '.join(item.get('type_categories') or [])}`",
            ])

    lines.extend([
        "",
        "## 复核样本",
        "",
    ])
    review_rows = payload.get("sample_missing_review_rows") or []
    if not review_rows:
        lines.append("- 无")
    else:
        for item in review_rows:
            lines.append(
                f"- `{item.get('fcode')}` {item.get('shortname')} | type={item.get('type_category')} "
                f"| share={item.get('share_class')} | currency={item.get('currency_bucket')} "
                f"| pattern={item.get('family_pattern')} | family={item.get('family_name')}"
            )
    return "\n".join(lines).strip() + "\n"


def write_sales_fee_semantics_artifacts(
    payload: dict,
    output_dir: str = DEFAULT_AUDIT_DIR,
    timestamp: Optional[str] = None,
) -> Tuple[Path, Path]:
    """写出销售服务费语义审计 JSON / Markdown。"""
    ts = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
    artifact_dir = Path(output_dir)
    if not artifact_dir.is_absolute():
        artifact_dir = (PROJECT_ROOT / artifact_dir).resolve()
    artifact_dir.mkdir(parents=True, exist_ok=True)

    json_path = artifact_dir / f"sales_fee_semantics_audit_{ts}.json"
    md_path = artifact_dir / f"sales_fee_semantics_audit_{ts}.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(render_sales_fee_semantics_markdown(payload), encoding="utf-8")
    return json_path, md_path


if __name__ == "__main__":
    payload = build_sales_fee_semantics_audit_payload()
    json_path, md_path = write_sales_fee_semantics_artifacts(payload)
    print(json.dumps({
        "json_path": str(json_path),
        "md_path": str(md_path),
        "summary": payload.get("summary"),
        "findings": payload.get("findings"),
    }, ensure_ascii=False, indent=2))
