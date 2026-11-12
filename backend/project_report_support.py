from __future__ import annotations

from collections import Counter
from typing import Any, Callable

import pandas as pd

from core.utils.buyability import buyability_label
from backend.project_formal_groups import (
    ENHANCED_BOND_GROUP_LABEL,
    FORMAL_GROUP_LABEL,
    STABLE_BOND_GROUP_LABEL,
)
from backend.project_publish_decisions import (
    AUTO_DIAGNOSIS_CATEGORY_COLUMN,
    AUTO_DIAGNOSIS_POOL_LABEL,
    AUTO_DIAGNOSIS_VERDICT,
    PROJECT_RECOMMENDATION_REASONS,
)
from backend.project_publish_decisions import DATA_VALIDATION_COLUMNS, validation_status_value
from backend.project_publish_decisions import reason_code_group
from backend.project_publish_decisions import summarize_reason_groups


STAGE_B_REASON_PRIORITY = [
    "not_buyable",
    "buyability_unknown",
    "holding_period_limit",
    "hard_block",
    "stale_nav",
    "missing_key_metrics",
    "fee_too_high",
    "mdd_too_high",
    "volatility_too_high",
    "scale_too_small",
    "manager_tenure_short",
    "score_insufficient",
]

STAGE_B_ALLOWED_REASON_CODES = {
    "not_buyable",
    "buyability_unknown",
    "holding_period_limit",
    "hard_block",
    "stale_nav",
    "missing_key_metrics",
    "fee_too_high",
    "mdd_too_high",
    "volatility_too_high",
    "scale_too_small",
    "manager_tenure_short",
    "score_insufficient",
}


def _safe_float(value: Any) -> float | None:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(numeric):
        return None
    return numeric


def normalize_autostock_status(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    lowered = text.lower()
    mapping = {
        "未校验": "not_checked",
        "not_checked": "not_checked",
        "unknown": "",
        "none": "",
        "null": "",
        "nan": "",
        "<na>": "",
        "n/a": "",
        "通过": "strict_match",
        "严格一致": "strict_match",
        "通过（含软告警）": "soft_pass_with_warnings",
        "soft_pass_with_warnings": "soft_pass_with_warnings",
        "strict_match": "strict_match",
        "拦截": "hard_block",
        "hard_block": "hard_block",
    }
    return mapping.get(lowered, mapping.get(text, text))


def build_summary_sheet_rows(items: list[tuple[str, Any]]) -> pd.DataFrame:
    return pd.DataFrame([{"指标": label, "数值": value} for label, value in items])


def distribution_frame(df: pd.DataFrame, column: str, label: str, share_label: str) -> pd.DataFrame:
    if df.empty or column not in df.columns:
        return pd.DataFrame(columns=[label, "数量", share_label])

    counter = Counter(str(value or "未知").strip() or "未知" for value in df[column].tolist())
    total = len(df.index)
    return pd.DataFrame(
        [
            {
                label: name,
                "数量": int(count),
                share_label: round(count / total * 100, 2) if total else 0.0,
            }
            for name, count in counter.most_common()
        ]
    )


def build_fee_band_frame(df: pd.DataFrame) -> pd.DataFrame:
    values = [_safe_float(value) or 0.0 for value in df.get("费率合计(%)", [])] if not df.empty else []
    return pd.DataFrame(
        [
            {"费率区间": "≤0.30%", "数量": int(sum(value <= 0.30 for value in values))},
            {"费率区间": "0.30%-0.50%", "数量": int(sum(0.30 < value <= 0.50 for value in values))},
            {"费率区间": ">0.50%", "数量": int(sum(value > 0.50 for value in values))},
        ]
    )


def build_formal_group_distribution(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or FORMAL_GROUP_LABEL not in df.columns:
        return pd.DataFrame(columns=[FORMAL_GROUP_LABEL, "数量", "占正式推荐比例(%)"])

    counter = Counter(str(value or "").strip() or STABLE_BOND_GROUP_LABEL for value in df[FORMAL_GROUP_LABEL].tolist())
    total = len(df.index)
    rows: list[dict[str, Any]] = []
    for label in [STABLE_BOND_GROUP_LABEL, ENHANCED_BOND_GROUP_LABEL]:
        count = int(counter.get(label, 0))
        rows.append(
            {
                FORMAL_GROUP_LABEL: label,
                "数量": count,
                "占正式推荐比例(%)": round(count / total * 100, 2) if total else 0.0,
            }
        )
    return pd.DataFrame(rows)


def build_watch_summary_frame(df: pd.DataFrame) -> pd.DataFrame:
    counter = Counter(
        str(value or "未分类").strip() or "未分类"
        for value in df.get(AUTO_DIAGNOSIS_CATEGORY_COLUMN, pd.Series(dtype=object)).tolist()
    )
    total = len(df.index)
    return pd.DataFrame(
        [
            {
                AUTO_DIAGNOSIS_CATEGORY_COLUMN: name,
                "数量": int(count),
                "占自动诊断池比例(%)": round(count / total * 100, 2) if total else 0.0,
            }
            for name, count in counter.most_common()
        ]
    )


def split_reason_codes(value: Any) -> list[str]:
    return [item.strip() for item in str(value or "").split("|") if item.strip()]


def normalize_stage_b_reason_code(reason_code: str) -> str:
    normalized = str(reason_code or "").strip()
    if normalized in {
        "below_weighted_floor",
        "below_short_floor",
        "below_long_floor",
        "below_annual_floor",
        "below_sharpe_floor",
        "below_sortino_floor",
        "below_calmar_floor",
        "below_positive_ratio_floor",
        "low_publish_confidence",
        "soft_warning",
        "manager_data_missing",
        "manager_score_low",
        "scale_report_stale",
    }:
        return "score_insufficient"
    return normalized


def pick_stage_b_primary_reason(reason_codes: list[str]) -> str:
    available = [
        normalize_stage_b_reason_code(code)
        for code in reason_codes
        if normalize_stage_b_reason_code(code) in STAGE_B_ALLOWED_REASON_CODES
    ]
    if not available:
        return "score_insufficient"
    for code in STAGE_B_REASON_PRIORITY:
        if code in available:
            return code
    return available[0]


def pick_stage_b_secondary_reasons(reason_codes: list[str], primary_reason: str) -> list[str]:
    secondary: list[str] = []
    seen: set[str] = {primary_reason}
    normalized_reason_codes = [
        normalize_stage_b_reason_code(code)
        for code in reason_codes
        if normalize_stage_b_reason_code(code) in STAGE_B_ALLOWED_REASON_CODES
    ]
    for code in STAGE_B_REASON_PRIORITY:
        if code == primary_reason or code in seen:
            continue
        if code in normalized_reason_codes:
            secondary.append(code)
            seen.add(code)
    for code in normalized_reason_codes:
        if code in seen or code not in STAGE_B_ALLOWED_REASON_CODES:
            continue
        secondary.append(code)
        seen.add(code)
    return secondary


def build_stage_b_reason_ledger(
    *,
    candidate_df: pd.DataFrame,
    excluded_df: pd.DataFrame,
    official_codes: set[str],
    normalize_code: Callable[[Any], str],
) -> tuple[pd.DataFrame, dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    for row in excluded_df.to_dict(orient="records"):
        code = normalize_code(row.get("基金代码") or row.get("fcode"))
        if not code:
            continue
        annual_return = _safe_float(row.get("1年-年化收益(%)"))
        primary_reason = "holding_period_limit"
        secondary_reason_codes = pick_stage_b_secondary_reasons([], primary_reason)
        primary_reason_group = reason_code_group(primary_reason)
        all_reason_group = primary_reason_group
        rows.append(
            {
                "基金代码": code,
                "基金名称": str(row.get("基金名称") or row.get("shortname") or "").strip(),
                "基金类型": str(row.get("基金类型") or row.get("ftype") or "").strip(),
                "债基风格标签": str(row.get("债基风格标签") or "").strip(),
                "基金公司": str(row.get("基金公司") or row.get("company_name") or "").strip(),
                "项目推荐结论": "未入正式榜",
                "发布结论状态": "结构性排除",
                "可投资性状态": str(row.get("可投资性状态") or "").strip(),
                "数据就绪状态": str(row.get("数据就绪状态") or "").strip(),
                "质量结论状态": str(row.get("质量结论状态") or "").strip(),
                "未入榜阶段": "持有期排除",
                "原始原因代码": primary_reason,
                "主排除原因代码": primary_reason,
                "主原因分组": primary_reason_group,
                "主排除原因": "持有期超过90天",
                "次排除原因代码": "|".join(secondary_reason_codes),
                "次排除原因": "",
                "原因分组汇总": all_reason_group,
                "1年-年化收益(%)": "" if annual_return is None else round(annual_return, 4),
                "综合加权得分": row.get("综合加权得分"),
                "近期得分": row.get("近期得分"),
                "长期得分": row.get("长期得分"),
                "是否可申购": str(row.get("是否可申购") or buyability_label(row.get("is_buyable"))).strip(),
                "AutoStock原始校验状态": normalize_autostock_status(row.get("AutoStock原始校验状态") or (row.get("autostock_validation_status") if not row.get("validation_source") else "")),
                **{column: row.get(column, "") for column in DATA_VALIDATION_COLUMNS if column in row},
                "推荐理由": str(row.get("排除原因") or "").strip(),
            }
        )

    for row in candidate_df.to_dict(orient="records"):
        code = normalize_code(row.get("基金代码") or row.get("fcode"))
        if not code or code in official_codes:
            continue
        reason_codes = split_reason_codes(row.get("项目推荐原因代码"))
        filtered_reason_codes = [code_name for code_name in reason_codes if code_name in STAGE_B_ALLOWED_REASON_CODES]
        primary_reason = pick_stage_b_primary_reason(filtered_reason_codes)
        secondary_reason_codes = pick_stage_b_secondary_reasons(filtered_reason_codes, primary_reason)
        primary_reason_group, all_reason_group = summarize_reason_groups([primary_reason, *secondary_reason_codes])
        annual_return = _safe_float(row.get("1年-年化收益(%)"))
        verdict = str(row.get("项目推荐结论") or "").strip() or AUTO_DIAGNOSIS_VERDICT
        stage = str(row.get(AUTO_DIAGNOSIS_CATEGORY_COLUMN) or "").strip() or f"{AUTO_DIAGNOSIS_POOL_LABEL}分流"
        rows.append(
            {
                "基金代码": code,
                "基金名称": str(row.get("基金名称") or "").strip(),
                "基金类型": str(row.get("基金类型") or row.get("ftype") or "").strip(),
                "债基风格标签": str(row.get("债基风格标签") or "").strip(),
                "基金公司": str(row.get("基金公司") or row.get("company_name") or "").strip(),
                "项目推荐结论": verdict,
                "发布结论状态": str(row.get("发布结论状态") or "").strip(),
                "可投资性状态": str(row.get("可投资性状态") or "").strip(),
                "数据就绪状态": str(row.get("数据就绪状态") or "").strip(),
                "质量结论状态": str(row.get("质量结论状态") or "").strip(),
                "未入榜阶段": stage,
                "原始原因代码": "|".join(reason_codes),
                "主排除原因代码": primary_reason,
                "主原因分组": primary_reason_group,
                "主排除原因": PROJECT_RECOMMENDATION_REASONS.get(primary_reason, primary_reason),
                "次排除原因代码": "|".join(secondary_reason_codes),
                "次排除原因": "；".join(PROJECT_RECOMMENDATION_REASONS.get(item, item) for item in secondary_reason_codes),
                "原因分组汇总": all_reason_group,
                "1年-年化收益(%)": "" if annual_return is None else round(annual_return, 4),
                "综合加权得分": row.get("综合加权得分"),
                "近期得分": row.get("近期得分"),
                "长期得分": row.get("长期得分"),
                "是否可申购": str(row.get("是否可申购") or "").strip(),
                "AutoStock原始校验状态": normalize_autostock_status(row.get("AutoStock原始校验状态")),
                **{column: row.get(column, "") for column in DATA_VALIDATION_COLUMNS if column in row},
                "推荐理由": str(row.get("推荐理由") or "").strip(),
            }
        )

    ledger_df = pd.DataFrame(rows)
    if ledger_df.empty:
        empty_summary = {
            "available": False,
            "excluded_total": 0,
            "top_primary_reasons": [],
            "high_yield_thresholds": [],
            "preview": [],
        }
        return ledger_df, empty_summary

    ledger_df["1年-年化收益(%)"] = pd.to_numeric(ledger_df.get("1年-年化收益(%)"), errors="coerce")
    ledger_df["综合加权得分"] = pd.to_numeric(ledger_df.get("综合加权得分"), errors="coerce")
    ledger_df["近期得分"] = pd.to_numeric(ledger_df.get("近期得分"), errors="coerce")
    ledger_df["长期得分"] = pd.to_numeric(ledger_df.get("长期得分"), errors="coerce")
    ledger_df["收益档位"] = pd.cut(
        ledger_df["1年-年化收益(%)"],
        bins=[-float("inf"), 5.0, 8.0, 10.0, float("inf")],
        labels=["<5%", "5%-8%", "8%-10%", ">=10%"],
        right=False,
    ).astype(str).replace("nan", "")
    ledger_df = ledger_df.sort_values(
        by=["1年-年化收益(%)", "综合加权得分", "基金代码"],
        ascending=[False, False, True],
        kind="mergesort",
    ).reset_index(drop=True)

    top_primary_counter = Counter(
        str(value or "未知原因").strip() or "未知原因"
        for value in ledger_df.get("主排除原因", pd.Series(dtype=object)).tolist()
    )

    high_yield_thresholds: list[dict[str, Any]] = []
    for threshold in [5.0, 8.0, 10.0]:
        current_df = ledger_df[pd.to_numeric(ledger_df.get("1年-年化收益(%)"), errors="coerce").ge(threshold)].copy()
        reason_counter = Counter(
            str(value or "未知原因").strip() or "未知原因"
            for value in current_df.get("主排除原因", pd.Series(dtype=object)).tolist()
        )
        high_yield_thresholds.append(
            {
                "threshold": threshold,
                "count": int(len(current_df.index)),
                "top_primary_reasons": [
                    {"reason": reason, "count": int(count)}
                    for reason, count in reason_counter.most_common(5)
                ],
                "sample_preview": current_df.head(5).fillna("").astype(str).to_dict(orient="records"),
            }
        )

    summary = {
        "available": True,
        "excluded_total": int(len(ledger_df.index)),
        "top_primary_reasons": [
            {"reason": reason, "count": int(count)}
            for reason, count in top_primary_counter.most_common(10)
        ],
        "high_yield_thresholds": high_yield_thresholds,
        "preview": ledger_df.head(15).fillna("").astype(str).to_dict(orient="records"),
    }
    return ledger_df, summary


__all__ = [
    "STAGE_B_ALLOWED_REASON_CODES",
    "STAGE_B_REASON_PRIORITY",
    "build_fee_band_frame",
    "build_formal_group_distribution",
    "build_stage_b_reason_ledger",
    "build_summary_sheet_rows",
    "build_watch_summary_frame",
    "distribution_frame",
    "normalize_stage_b_reason_code",
    "pick_stage_b_primary_reason",
    "pick_stage_b_secondary_reasons",
    "split_reason_codes",
]
