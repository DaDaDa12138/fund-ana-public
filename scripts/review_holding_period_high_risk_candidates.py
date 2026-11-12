from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime
from pathlib import Path
import sys
from typing import Any

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CORE_ROOT = PROJECT_ROOT / "core"
for path in [PROJECT_ROOT, CORE_ROOT]:
    path_text = str(path)
    if path_text not in sys.path:
        sys.path.insert(0, path_text)

from backend.config import load_settings
from bond_fund_screening.share_class_utils import normalize_base_fund_name
from scripts.audit_holding_period_no_signal_edges import _normalize_text
from utils.holding_period_parser import parse_holding_with_priority


REVIEW_PHRASES: list[tuple[str, str]] = [
    ("has_closed_operation_term", "封闭运作期"),
    ("has_remaining_closed_term", "剩余封闭期限"),
    ("has_open_period", "开放期"),
    ("has_closed_period", "封闭期"),
    ("has_holding_return_hint", "持有期收益"),
    ("has_hold_to_maturity_hint", "持有到期"),
]


def _resolve_latest_no_signal_audit(reports_dir: Path) -> Path:
    audit_dir = reports_dir / "audit" / "holding_period_no_signal_edges"
    candidates = sorted(audit_dir.glob("holding_period_no_signal_edges_*.json"))
    if not candidates:
        raise RuntimeError("未找到 holding_period_no_signal_edges 审计产物。")
    return candidates[-1]


def _load_high_risk_codes(source_json: Path) -> list[str]:
    payload = json.loads(source_json.read_text(encoding="utf-8"))
    rows = payload.get("high_risk_preview") or []
    codes = [str(row.get("fcode") or "").strip() for row in rows if str(row.get("fcode") or "").strip()]
    if not codes:
        raise RuntimeError("高风险文本信号为空，无法生成复核清单。")
    return codes


def _load_candidate_frame(db_path: Path, codes: list[str]) -> pd.DataFrame:
    conn = sqlite3.connect(str(db_path))
    try:
        placeholders = ",".join(["?"] * len(codes))
        sql = f"""
            SELECT
                TRIM(COALESCE(fcode, '')) AS fcode,
                COALESCE(shortname, '') AS fund_name,
                COALESCE(fullname, '') AS full_name,
                COALESCE(company_name, '') AS company_name,
                COALESCE(ftype, '') AS ftype,
                COALESCE(purchase_status, '') AS purchase_status,
                COALESCE(redemption_status, '') AS redemption_status,
                COALESCE(is_buyable, 0) AS is_buyable,
                COALESCE(cycle, '') AS cycle,
                COALESCE(preserve_period, '') AS preserve_period,
                COALESCE(buy_time, '') AS buy_time,
                COALESCE(investment_strategy, '') AS investment_strategy,
                COALESCE(investment_target, '') AS investment_target,
                COALESCE(has_holding_period, 0) AS has_holding_period,
                CAST(COALESCE(holding_days, 0) AS INTEGER) AS holding_days,
                TRIM(COALESCE(holding_period_source, '')) AS holding_period_source
            FROM funds
            WHERE fcode IN ({placeholders})
            ORDER BY fcode
        """
        df = pd.read_sql_query(sql, conn, params=codes)
    finally:
        conn.close()
    return df


def _extract_snippet(text: str, phrase: str, *, radius: int = 40) -> str:
    source = _normalize_text(text)
    idx = source.find(phrase)
    if idx < 0:
        return ""
    start = max(0, idx - radius)
    end = min(len(source), idx + len(phrase) + radius)
    return source[start:end].strip()


def _collect_phrase_tags(detail_text: str) -> list[str]:
    return [phrase for _column, phrase in REVIEW_PHRASES if phrase in detail_text]


def _suggest_family_review(
    *,
    has_open_period: bool,
    has_closed_period: bool,
    has_closed_operation_term: bool,
    has_remaining_closed_term: bool,
    buyable_count: int,
    parser_hit_count: int,
    struct_field_present_count: int,
) -> dict[str, str]:
    if has_open_period and (has_closed_period or has_remaining_closed_term):
        return {
            "review_priority": "high",
            "review_action": "人工复核历史封闭/转开放模板，不直接扩生产规则",
            "review_judgement": "开放期与封闭期并存，优先确认为历史说明还是当前限制",
        }
    if (has_closed_operation_term or has_remaining_closed_term) and buyable_count > 0 and parser_hit_count == 0 and struct_field_present_count == 0:
        return {
            "review_priority": "medium",
            "review_action": "更像历史封闭运作模板噪音，维持无信号观察",
            "review_judgement": "当前可申购且结构化字段为空，不支持直接升级为持有期拦截",
        }
    return {
        "review_priority": "medium",
        "review_action": "继续人工复核，不直接改解析规则",
        "review_judgement": "证据不足以自动确认持有期限制",
    }


def _build_candidate_review_frame(source_df: pd.DataFrame) -> pd.DataFrame:
    current = source_df.copy()
    if current.empty:
        return current

    current["fund_name"] = current["fund_name"].map(_normalize_text)
    current["company_name"] = current["company_name"].map(_normalize_text)
    current["investment_strategy"] = current["investment_strategy"].map(_normalize_text)
    current["investment_target"] = current["investment_target"].map(_normalize_text)
    current["detail_text"] = (
        current["investment_strategy"].fillna("").astype(str)
        + " "
        + current["investment_target"].fillna("").astype(str)
    ).str.strip()
    current["base_fund_name"] = current["fund_name"].map(normalize_base_fund_name)
    current["struct_field_present"] = (
        current["cycle"].fillna("").astype(str).str.strip().ne("")
        | current["preserve_period"].fillna("").astype(str).str.strip().ne("")
        | current["buy_time"].fillna("").astype(str).str.strip().ne("")
    )

    for column, phrase in REVIEW_PHRASES:
        current[column] = current["detail_text"].str.contains(phrase, regex=False)

    matched_tags = current["detail_text"].map(_collect_phrase_tags)
    current["matched_phrase_tags"] = matched_tags.map(lambda values: "；".join(values))
    current["evidence_snippet"] = current["detail_text"].map(
        lambda text: " | ".join(
            snippet
            for _column, phrase in REVIEW_PHRASES
            if (snippet := _extract_snippet(text, phrase))
        )[:320]
    )

    parser_results = current.apply(
        lambda row: parse_holding_with_priority(
            shortname=row.get("fund_name"),
            fullname=row.get("full_name"),
            investment_strategy=row.get("investment_strategy"),
            investment_target=row.get("investment_target"),
            cycle=row.get("cycle"),
            preserve_period=row.get("preserve_period"),
            buy_time=row.get("buy_time"),
        ),
        axis=1,
    )
    current["parser_layer"] = parser_results.map(lambda item: item[1])
    current["parser_match_days"] = parser_results.map(lambda item: None if item[0] is None else item[0].days)
    current["parser_match_keyword"] = parser_results.map(lambda item: "" if item[0] is None else str(item[0].keyword or ""))
    current["parser_hit"] = current["parser_match_days"].notna()
    return current


def _merge_tags(series: pd.Series) -> str:
    values: list[str] = []
    for text in series.fillna("").astype(str):
        for item in text.split("；"):
            item = item.strip()
            if item and item not in values:
                values.append(item)
    return "；".join(values)


def _build_family_review_frame(candidate_df: pd.DataFrame) -> pd.DataFrame:
    if candidate_df.empty:
        return candidate_df

    rows: list[dict[str, Any]] = []
    grouped = candidate_df.groupby(["company_name", "base_fund_name"], sort=True, dropna=False)
    for (company_name, base_fund_name), group in grouped:
        suggestion = _suggest_family_review(
            has_open_period=bool(group["has_open_period"].any()),
            has_closed_period=bool(group["has_closed_period"].any()),
            has_closed_operation_term=bool(group["has_closed_operation_term"].any()),
            has_remaining_closed_term=bool(group["has_remaining_closed_term"].any()),
            buyable_count=int(group["is_buyable"].fillna(0).astype(int).sum()),
            parser_hit_count=int(group["parser_hit"].sum()),
            struct_field_present_count=int(group["struct_field_present"].sum()),
        )
        rows.append(
            {
                "company_name": company_name,
                "base_fund_name": base_fund_name,
                "candidate_count": int(len(group.index)),
                "buyable_count": int(group["is_buyable"].fillna(0).astype(int).sum()),
                "open_purchase_count": int(group["purchase_status"].fillna("").astype(str).eq("开放申购").sum()),
                "parser_hit_count": int(group["parser_hit"].sum()),
                "struct_field_present_count": int(group["struct_field_present"].sum()),
                "matched_phrase_tags": _merge_tags(group["matched_phrase_tags"]),
                "share_class_codes": " / ".join(group["fcode"].fillna("").astype(str).tolist()),
                "share_class_names": " / ".join(group["fund_name"].fillna("").astype(str).tolist()),
                "sample_evidence_snippet": next(
                    (text for text in group["evidence_snippet"].fillna("").astype(str).tolist() if text),
                    "",
                ),
                **suggestion,
            }
        )
    return pd.DataFrame(rows).sort_values(
        by=["review_priority", "candidate_count", "company_name", "base_fund_name"],
        ascending=[True, False, True, True],
        kind="mergesort",
    ).reset_index(drop=True)


def _build_summary(
    *,
    source_json: Path,
    candidate_df: pd.DataFrame,
    family_df: pd.DataFrame,
) -> dict[str, Any]:
    priority_counts = family_df["review_priority"].fillna("").astype(str).value_counts().to_dict() if not family_df.empty else {}
    action_counts = family_df["review_action"].fillna("").astype(str).value_counts().to_dict() if not family_df.empty else {}
    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "source_no_signal_audit_json": str(source_json),
        "high_risk_candidate_count": int(len(candidate_df.index)),
        "high_risk_family_count": int(len(family_df.index)),
        "review_priority_counts": {str(k): int(v) for k, v in priority_counts.items()},
        "review_action_counts": {str(k): int(v) for k, v in action_counts.items()},
        "family_review_preview": family_df.head(20).fillna("").to_dict(orient="records"),
        "candidate_preview": candidate_df[
            [
                "fcode",
                "fund_name",
                "company_name",
                "base_fund_name",
                "purchase_status",
                "is_buyable",
                "matched_phrase_tags",
                "parser_layer",
                "parser_match_days",
                "evidence_snippet",
            ]
        ].head(30).fillna("").to_dict(orient="records"),
        "conclusion": {
            "no_production_rule_change_recommended": True,
            "summary": "当前高风险边界样本高度集中在少数公司/母基金模板说明，优先做人工复核与持续观察，不直接扩生产持有期解析规则。",
        },
    }


def _write_markdown(summary: dict[str, Any], output_path: Path) -> None:
    lines = [
        "# 持有期高风险边界复核清单",
        "",
        f"- 生成时间：`{summary.get('generated_at')}`",
        f"- 来源审计：`{summary.get('source_no_signal_audit_json')}`",
        f"- 高风险样本：`{summary.get('high_risk_candidate_count')}`",
        f"- 高风险母基金簇：`{summary.get('high_risk_family_count')}`",
        "",
        "## 复核结论",
        "",
        f"- `{((summary.get('conclusion') or {}).get('summary', ''))}`",
        "",
    ]
    output_path.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")


def run_holding_period_high_risk_review(
    settings: Any,
    *,
    source_json: Path | None = None,
) -> dict[str, Any]:
    resolved_source_json = (
        Path(source_json).resolve()
        if source_json is not None and str(source_json).strip()
        else _resolve_latest_no_signal_audit(Path(settings.reports_dir))
    )
    codes = _load_high_risk_codes(resolved_source_json)
    raw_df = _load_candidate_frame(Path(settings.database_path), codes)
    candidate_df = _build_candidate_review_frame(raw_df)
    family_df = _build_family_review_frame(candidate_df)
    summary = _build_summary(
        source_json=resolved_source_json,
        candidate_df=candidate_df,
        family_df=family_df,
    )

    audit_dir = Path(settings.reports_dir) / "audit" / "holding_period_high_risk_review"
    audit_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base_name = f"holding_period_high_risk_review_{timestamp}"
    summary_path = audit_dir / f"{base_name}.json"
    family_csv_path = audit_dir / f"{base_name}_families.csv"
    candidate_csv_path = audit_dir / f"{base_name}_candidates.csv"
    markdown_path = audit_dir / f"{base_name}.md"

    family_df.to_csv(family_csv_path, index=False, encoding="utf-8-sig")
    candidate_df.to_csv(candidate_csv_path, index=False, encoding="utf-8-sig")
    summary["artifact_path"] = str(summary_path)
    summary["family_csv_path"] = str(family_csv_path)
    summary["candidate_csv_path"] = str(candidate_csv_path)
    summary["markdown_path"] = str(markdown_path)
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_markdown(summary, markdown_path)
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description="复核持有期高风险边界样本。")
    parser.add_argument("--source-json", default="", help="指定 holding_period_no_signal_edges 审计 JSON。")
    args = parser.parse_args()

    settings = load_settings()
    source_json = Path(args.source_json).resolve() if str(args.source_json or "").strip() else None
    summary = run_holding_period_high_risk_review(settings, source_json=source_json)
    print(str(summary["artifact_path"]))
    print(str(summary["family_csv_path"]))
    print(str(summary["candidate_csv_path"]))
    print(str(summary["markdown_path"]))
    print(json.dumps(summary, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
