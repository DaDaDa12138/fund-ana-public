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
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.config import load_settings
from scripts.audit_runtime_utils import (
    build_audit_artifact_path as _build_audit_artifact_path,
    resolve_run_payload as _resolve_run_payload,
)


EDGE_PHRASE_SPECS: list[tuple[str, str]] = [
    ("detail_contains_closed_operation_term", "封闭运作期"),
    ("detail_contains_closed_period", "封闭期"),
    ("detail_contains_open_period", "开放期"),
    ("detail_contains_periodic_open", "定期开放"),
    ("detail_contains_holding_period", "持有期"),
]

FALSE_POSITIVE_HINT_SPECS: list[tuple[str, str]] = [
    ("detail_contains_holding_period_return", "持有期收益"),
    ("detail_contains_hold_to_maturity", "持有到期"),
    ("detail_contains_closed_period_investment", "封闭期内"),
]

PRECISE_HIGH_RISK_PHRASE_SPECS: list[tuple[str, str]] = [
    ("detail_contains_min_hold", "最短持有"),
    ("detail_contains_per_lot_min_hold", "每笔份额最短持有"),
    ("detail_contains_hold_term_equals", "持有期限为"),
    ("detail_contains_closed_term_remaining", "剩余封闭期限"),
    ("detail_contains_closed_operation_due", "封闭运作期到期日"),
    ("detail_contains_periodic_open_mode", "定期开放运作方式"),
]


def _read_no_signal_frame(db_path: Path) -> pd.DataFrame:
    conn = sqlite3.connect(str(db_path))
    try:
        df = pd.read_sql_query(
            """
            SELECT
                TRIM(COALESCE(fcode, '')) AS fcode,
                COALESCE(shortname, '') AS fund_name,
                COALESCE(fullname, '') AS full_name,
                COALESCE(ftype, '') AS ftype,
                COALESCE(investment_strategy, '') AS investment_strategy,
                COALESCE(investment_target, '') AS investment_target,
                COALESCE(has_holding_period, 0) AS has_holding_period,
                CAST(COALESCE(holding_days, 0) AS INTEGER) AS holding_days,
                TRIM(COALESCE(holding_keywords, '')) AS holding_keywords,
                TRIM(COALESCE(holding_period_source, '')) AS holding_period_source
            FROM funds
            WHERE ftype LIKE '债券型%'
              AND COALESCE(has_holding_period, 0) = 0
            ORDER BY fcode
            """,
            conn,
        )
    finally:
        conn.close()
    return df


def _normalize_text(value: Any) -> str:
    return str(value or "").replace("\r", " ").replace("\n", " ").strip()


def _build_edge_frame(source_df: pd.DataFrame) -> pd.DataFrame:
    current = source_df.copy()
    if current.empty:
        return current

    current["fund_name"] = current["fund_name"].map(_normalize_text)
    current["investment_strategy"] = current["investment_strategy"].map(_normalize_text)
    current["investment_target"] = current["investment_target"].map(_normalize_text)
    current["detail_text"] = (
        current["investment_strategy"].fillna("").astype(str)
        + " "
        + current["investment_target"].fillna("").astype(str)
    ).str.strip()

    current["name_contains_year"] = current["fund_name"].str.contains("年", regex=False)
    current["name_contains_month"] = current["fund_name"].str.contains("月", regex=False)
    current["name_numeric_period_like"] = current["fund_name"].str.contains(r"\d+\s*(?:年|个月|月)")
    current["name_hold_like_edge"] = current["fund_name"].str.contains(
        r"(?:持有期|滚动持有|定开|定期开放|封闭|开放期)"
    )

    for column, phrase in EDGE_PHRASE_SPECS:
        current[column] = current["detail_text"].str.contains(phrase, regex=False)
    for column, phrase in FALSE_POSITIVE_HINT_SPECS:
        current[column] = current["detail_text"].str.contains(phrase, regex=False)
    for column, phrase in PRECISE_HIGH_RISK_PHRASE_SPECS:
        current[column] = current["detail_text"].str.contains(phrase, regex=False)

    current["name_edge"] = (
        current["name_numeric_period_like"]
        | current["name_contains_year"]
        | current["name_contains_month"]
    )
    current["name_period_noise"] = current["name_edge"] & ~current["name_hold_like_edge"]
    current["detail_edge"] = False
    for column, _phrase in EDGE_PHRASE_SPECS:
        current["detail_edge"] = current["detail_edge"] | current[column]

    current["false_positive_hint"] = False
    for column, _phrase in FALSE_POSITIVE_HINT_SPECS:
        current["false_positive_hint"] = current["false_positive_hint"] | current[column]

    precise_high_risk = False
    for column, _phrase in PRECISE_HIGH_RISK_PHRASE_SPECS:
        precise_high_risk = precise_high_risk | current[column]
    current["high_risk_detail_edge"] = (
        current["detail_contains_closed_operation_term"]
        | current["detail_contains_periodic_open"]
        | current["detail_contains_open_period"]
        | precise_high_risk
        | (
            current["detail_contains_closed_period"]
            & current["detail_contains_open_period"]
            & ~current["detail_contains_closed_period_investment"]
        )
    )

    def _classify(row: pd.Series) -> str:
        if bool(row.get("high_risk_detail_edge")):
            return "高风险文本信号"
        if bool(row.get("name_hold_like_edge")):
            return "名称高风险边界"
        if bool(row.get("name_period_noise")):
            return "名称期限/品牌噪音"
        if bool(row.get("detail_edge")):
            return "文本噪音疑似误报"
        return "普通无信号"

    current["audit_bucket"] = current.apply(_classify, axis=1)

    def _reason_tags(row: pd.Series) -> str:
        tags: list[str] = []
        if bool(row.get("name_hold_like_edge")):
            tags.append("名称含持有期样式词")
        if bool(row.get("name_numeric_period_like")):
            tags.append("名称含数字年限")
        elif bool(row.get("name_contains_year")):
            tags.append("名称含年")
        elif bool(row.get("name_contains_month")):
            tags.append("名称含月")
        for column, phrase in EDGE_PHRASE_SPECS:
            if bool(row.get(column)):
                tags.append(phrase)
        for column, phrase in FALSE_POSITIVE_HINT_SPECS:
            if bool(row.get(column)):
                tags.append(f"误报提示:{phrase}")
        return "；".join(tags)

    current["reason_tags"] = current.apply(_reason_tags, axis=1)
    return current


def _preview_rows(df: pd.DataFrame, limit: int) -> list[dict[str, Any]]:
    if df.empty:
        return []
    preview_columns = [
        "fcode",
        "fund_name",
        "audit_bucket",
        "reason_tags",
        "holding_period_source",
        "holding_days",
        "investment_strategy",
        "investment_target",
    ]
    preview_df = df[preview_columns].copy().head(limit)
    return preview_df.fillna("").to_dict(orient="records")


def _value_counts(series: pd.Series) -> list[dict[str, Any]]:
    counts = series.fillna("").astype(str).map(lambda text: text.strip() or "empty").value_counts()
    return [{"label": str(index), "count": int(value)} for index, value in counts.items()]


def _build_summary(df: pd.DataFrame) -> dict[str, Any]:
    high_risk_df = df[df["audit_bucket"] == "高风险文本信号"].copy()
    name_high_risk_df = df[df["audit_bucket"] == "名称高风险边界"].copy()
    name_noise_df = df[df["audit_bucket"] == "名称期限/品牌噪音"].copy()
    noise_df = df[df["audit_bucket"] == "文本噪音疑似误报"].copy()

    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "scope": "债券型基金 + has_holding_period=0",
        "no_signal_total": int(len(df.index)),
        "bucket_counts": {
            "高风险文本信号": int(len(high_risk_df.index)),
            "名称高风险边界": int(len(name_high_risk_df.index)),
            "名称期限/品牌噪音": int(len(name_noise_df.index)),
            "文本噪音疑似误报": int(len(noise_df.index)),
            "普通无信号": int((df["audit_bucket"] == "普通无信号").sum()),
        },
        "name_edge_breakdown": {
            "名称含年": int(df["name_contains_year"].sum()),
            "名称含月": int(df["name_contains_month"].sum()),
            "名称含数字年限": int(df["name_numeric_period_like"].sum()),
            "名称含持有期样式词": int(df["name_hold_like_edge"].sum()),
            "名称期限/品牌噪音": int(df["name_period_noise"].sum()),
        },
        "detail_edge_breakdown": {
            phrase: int(df[column].sum()) for column, phrase in EDGE_PHRASE_SPECS
        },
        "precise_high_risk_phrase_breakdown": {
            phrase: int(df[column].sum()) for column, phrase in PRECISE_HIGH_RISK_PHRASE_SPECS
        },
        "false_positive_hints": {
            phrase: int(df[column].sum()) for column, phrase in FALSE_POSITIVE_HINT_SPECS
        },
        "holding_period_source_counts": _value_counts(df["holding_period_source"]),
        "high_risk_preview": _preview_rows(high_risk_df.sort_values(["fcode"]), 30),
        "name_high_risk_preview": _preview_rows(name_high_risk_df.sort_values(["fcode"]), 30),
        "name_noise_preview": _preview_rows(name_noise_df.sort_values(["fcode"]), 30),
        "noise_preview": _preview_rows(noise_df.sort_values(["fcode"]), 30),
        "conclusion": {
            "real_unparsed_name_like_count": int(len(name_high_risk_df.index)),
            "real_high_risk_detail_count": int(len(high_risk_df.index)),
            "detail_noise_majority": bool(len(noise_df.index) > len(high_risk_df.index)),
            "strategy_recommendation": (
                "优先围绕高风险文本信号建立人工复核清单，不继续基于泛化“持有期/封闭期”词频扩解析规则。"
            ),
        },
    }


def _write_markdown(summary: dict[str, Any], output_path: Path) -> None:
    bucket_counts = summary.get("bucket_counts") or {}
    lines = [
        "# 持有期无信号边界审计",
        "",
        f"- 生成时间：`{summary.get('generated_at')}`",
        f"- 审计范围：`{summary.get('scope')}`",
        f"- 无信号样本总数：`{summary.get('no_signal_total')}`",
        "",
        "## 核心判断",
        "",
        f"- 高风险文本信号：`{bucket_counts.get('高风险文本信号', 0)}`",
        f"- 名称高风险边界：`{bucket_counts.get('名称高风险边界', 0)}`",
        f"- 名称期限/品牌噪音：`{bucket_counts.get('名称期限/品牌噪音', 0)}`",
        f"- 文本噪音疑似误报：`{bucket_counts.get('文本噪音疑似误报', 0)}`",
        f"- 普通无信号：`{bucket_counts.get('普通无信号', 0)}`",
        "",
        "## 结论",
        "",
        f"- `real_unparsed_name_like_count = {((summary.get('conclusion') or {}).get('real_unparsed_name_like_count', 0))}`",
        f"- `real_high_risk_detail_count = {((summary.get('conclusion') or {}).get('real_high_risk_detail_count', 0))}`",
        f"- 当前建议：{((summary.get('conclusion') or {}).get('strategy_recommendation', ''))}",
        "",
    ]
    output_path.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")


def run_holding_period_no_signal_edges_audit(
    settings: Any,
    *,
    run_id: str = "",
    project_root: Path | None = None,
    limit: int = 30,
) -> dict[str, Any]:
    resolved_project_root = project_root or Path(getattr(settings, "project_root", PROJECT_ROOT))
    latest_run_id, _latest_payload, _artifacts, _latest_replay_precision = _resolve_run_payload(
        settings,
        project_root=resolved_project_root,
    )
    target_run_id, _payload, _artifacts, replay_precision = _resolve_run_payload(
        settings,
        run_id=run_id,
        project_root=resolved_project_root,
    )
    db_path = Path(settings.database_path)
    source_df = _read_no_signal_frame(db_path)
    audit_df = _build_edge_frame(source_df)
    summary = _build_summary(audit_df)

    audit_dir = Path(settings.reports_dir) / "audit" / "holding_period_no_signal_edges"
    summary_path = _build_audit_artifact_path(
        audit_dir,
        "holding_period_no_signal_edges",
        run_id=target_run_id,
    )
    base_stem = summary_path.stem
    markdown_path = audit_dir / f"{base_stem}.md"
    preview_path = audit_dir / f"{base_stem}.csv"

    preview_df = audit_df[
        audit_df["audit_bucket"].isin(
            ["高风险文本信号", "名称高风险边界", "名称期限/品牌噪音", "文本噪音疑似误报"]
        )
    ].copy()
    preview_df = preview_df[
        [
            "fcode",
            "fund_name",
            "audit_bucket",
            "reason_tags",
            "holding_period_source",
            "holding_days",
            "investment_strategy",
            "investment_target",
        ]
    ].head(max(1, int(limit or 30)))
    preview_df.to_csv(preview_path, index=False, encoding="utf-8-sig")

    auxiliary_scope = (
        "same_run_current_db_state" if target_run_id == latest_run_id else "cross_run_current_db_state"
    )
    auxiliary_scope_label = "latest + 当前库状态" if target_run_id == latest_run_id else "历史 run + 当前库状态"
    scope_detail = (
        "当前审计完全依赖当前数据库中的基金文本字段，适合 latest 同期排查，但项目尚未保留 run-local 历史文本快照。"
        if target_run_id == latest_run_id
        else "当前审计完全依赖当前数据库中的基金文本字段；即使指定历史 run，也只能做跨期近似排查，不能视为历史精确复盘。"
    )

    summary["run_id"] = target_run_id
    summary["latest_canonical_run_id"] = latest_run_id
    summary["replay_precision"] = replay_precision
    summary["replay_precision_status"] = str(replay_precision.get("status") or "").strip()
    summary["replay_precision_status_label"] = str(replay_precision.get("status_label") or "").strip()
    summary["auxiliary_scope"] = auxiliary_scope
    summary["auxiliary_scope_label"] = auxiliary_scope_label
    summary["scope_detail"] = scope_detail
    summary["artifact_path"] = str(summary_path)
    summary["preview_csv_path"] = str(preview_path)
    summary["markdown_path"] = str(markdown_path)

    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_markdown(summary, markdown_path)
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description="审计持有期无信号样本中的高风险边界。")
    parser.add_argument("--run-id", default="", help="指定 run_id；默认读取 latest canonical。")
    parser.add_argument("--limit", type=int, default=30, help="CSV 预览输出上限")
    args = parser.parse_args()

    settings = load_settings()
    project_root = Path(getattr(settings, "project_root", PROJECT_ROOT))
    summary = run_holding_period_no_signal_edges_audit(
        settings,
        run_id=args.run_id,
        project_root=project_root,
        limit=args.limit,
    )
    print(str(summary["artifact_path"]))
    print(str(summary["preview_csv_path"]))
    print(str(summary["markdown_path"]))
    print(json.dumps(summary, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
