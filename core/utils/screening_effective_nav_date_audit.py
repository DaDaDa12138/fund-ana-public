#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
筛选有效净值日期候选审计工具。

目标：
1. 只读分析当前 fund_returns.latest_nav_date 分布；
2. 对比 fund_nav_history 原始最大日期与 fund_returns 覆盖度；
3. 给出若干“覆盖率阈值”下的候选有效净值日期；
4. 为后续定义“规范化净值推进日期”提供证据，而不是直接把原始 MAX(nav_date) 用进生产门禁。
"""

from __future__ import annotations

import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from database.connection import resolve_db_path
from utils.fund_code_alias_audit import DEFAULT_AUDIT_DIR, load_latest_json_artifact
from utils.screening_storage_growth_audit import open_readonly_db


SCREENING_EFFECTIVE_NAV_DATE_AUDIT_JSON_PATTERN = "screening_effective_nav_date_audit_*.json"
DEFAULT_THRESHOLD_RATIOS = (0.5, 0.7, 0.8, 0.9)


def load_latest_screening_effective_nav_date_audit_payload(
    output_dir: str = DEFAULT_AUDIT_DIR,
) -> Tuple[Path, dict]:
    """读取最新有效净值日期候选审计产物。"""
    return load_latest_json_artifact(
        SCREENING_EFFECTIVE_NAV_DATE_AUDIT_JSON_PATTERN,
        output_dir=output_dir,
    )


def _normalize_thresholds(thresholds: Optional[Sequence[float]]) -> List[float]:
    normalized: List[float] = []
    for value in thresholds or DEFAULT_THRESHOLD_RATIOS:
        try:
            ratio = float(value)
        except (TypeError, ValueError):
            continue
        if 0 < ratio <= 1:
            normalized.append(ratio)
    return sorted(set(normalized))


def _fetch_scalar(conn, sql: str, params: Sequence[object] | None = None):
    row = conn.execute(sql, params or []).fetchone()
    if row is None:
        return None
    return row[0]


def _fetch_latest_nav_distribution(conn, *, limit: int = 20) -> List[dict]:
    sql = """
        SELECT latest_nav_date, COUNT(*) AS cnt
        FROM fund_returns
        GROUP BY latest_nav_date
        ORDER BY latest_nav_date DESC
        LIMIT ?
    """
    return [
        {
            "latest_nav_date": row["latest_nav_date"],
            "count": int(row["cnt"] or 0),
        }
        for row in conn.execute(sql, [int(limit)]).fetchall()
    ]


def _fetch_recent_nav_history_distribution(conn, *, limit: int = 20) -> List[dict]:
    sql = """
        SELECT nav_date, COUNT(*) AS cnt
        FROM fund_nav_history
        GROUP BY nav_date
        ORDER BY nav_date DESC
        LIMIT ?
    """
    return [
        {
            "nav_date": row["nav_date"],
            "count": int(row["cnt"] or 0),
        }
        for row in conn.execute(sql, [int(limit)]).fetchall()
    ]


def _weekday_label(date_text: Optional[str]) -> Optional[str]:
    if not date_text:
        return None
    try:
        dt = datetime.strptime(str(date_text), "%Y-%m-%d")
    except ValueError:
        return None
    return ["周一", "周二", "周三", "周四", "周五", "周六", "周日"][dt.weekday()]


def _compute_threshold_candidates(
    distribution: Sequence[dict],
    *,
    total_funds: int,
    non_null_count: int,
    thresholds: Sequence[float],
) -> List[dict]:
    candidates: List[dict] = []
    for ratio in thresholds:
        threshold_by_total = total_funds * ratio if total_funds > 0 else None
        threshold_by_non_null = non_null_count * ratio if non_null_count > 0 else None

        candidate_total = None
        candidate_non_null = None
        for row in distribution:
            count = int(row.get("count") or 0)
            latest_nav_date = row.get("latest_nav_date")
            if candidate_total is None and threshold_by_total is not None and count >= threshold_by_total:
                candidate_total = latest_nav_date
            if candidate_non_null is None and threshold_by_non_null is not None and count >= threshold_by_non_null:
                candidate_non_null = latest_nav_date
            if candidate_total is not None and candidate_non_null is not None:
                break

        candidates.append({
            "threshold_ratio": ratio,
            "threshold_pct": round(ratio * 100, 2),
            "candidate_by_total_funds": candidate_total,
            "candidate_by_non_null_latest_nav": candidate_non_null,
        })
    return candidates


def _find_distribution_entry(distribution: Sequence[dict], target_date: Optional[str], key: str) -> Optional[dict]:
    if not target_date:
        return None
    for row in distribution:
        if str(row.get(key) or "") == str(target_date):
            return row
    return None


def _build_summary(
    *,
    total_funds: int,
    fund_returns_total: int,
    non_null_latest_nav_count: int,
    raw_nav_history_max_date: Optional[str],
    latest_nav_distribution: Sequence[dict],
    recent_nav_history_distribution: Sequence[dict],
    threshold_candidates: Sequence[dict],
) -> dict:
    dominant_returns_entry = max(latest_nav_distribution, key=lambda row: int(row.get("count") or 0), default=None)
    raw_max_entry = _find_distribution_entry(latest_nav_distribution, raw_nav_history_max_date, "latest_nav_date")
    weekend_outlier_count = sum(
        int(row.get("count") or 0)
        for row in recent_nav_history_distribution
        if (_weekday_label(row.get("nav_date")) or "").startswith("周六")
        or (_weekday_label(row.get("nav_date")) or "").startswith("周日")
    )

    return {
        "total_funds": int(total_funds or 0),
        "fund_returns_total": int(fund_returns_total or 0),
        "non_null_latest_nav_count": int(non_null_latest_nav_count or 0),
        "raw_nav_history_max_date": raw_nav_history_max_date,
        "raw_nav_history_max_date_weekday": _weekday_label(raw_nav_history_max_date),
        "raw_nav_history_max_date_fund_returns_count": int((raw_max_entry or {}).get("count") or 0),
        "raw_nav_history_max_date_coverage_vs_total_funds": round(
            (int((raw_max_entry or {}).get("count") or 0) / total_funds), 6
        ) if total_funds else None,
        "raw_nav_history_max_date_coverage_vs_non_null_latest_nav": round(
            (int((raw_max_entry or {}).get("count") or 0) / non_null_latest_nav_count), 6
        ) if non_null_latest_nav_count else None,
        "dominant_fund_returns_latest_nav_date": (dominant_returns_entry or {}).get("latest_nav_date"),
        "dominant_fund_returns_latest_nav_count": int((dominant_returns_entry or {}).get("count") or 0),
        "dominant_fund_returns_latest_nav_weekday": _weekday_label((dominant_returns_entry or {}).get("latest_nav_date")),
        "weekend_outlier_nav_history_row_count_recent": weekend_outlier_count,
        "threshold_candidate_count": len(list(threshold_candidates)),
    }


def _build_findings(summary: dict, threshold_candidates: Sequence[dict]) -> List[str]:
    findings: List[str] = []
    raw_date = summary.get("raw_nav_history_max_date")
    raw_cov = summary.get("raw_nav_history_max_date_coverage_vs_total_funds")
    dominant_date = summary.get("dominant_fund_returns_latest_nav_date")
    weekend_outlier_count = int(summary.get("weekend_outlier_nav_history_row_count_recent") or 0)

    if raw_date and raw_cov is not None:
        findings.append(
            f"fund_nav_history 原始最大日期为 {raw_date}，但它在 fund_returns.latest_nav_date 中的覆盖率仅为 {raw_cov:.2%}（相对于全部基金）。"
        )
    if dominant_date and raw_date and dominant_date != raw_date:
        findings.append(
            f"fund_returns 主导最新净值日期为 {dominant_date}，与原始 nav_history 最大日期 {raw_date} 不一致，说明不能只看原始 MAX(nav_date)。"
        )
    if weekend_outlier_count > 0:
        findings.append(
            f"最近 nav_history 分布中检测到周末日期记录，共 {weekend_outlier_count} 行，后续设计规范化净值日期时需要显式处理周末离群值。"
        )

    candidate_80 = next((item for item in threshold_candidates if abs(float(item.get("threshold_ratio") or 0) - 0.8) < 1e-9), None)
    if candidate_80:
        findings.append(
            "80% 覆盖率候选日期："
            f"按全部基金={candidate_80.get('candidate_by_total_funds')}，"
            f"按非空 latest_nav={candidate_80.get('candidate_by_non_null_latest_nav')}。"
        )

    if not findings:
        findings.append("当前窗口内未发现足以支撑有效净值日期归一化判断的明显信号。")
    return findings


def build_screening_effective_nav_date_audit_payload(
    *,
    db_path: Optional[str] = None,
    thresholds: Optional[Sequence[float]] = None,
    distribution_limit: int = 20,
) -> Dict[str, object]:
    """构建有效净值日期候选审计结果。"""
    normalized_thresholds = _normalize_thresholds(thresholds)

    with open_readonly_db(db_path) as conn:
        total_funds = int(_fetch_scalar(conn, "SELECT COUNT(*) FROM funds") or 0)
        fund_returns_total = int(_fetch_scalar(conn, "SELECT COUNT(*) FROM fund_returns") or 0)
        non_null_latest_nav_count = int(_fetch_scalar(conn, "SELECT COUNT(*) FROM fund_returns WHERE latest_nav_date IS NOT NULL") or 0)
        raw_nav_history_max_date = _fetch_scalar(conn, "SELECT MAX(nav_date) FROM fund_nav_history")
        latest_nav_distribution = _fetch_latest_nav_distribution(conn, limit=distribution_limit)
        recent_nav_history_distribution = _fetch_recent_nav_history_distribution(conn, limit=distribution_limit)

    threshold_candidates = _compute_threshold_candidates(
        latest_nav_distribution,
        total_funds=total_funds,
        non_null_count=non_null_latest_nav_count,
        thresholds=normalized_thresholds,
    )
    summary = _build_summary(
        total_funds=total_funds,
        fund_returns_total=fund_returns_total,
        non_null_latest_nav_count=non_null_latest_nav_count,
        raw_nav_history_max_date=raw_nav_history_max_date,
        latest_nav_distribution=latest_nav_distribution,
        recent_nav_history_distribution=recent_nav_history_distribution,
        threshold_candidates=threshold_candidates,
    )

    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "db_path": resolve_db_path(db_path or "database/bond_funds.db"),
        "meta": {
            "thresholds": normalized_thresholds,
            "distribution_limit": int(distribution_limit),
        },
        "summary": summary,
        "findings": _build_findings(summary, threshold_candidates),
        "fund_returns_latest_nav_distribution": latest_nav_distribution,
        "fund_nav_history_recent_distribution": recent_nav_history_distribution,
        "threshold_candidates": threshold_candidates,
    }


def render_screening_effective_nav_date_audit_markdown(payload: Dict[str, object]) -> str:
    summary = payload.get("summary") or {}
    lines = [
        "# screening effective nav date audit",
        "",
        f"- generated_at: `{payload.get('generated_at')}`",
        f"- db_path: `{payload.get('db_path')}`",
        f"- thresholds: `{json.dumps((payload.get('meta') or {}).get('thresholds', []), ensure_ascii=False)}`",
        f"- distribution_limit: `{(payload.get('meta') or {}).get('distribution_limit')}`",
        "",
        "## Summary",
        f"- total_funds: `{summary.get('total_funds', 0)}`",
        f"- fund_returns_total: `{summary.get('fund_returns_total', 0)}`",
        f"- non_null_latest_nav_count: `{summary.get('non_null_latest_nav_count', 0)}`",
        f"- raw_nav_history_max_date: `{summary.get('raw_nav_history_max_date')}`",
        f"- raw_nav_history_max_date_weekday: `{summary.get('raw_nav_history_max_date_weekday')}`",
        f"- raw_nav_history_max_date_coverage_vs_total_funds: `{summary.get('raw_nav_history_max_date_coverage_vs_total_funds')}`",
        f"- dominant_fund_returns_latest_nav_date: `{summary.get('dominant_fund_returns_latest_nav_date')}`",
        f"- dominant_fund_returns_latest_nav_count: `{summary.get('dominant_fund_returns_latest_nav_count')}`",
        f"- weekend_outlier_nav_history_row_count_recent: `{summary.get('weekend_outlier_nav_history_row_count_recent')}`",
        "",
        "## Findings",
    ]

    for finding in payload.get("findings") or []:
        lines.append(f"- {finding}")

    lines.extend(["", "## Threshold Candidates"])
    for row in payload.get("threshold_candidates") or []:
        lines.append("- " + json.dumps(row, ensure_ascii=False, sort_keys=True))

    lines.extend(["", "## fund_returns.latest_nav_date Distribution"])
    for row in payload.get("fund_returns_latest_nav_distribution") or []:
        lines.append("- " + json.dumps(row, ensure_ascii=False, sort_keys=True))

    lines.extend(["", "## fund_nav_history Recent Distribution"])
    for row in payload.get("fund_nav_history_recent_distribution") or []:
        lines.append("- " + json.dumps(row, ensure_ascii=False, sort_keys=True))

    return "\n".join(lines) + "\n"


def write_screening_effective_nav_date_audit_artifacts(
    payload: Dict[str, object],
    *,
    output_dir: str | Path = DEFAULT_AUDIT_DIR,
    timestamp: str | None = None,
) -> Tuple[Path, Path]:
    """写出 JSON / Markdown 审计产物。"""
    out_dir = Path(output_dir)
    if not out_dir.is_absolute():
        out_dir = (Path.cwd() / out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    suffix = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    json_path = out_dir / f"screening_effective_nav_date_audit_{suffix}.json"
    md_path = out_dir / f"screening_effective_nav_date_audit_{suffix}.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(render_screening_effective_nav_date_audit_markdown(payload), encoding="utf-8")
    return json_path, md_path


__all__ = [
    "build_screening_effective_nav_date_audit_payload",
    "load_latest_screening_effective_nav_date_audit_payload",
    "render_screening_effective_nav_date_audit_markdown",
    "write_screening_effective_nav_date_audit_artifacts",
]
