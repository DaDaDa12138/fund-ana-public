#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
筛选规范化净值日期灰度决策报告工具。

目标：
1. 汇总多个审计窗口（默认 7/14/30 天）的候选规则审计结果；
2. 从“单窗口证据”提升到“跨窗口灰度决策”；
3. 明确哪些规则只能继续观察，哪些规则连 shadow gray 都不应进入。
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Mapping, Optional, Sequence, Tuple

from utils.fund_code_alias_audit import DEFAULT_AUDIT_DIR, load_latest_json_artifact
from utils.screening_normalized_nav_candidate_audit import (
    build_screening_normalized_nav_candidate_audit_payload,
)


SCREENING_NORMALIZED_NAV_GRAY_REPORT_JSON_PATTERN = "screening_normalized_nav_gray_report_*.json"
DEFAULT_DAYS_LIST = (7, 14, 30)
DEFAULT_GRAY_REPORT_FUND_TYPES = ("bond", "stock", "hybrid")
DEFAULT_GRAY_REPORT_THRESHOLDS = (0.5, 0.7, 0.8, 0.85, 0.9)
DEFAULT_GRAY_REPORT_MAX_RUNS_BY_WINDOW = {
    7: 400,
    14: 400,
    30: 2000,
}


def load_latest_screening_normalized_nav_gray_report_payload(
    output_dir: str = DEFAULT_AUDIT_DIR,
) -> Tuple[Path, dict]:
    """读取最新规范化净值日期灰度决策报告。"""
    return load_latest_json_artifact(
        SCREENING_NORMALIZED_NAV_GRAY_REPORT_JSON_PATTERN,
        output_dir=output_dir,
    )


def _normalize_days_list(days_list: Optional[Sequence[int]]) -> List[int]:
    normalized: List[int] = []
    for value in days_list or DEFAULT_DAYS_LIST:
        try:
            days = int(value)
        except (TypeError, ValueError):
            continue
        if days > 0 and days not in normalized:
            normalized.append(days)
    return normalized


def _normalize_max_runs_by_window(
    max_runs_by_window: Optional[Mapping[int, int] | Mapping[str, int]],
) -> Dict[int, int]:
    normalized: Dict[int, int] = {}
    for raw_key, raw_value in (max_runs_by_window or {}).items():
        try:
            key = int(raw_key)
            value = int(raw_value)
        except (TypeError, ValueError):
            continue
        if key > 0 and value > 0:
            normalized[key] = value
    return normalized


def _rule_window_status(window_row: dict) -> dict:
    false_skip_pair_count = int(window_row.get("false_skip_pair_count") or 0)
    lead_count = int(window_row.get("candidate_lead_actual_nav_count") or 0)
    skip_signal_available = bool(window_row.get("skip_signal_available"))

    if not skip_signal_available:
        return {
            "status": "no_skip_signal",
            "status_label": "无跳筛信号",
        }
    if false_skip_pair_count > 0:
        return {
            "status": "false_skip_detected",
            "status_label": "发现 false skip",
        }
    if lead_count > 0:
        return {
            "status": "lead_detected",
            "status_label": "候选日期领先实际净值日期",
        }
    return {
        "status": "clean_candidate",
        "status_label": "窗口内满足 shadow gray 基线",
    }


def _build_rule_decision(window_results: Sequence[dict], total_windows: int) -> Tuple[str, str]:
    skip_signal_windows = [row for row in window_results if row.get("skip_signal_available")]
    false_skip_windows = [row for row in window_results if int(row.get("false_skip_pair_count") or 0) > 0]
    lead_windows = [row for row in window_results if int(row.get("candidate_lead_actual_nav_count") or 0) > 0]

    if not skip_signal_windows:
        return "not_ready", "所有窗口都没有形成可用 skip 信号。"

    if false_skip_windows:
        days_text = ",".join(str(row.get("days")) for row in false_skip_windows)
        total_false_skip = sum(int(row.get("false_skip_pair_count") or 0) for row in false_skip_windows)
        return "not_ready", f"窗口 {days_text} 仍存在 false skip，累计 {total_false_skip} 对。"

    observe_reasons: List[str] = []
    if len(skip_signal_windows) < int(total_windows):
        days_text = ",".join(str(row.get("days")) for row in skip_signal_windows)
        observe_reasons.append(f"仅窗口 {days_text} 形成 skip 信号，跨窗口证据不足。")

    if lead_windows:
        days_text = ",".join(str(row.get("days")) for row in lead_windows)
        lead_total = sum(int(row.get("candidate_lead_actual_nav_count") or 0) for row in lead_windows)
        observe_reasons.append(f"窗口 {days_text} 出现 candidate lead actual，累计 {lead_total} 次。")

    if observe_reasons:
        return "observe_only", "；".join(observe_reasons)

    return "can_enter_shadow_gray", "所有窗口 false skip = 0，且未出现 candidate lead actual。"


def _aggregate_rule_decisions(window_payloads: Sequence[dict]) -> List[dict]:
    aggregated: Dict[str, dict] = {}
    total_windows = len(window_payloads)

    for payload in window_payloads:
        days = int(payload.get("days") or 0)
        window_summary = payload.get("summary") or {}
        comparison_count_total = int(window_summary.get("comparison_count_total") or 0)

        for row in payload.get("rule_results") or []:
            rule_key = str(row.get("rule_key") or "").strip()
            if not rule_key:
                continue

            alignment = row.get("alignment") or {}
            window_row = {
                "days": days,
                "comparison_count_total": comparison_count_total,
                "skip_signal_available": bool(row.get("skip_signal_available")),
                "candidate_same_pair_count": int(row.get("candidate_same_pair_count") or 0),
                "candidate_changed_pair_count": int(row.get("candidate_changed_pair_count") or 0),
                "false_skip_pair_count": int(row.get("false_skip_pair_count") or 0),
                "unchanged_pick_skip_pair_count": int(row.get("unchanged_pick_skip_pair_count") or 0),
                "skip_pair_ratio": row.get("skip_pair_ratio"),
                "false_skip_ratio_within_skip_pairs": row.get("false_skip_ratio_within_skip_pairs"),
                "candidate_match_actual_nav_count": int(alignment.get("candidate_match_actual_nav_count") or 0),
                "candidate_lag_actual_nav_count": int(alignment.get("candidate_lag_actual_nav_count") or 0),
                "candidate_lead_actual_nav_count": int(alignment.get("candidate_lead_actual_nav_count") or 0),
                "candidate_missing_actual_nav_count": int(alignment.get("candidate_missing_actual_nav_count") or 0),
            }
            window_row.update(_rule_window_status(window_row))

            entry = aggregated.setdefault(
                rule_key,
                {
                    "rule_key": rule_key,
                    "rule_label": row.get("rule_label"),
                    "rule_description": row.get("rule_description"),
                    "min_coverage_ratio": row.get("min_coverage_ratio"),
                    "weekdays_only": bool(row.get("weekdays_only")),
                    "window_results": [],
                },
            )
            entry["window_results"].append(window_row)

    aggregated_rows: List[dict] = []
    for row in aggregated.values():
        window_results = sorted(
            row.get("window_results") or [],
            key=lambda item: int(item.get("days") or 0),
        )
        decision, decision_reason = _build_rule_decision(window_results, total_windows=total_windows)
        aggregated_rows.append({
            **row,
            "window_count_total": len(window_results),
            "window_count_with_skip_signal": sum(1 for item in window_results if item.get("skip_signal_available")),
            "false_skip_window_count": sum(1 for item in window_results if int(item.get("false_skip_pair_count") or 0) > 0),
            "candidate_same_pair_count_total": sum(int(item.get("candidate_same_pair_count") or 0) for item in window_results),
            "false_skip_pair_count_total": sum(int(item.get("false_skip_pair_count") or 0) for item in window_results),
            "candidate_lead_actual_nav_count_total": sum(int(item.get("candidate_lead_actual_nav_count") or 0) for item in window_results),
            "candidate_lag_actual_nav_count_total": sum(int(item.get("candidate_lag_actual_nav_count") or 0) for item in window_results),
            "decision": decision,
            "decision_reason": decision_reason,
            "window_results": window_results,
        })

    decision_order = {
        "can_enter_shadow_gray": 0,
        "observe_only": 1,
        "not_ready": 2,
    }
    return sorted(
        aggregated_rows,
        key=lambda item: (
            decision_order.get(str(item.get("decision") or ""), 9),
            int(item.get("false_skip_pair_count_total") or 0),
            int(item.get("candidate_lead_actual_nav_count_total") or 0),
            -int(item.get("window_count_with_skip_signal") or 0),
            str(item.get("rule_key") or ""),
        ),
    )


def _build_summary(window_payloads: Sequence[dict], rule_decisions: Sequence[dict]) -> dict:
    return {
        "window_count_total": len(window_payloads),
        "rule_count_total": len(rule_decisions),
        "not_ready_rule_count": sum(1 for row in rule_decisions if row.get("decision") == "not_ready"),
        "observe_only_rule_count": sum(1 for row in rule_decisions if row.get("decision") == "observe_only"),
        "can_enter_shadow_gray_rule_count": sum(
            1 for row in rule_decisions if row.get("decision") == "can_enter_shadow_gray"
        ),
        "window_days": [int(row.get("days") or 0) for row in window_payloads],
    }


def _build_findings(summary: dict, rule_decisions: Sequence[dict]) -> List[str]:
    if not rule_decisions:
        return ["没有可用的窗口审计结果，无法生成灰度决策报告。"]

    findings: List[str] = []
    can_enter_shadow_gray = [row for row in rule_decisions if row.get("decision") == "can_enter_shadow_gray"]
    observe_only = [row for row in rule_decisions if row.get("decision") == "observe_only"]

    if can_enter_shadow_gray:
        labels = ", ".join(str(row.get("rule_key")) for row in can_enter_shadow_gray[:5])
        findings.append(
            f"共有 {len(can_enter_shadow_gray)} 条规则可进入 shadow gray：{labels}。"
        )
    else:
        findings.append("当前没有任何规则满足 shadow gray 准入条件，不能推进到生产跳筛灰度。")

    if observe_only:
        labels = ", ".join(str(row.get("rule_key")) for row in observe_only[:5])
        findings.append(
            f"共有 {len(observe_only)} 条规则只能继续观察，原因通常是跨窗口证据不足或仍存在 candidate lead：{labels}。"
        )

    strongest = rule_decisions[0]
    findings.append(
        "当前综合排序最靠前的规则为 "
        f"{strongest.get('rule_key')}，decision={strongest.get('decision')}，"
        f"false_skip_pair_count_total={strongest.get('false_skip_pair_count_total')}，"
        f"candidate_lead_actual_nav_count_total={strongest.get('candidate_lead_actual_nav_count_total')}。"
    )
    return findings


def build_screening_normalized_nav_gray_report_payload(
    *,
    db_path: Optional[str] = None,
    days_list: Optional[Sequence[int]] = None,
    fund_types: Optional[Sequence[str]] = None,
    max_runs: Optional[int] = None,
    max_runs_by_window: Optional[Mapping[int, int] | Mapping[str, int]] = None,
    thresholds: Optional[Sequence[float]] = None,
    distribution_limit: int = 10,
) -> Dict[str, object]:
    """构建规范化净值日期灰度决策报告。"""
    normalized_days_list = _normalize_days_list(days_list)
    normalized_max_runs_by_window = _normalize_max_runs_by_window(max_runs_by_window)

    window_payloads: List[dict] = []
    for days in normalized_days_list:
        window_max_runs = normalized_max_runs_by_window.get(days, max_runs)
        payload = build_screening_normalized_nav_candidate_audit_payload(
            db_path=db_path,
            days=days,
            fund_types=fund_types,
            max_runs=window_max_runs,
            thresholds=thresholds,
            distribution_limit=distribution_limit,
        )
        window_payloads.append({
            "days": days,
            "max_runs": int(window_max_runs) if window_max_runs is not None else None,
            "summary": payload.get("summary") or {},
            "findings": payload.get("findings") or [],
            "rule_results": payload.get("rule_results") or [],
        })

    rule_decisions = _aggregate_rule_decisions(window_payloads)
    summary = _build_summary(window_payloads, rule_decisions)
    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "meta": {
            "days_list": normalized_days_list,
            "fund_types": [str(item).strip() for item in (fund_types or []) if str(item).strip()],
            "max_runs": int(max_runs) if max_runs is not None else None,
            "max_runs_by_window": {str(key): value for key, value in normalized_max_runs_by_window.items()},
            "thresholds": [float(value) for value in (thresholds or [])],
            "distribution_limit": int(distribution_limit),
        },
        "summary": summary,
        "findings": _build_findings(summary, rule_decisions),
        "window_summaries": window_payloads,
        "rule_decisions": rule_decisions,
    }


def render_screening_normalized_nav_gray_report_markdown(payload: Dict[str, object]) -> str:
    summary = payload.get("summary") or {}
    lines = [
        "# screening normalized nav gray report",
        "",
        f"- generated_at: `{payload.get('generated_at')}`",
        f"- days_list: `{json.dumps((payload.get('meta') or {}).get('days_list', []), ensure_ascii=False)}`",
        f"- fund_types: `{', '.join((payload.get('meta') or {}).get('fund_types') or ['all'])}`",
        f"- max_runs: `{(payload.get('meta') or {}).get('max_runs')}`",
        f"- max_runs_by_window: `{json.dumps((payload.get('meta') or {}).get('max_runs_by_window', {}), ensure_ascii=False, sort_keys=True)}`",
        "",
        "## Summary",
        f"- window_count_total: `{summary.get('window_count_total', 0)}`",
        f"- rule_count_total: `{summary.get('rule_count_total', 0)}`",
        f"- can_enter_shadow_gray_rule_count: `{summary.get('can_enter_shadow_gray_rule_count', 0)}`",
        f"- observe_only_rule_count: `{summary.get('observe_only_rule_count', 0)}`",
        f"- not_ready_rule_count: `{summary.get('not_ready_rule_count', 0)}`",
        "",
        "## Findings",
    ]

    for finding in payload.get("findings") or []:
        lines.append(f"- {finding}")

    lines.extend(["", "## Window Summaries"])
    for row in payload.get("window_summaries") or []:
        lines.append("- " + json.dumps(row, ensure_ascii=False, sort_keys=True))

    lines.extend(["", "## Rule Decisions"])
    for row in payload.get("rule_decisions") or []:
        lines.append("- " + json.dumps(row, ensure_ascii=False, sort_keys=True))

    return "\n".join(lines) + "\n"


def write_screening_normalized_nav_gray_report_artifacts(
    payload: Dict[str, object],
    *,
    output_dir: str | Path = DEFAULT_AUDIT_DIR,
    timestamp: str | None = None,
) -> Tuple[Path, Path]:
    """写出 JSON / Markdown 灰度决策报告。"""
    out_dir = Path(output_dir)
    if not out_dir.is_absolute():
        out_dir = (Path.cwd() / out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    suffix = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    json_path = out_dir / f"screening_normalized_nav_gray_report_{suffix}.json"
    md_path = out_dir / f"screening_normalized_nav_gray_report_{suffix}.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(render_screening_normalized_nav_gray_report_markdown(payload), encoding="utf-8")
    return json_path, md_path


__all__ = [
    "build_screening_normalized_nav_gray_report_payload",
    "DEFAULT_DAYS_LIST",
    "DEFAULT_GRAY_REPORT_FUND_TYPES",
    "DEFAULT_GRAY_REPORT_MAX_RUNS_BY_WINDOW",
    "DEFAULT_GRAY_REPORT_THRESHOLDS",
    "load_latest_screening_normalized_nav_gray_report_payload",
    "render_screening_normalized_nav_gray_report_markdown",
    "write_screening_normalized_nav_gray_report_artifacts",
]
