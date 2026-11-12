#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
筛选规范化净值日期候选规则对比审计工具。

目标：
1. 基于相邻成功筛选 run 的真实 picks 变化，评估不同候选规则如果被用作“数据未推进则跳过筛选”门禁，会不会误杀结果变化；
2. 基于 fund_nav_history 的历史日期覆盖度，为每个 cutoff_date 计算候选规范化净值日期；
3. 保持证据边界真实：不伪造历史 fund_returns.latest_nav_date 快照，不直接修改任何调度与筛选逻辑。
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

from database.connection import resolve_db_path
from utils.fund_code_alias_audit import DEFAULT_AUDIT_DIR, load_latest_json_artifact
from utils.screening_effective_nav_date_audit import (
    DEFAULT_THRESHOLD_RATIOS as DEFAULT_EFFECTIVE_NAV_THRESHOLD_RATIOS,
    build_screening_effective_nav_date_audit_payload,
)
from utils.screening_nav_gate_audit import build_screening_nav_gate_audit_payload
from utils.screening_storage_growth_audit import open_readonly_db


SCREENING_NORMALIZED_NAV_CANDIDATE_AUDIT_JSON_PATTERN = "screening_normalized_nav_candidate_audit_*.json"


@dataclass(frozen=True)
class CandidateRule:
    key: str
    label: str
    description: str
    min_coverage_ratio: Optional[float] = None
    weekdays_only: bool = False


def load_latest_screening_normalized_nav_candidate_audit_payload(
    output_dir: str = DEFAULT_AUDIT_DIR,
) -> Tuple[Path, dict]:
    """读取最新规范化净值日期候选规则审计产物。"""
    return load_latest_json_artifact(
        SCREENING_NORMALIZED_NAV_CANDIDATE_AUDIT_JSON_PATTERN,
        output_dir=output_dir,
    )


def _normalize_thresholds(thresholds: Optional[Sequence[float]]) -> List[float]:
    normalized: List[float] = []
    for value in thresholds or DEFAULT_EFFECTIVE_NAV_THRESHOLD_RATIOS:
        try:
            ratio = float(value)
        except (TypeError, ValueError):
            continue
        if 0 < ratio <= 1:
            normalized.append(ratio)
    return sorted(set(normalized))


def _format_threshold_pct_key(ratio: float) -> str:
    pct = round(float(ratio) * 100, 4)
    if abs(pct - round(pct)) < 1e-9:
        return str(int(round(pct)))
    return str(pct).replace(".", "_")


def _build_candidate_rules(thresholds: Sequence[float]) -> List[CandidateRule]:
    rules = [
        CandidateRule(
            key="raw_max_nav_date",
            label="原始最大净值日期",
            description="直接使用 cutoff_date 之前的最大 nav_date，不排除周末离群值。",
        ),
        CandidateRule(
            key="raw_max_nav_date_weekday_only",
            label="原始最大净值日期（仅工作日）",
            description="使用 cutoff_date 之前的最大工作日 nav_date，显式排除周末离群值。",
            weekdays_only=True,
        ),
    ]

    for ratio in thresholds:
        pct_key = _format_threshold_pct_key(ratio)
        pct_label = f"{round(float(ratio) * 100, 2):g}%"
        rules.append(
            CandidateRule(
                key=f"coverage_ge_{pct_key}_total_funds",
                label=f"覆盖率>={pct_label}（全部基金）",
                description=f"使用 cutoff_date 之前，fund_nav_history 行数达到全部基金 {pct_label} 覆盖率的最新日期。",
                min_coverage_ratio=ratio,
            )
        )
        rules.append(
            CandidateRule(
                key=f"coverage_ge_{pct_key}_total_funds_weekday_only",
                label=f"覆盖率>={pct_label}（全部基金，仅工作日）",
                description=f"使用 cutoff_date 之前、且排除周末离群值后，覆盖率达到全部基金 {pct_label} 的最新日期。",
                min_coverage_ratio=ratio,
                weekdays_only=True,
            )
        )

    return rules


def _weekday_label(date_text: Optional[str]) -> Optional[str]:
    if not date_text:
        return None
    try:
        dt = datetime.strptime(str(date_text), "%Y-%m-%d")
    except ValueError:
        return None
    return ["周一", "周二", "周三", "周四", "周五", "周六", "周日"][dt.weekday()]


def _is_weekend(date_text: Optional[str]) -> bool:
    label = _weekday_label(date_text) or ""
    return label in {"周六", "周日"}


def _fetch_scalar(conn, sql: str, params: Optional[Sequence[object]] = None):
    row = conn.execute(sql, params or []).fetchone()
    if row is None:
        return None
    return row[0]


def _fetch_nav_date_counts(conn) -> List[dict]:
    rows = conn.execute(
        """
        SELECT nav_date, COUNT(*) AS cnt
        FROM fund_nav_history
        GROUP BY nav_date
        ORDER BY nav_date DESC
        """
    ).fetchall()
    return [
        {
            "nav_date": row["nav_date"],
            "count": int(row["cnt"] or 0),
            "weekday": _weekday_label(row["nav_date"]),
            "is_weekend": _is_weekend(row["nav_date"]),
        }
        for row in rows
    ]


def _collect_run_records(comparisons: Sequence[dict]) -> Dict[str, dict]:
    run_map: Dict[str, dict] = {}
    for row in comparisons:
        prev_run_id = str(row.get("prev_run_id") or "").strip()
        curr_run_id = str(row.get("curr_run_id") or "").strip()
        if prev_run_id:
            run_map[prev_run_id] = {
                "run_id": prev_run_id,
                "run_day": row.get("prev_run_day"),
                "cutoff_date": row.get("prev_data_cutoff_date") or row.get("prev_run_day"),
                "actual_nav_latest_date": row.get("prev_nav_latest_date"),
                "fund_type": row.get("fund_type"),
                "period_years": row.get("period_years"),
            }
        if curr_run_id:
            run_map[curr_run_id] = {
                "run_id": curr_run_id,
                "run_day": row.get("curr_run_day"),
                "cutoff_date": row.get("curr_data_cutoff_date") or row.get("curr_run_day"),
                "actual_nav_latest_date": row.get("curr_nav_latest_date"),
                "fund_type": row.get("fund_type"),
                "period_years": row.get("period_years"),
            }
    return run_map


def _resolve_candidate_date(
    cutoff_date: Optional[str],
    *,
    rule: CandidateRule,
    nav_date_counts: Sequence[dict],
    total_funds: int,
) -> Optional[str]:
    normalized_cutoff = str(cutoff_date or "").strip()
    if not normalized_cutoff:
        return None

    coverage_threshold = None
    if rule.min_coverage_ratio is not None and total_funds > 0:
        coverage_threshold = float(total_funds) * float(rule.min_coverage_ratio)

    for row in nav_date_counts:
        nav_date = str(row.get("nav_date") or "").strip()
        if not nav_date or nav_date > normalized_cutoff:
            continue
        if rule.weekdays_only and bool(row.get("is_weekend")):
            continue
        if coverage_threshold is not None and float(row.get("count") or 0) < coverage_threshold:
            continue
        return nav_date
    return None


def _build_rule_run_candidates(
    *,
    rule: CandidateRule,
    runs_by_id: Dict[str, dict],
    nav_date_counts: Sequence[dict],
    total_funds: int,
) -> Dict[str, Optional[str]]:
    cache: Dict[str, Optional[str]] = {}
    candidates: Dict[str, Optional[str]] = {}
    for run_id, run in runs_by_id.items():
        cutoff_date = str(run.get("cutoff_date") or "").strip()
        cache_key = cutoff_date
        if cache_key not in cache:
            cache[cache_key] = _resolve_candidate_date(
                cutoff_date,
                rule=rule,
                nav_date_counts=nav_date_counts,
                total_funds=total_funds,
            )
        candidates[run_id] = cache[cache_key]
    return candidates


def _build_alignment_summary(
    *,
    runs_by_id: Dict[str, dict],
    candidates_by_run_id: Dict[str, Optional[str]],
) -> dict:
    match_count = 0
    lag_count = 0
    lead_count = 0
    missing_count = 0

    for run_id, run in runs_by_id.items():
        candidate_date = candidates_by_run_id.get(run_id)
        actual_date = str(run.get("actual_nav_latest_date") or "").strip() or None
        if not candidate_date or not actual_date:
            missing_count += 1
            continue
        if candidate_date == actual_date:
            match_count += 1
        elif candidate_date < actual_date:
            lag_count += 1
        else:
            lead_count += 1

    run_count_total = len(runs_by_id)
    return {
        "run_count_total": run_count_total,
        "candidate_match_actual_nav_count": match_count,
        "candidate_lag_actual_nav_count": lag_count,
        "candidate_lead_actual_nav_count": lead_count,
        "candidate_missing_actual_nav_count": missing_count,
        "candidate_match_actual_nav_ratio": round(match_count / run_count_total, 6) if run_count_total else None,
    }


def _evaluate_rule(
    *,
    rule: CandidateRule,
    comparisons: Sequence[dict],
    runs_by_id: Dict[str, dict],
    nav_date_counts: Sequence[dict],
    total_funds: int,
) -> dict:
    candidates_by_run_id = _build_rule_run_candidates(
        rule=rule,
        runs_by_id=runs_by_id,
        nav_date_counts=nav_date_counts,
        total_funds=total_funds,
    )
    alignment = _build_alignment_summary(
        runs_by_id=runs_by_id,
        candidates_by_run_id=candidates_by_run_id,
    )

    candidate_same_pair_count = 0
    candidate_changed_pair_count = 0
    candidate_missing_pair_count = 0
    candidate_advanced_pair_count = 0
    candidate_regressed_pair_count = 0
    false_skip_pair_count = 0
    unchanged_pick_skip_pair_count = 0
    false_skip_examples: List[dict] = []

    for row in comparisons:
        prev_run_id = str(row.get("prev_run_id") or "").strip()
        curr_run_id = str(row.get("curr_run_id") or "").strip()
        prev_candidate = candidates_by_run_id.get(prev_run_id)
        curr_candidate = candidates_by_run_id.get(curr_run_id)
        same_pick_set = bool(row.get("same_pick_set"))

        if not prev_candidate or not curr_candidate:
            candidate_missing_pair_count += 1
            continue

        if prev_candidate == curr_candidate:
            candidate_same_pair_count += 1
            if same_pick_set:
                unchanged_pick_skip_pair_count += 1
            else:
                false_skip_pair_count += 1
                false_skip_examples.append(
                    {
                        "fund_type": row.get("fund_type"),
                        "period_years": row.get("period_years"),
                        "prev_run_day": row.get("prev_run_day"),
                        "curr_run_day": row.get("curr_run_day"),
                        "prev_data_cutoff_date": row.get("prev_data_cutoff_date"),
                        "curr_data_cutoff_date": row.get("curr_data_cutoff_date"),
                        "prev_candidate_date": prev_candidate,
                        "curr_candidate_date": curr_candidate,
                        "prev_nav_latest_date": row.get("prev_nav_latest_date"),
                        "curr_nav_latest_date": row.get("curr_nav_latest_date"),
                        "changed_pick_total": int(row.get("changed_pick_total") or 0),
                    }
                )
        else:
            candidate_changed_pair_count += 1
            if curr_candidate > prev_candidate:
                candidate_advanced_pair_count += 1
            elif curr_candidate < prev_candidate:
                candidate_regressed_pair_count += 1

    false_skip_examples = sorted(
        false_skip_examples,
        key=lambda item: int(item.get("changed_pick_total") or 0),
        reverse=True,
    )[:5]

    comparison_count_total = len(comparisons)
    return {
        "rule_key": rule.key,
        "rule_label": rule.label,
        "rule_description": rule.description,
        "min_coverage_ratio": rule.min_coverage_ratio,
        "weekdays_only": rule.weekdays_only,
        "candidate_same_pair_count": candidate_same_pair_count,
        "candidate_changed_pair_count": candidate_changed_pair_count,
        "candidate_missing_pair_count": candidate_missing_pair_count,
        "candidate_advanced_pair_count": candidate_advanced_pair_count,
        "candidate_regressed_pair_count": candidate_regressed_pair_count,
        "false_skip_pair_count": false_skip_pair_count,
        "unchanged_pick_skip_pair_count": unchanged_pick_skip_pair_count,
        "skip_pair_ratio": round(candidate_same_pair_count / comparison_count_total, 6) if comparison_count_total else None,
        "false_skip_ratio_within_skip_pairs": round(false_skip_pair_count / candidate_same_pair_count, 6)
        if candidate_same_pair_count
        else None,
        "false_skip_free_in_current_window": candidate_same_pair_count > 0 and false_skip_pair_count == 0,
        "skip_signal_available": candidate_same_pair_count > 0,
        "alignment": alignment,
        "top_false_skip_examples": false_skip_examples,
    }


def _build_summary(
    *,
    comparisons: Sequence[dict],
    runs_by_id: Dict[str, dict],
    rule_results: Sequence[dict],
    total_funds: int,
) -> dict:
    cutoff_dates = sorted(
        {
            str(run.get("cutoff_date") or "").strip()
            for run in runs_by_id.values()
            if str(run.get("cutoff_date") or "").strip()
        }
    )
    false_skip_free_rule_count = sum(1 for row in rule_results if row.get("false_skip_free_in_current_window"))
    rules_with_skip_signal = sum(1 for row in rule_results if row.get("skip_signal_available"))
    rules_with_lead_count = sum(
        1
        for row in rule_results
        if int(((row.get("alignment") or {}).get("candidate_lead_actual_nav_count") or 0)) > 0
    )

    return {
        "total_funds": int(total_funds or 0),
        "comparison_count_total": len(comparisons),
        "run_count_total": len(runs_by_id),
        "rule_count_total": len(rule_results),
        "rules_with_skip_signal_count": rules_with_skip_signal,
        "false_skip_free_rule_count": false_skip_free_rule_count,
        "rules_with_candidate_lead_actual_nav_count": rules_with_lead_count,
        "cutoff_date_min": cutoff_dates[0] if cutoff_dates else None,
        "cutoff_date_max": cutoff_dates[-1] if cutoff_dates else None,
    }


def _build_findings(summary: dict, rule_results: Sequence[dict]) -> List[str]:
    if not rule_results:
        return ["最近审计窗口内没有足够的比较样本，暂时无法评估规范化净值日期候选规则。"]

    findings: List[str] = []
    rules_with_skip_signal = [row for row in rule_results if row.get("skip_signal_available")]
    if not rules_with_skip_signal:
        return ["最近审计窗口内所有候选规则都没有形成可用的“跳过筛选”信号，当前不应推进生产门禁。"]

    false_skip_free_rules = [row for row in rules_with_skip_signal if row.get("false_skip_free_in_current_window")]
    if false_skip_free_rules:
        labels = ", ".join(str(row.get("rule_key")) for row in false_skip_free_rules)
        findings.append(
            f"当前窗口内有 {len(false_skip_free_rules)} 条候选规则未观测到 false skip，可作为后续灰度验证候选，但仍不能直接替换生产逻辑：{labels}。"
        )
    else:
        findings.append("当前窗口内没有任何候选规则达到 false skip = 0，不能把规范化净值日期跳筛门禁直接上线。")

    strongest = sorted(
        rules_with_skip_signal,
        key=lambda row: (
            int(row.get("false_skip_pair_count") or 0),
            -int(row.get("candidate_same_pair_count") or 0),
            int(((row.get("alignment") or {}).get("candidate_lead_actual_nav_count") or 0)),
        ),
    )[0]
    findings.append(
        "当前相对最保守的规则为 "
        f"{strongest.get('rule_key')}：skip_pair_count={strongest.get('candidate_same_pair_count')}, "
        f"false_skip_pair_count={strongest.get('false_skip_pair_count')}, "
        f"candidate_lead_actual_nav_count={((strongest.get('alignment') or {}).get('candidate_lead_actual_nav_count') or 0)}。"
    )

    raw_max_rule = next((row for row in rule_results if row.get("rule_key") == "raw_max_nav_date"), None)
    if raw_max_rule and int(raw_max_rule.get("false_skip_pair_count") or 0) > 0:
        findings.append(
            "直接使用 raw_max_nav_date 作为跳筛门禁仍然不安全："
            f"false_skip_pair_count={raw_max_rule.get('false_skip_pair_count')}。"
        )

    weekday_rule = next((row for row in rule_results if row.get("rule_key") == "raw_max_nav_date_weekday_only"), None)
    if raw_max_rule and weekday_rule:
        raw_lead = int(((raw_max_rule.get("alignment") or {}).get("candidate_lead_actual_nav_count") or 0))
        weekday_lead = int(((weekday_rule.get("alignment") or {}).get("candidate_lead_actual_nav_count") or 0))
        if weekday_lead < raw_lead:
            findings.append(
                "仅工作日规则能减少候选日期领先实际 run.nav_latest_date 的次数，"
                f"lead_count {raw_lead} -> {weekday_lead}，说明周末离群值过滤是必要方向。"
            )

    return findings


def build_screening_normalized_nav_candidate_audit_payload(
    *,
    db_path: Optional[str] = None,
    days: int = 14,
    fund_types: Optional[Sequence[str]] = None,
    max_runs: Optional[int] = None,
    thresholds: Optional[Sequence[float]] = None,
    distribution_limit: int = 10,
) -> Dict[str, object]:
    """构建规范化净值日期候选规则对比审计结果。"""
    nav_gate_payload = build_screening_nav_gate_audit_payload(
        db_path=db_path,
        days=days,
        fund_types=fund_types,
        max_runs=max_runs,
    )
    comparisons = list(nav_gate_payload.get("comparisons") or [])
    runs_by_id = _collect_run_records(comparisons)
    normalized_thresholds = _normalize_thresholds(thresholds)
    rules = _build_candidate_rules(normalized_thresholds)

    with open_readonly_db(db_path) as conn:
        total_funds = int(_fetch_scalar(conn, "SELECT COUNT(*) FROM funds") or 0)
        nav_date_counts = _fetch_nav_date_counts(conn)

    rule_results = [
        _evaluate_rule(
            rule=rule,
            comparisons=comparisons,
            runs_by_id=runs_by_id,
            nav_date_counts=nav_date_counts,
            total_funds=total_funds,
        )
        for rule in rules
    ]
    summary = _build_summary(
        comparisons=comparisons,
        runs_by_id=runs_by_id,
        rule_results=rule_results,
        total_funds=total_funds,
    )
    current_effective_nav_snapshot = build_screening_effective_nav_date_audit_payload(
        db_path=db_path,
        thresholds=normalized_thresholds,
        distribution_limit=distribution_limit,
    )

    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "db_path": resolve_db_path(db_path or "database/bond_funds.db"),
        "meta": {
            "days": int(days),
            "fund_types": [str(item).strip() for item in (fund_types or []) if str(item).strip()],
            "max_runs": int(max_runs) if max_runs is not None else None,
            "thresholds": normalized_thresholds,
            "distribution_limit": int(distribution_limit),
            "comparison_basis": str((nav_gate_payload.get("meta") or {}).get("comparison_basis") or ""),
        },
        "summary": summary,
        "findings": _build_findings(summary, rule_results),
        "rule_results": rule_results,
        "nav_gate_reference_summary": nav_gate_payload.get("summary") or {},
        "current_effective_nav_snapshot": {
            "summary": current_effective_nav_snapshot.get("summary") or {},
            "threshold_candidates": current_effective_nav_snapshot.get("threshold_candidates") or [],
        },
    }


def render_screening_normalized_nav_candidate_audit_markdown(payload: Dict[str, object]) -> str:
    summary = payload.get("summary") or {}
    current_effective_nav_snapshot = payload.get("current_effective_nav_snapshot") or {}
    lines = [
        "# screening normalized nav candidate audit",
        "",
        f"- generated_at: `{payload.get('generated_at')}`",
        f"- db_path: `{payload.get('db_path')}`",
        f"- days: `{(payload.get('meta') or {}).get('days')}`",
        f"- fund_types: `{', '.join((payload.get('meta') or {}).get('fund_types') or ['all'])}`",
        f"- max_runs: `{(payload.get('meta') or {}).get('max_runs')}`",
        f"- thresholds: `{json.dumps((payload.get('meta') or {}).get('thresholds', []), ensure_ascii=False)}`",
        "",
        "## Summary",
        f"- total_funds: `{summary.get('total_funds', 0)}`",
        f"- comparison_count_total: `{summary.get('comparison_count_total', 0)}`",
        f"- run_count_total: `{summary.get('run_count_total', 0)}`",
        f"- rule_count_total: `{summary.get('rule_count_total', 0)}`",
        f"- rules_with_skip_signal_count: `{summary.get('rules_with_skip_signal_count', 0)}`",
        f"- false_skip_free_rule_count: `{summary.get('false_skip_free_rule_count', 0)}`",
        f"- rules_with_candidate_lead_actual_nav_count: `{summary.get('rules_with_candidate_lead_actual_nav_count', 0)}`",
        f"- cutoff_date_min: `{summary.get('cutoff_date_min')}`",
        f"- cutoff_date_max: `{summary.get('cutoff_date_max')}`",
        "",
        "## Findings",
    ]

    for finding in payload.get("findings") or []:
        lines.append(f"- {finding}")

    lines.extend(["", "## Rule Results"])
    for row in payload.get("rule_results") or []:
        lines.append("- " + json.dumps(row, ensure_ascii=False, sort_keys=True))

    lines.extend(["", "## Nav Gate Reference Summary"])
    lines.append("- " + json.dumps(payload.get("nav_gate_reference_summary") or {}, ensure_ascii=False, sort_keys=True))

    lines.extend(["", "## Current Effective Nav Snapshot Summary"])
    lines.append("- " + json.dumps(current_effective_nav_snapshot.get("summary") or {}, ensure_ascii=False, sort_keys=True))

    lines.extend(["", "## Current Effective Nav Threshold Candidates"])
    for row in current_effective_nav_snapshot.get("threshold_candidates") or []:
        lines.append("- " + json.dumps(row, ensure_ascii=False, sort_keys=True))

    return "\n".join(lines) + "\n"


def write_screening_normalized_nav_candidate_audit_artifacts(
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
    json_path = out_dir / f"screening_normalized_nav_candidate_audit_{suffix}.json"
    md_path = out_dir / f"screening_normalized_nav_candidate_audit_{suffix}.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(render_screening_normalized_nav_candidate_audit_markdown(payload), encoding="utf-8")
    return json_path, md_path


__all__ = [
    "build_screening_normalized_nav_candidate_audit_payload",
    "load_latest_screening_normalized_nav_candidate_audit_payload",
    "render_screening_normalized_nav_candidate_audit_markdown",
    "write_screening_normalized_nav_candidate_audit_artifacts",
]
