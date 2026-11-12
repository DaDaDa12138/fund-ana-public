#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
筛选净值门禁审计工具。

目标：
1. 只读扫描最近 N 天 screening_runs / screening_picks；
2. 对比相邻交易日同 fund_type + period_years 的最新成功 run；
3. 判断“nav_latest_date 未推进时，最终 picks 是否仍会变化”；
4. 为后续是否引入“净值未推进则跳过筛选”提供证据，而不是直接拍脑袋上线。
"""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from database.connection import resolve_db_path
from utils.fund_code_alias_audit import DEFAULT_AUDIT_DIR, load_latest_json_artifact
from utils.screening_storage_growth_audit import open_readonly_db


SCREENING_NAV_GATE_AUDIT_JSON_PATTERN = "screening_nav_gate_audit_*.json"


def load_latest_screening_nav_gate_audit_payload(
    output_dir: str = DEFAULT_AUDIT_DIR,
) -> Tuple[Path, dict]:
    """读取最新筛选净值门禁审计产物。"""
    return load_latest_json_artifact(
        SCREENING_NAV_GATE_AUDIT_JSON_PATTERN,
        output_dir=output_dir,
    )


def _fetch_recent_success_runs(
    conn,
    *,
    days: int,
    fund_types: Optional[Sequence[str]],
    max_runs: Optional[int],
) -> List[dict]:
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
    sql = """
        SELECT
            run_id,
            started_at,
            finished_at,
            status,
            fund_type,
            period_years,
            data_cutoff_date,
            nav_latest_date
        FROM screening_runs
        WHERE status = 'success'
          AND started_at >= ?
    """
    params: List[object] = [cutoff]

    normalized_fund_types = [str(item).strip() for item in (fund_types or []) if str(item).strip()]
    if normalized_fund_types:
        placeholders = ",".join("?" for _ in normalized_fund_types)
        sql += f" AND fund_type IN ({placeholders})"
        params.extend(normalized_fund_types)

    sql += " ORDER BY started_at DESC"
    if max_runs is not None and int(max_runs) > 0:
        sql += " LIMIT ?"
        params.append(int(max_runs))

    return [dict(row) for row in conn.execute(sql, params).fetchall()]


def _pick_latest_run_per_day(run_rows: Sequence[dict]) -> List[dict]:
    """同一日同类型同周期只保留最新一条成功 run。"""
    latest_by_key: Dict[Tuple[str, str, str], dict] = {}
    for row in run_rows:
        run_day = str(row.get("started_at") or "")[:10]
        fund_type = str(row.get("fund_type") or "unknown")
        period_key = str(row.get("period_years"))
        if not run_day:
            continue
        key = (run_day, fund_type, period_key)
        if key not in latest_by_key:
            latest_by_key[key] = {
                **row,
                "run_day": run_day,
            }
    return sorted(
        latest_by_key.values(),
        key=lambda item: (
            str(item.get("fund_type") or ""),
            float(item.get("period_years") or 0),
            str(item.get("run_day") or ""),
        ),
    )


def _fetch_pick_sets(conn, run_ids: Iterable[str], chunk_size: int = 500) -> Dict[str, set]:
    pick_sets: Dict[str, set] = defaultdict(set)
    normalized_run_ids = [str(run_id).strip() for run_id in run_ids if str(run_id).strip()]
    if not normalized_run_ids:
        return {}

    for i in range(0, len(normalized_run_ids), max(1, int(chunk_size))):
        chunk = normalized_run_ids[i:i + max(1, int(chunk_size))]
        placeholders = ",".join("?" for _ in chunk)
        sql = f"""
            SELECT run_id, fcode
            FROM screening_picks
            WHERE run_id IN ({placeholders})
        """
        for row in conn.execute(sql, chunk).fetchall():
            pick_sets[str(row["run_id"])].add(str(row["fcode"]).strip())
    return dict(pick_sets)


def _build_comparisons(daily_runs: Sequence[dict], pick_sets: Dict[str, set]) -> List[dict]:
    grouped: Dict[Tuple[str, str], List[dict]] = defaultdict(list)
    for row in daily_runs:
        key = (
            str(row.get("fund_type") or "unknown"),
            str(row.get("period_years")),
        )
        grouped[key].append(row)

    comparisons: List[dict] = []
    for (fund_type, period_key), rows in grouped.items():
        ordered = sorted(rows, key=lambda item: str(item.get("run_day") or ""))
        for prev, curr in zip(ordered, ordered[1:]):
            prev_run_id = str(prev.get("run_id") or "")
            curr_run_id = str(curr.get("run_id") or "")
            prev_codes = set(pick_sets.get(prev_run_id, set()))
            curr_codes = set(pick_sets.get(curr_run_id, set()))
            same_nav = str(prev.get("nav_latest_date") or "") == str(curr.get("nav_latest_date") or "")
            only_prev = prev_codes - curr_codes
            only_curr = curr_codes - prev_codes
            comparisons.append({
                "fund_type": fund_type,
                "period_years": prev.get("period_years"),
                "prev_run_day": prev.get("run_day"),
                "curr_run_day": curr.get("run_day"),
                "prev_run_id": prev_run_id,
                "curr_run_id": curr_run_id,
                "prev_nav_latest_date": prev.get("nav_latest_date"),
                "curr_nav_latest_date": curr.get("nav_latest_date"),
                "prev_data_cutoff_date": prev.get("data_cutoff_date"),
                "curr_data_cutoff_date": curr.get("data_cutoff_date"),
                "same_nav_latest_date": same_nav,
                "same_pick_set": prev_codes == curr_codes,
                "prev_pick_count": len(prev_codes),
                "curr_pick_count": len(curr_codes),
                "only_prev_count": len(only_prev),
                "only_curr_count": len(only_curr),
                "changed_pick_total": len(only_prev) + len(only_curr),
            })
    return comparisons


def _build_summary(comparisons: Sequence[dict]) -> dict:
    same_nav_pairs = [item for item in comparisons if item.get("same_nav_latest_date")]
    changed_when_same_nav = [item for item in same_nav_pairs if not item.get("same_pick_set")]
    unchanged_when_same_nav = [item for item in same_nav_pairs if item.get("same_pick_set")]
    changed_counter = Counter(str(item.get("fund_type") or "unknown") for item in changed_when_same_nav)

    return {
        "comparison_count_total": len(comparisons),
        "same_nav_pair_count": len(same_nav_pairs),
        "same_nav_and_changed_pick_pair_count": len(changed_when_same_nav),
        "same_nav_and_unchanged_pick_pair_count": len(unchanged_when_same_nav),
        "nav_advanced_pair_count": len(comparisons) - len(same_nav_pairs),
        "changed_when_same_nav_fund_type_breakdown": dict(sorted(changed_counter.items())),
        "safe_to_enable_raw_nav_unchanged_skip": len(changed_when_same_nav) == 0,
    }


def _build_findings(summary: dict, comparisons: Sequence[dict]) -> List[str]:
    if not comparisons:
        return [
            "最近审计窗口内没有足够的相邻成功筛选 run，暂时无法判断净值未推进门禁是否安全。",
        ]

    same_nav_pairs = [item for item in comparisons if item.get("same_nav_latest_date")]
    if not same_nav_pairs:
        return [
            "最近审计窗口内没有发现相邻 run 使用相同 nav_latest_date 的情况，本轮无法验证“净值未推进则跳过”是否安全。",
        ]

    changed_when_same_nav = [item for item in same_nav_pairs if not item.get("same_pick_set")]
    if changed_when_same_nav:
        worst = max(changed_when_same_nav, key=lambda item: int(item.get("changed_pick_total") or 0))
        return [
            (
                f"发现 {len(changed_when_same_nav)} 对相邻 run 在 nav_latest_date 未推进时仍发生最终 picks 变化，"
                "说明当前不能直接按原始 nav_latest_date 不变来跳过筛选。"
            ),
            (
                f"变化最明显的一对为 fund_type={worst.get('fund_type')} period_years={worst.get('period_years')} "
                f"{worst.get('prev_run_day')} -> {worst.get('curr_run_day')}，"
                f"changed_pick_total={worst.get('changed_pick_total')}。"
            ),
        ]

    return [
        "最近审计窗口内所有 nav_latest_date 未推进的相邻 run，其最终 picks 集合也未发生变化，可以考虑进入下一阶段灰度验证。",
    ]


def build_screening_nav_gate_audit_payload(
    *,
    db_path: Optional[str] = None,
    days: int = 7,
    fund_types: Optional[Sequence[str]] = None,
    max_runs: Optional[int] = None,
) -> Dict[str, object]:
    """构建筛选净值门禁审计结果。"""
    with open_readonly_db(db_path) as conn:
        run_rows = _fetch_recent_success_runs(
            conn,
            days=days,
            fund_types=fund_types,
            max_runs=max_runs,
        )
        daily_runs = _pick_latest_run_per_day(run_rows)
        pick_sets = _fetch_pick_sets(conn, [row.get("run_id") for row in daily_runs])

    comparisons = _build_comparisons(daily_runs, pick_sets)
    summary = _build_summary(comparisons)

    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "db_path": resolve_db_path(db_path or "database/bond_funds.db"),
        "meta": {
            "days": int(days),
            "fund_types": [str(item).strip() for item in (fund_types or []) if str(item).strip()],
            "max_runs": int(max_runs) if max_runs is not None else None,
            "comparison_basis": "latest_success_run_per_day_and_fund_type_period",
        },
        "summary": summary,
        "findings": _build_findings(summary, comparisons),
        "comparisons": comparisons,
    }


def render_screening_nav_gate_audit_markdown(payload: Dict[str, object]) -> str:
    summary = payload.get("summary") or {}
    lines = [
        "# screening nav gate audit",
        "",
        f"- generated_at: `{payload.get('generated_at')}`",
        f"- db_path: `{payload.get('db_path')}`",
        f"- days: `{(payload.get('meta') or {}).get('days')}`",
        f"- fund_types: `{', '.join((payload.get('meta') or {}).get('fund_types') or ['all'])}`",
        f"- max_runs: `{(payload.get('meta') or {}).get('max_runs')}`",
        f"- comparison_basis: `{(payload.get('meta') or {}).get('comparison_basis')}`",
        "",
        "## Summary",
        f"- comparison_count_total: `{summary.get('comparison_count_total', 0)}`",
        f"- same_nav_pair_count: `{summary.get('same_nav_pair_count', 0)}`",
        f"- same_nav_and_changed_pick_pair_count: `{summary.get('same_nav_and_changed_pick_pair_count', 0)}`",
        f"- same_nav_and_unchanged_pick_pair_count: `{summary.get('same_nav_and_unchanged_pick_pair_count', 0)}`",
        f"- nav_advanced_pair_count: `{summary.get('nav_advanced_pair_count', 0)}`",
        f"- safe_to_enable_raw_nav_unchanged_skip: `{summary.get('safe_to_enable_raw_nav_unchanged_skip')}`",
        f"- changed_when_same_nav_fund_type_breakdown: `{json.dumps(summary.get('changed_when_same_nav_fund_type_breakdown', {}), ensure_ascii=False, sort_keys=True)}`",
        "",
        "## Findings",
    ]

    for finding in payload.get("findings") or []:
        lines.append(f"- {finding}")

    lines.extend(["", "## Comparisons"])
    for row in payload.get("comparisons") or []:
        lines.append("- " + json.dumps(row, ensure_ascii=False, sort_keys=True))

    return "\n".join(lines) + "\n"


def write_screening_nav_gate_audit_artifacts(
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
    json_path = out_dir / f"screening_nav_gate_audit_{suffix}.json"
    md_path = out_dir / f"screening_nav_gate_audit_{suffix}.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(render_screening_nav_gate_audit_markdown(payload), encoding="utf-8")
    return json_path, md_path


__all__ = [
    "build_screening_nav_gate_audit_payload",
    "load_latest_screening_nav_gate_audit_payload",
    "render_screening_nav_gate_audit_markdown",
    "write_screening_nav_gate_audit_artifacts",
]
