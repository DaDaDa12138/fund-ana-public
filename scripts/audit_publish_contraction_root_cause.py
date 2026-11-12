#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.config import load_settings  # noqa: E402
from backend.runtime_replay import resolve_runtime_artifact_path  # noqa: E402
from scripts.audit_runtime_utils import (  # noqa: E402
    build_audit_artifact_path,
    clean_text,
    load_json,
)

AUDIT_DIR = PROJECT_ROOT / "reports" / "audit" / "publish_contraction_root_cause"
DEFAULT_RUN_IDS = (
    "20260618_083908_sync",
    "20260623_215948_sync",
    "20260623_222953_sync",
)


def _safe_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _safe_float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _load_run_report(run_id: str) -> dict[str, Any]:
    settings = load_settings()
    run_dir = settings.runs_dir / run_id
    report_path = run_dir / f"bond_analysis_report_{run_id}.json"
    payload = load_json(report_path)
    if not payload:
        raise RuntimeError(f"run_report_missing_or_invalid: {report_path}")
    return payload


def _artifact_path_from_report(report: dict[str, Any], key: str) -> Path | None:
    artifacts = report.get("artifacts") or {}
    if not isinstance(artifacts, dict):
        return None
    path_value = clean_text(artifacts.get(key))
    if not path_value:
        core_artifacts = artifacts.get("core") or {}
        if isinstance(core_artifacts, dict):
            path_value = clean_text(core_artifacts.get(key))
    if not path_value:
        return None
    return resolve_runtime_artifact_path(path_value, project_root=PROJECT_ROOT)


def _split_reason_codes(value: Any) -> list[str]:
    if isinstance(value, list):
        parts = [clean_text(item) for item in value]
    else:
        text = clean_text(value)
        if not text:
            return []
        parts = []
        for delimiter in ("|", "；", ";", ","):
            if delimiter in text:
                parts = [clean_text(item) for item in text.split(delimiter)]
                break
        if not parts:
            parts = [text]
    return [item for item in parts if item]


def _code_from_row(row: dict[str, Any]) -> str:
    return clean_text(row.get("基金代码") or row.get("fcode") or row.get("code"))


def _rows_by_code(rows: Any) -> dict[str, dict[str, Any]]:
    if not isinstance(rows, list):
        return {}
    result: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        code = _code_from_row(row)
        if code and code not in result:
            result[code] = row
    return result


def _counter_distribution(counter: Counter[str], *, key: str = "label", limit: int = 12) -> list[dict[str, Any]]:
    return [
        {key: label, "count": int(count)}
        for label, count in counter.most_common(limit)
        if label or count > 0
    ]


def _records_distribution(rows: list[dict[str, Any]], column: str, *, key: str = "label") -> list[dict[str, Any]]:
    return _counter_distribution(
        Counter(clean_text(row.get(column)) for row in rows if isinstance(row, dict)),
        key=key,
    )


def _reason_distribution(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counter: Counter[str] = Counter()
    for row in rows:
        if not isinstance(row, dict):
            continue
        for reason in _split_reason_codes(row.get("项目推荐原因代码")):
            counter[reason] += 1
    return _counter_distribution(counter, key="reason", limit=20)


def _row_has_reason(row: dict[str, Any], reason_code: str) -> bool:
    return reason_code in set(_split_reason_codes(row.get("项目推荐原因代码")))


def _row_is_hard_block(row: dict[str, Any]) -> bool:
    return (
        clean_text(row.get("AutoStock原始校验状态")) == "hard_block"
        or _row_has_reason(row, "hard_block")
    )


def _sample_rows(rows: list[dict[str, Any]], *, limit: int = 12) -> list[dict[str, Any]]:
    fields = [
        "基金代码",
        "基金名称",
        "正式榜分组",
        "债基风格标签",
        "正式投资评分",
        "AutoStock原始校验状态",
        "AutoStock告警原因",
        "项目推荐原因代码",
        "推荐理由",
        "推荐备注",
    ]
    samples: list[dict[str, Any]] = []
    for row in rows[:limit]:
        samples.append({field: row.get(field) for field in fields if field in row})
    return samples


def _snapshot_rows(report: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    path = _artifact_path_from_report(report, "publish_chain_snapshot_json")
    payload = load_json(path) if path else {}
    if not payload:
        return {}
    result: dict[str, list[dict[str, Any]]] = {}
    for key in (
        "final_official_records",
        "dynamic_reference_export_records",
        "dynamic_post_review_records",
        "final_selection_removed_records",
        "final_selection_contraction_records",
    ):
        rows = payload.get(key)
        result[key] = rows if isinstance(rows, list) else []
    return result


def _summarize_snapshot_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    hard_block_rows = [row for row in rows if _row_is_hard_block(row)]
    stability_rows = [row for row in rows if _row_has_reason(row, "final_selection_stability_cap")]
    return {
        "count": len(rows),
        "code_count": len(_rows_by_code(rows)),
        "hard_block_count": len(hard_block_rows),
        "final_selection_stability_cap_count": len(stability_rows),
        "autostock_status_counts": _records_distribution(rows, "AutoStock原始校验状态", key="status"),
        "reason_counts": _reason_distribution(rows),
        "group_counts": _records_distribution(rows, "正式榜分组"),
        "style_counts": _records_distribution(rows, "债基风格标签"),
        "hard_block_samples": _sample_rows(hard_block_rows),
        "stability_cap_samples": _sample_rows(stability_rows),
    }


def _build_publish_chain_summary(report: dict[str, Any]) -> dict[str, Any]:
    rows_by_key = _snapshot_rows(report)
    return {
        "available": bool(rows_by_key),
        "raw_records": rows_by_key,
        "records": {
            key: _summarize_snapshot_rows(rows)
            for key, rows in rows_by_key.items()
        },
    }


def _compact_row_for_diff(row: dict[str, Any], *, mapped_removed_row: dict[str, Any] | None = None) -> dict[str, Any]:
    mapped = mapped_removed_row or {}
    return {
        "code": _code_from_row(row),
        "name": clean_text(row.get("基金名称")),
        "group": clean_text(row.get("正式榜分组")),
        "style": clean_text(row.get("债基风格标签")),
        "score": row.get("正式投资评分"),
        "autostock_status": clean_text(row.get("AutoStock原始校验状态")),
        "reason_codes": _split_reason_codes(row.get("项目推荐原因代码")),
        "mapped_removed_autostock_status": clean_text(mapped.get("AutoStock原始校验状态")),
        "mapped_removed_reason_codes": _split_reason_codes(mapped.get("项目推荐原因代码")),
        "mapped_removed_reason": clean_text(mapped.get("推荐理由")),
        "note": clean_text(row.get("推荐备注")),
    }


def _build_record_diff(
    previous: dict[str, Any],
    current: dict[str, Any],
    record_key: str,
) -> dict[str, Any]:
    previous_chain = previous.get("publish_chain") if isinstance(previous.get("publish_chain"), dict) else {}
    current_chain = current.get("publish_chain") if isinstance(current.get("publish_chain"), dict) else {}
    previous_raw = previous_chain.get("raw_records") if isinstance(previous_chain.get("raw_records"), dict) else {}
    current_raw = current_chain.get("raw_records") if isinstance(current_chain.get("raw_records"), dict) else {}
    previous_rows = previous_raw.get(record_key) if isinstance(previous_raw.get(record_key), list) else []
    current_rows = current_raw.get(record_key) if isinstance(current_raw.get(record_key), list) else []
    previous_by_code = _rows_by_code(previous_rows)
    current_by_code = _rows_by_code(current_rows)
    current_removed_by_code = _rows_by_code(
        current_raw.get("final_selection_removed_records")
        if isinstance(current_raw.get("final_selection_removed_records"), list)
        else []
    )
    lost_codes = sorted(set(previous_by_code) - set(current_by_code))
    gained_codes = sorted(set(current_by_code) - set(previous_by_code))
    lost_rows = [
        _compact_row_for_diff(
            previous_by_code[code],
            mapped_removed_row=current_removed_by_code.get(code),
        )
        for code in lost_codes
    ]
    gained_rows = [_compact_row_for_diff(current_by_code[code]) for code in gained_codes]
    mapped_removed_reasons: Counter[str] = Counter()
    mapped_removed_status: Counter[str] = Counter()
    for code in lost_codes:
        removed = current_removed_by_code.get(code) or {}
        if removed:
            mapped_removed_status[clean_text(removed.get("AutoStock原始校验状态"))] += 1
            reasons = _split_reason_codes(removed.get("项目推荐原因代码"))
            for reason in reasons:
                mapped_removed_reasons[reason] += 1
    return {
        "record_key": record_key,
        "lost_count": len(lost_codes),
        "gained_count": len(gained_codes),
        "net_delta": len(current_by_code) - len(previous_by_code),
        "lost_mapped_removed_autostock_status_counts": _counter_distribution(
            mapped_removed_status,
            key="status",
        ),
        "lost_mapped_removed_reason_counts": _counter_distribution(
            mapped_removed_reasons,
            key="reason",
            limit=20,
        ),
        "lost_samples": lost_rows[:20],
        "gained_samples": gained_rows[:20],
    }


def _normalize_reason_counts(items: Any) -> list[dict[str, Any]]:
    if not isinstance(items, list):
        return []
    normalized: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        reason = clean_text(item.get("reason") or item.get("code") or item.get("label"))
        count = _safe_int(item.get("count"))
        if not reason and count <= 0:
            continue
        normalized.append({"reason": reason, "count": count})
    return normalized


def _extract_stage_rows(report: dict[str, Any]) -> list[dict[str, Any]]:
    project_publish_summary = report.get("project_publish_summary") or {}
    if not isinstance(project_publish_summary, dict):
        return []
    publish_stage_audit = project_publish_summary.get("publish_stage_audit") or {}
    if not isinstance(publish_stage_audit, dict):
        return []
    rows = publish_stage_audit.get("stage_rows") or []
    if not isinstance(rows, list):
        return []
    normalized: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        top_reasons = []
        for reason in row.get("top_reasons") or []:
            if not isinstance(reason, dict):
                continue
            top_reasons.append(
                {
                    "code": clean_text(reason.get("code")),
                    "reason": clean_text(reason.get("reason")),
                    "display_reason": clean_text(reason.get("display_reason")),
                    "count": _safe_int(reason.get("count")),
                }
            )
        normalized.append(
            {
                "stage_key": clean_text(row.get("stage_key")),
                "stage_label": clean_text(row.get("stage_label")),
                "scope": clean_text(row.get("scope")),
                "input_count": _safe_int(row.get("input_count")),
                "passed_count": _safe_int(row.get("passed_count")),
                "removed_count": _safe_int(row.get("removed_count")),
                "removed_rate_pct": _safe_float(row.get("removed_rate_pct")),
                "top_reasons": top_reasons,
            }
        )
    return normalized


def _reason_count_from_stage(stage: dict[str, Any], reason_code: str) -> int:
    for item in stage.get("top_reasons") or []:
        if clean_text(item.get("code")) == reason_code:
            return _safe_int(item.get("count"))
    return 0


def _load_autostock_summary(report: dict[str, Any]) -> dict[str, Any]:
    path = _artifact_path_from_report(report, "autostock_validation_audit_file")
    payload = load_json(path) if path else {}
    summary = payload.get("summary") if isinstance(payload, dict) else {}
    if not isinstance(summary, dict):
        summary = {}

    detail_records = payload.get("detail_records") if isinstance(payload, dict) else []
    status_counts: Counter[str] = Counter()
    warning_reason_counts: Counter[str] = Counter()
    if isinstance(detail_records, list):
        for item in detail_records:
            if not isinstance(item, dict):
                continue
            status = clean_text(item.get("autostock_validation_status") or item.get("校验状态"))
            if status:
                status_counts[status] += 1
            reasons = item.get("warning_reasons") or item.get("告警原因")
            if isinstance(reasons, list):
                for reason in reasons:
                    reason_text = clean_text(reason)
                    if reason_text:
                        warning_reason_counts[reason_text] += 1
            else:
                for reason in clean_text(reasons).split("；"):
                    reason_text = clean_text(reason)
                    if reason_text:
                        warning_reason_counts[reason_text] += 1

    return {
        "artifact_path": str(path.relative_to(PROJECT_ROOT)) if path and path.exists() else "",
        "candidate_pool_count": _safe_int(payload.get("candidate_pool_count") if isinstance(payload, dict) else 0),
        "validated_count": _safe_int(payload.get("validated_count") if isinstance(payload, dict) else 0),
        "failed_count": _safe_int(payload.get("failed_count") if isinstance(payload, dict) else 0),
        "checked_count": _safe_int(summary.get("checked_count")),
        "filtered_count": _safe_int(summary.get("filtered_count")),
        "passed_count": _safe_int(summary.get("passed_count")),
        "cache_hit": bool(summary.get("cache_hit")),
        "cache_reused_count": _safe_int(summary.get("cache_reused_count")),
        "warning_counts": _normalize_reason_counts(summary.get("warning_counts")),
        "reason_counts": _normalize_reason_counts(summary.get("reason_counts")),
        "detail_status_counts": [
            {"status": status, "count": int(count)}
            for status, count in status_counts.most_common()
        ],
        "detail_warning_reason_counts": [
            {"reason": reason, "count": int(count)}
            for reason, count in warning_reason_counts.most_common(10)
        ],
    }


def _extract_counts(report: dict[str, Any]) -> dict[str, Any]:
    counts = report.get("counts") or {}
    if not isinstance(counts, dict):
        counts = {}
    project_publish_summary = report.get("project_publish_summary") or {}
    if not isinstance(project_publish_summary, dict):
        project_publish_summary = {}
    publish_reason_summary = project_publish_summary.get("publish_reason_summary") or {}
    if not isinstance(publish_reason_summary, dict):
        publish_reason_summary = {}
    return {
        "final_count": _safe_int(counts.get("final_count")),
        "dynamic_final_count": _safe_int(counts.get("dynamic_final_count")),
        "stable_bond_final_count": _safe_int(counts.get("stable_bond_final_count")),
        "enhanced_bond_final_count": _safe_int(counts.get("enhanced_bond_final_count")),
        "final_selected_count_from_reason_summary": _safe_int(
            publish_reason_summary.get("final_selected_count")
        ),
    }


def _build_run_summary(run_id: str) -> dict[str, Any]:
    report = _load_run_report(run_id)
    stage_rows = _extract_stage_rows(report)
    stage_map = {row["stage_key"]: row for row in stage_rows if row.get("stage_key")}
    base_gate = stage_map.get("base_gate") or {}
    publish_chain = _build_publish_chain_summary(report)
    return {
        "run_id": run_id,
        "generated_at": clean_text(report.get("generated_at")),
        "candidate_date": clean_text((report.get("database_freshness_summary") or {}).get("candidate_date")),
        "counts": _extract_counts(report),
        "stage_rows": stage_rows,
        "base_gate_soft_warning_removed_count": _reason_count_from_stage(base_gate, "soft_warning"),
        "base_gate_passed_count": _safe_int(base_gate.get("passed_count")),
        "base_gate_input_count": _safe_int(base_gate.get("input_count")),
        "autostock": _load_autostock_summary(report),
        "publish_chain": publish_chain,
    }


def _delta(current: int, previous: int) -> int:
    return int(current) - int(previous)


def _build_pair_delta(previous: dict[str, Any], current: dict[str, Any]) -> dict[str, Any]:
    previous_counts = previous.get("counts") or {}
    current_counts = current.get("counts") or {}
    previous_chain = ((previous.get("publish_chain") or {}).get("records") or {})
    current_chain = ((current.get("publish_chain") or {}).get("records") or {})
    previous_removed = previous_chain.get("final_selection_removed_records") or {}
    current_removed = current_chain.get("final_selection_removed_records") or {}
    previous_final = previous_chain.get("final_official_records") or {}
    current_final = current_chain.get("final_official_records") or {}
    previous_dynamic = previous_chain.get("dynamic_reference_export_records") or {}
    current_dynamic = current_chain.get("dynamic_reference_export_records") or {}
    return {
        "from_run_id": previous.get("run_id"),
        "to_run_id": current.get("run_id"),
        "final_count_delta": _delta(
            _safe_int(current_counts.get("final_count")),
            _safe_int(previous_counts.get("final_count")),
        ),
        "dynamic_final_count_delta": _delta(
            _safe_int(current_counts.get("dynamic_final_count")),
            _safe_int(previous_counts.get("dynamic_final_count")),
        ),
        "base_gate_passed_delta": _delta(
            _safe_int(current.get("base_gate_passed_count")),
            _safe_int(previous.get("base_gate_passed_count")),
        ),
        "base_gate_soft_warning_removed_delta": _delta(
            _safe_int(current.get("base_gate_soft_warning_removed_count")),
            _safe_int(previous.get("base_gate_soft_warning_removed_count")),
        ),
        "autostock_filtered_delta": _delta(
            _safe_int((current.get("autostock") or {}).get("filtered_count")),
            _safe_int((previous.get("autostock") or {}).get("filtered_count")),
        ),
        "autostock_passed_delta": _delta(
            _safe_int((current.get("autostock") or {}).get("passed_count")),
            _safe_int((previous.get("autostock") or {}).get("passed_count")),
        ),
        "final_official_code_delta": _delta(
            _safe_int(current_final.get("code_count")),
            _safe_int(previous_final.get("code_count")),
        ),
        "dynamic_reference_code_delta": _delta(
            _safe_int(current_dynamic.get("code_count")),
            _safe_int(previous_dynamic.get("code_count")),
        ),
        "final_selection_removed_delta": _delta(
            _safe_int(current_removed.get("code_count")),
            _safe_int(previous_removed.get("code_count")),
        ),
        "final_selection_hard_block_delta": _delta(
            _safe_int(current_removed.get("hard_block_count")),
            _safe_int(previous_removed.get("hard_block_count")),
        ),
        "final_selection_stability_cap_delta": _delta(
            _safe_int(current_removed.get("final_selection_stability_cap_count")),
            _safe_int(previous_removed.get("final_selection_stability_cap_count")),
        ),
        "record_diffs": [
            _build_record_diff(previous, current, "final_official_records"),
            _build_record_diff(previous, current, "dynamic_reference_export_records"),
            _build_record_diff(previous, current, "dynamic_post_review_records"),
        ],
    }


def _reason_count(items: list[dict[str, Any]], reason: str) -> int:
    for item in items:
        if clean_text(item.get("reason")) == reason:
            return _safe_int(item.get("count"))
    return 0


def _build_findings(run_summaries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    if len(run_summaries) < 2:
        return findings
    first = run_summaries[0]
    latest = run_summaries[-1]
    previous = run_summaries[-2]
    latest_autostock = latest.get("autostock") or {}
    first_counts = first.get("counts") or {}
    latest_counts = latest.get("counts") or {}
    previous_counts = previous.get("counts") or {}
    first_chain = ((first.get("publish_chain") or {}).get("records") or {})
    latest_chain = ((latest.get("publish_chain") or {}).get("records") or {})
    previous_chain = ((previous.get("publish_chain") or {}).get("records") or {})
    latest_removed = latest_chain.get("final_selection_removed_records") or {}
    previous_removed = previous_chain.get("final_selection_removed_records") or {}
    latest_removed_hard_block = _safe_int(latest_removed.get("hard_block_count"))
    previous_removed_hard_block = _safe_int(previous_removed.get("hard_block_count"))
    latest_target_total = _safe_int(first_counts.get("final_count"))
    latest_final_count = _safe_int(latest_counts.get("final_count"))
    latest_dynamic_count = _safe_int(latest_counts.get("dynamic_final_count"))
    first_dynamic_count = _safe_int(first_counts.get("dynamic_final_count"))
    previous_final_count = _safe_int(previous_counts.get("final_count"))
    previous_dynamic_count = _safe_int(previous_counts.get("dynamic_final_count"))
    latest_removed_total = _safe_int(latest_removed.get("code_count"))
    latest_soft_warning_removed = _safe_int(latest.get("base_gate_soft_warning_removed_count"))
    latest_nav_soft_lag = _reason_count(
        latest_autostock.get("warning_counts") or [],
        "latest_nav_date_soft_lag",
    )
    previous_nav_soft_lag = _reason_count(
        (previous.get("autostock") or {}).get("warning_counts") or [],
        "latest_nav_date_soft_lag",
    )

    if latest_target_total and latest_final_count < latest_target_total and latest_removed_hard_block:
        unexplained_after_hard_block = latest_target_total - latest_final_count - latest_removed_hard_block
        findings.append(
            {
                "severity": "P1",
                "topic": "Final list contraction explained by AutoStock hard blocks",
                "evidence": (
                    f"{latest['run_id']} target/current-published final baseline was "
                    f"{latest_target_total}, final_count={latest_final_count}, and "
                    f"publish_chain final_selection_removed hard_block={latest_removed_hard_block}."
                ),
                "interpretation": (
                    "The latest successful run did not shrink because the frontend cached an old "
                    "date. It replayed the final-selection target and then removed AutoStock "
                    "hard-blocked funds, leaving the current final list size. Do not promote the "
                    "run until the hard-block source evidence is reviewed."
                ),
                "residual_count_after_hard_block": unexplained_after_hard_block,
                "source": "publish_chain_snapshot.final_selection_removed_records",
            }
        )

    if latest_removed_hard_block > previous_removed_hard_block:
        findings.append(
            {
                "severity": "P1",
                "topic": "Second contraction caused by additional hard blocks",
                "evidence": (
                    f"{previous['run_id']} -> {latest['run_id']} final_count "
                    f"{previous_final_count}->{latest_final_count}, dynamic_final_count "
                    f"{previous_dynamic_count}->{latest_dynamic_count}, while final-selection "
                    f"hard_block removals increased {previous_removed_hard_block}->{latest_removed_hard_block}."
                ),
                "interpretation": (
                    "The second same-day contraction is not caused by a smaller input universe or "
                    "lower AutoStock checked/passed totals; it is explained by more funds being "
                    "removed at the final-selection publish review as hard_block."
                ),
                "source": "publish_chain_snapshot.final_selection_removed_records",
            }
        )

    if (
        _safe_int(latest_autostock.get("filtered_count"))
        > _safe_int((first.get("autostock") or {}).get("filtered_count"))
    ):
        findings.append(
            {
                "severity": "P1",
                "topic": "AutoStock filter shock versus current published baseline",
                "evidence": (
                    f"AutoStock filtered_count changed from "
                    f"{_safe_int((first.get('autostock') or {}).get('filtered_count'))} "
                    f"in {first['run_id']} to {_safe_int(latest_autostock.get('filtered_count'))} "
                    f"in {latest['run_id']}; passed_count changed from "
                    f"{_safe_int((first.get('autostock') or {}).get('passed_count'))} to "
                    f"{_safe_int(latest_autostock.get('passed_count'))}."
                ),
                "interpretation": (
                    "This is the largest upstream gate change between current published and latest "
                    "successful runs. Review AutoStock return mismatch and hard-block evidence "
                    "before changing screening thresholds or publication guards."
                ),
                "source": "autostock_validation_audit.summary",
            }
        )

    if latest_removed_total and latest_dynamic_count < first_dynamic_count:
        findings.append(
            {
                "severity": "P1",
                "topic": "Dynamic reference contraction follows publish review removals",
                "evidence": (
                    f"dynamic_final_count changed {first_dynamic_count}->{latest_dynamic_count}; "
                    f"latest final_selection_removed total={latest_removed_total}, "
                    f"hard_block={latest_removed_hard_block}."
                ),
                "interpretation": (
                    "The dynamic reference list also contracted after publish review. Treat it as "
                    "diagnostic evidence only; it is not a reason to bypass current-published semantics."
                ),
                "source": "publish_chain_snapshot.dynamic_reference_export_records",
            }
        )

    if latest_soft_warning_removed > 0 and latest_nav_soft_lag > 0:
        findings.append(
            {
                "severity": "P1",
                "topic": "AutoStock soft warning gate semantics",
                "evidence": (
                    f"{latest['run_id']} base_gate removed {latest_soft_warning_removed} "
                    f"soft_warning rows while AutoStock warning_counts reported "
                    f"latest_nav_date_soft_lag={latest_nav_soft_lag}."
                ),
                "interpretation": (
                    "AutoStock validated these rows as passed with warnings, but the report "
                    "base gate treated soft warnings as a blocking reason. This contradicts "
                    "the documented policy that soft warnings keep explicit labels and enter "
                    "downstream quality convergence."
                ),
            }
        )

    if latest_nav_soft_lag > 0 and previous_nav_soft_lag == 0:
        findings.append(
            {
                "severity": "P1",
                "topic": "NAV-date lag shock",
                "evidence": (
                    f"latest_nav_date_soft_lag changed from {previous_nav_soft_lag} "
                    f"in {previous['run_id']} to {latest_nav_soft_lag} in {latest['run_id']}."
                ),
                "interpretation": (
                    "The contraction is tied to a broad one-run AutoStock NAV-date soft-lag "
                    "signal, so the follow-up validation should rerun the workflow after the "
                    "gate fix instead of manually promoting the old run."
                ),
            }
        )

    return findings


def _write_markdown(payload: dict[str, Any], path: Path) -> None:
    lines = [
        "# Publish Contraction Root Cause Audit",
        "",
        f"- generated_at: `{payload.get('generated_at')}`",
        f"- run_ids: `{', '.join(payload.get('run_ids') or [])}`",
        "",
        "## Findings",
    ]
    findings = payload.get("findings") or []
    if not findings:
        lines.append("- No finding generated from the selected evidence.")
    for item in findings:
        lines.append(
            f"- **{item.get('severity')} {item.get('topic')}**: "
            f"{item.get('evidence')} {item.get('interpretation')}"
        )

    lines.extend(["", "## Run Counts"])
    for run in payload.get("runs") or []:
        counts = run.get("counts") or {}
        autostock = run.get("autostock") or {}
        removed = (
            ((run.get("publish_chain") or {}).get("records") or {}).get("final_selection_removed_records")
            or {}
        )
        lines.append(
            f"- `{run.get('run_id')}`: final={counts.get('final_count')}, "
            f"dynamic={counts.get('dynamic_final_count')}, "
            f"base_gate_passed={run.get('base_gate_passed_count')}, "
            f"base_soft_warning_removed={run.get('base_gate_soft_warning_removed_count')}, "
            f"autostock_checked={autostock.get('checked_count')}, "
            f"autostock_passed={autostock.get('passed_count')}, "
            f"autostock_filtered={autostock.get('filtered_count')}, "
            f"final_removed={removed.get('code_count')}, "
            f"final_removed_hard_block={removed.get('hard_block_count')}"
        )

    lines.extend(["", "## Pair Deltas"])
    for delta in payload.get("pair_deltas") or []:
        lines.append(
            f"- `{delta.get('from_run_id')}` -> `{delta.get('to_run_id')}`: "
            f"final {delta.get('final_count_delta')}, "
            f"dynamic {delta.get('dynamic_final_count_delta')}, "
            f"base_gate_passed {delta.get('base_gate_passed_delta')}, "
            f"base_soft_warning_removed {delta.get('base_gate_soft_warning_removed_delta')}, "
            f"AutoStock passed {delta.get('autostock_passed_delta')}, "
            f"filtered {delta.get('autostock_filtered_delta')}, "
            f"final removed {delta.get('final_selection_removed_delta')}, "
            f"hard_block removed {delta.get('final_selection_hard_block_delta')}"
        )
        for record_diff in delta.get("record_diffs") or []:
            lines.append(
                f"  - {record_diff.get('record_key')}: lost={record_diff.get('lost_count')}, "
                f"gained={record_diff.get('gained_count')}, net={record_diff.get('net_delta')}"
            )
            status_counts = record_diff.get("lost_mapped_removed_autostock_status_counts") or []
            reason_counts = record_diff.get("lost_mapped_removed_reason_counts") or []
            if status_counts:
                lines.append(
                    "    - lost mapped removed AutoStock: "
                    + ", ".join(
                        f"{item.get('status') or '<missing>'}={item.get('count')}"
                        for item in status_counts[:6]
                    )
                )
            if reason_counts:
                lines.append(
                    "    - lost mapped removed reasons: "
                    + ", ".join(
                        f"{item.get('reason') or '<missing>'}={item.get('count')}"
                        for item in reason_counts[:8]
                    )
                )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def build_audit(run_ids: list[str]) -> dict[str, Any]:
    runs = [_build_run_summary(run_id) for run_id in run_ids]
    pair_deltas = [
        _build_pair_delta(runs[index - 1], runs[index])
        for index in range(1, len(runs))
    ]
    output_runs = json.loads(json.dumps(runs, ensure_ascii=False))
    for run in output_runs:
        publish_chain = run.get("publish_chain") if isinstance(run.get("publish_chain"), dict) else {}
        publish_chain.pop("raw_records", None)
    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "audit_scope": "read_only_existing_run_artifacts",
        "run_ids": run_ids,
        "runs": output_runs,
        "pair_deltas": pair_deltas,
        "findings": _build_findings(runs),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Audit publish contraction root cause from existing run artifacts."
    )
    parser.add_argument(
        "--run-id",
        action="append",
        dest="run_ids",
        help="Run id to include. Repeat in chronological order. Defaults to the latest contraction triad.",
    )
    parser.add_argument("--output-dir", default=str(AUDIT_DIR))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    run_ids = [clean_text(item) for item in (args.run_ids or DEFAULT_RUN_IDS) if clean_text(item)]
    if len(run_ids) < 2:
        raise RuntimeError("at_least_two_run_ids_required")
    payload = build_audit(run_ids)
    output_dir = Path(args.output_dir)
    json_path = build_audit_artifact_path(
        output_dir,
        "publish_contraction_root_cause",
        run_id=run_ids[-1],
    )
    md_path = json_path.with_suffix(".md")
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_markdown(payload, md_path)
    print(json.dumps({"json": str(json_path), "markdown": str(md_path)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
