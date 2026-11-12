#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
债券型正式主榜专项审计。
"""

from __future__ import annotations

import json
import sqlite3
from collections import Counter
from datetime import datetime
from pathlib import Path
from statistics import median
from typing import Iterable, Optional, Sequence, Tuple

from utils.fund_code_alias_audit import DEFAULT_AUDIT_DIR, load_latest_json_artifact, open_readonly_db


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_BOND_DECISION_DIR = (
    PROJECT_ROOT / "bond_fund_screening" / "output" / ".audit" / "final_screening_decisions"
)
BOND_DECISION_JSON_PATTERN = "bond_final_screening_decisions_*.json"
BOND_MAINLIST_AUDIT_JSON_PATTERN = "bond_mainlist_quality_audit_*.json"
BOND_CANDIDATE_LAYERS_JSON_PATTERN = "bond_mainlist_candidate_layers_*.json"


def load_latest_bond_mainlist_audit_payload(
    output_dir: str = DEFAULT_AUDIT_DIR,
) -> Tuple[Path, dict]:
    return load_latest_json_artifact(BOND_MAINLIST_AUDIT_JSON_PATTERN, output_dir=output_dir)


def load_latest_bond_candidate_layers_payload(
    output_dir: str = DEFAULT_AUDIT_DIR,
) -> Tuple[Path, dict]:
    return load_latest_json_artifact(BOND_CANDIDATE_LAYERS_JSON_PATTERN, output_dir=output_dir)


def _load_json_payload(path: Path) -> dict | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        return None
    return payload if isinstance(payload, dict) else None


def _build_audit_history_identity(payload: dict, path: Path) -> str:
    decision_artifact = str(payload.get("decision_artifact") or "").strip()
    if decision_artifact:
        return f"decision_artifact:{decision_artifact}"

    summary = payload.get("summary") or {}
    decision_generated_at = str(summary.get("decision_generated_at") or "").strip()
    if decision_generated_at:
        return f"decision_generated_at:{decision_generated_at}"

    return f"path:{path}"


def resolve_previous_bond_mainlist_audit_payload(
    *,
    current_decision_artifact: str | None,
    current_decision_generated_at: str | None,
    output_dir: str | Path = DEFAULT_AUDIT_DIR,
) -> tuple[Path | None, dict | None]:
    artifact_dir = Path(output_dir)
    if not artifact_dir.is_absolute():
        artifact_dir = (PROJECT_ROOT / artifact_dir).resolve()
    candidates = sorted(artifact_dir.glob(BOND_MAINLIST_AUDIT_JSON_PATTERN), reverse=True)
    normalized_current_artifact = str(current_decision_artifact or "").strip()
    normalized_current_generated_at = str(current_decision_generated_at or "").strip()

    for path in candidates:
        payload = _load_json_payload(path)
        if payload is None:
            continue
        if normalized_current_artifact and str(payload.get("decision_artifact") or "").strip() == normalized_current_artifact:
            continue
        previous_summary = payload.get("summary") or {}
        if (
            normalized_current_generated_at
            and str(previous_summary.get("decision_generated_at") or "").strip() == normalized_current_generated_at
        ):
            continue
        return path, payload
    return None, None


def load_recent_bond_mainlist_audit_history(
    *,
    current_decision_artifact: str | None,
    current_decision_generated_at: str | None,
    output_dir: str | Path = DEFAULT_AUDIT_DIR,
    limit: int = 14,
) -> list[tuple[Path, dict]]:
    artifact_dir = Path(output_dir)
    if not artifact_dir.is_absolute():
        artifact_dir = (PROJECT_ROOT / artifact_dir).resolve()
    candidates = sorted(artifact_dir.glob(BOND_MAINLIST_AUDIT_JSON_PATTERN), reverse=True)
    normalized_current_artifact = str(current_decision_artifact or "").strip()
    normalized_current_generated_at = str(current_decision_generated_at or "").strip()
    rows: list[tuple[Path, dict]] = []
    seen_identities: set[str] = set()

    for path in candidates:
        payload = _load_json_payload(path)
        if payload is None:
            continue
        if normalized_current_artifact and str(payload.get("decision_artifact") or "").strip() == normalized_current_artifact:
            continue
        previous_summary = payload.get("summary") or {}
        if (
            normalized_current_generated_at
            and str(previous_summary.get("decision_generated_at") or "").strip() == normalized_current_generated_at
        ):
            continue
        identity = _build_audit_history_identity(payload, path)
        if identity in seen_identities:
            continue
        seen_identities.add(identity)
        rows.append((path, payload))
        if len(rows) >= int(limit):
            break

    rows.reverse()
    return rows


def _extract_input_lineage(decision_payload: dict) -> dict:
    report_meta = decision_payload.get("report_meta") or {}
    input_lineage = report_meta.get("input_lineage")
    if isinstance(input_lineage, dict):
        return dict(input_lineage)

    input_lineage = decision_payload.get("input_lineage")
    if isinstance(input_lineage, dict):
        return dict(input_lineage)
    return {}


def _extract_final_output_count(decision_payload: dict) -> int:
    summary = decision_payload.get("final_screening_summary") or {}
    value = summary.get("final_output_count")
    if value is not None:
        return int(value or 0)
    return int(sum(
        1 for record in (decision_payload.get("candidate_records") or [])
        if record.get("candidate_stage") == "final_mainlist"
    ))


def _is_valid_production_bond_decision(decision_payload: dict) -> bool:
    input_lineage = _extract_input_lineage(decision_payload)
    if not str(input_lineage.get("today_date") or "").strip():
        return False

    final_output_count = _extract_final_output_count(decision_payload)
    if final_output_count <= 0:
        return False

    mainlist_count = int(sum(
        1 for record in (decision_payload.get("candidate_records") or [])
        if record.get("candidate_stage") == "final_mainlist"
    ))
    if mainlist_count and mainlist_count != final_output_count:
        return False
    return True


def _build_decision_source_meta(
    *,
    decision_file: Path,
    decision_payload: dict,
    selection_mode: str,
    selection_reason: str,
    discovered_candidate_count: int,
) -> dict:
    input_lineage = _extract_input_lineage(decision_payload)
    return {
        "selection_mode": selection_mode,
        "selection_reason": selection_reason,
        "discovered_candidate_count": int(discovered_candidate_count),
        "decision_filename": decision_file.name,
        "decision_generated_at": decision_payload.get("generated_at"),
        "has_input_lineage": bool(input_lineage),
        "input_lineage": input_lineage,
        "final_output_count": _extract_final_output_count(decision_payload),
        "candidate_records_count": int(len(decision_payload.get("candidate_records") or [])),
        "is_valid_production_decision": _is_valid_production_bond_decision(decision_payload),
    }


def resolve_bond_decision_payload(
    decision_path: Optional[str | Path] = None,
    *,
    decision_dir: Optional[str | Path] = None,
) -> Tuple[Path, dict, dict]:
    """解析债券型最终筛选决策账本，并附带来源元数据。"""
    if decision_path:
        path = Path(decision_path)
        if not path.is_absolute():
            path = (PROJECT_ROOT / path).resolve()
        if not path.exists():
            raise FileNotFoundError(f"未找到债券型决策账本: {path}")
        payload = json.loads(path.read_text(encoding="utf-8"))
        meta = _build_decision_source_meta(
            decision_file=path,
            decision_payload=payload,
            selection_mode="explicit_path",
            selection_reason="使用命令行显式指定的债券型决策账本。",
            discovered_candidate_count=1,
        )
        return path, payload, meta

    artifact_dir = Path(decision_dir) if decision_dir else DEFAULT_BOND_DECISION_DIR
    if not artifact_dir.is_absolute():
        artifact_dir = (PROJECT_ROOT / artifact_dir).resolve()

    candidates = sorted(artifact_dir.glob(BOND_DECISION_JSON_PATTERN))
    if not candidates:
        raise FileNotFoundError(
            f"未在 {artifact_dir} 找到 {BOND_DECISION_JSON_PATTERN} 产物"
        )

    readable_candidates: list[tuple[Path, dict]] = []
    for path in candidates:
        try:
            readable_candidates.append((path, json.loads(path.read_text(encoding="utf-8"))))
        except json.JSONDecodeError:
            continue

    if not readable_candidates:
        raise FileNotFoundError(f"{artifact_dir} 下存在匹配文件，但均不是可解析的 JSON")

    valid_candidates = [
        (path, payload)
        for path, payload in readable_candidates
        if _is_valid_production_bond_decision(payload)
    ]
    if valid_candidates:
        selected_path, selected_payload = valid_candidates[-1]
        meta = _build_decision_source_meta(
            decision_file=selected_path,
            decision_payload=selected_payload,
            selection_mode="auto_latest_valid_production",
            selection_reason=(
                "自动选择最近一份包含 input_lineage.today_date 且 "
                "final_output_count 与正式主榜记录数一致的有效生产账本。"
            ),
            discovered_candidate_count=len(readable_candidates),
        )
        return selected_path, selected_payload, meta

    fallback_path, fallback_payload = readable_candidates[-1]
    meta = _build_decision_source_meta(
        decision_file=fallback_path,
        decision_payload=fallback_payload,
        selection_mode="auto_latest_file_fallback",
        selection_reason=(
            "未找到满足生产账本校验条件的文件，回退到最近可解析账本。"
        ),
        discovered_candidate_count=len(readable_candidates),
    )
    return fallback_path, fallback_payload, meta


def load_latest_bond_decision_payload(
    decision_path: Optional[str | Path] = None,
    *,
    decision_dir: Optional[str | Path] = None,
) -> Tuple[Path, dict]:
    """读取最新债券型最终筛选决策账本。"""
    path, payload, _ = resolve_bond_decision_payload(
        decision_path=decision_path,
        decision_dir=decision_dir,
    )
    return path, payload


def _bond_benchmark_family(text: object) -> str:
    value = str(text or "").strip()
    if not value:
        return "unknown"
    if any(token in value for token in ("国开", "国债", "政金债", "农发", "国债指数")):
        return "rate_policy"
    if any(token in value for token in ("信用", "中高等级", "产业债", "城投")):
        return "credit"
    if "可转债" in value:
        return "convertible"
    if any(token in value for token in ("存款", "活期")):
        return "cash_plus"
    if any(token in value for token in ("综合", "全债", "中债")):
        return "aggregate"
    return "other"


def _percentile(values: Sequence[float], target: Optional[float], reverse: bool = False) -> Optional[float]:
    clean_values = [float(item) for item in values if item is not None]
    if not clean_values or target is None:
        return None
    if reverse:
        matched = sum(1 for item in clean_values if item >= float(target))
    else:
        matched = sum(1 for item in clean_values if item <= float(target))
    return round(matched / len(clean_values) * 100, 2)


def _summarize_numeric(values: Sequence[Optional[float]]) -> Optional[dict]:
    clean_values = [float(item) for item in values if item is not None]
    if not clean_values:
        return None
    return {
        "min": round(min(clean_values), 4),
        "median": round(float(median(clean_values)), 4),
        "max": round(max(clean_values), 4),
    }


def _normalize_counter(
    counter: Counter,
    *,
    selected_total: int,
    universe_counter: Counter | None = None,
    universe_total: int = 0,
) -> list[dict]:
    items = []
    for name, count in counter.most_common():
        selected_share_pct = round(count / selected_total * 100, 2) if selected_total else 0.0
        item = {
            "name": name,
            "count": int(count),
            "selected_share_pct": selected_share_pct,
        }
        if universe_counter is not None and universe_total:
            universe_count = int(universe_counter.get(name, 0))
            universe_share_pct = round(universe_count / universe_total * 100, 2)
            item["universe_count"] = universe_count
            item["universe_share_pct"] = universe_share_pct
            item["share_multiple"] = (
                round(selected_share_pct / universe_share_pct, 2)
                if universe_share_pct > 0 else None
            )
        items.append(item)
    return items


def _calculate_counter_hhi(counter: Counter, *, total: int) -> float | None:
    if total <= 0:
        return None
    return round(
        sum((int(count) / total) ** 2 for count in counter.values() if int(count) > 0),
        4,
    )


def _build_subtype_concentration_signal(
    mainlist_rows: Sequence[dict],
    universe_rows: Sequence[dict],
) -> dict:
    selected_total = int(len(mainlist_rows))
    universe_total = int(len(universe_rows))
    selected_counter = Counter(
        str(row.get("ftype") or "").strip() or "<unknown>"
        for row in mainlist_rows
    )
    universe_counter = Counter(
        str(row.get("ftype") or "").strip() or "<unknown>"
        for row in universe_rows
    )

    selected_hhi = _calculate_counter_hhi(selected_counter, total=selected_total)
    effective_count = (
        round(1.0 / float(selected_hhi), 2)
        if selected_hhi not in (None, 0.0) else None
    )
    dominant_name = None
    dominant_count = 0
    dominant_share_pct = 0.0
    dominant_universe_share_pct = None
    dominant_share_multiple = None
    if selected_counter:
        dominant_name, dominant_count = selected_counter.most_common(1)[0]
        dominant_share_pct = round(dominant_count / selected_total * 100, 2) if selected_total else 0.0
        universe_count = int(universe_counter.get(dominant_name, 0))
        if universe_total > 0:
            dominant_universe_share_pct = round(universe_count / universe_total * 100, 2)
            dominant_share_multiple = (
                round(dominant_share_pct / dominant_universe_share_pct, 2)
                if dominant_universe_share_pct > 0 else None
            )

    if dominant_share_pct >= 60.0 or (selected_hhi is not None and selected_hhi >= 0.35):
        status = "high_concentration"
    elif dominant_share_pct >= 45.0 or (selected_hhi is not None and selected_hhi >= 0.25):
        status = "moderate_concentration"
    else:
        status = "balanced"

    return {
        "status": status,
        "selected_subtype_count": int(len(selected_counter)),
        "dominant_ftype": dominant_name,
        "dominant_ftype_count": int(dominant_count),
        "dominant_ftype_selected_share_pct": dominant_share_pct,
        "dominant_ftype_universe_share_pct": dominant_universe_share_pct,
        "dominant_ftype_share_multiple": dominant_share_multiple,
        "selected_ftype_hhi": selected_hhi,
        "selected_ftype_effective_count": effective_count,
    }


def _build_topn_boundary_pressure_signal(
    *,
    final_mainlist_count: int,
    near_miss_rows: Sequence[dict],
    cutoff_boundary_comparison: dict,
    no_topn_simulation: dict,
) -> dict:
    strict_near_miss_within_1 = [
        row for row in near_miss_rows
        if str(row.get("validation_status") or "").strip() == "strict_match"
        and float(row.get("gap_to_cutoff", 999.0) or 999.0) <= 1.0
    ]
    strict_count = int(len(strict_near_miss_within_1))
    share_vs_mainlist_pct = round(strict_count / final_mainlist_count * 100, 2) if final_mainlist_count > 0 else None
    near_metric_edge = int(cutoff_boundary_comparison.get("near_miss_better_metric_count", 0) or 0)
    bottom_metric_edge = int(cutoff_boundary_comparison.get("bottom_better_metric_count", 0) or 0)
    pair_review_count = int(cutoff_boundary_comparison.get("pair_review_count", 0) or 0)
    no_topn_multiple = no_topn_simulation.get("count_multiple")
    if no_topn_multiple is not None:
        no_topn_multiple = round(float(no_topn_multiple), 2)

    if strict_count <= 0:
        status = "low_pressure"
    elif (
        (share_vs_mainlist_pct is not None and share_vs_mainlist_pct >= 15.0)
        or (pair_review_count > 0 and near_metric_edge >= bottom_metric_edge and near_metric_edge > 0)
    ):
        status = "high_pressure"
    elif share_vs_mainlist_pct is not None and share_vs_mainlist_pct >= 5.0:
        status = "moderate_pressure"
    else:
        status = "low_pressure"

    return {
        "status": status,
        "strict_near_miss_within_1_score_count": strict_count,
        "strict_near_miss_within_1_score_share_vs_mainlist_pct": share_vs_mainlist_pct,
        "pair_review_count": pair_review_count,
        "near_miss_metric_edge_count": near_metric_edge,
        "bottom_mainlist_metric_edge_count": bottom_metric_edge,
        "no_topn_count_multiple": no_topn_multiple,
    }


def _safe_float_delta(current, previous) -> float | None:
    if current is None or previous is None:
        return None
    return round(float(current) - float(previous), 4)


def _safe_int_delta(current, previous) -> int | None:
    if current is None or previous is None:
        return None
    return int(current) - int(previous)


def _status_changed(current: str | None, previous: str | None) -> bool:
    return bool(current and previous and current != previous)


def _build_quality_signal_comparison(
    current_payload: dict,
    previous_payload: dict | None,
    previous_path: Path | None,
) -> dict:
    if not previous_payload or previous_path is None:
        return {
            "available": False,
            "reason": "no_previous_audit",
        }

    current_summary = current_payload.get("summary") or {}
    previous_summary = previous_payload.get("summary") or {}
    current_quality = current_payload.get("quality_signals") or {}
    previous_quality = previous_payload.get("quality_signals") or {}
    current_subtype = current_quality.get("subtype_concentration") or {}
    previous_subtype = previous_quality.get("subtype_concentration") or {}
    current_topn = current_quality.get("topn_boundary_pressure") or {}
    previous_topn = previous_quality.get("topn_boundary_pressure") or {}

    warnings: list[str] = []
    if _status_changed(current_subtype.get("status"), previous_subtype.get("status")):
        warnings.append(
            f"子类型集中度状态变化: {previous_subtype.get('status')} -> {current_subtype.get('status')}"
        )
    if _status_changed(current_topn.get("status"), previous_topn.get("status")):
        warnings.append(
            f"主榜边界压力状态变化: {previous_topn.get('status')} -> {current_topn.get('status')}"
        )

    dominant_changed = (
        str(current_subtype.get("dominant_ftype") or "").strip()
        != str(previous_subtype.get("dominant_ftype") or "").strip()
    )
    if dominant_changed:
        warnings.append(
            f"主导子类型变化: {previous_subtype.get('dominant_ftype')} -> {current_subtype.get('dominant_ftype')}"
        )

    return {
        "available": True,
        "previous_artifact_path": str(previous_path),
        "previous_decision_generated_at": previous_summary.get("decision_generated_at"),
        "previous_mainlist_count": previous_summary.get("final_mainlist_count"),
        "summary_delta": {
            "final_mainlist_count_delta": _safe_int_delta(
                current_summary.get("final_mainlist_count"),
                previous_summary.get("final_mainlist_count"),
            ),
            "top_n_trimmed_delta": _safe_int_delta(
                current_summary.get("top_n_trimmed"),
                previous_summary.get("top_n_trimmed"),
            ),
            "mainlist_soft_warning_count_delta": _safe_int_delta(
                current_summary.get("mainlist_soft_warning_count"),
                previous_summary.get("mainlist_soft_warning_count"),
            ),
            "near_miss_within_1_score_count_delta": _safe_int_delta(
                current_summary.get("near_miss_within_1_score_count"),
                previous_summary.get("near_miss_within_1_score_count"),
            ),
        },
        "subtype_concentration_delta": {
            "status_changed": _status_changed(
                current_subtype.get("status"),
                previous_subtype.get("status"),
            ),
            "current_status": current_subtype.get("status"),
            "previous_status": previous_subtype.get("status"),
            "dominant_ftype_changed": dominant_changed,
            "current_dominant_ftype": current_subtype.get("dominant_ftype"),
            "previous_dominant_ftype": previous_subtype.get("dominant_ftype"),
            "dominant_share_pct_delta": _safe_float_delta(
                current_subtype.get("dominant_ftype_selected_share_pct"),
                previous_subtype.get("dominant_ftype_selected_share_pct"),
            ),
            "selected_ftype_hhi_delta": _safe_float_delta(
                current_subtype.get("selected_ftype_hhi"),
                previous_subtype.get("selected_ftype_hhi"),
            ),
        },
        "topn_boundary_pressure_delta": {
            "status_changed": _status_changed(
                current_topn.get("status"),
                previous_topn.get("status"),
            ),
            "current_status": current_topn.get("status"),
            "previous_status": previous_topn.get("status"),
            "strict_near_miss_within_1_score_count_delta": _safe_int_delta(
                current_topn.get("strict_near_miss_within_1_score_count"),
                previous_topn.get("strict_near_miss_within_1_score_count"),
            ),
            "strict_near_miss_share_vs_mainlist_pct_delta": _safe_float_delta(
                current_topn.get("strict_near_miss_within_1_score_share_vs_mainlist_pct"),
                previous_topn.get("strict_near_miss_within_1_score_share_vs_mainlist_pct"),
            ),
            "no_topn_count_multiple_delta": _safe_float_delta(
                current_topn.get("no_topn_count_multiple"),
                previous_topn.get("no_topn_count_multiple"),
            ),
        },
        "warnings": warnings,
    }


def _count_consecutive_status(
    statuses: Sequence[str],
    *,
    target_status: str,
) -> int:
    streak = 0
    for value in reversed(list(statuses)):
        if str(value or "").strip() == target_status:
            streak += 1
        else:
            break
    return int(streak)


def _build_quality_signal_trend_summary(
    current_payload: dict,
    history_rows: Sequence[tuple[Path, dict]],
) -> dict:
    current_summary = current_payload.get("summary") or {}
    current_quality = current_payload.get("quality_signals") or {}
    current_subtype = current_quality.get("subtype_concentration") or {}
    current_topn = current_quality.get("topn_boundary_pressure") or {}

    series = []
    for path, payload in history_rows:
        summary = payload.get("summary") or {}
        quality = payload.get("quality_signals") or {}
        series.append({
            "artifact_path": str(path),
            "decision_generated_at": summary.get("decision_generated_at"),
            "subtype_status": (quality.get("subtype_concentration") or {}).get("status"),
            "topn_status": (quality.get("topn_boundary_pressure") or {}).get("status"),
            "dominant_share_pct": (quality.get("subtype_concentration") or {}).get("dominant_ftype_selected_share_pct"),
            "strict_near_miss_share_vs_mainlist_pct": (
                (quality.get("topn_boundary_pressure") or {}).get("strict_near_miss_within_1_score_share_vs_mainlist_pct")
            ),
        })

    series.append({
        "artifact_path": None,
        "decision_generated_at": current_summary.get("decision_generated_at"),
        "subtype_status": current_subtype.get("status"),
        "topn_status": current_topn.get("status"),
        "dominant_share_pct": current_subtype.get("dominant_ftype_selected_share_pct"),
        "strict_near_miss_share_vs_mainlist_pct": current_topn.get("strict_near_miss_within_1_score_share_vs_mainlist_pct"),
    })

    subtype_statuses = [str(item.get("subtype_status") or "").strip() for item in series]
    topn_statuses = [str(item.get("topn_status") or "").strip() for item in series]

    def _window_stats(key: str, status_key: str, target_status: str, window: int) -> dict:
        window_rows = series[-int(window):]
        values = [
            float(item[key]) for item in window_rows
            if item.get(key) is not None
        ]
        status_hits = sum(
            1 for item in window_rows
            if str(item.get(status_key) or "").strip() == target_status
        )
        return {
            "window_size": int(len(window_rows)),
            "target_status_hits": int(status_hits),
            "target_status_hit_rate_pct": round(status_hits / len(window_rows) * 100, 2) if window_rows else None,
            "avg_value": round(sum(values) / len(values), 4) if values else None,
            "latest_value": (
                round(float(window_rows[-1][key]), 4)
                if window_rows and window_rows[-1].get(key) is not None else None
            ),
        }

    return {
        "available": bool(series),
        "history_sample_count": int(max(len(series) - 1, 0)),
        "subtype_concentration": {
            "current_status": current_subtype.get("status"),
            "consecutive_high_concentration_count": _count_consecutive_status(
                subtype_statuses,
                target_status="high_concentration",
            ),
            "last_7": _window_stats(
                "dominant_share_pct",
                "subtype_status",
                "high_concentration",
                7,
            ),
            "last_14": _window_stats(
                "dominant_share_pct",
                "subtype_status",
                "high_concentration",
                14,
            ),
        },
        "topn_boundary_pressure": {
            "current_status": current_topn.get("status"),
            "consecutive_high_pressure_count": _count_consecutive_status(
                topn_statuses,
                target_status="high_pressure",
            ),
            "last_7": _window_stats(
                "strict_near_miss_share_vs_mainlist_pct",
                "topn_status",
                "high_pressure",
                7,
            ),
            "last_14": _window_stats(
                "strict_near_miss_share_vs_mainlist_pct",
                "topn_status",
                "high_pressure",
                14,
            ),
        },
    }


def _query_rows_by_codes(conn, codes: Iterable[str]) -> dict[str, dict]:
    normalized_codes = [str(code or "").strip() for code in codes if str(code or "").strip()]
    if not normalized_codes:
        return {}
    placeholders = ",".join("?" for _ in normalized_codes)
    try:
        rows = conn.execute(
            f"""
            SELECT
                f.fcode,
                f.shortname,
                f.company_name,
                f.manager_names,
                f.benchmark,
                f.ftype,
                f.type_category,
                f.scale_billion,
                fr.return_1y,
                fr.return_3y,
                rm1.annual_return AS annual_return_1y,
                COALESCE(rm1.raw_sharpe_ratio, rm1.sharpe_ratio) AS sharpe_1y,
                rm1.max_drawdown AS mdd_1y,
                rm1.volatility AS vol_1y,
                rm3.annual_return AS annual_return_3y,
                COALESCE(rm3.raw_sharpe_ratio, rm3.sharpe_ratio) AS sharpe_3y,
                rm3.max_drawdown AS mdd_3y,
                rm3.volatility AS vol_3y
            FROM funds f
            LEFT JOIN fund_returns fr ON fr.fcode = f.fcode
            LEFT JOIN fund_risk_metrics rm1
                ON rm1.rowid = (
                    SELECT rm_latest.rowid
                    FROM fund_risk_metrics rm_latest
                    WHERE rm_latest.fcode = f.fcode
                      AND rm_latest.period = '1y'
                    ORDER BY rm_latest.end_date DESC, rm_latest.rowid DESC
                    LIMIT 1
                )
            LEFT JOIN fund_risk_metrics rm3
                ON rm3.rowid = (
                    SELECT rm_latest.rowid
                    FROM fund_risk_metrics rm_latest
                    WHERE rm_latest.fcode = f.fcode
                      AND rm_latest.period = '3y'
                    ORDER BY rm_latest.end_date DESC, rm_latest.rowid DESC
                    LIMIT 1
                )
            WHERE f.fcode IN ({placeholders})
            """,
            normalized_codes,
        ).fetchall()
    except sqlite3.Error:
        return {}
    return {str(row["fcode"]).strip(): dict(row) for row in rows}


def _query_bond_universe_rows(conn) -> list[dict]:
    try:
        rows = conn.execute(
            """
            SELECT
                f.fcode,
                f.ftype,
                f.company_name,
                f.benchmark,
                fr.return_1y,
                fr.return_3y,
                rm1.annual_return AS annual_return_1y,
                COALESCE(rm1.raw_sharpe_ratio, rm1.sharpe_ratio) AS sharpe_1y,
                rm1.max_drawdown AS mdd_1y,
                rm1.volatility AS vol_1y,
                rm3.annual_return AS annual_return_3y,
                COALESCE(rm3.raw_sharpe_ratio, rm3.sharpe_ratio) AS sharpe_3y,
                rm3.max_drawdown AS mdd_3y,
                rm3.volatility AS vol_3y
            FROM funds f
            LEFT JOIN fund_returns fr ON fr.fcode = f.fcode
            LEFT JOIN fund_risk_metrics rm1
                ON rm1.rowid = (
                    SELECT rm_latest.rowid
                    FROM fund_risk_metrics rm_latest
                    WHERE rm_latest.fcode = f.fcode
                      AND rm_latest.period = '1y'
                    ORDER BY rm_latest.end_date DESC, rm_latest.rowid DESC
                    LIMIT 1
                )
            LEFT JOIN fund_risk_metrics rm3
                ON rm3.rowid = (
                    SELECT rm_latest.rowid
                    FROM fund_risk_metrics rm_latest
                    WHERE rm_latest.fcode = f.fcode
                      AND rm_latest.period = '3y'
                    ORDER BY rm_latest.end_date DESC, rm_latest.rowid DESC
                    LIMIT 1
                )
            WHERE f.type_category = 'fixed_income'
            """
        ).fetchall()
    except sqlite3.Error:
        return []
    return [dict(row) for row in rows]


def _build_metric_summary(selected_rows: Sequence[dict], universe_rows: Sequence[dict]) -> dict:
    metric_specs = (
        ("return_1y", False),
        ("return_3y", False),
        ("annual_return_1y", False),
        ("annual_return_3y", False),
        ("sharpe_1y", False),
        ("sharpe_3y", False),
        ("mdd_1y", True),
        ("mdd_3y", True),
        ("vol_1y", True),
        ("vol_3y", True),
    )
    result: dict[str, dict] = {}
    for metric_key, reverse in metric_specs:
        selected_values = [row.get(metric_key) for row in selected_rows]
        summary = _summarize_numeric(selected_values)
        if summary is None:
            continue
        universe_values = [row.get(metric_key) for row in universe_rows if row.get(metric_key) is not None]
        summary["median_vs_universe_pctile"] = _percentile(
            universe_values,
            summary["median"],
            reverse=reverse,
        )
        result[metric_key] = summary
    return result


def _build_mainlist_soft_warning_samples(
    mainlist_records: Sequence[dict],
    record_lookup: dict[str, dict],
) -> list[dict]:
    rows = []
    for record in mainlist_records:
        if str(record.get("autostock_validation_status") or "").strip() == "strict_match":
            continue
        db_row = record_lookup.get(record["fcode"], {})
        rows.append(
            {
                "fcode": record["fcode"],
                "fund_name": record.get("fund_name"),
                "ftype": db_row.get("ftype"),
                "weighted_score": round(float(record.get("weighted_score", 0.0) or 0.0), 4),
                "top_n_rank": record.get("top_n_rank"),
                "validation_status": record.get("autostock_validation_status"),
                "warning_reasons": list(record.get("autostock_warning_reasons") or []),
                "warning_display": (
                    str(record.get("autostock_warning_display") or "").strip()
                    or "；".join(
                        _format_soft_warning_reason(reason)
                        for reason in (record.get("autostock_warning_reasons") or [])
                    )
                ),
                "return_1y": db_row.get("return_1y"),
                "return_3y": db_row.get("return_3y"),
                "annual_return_3y": db_row.get("annual_return_3y"),
                "sharpe_1y": db_row.get("sharpe_1y"),
                "sharpe_3y": db_row.get("sharpe_3y"),
                "mdd_1y": db_row.get("mdd_1y"),
                "mdd_3y": db_row.get("mdd_3y"),
            }
        )
    return rows


def _build_soft_warning_reason_counts(mainlist_soft_warning_samples: Sequence[dict]) -> list[dict]:
    """汇总正式主榜软告警原因分布。"""
    counter: Counter[str] = Counter()
    for item in mainlist_soft_warning_samples:
        reasons = item.get("warning_reasons") or []
        if not reasons:
            counter["<empty_warning_reason>"] += 1
            continue
        for reason in reasons:
            normalized_reason = str(reason or "").strip() or "<empty_warning_reason>"
            counter[normalized_reason] += 1
    return [
        {
            "reason": reason,
            "count": int(count),
        }
        for reason, count in counter.most_common()
    ]


def _format_soft_warning_reason(reason: object) -> str:
    label_map = {
        "latest_nav_date_soft_lag": "净值日期相差 1 天",
        "return_1w_soft_mismatch": "近1周收益口径差异",
        "return_1m_soft_due_to_nav_date_lag": "近1月收益受净值日期影响",
        "return_3m_soft_due_to_nav_date_lag": "近3月收益受净值日期影响",
        "return_6m_soft_due_to_nav_date_lag": "近6月收益受净值日期影响",
        "return_1y_soft_due_to_nav_date_lag": "近1年收益受净值日期影响",
        "type_mismatch": "基金类型口径差异",
        "name_mismatch": "基金名称口径差异",
    }
    normalized_reason = str(reason or "").strip()
    return label_map.get(normalized_reason, normalized_reason or "<empty_warning_reason>")


def _format_replacement_verdict(verdict: object) -> str:
    """格式化软告警替代候选审查结论。"""
    label_map = {
        "same_ftype_metric_edge": "同子类型候选且指标占优",
        "same_ftype_candidate_available": "存在同子类型候选",
        "cross_ftype_metric_edge": "仅跨子类型候选且指标占优",
        "cross_ftype_candidate_only": "仅跨子类型候选",
        "no_replacement_candidate": "未找到可用候选",
    }
    normalized = str(verdict or "").strip()
    return label_map.get(normalized, normalized or "未判定")


def _format_matching_strategy(strategy: object) -> str:
    """格式化替代候选匹配策略。"""
    label_map = {
        "one_to_one_greedy_same_ftype_first": "一对一贪心匹配，优先同子类型临界样本",
    }
    normalized = str(strategy or "").strip()
    return label_map.get(normalized, normalized or "未说明")


def _extract_timestamp_from_artifact_name(path: Path) -> str | None:
    """从标准产物文件名提取 YYYYMMDD_HHMMSS 时间戳。"""
    stem = path.stem
    if "_" not in stem:
        return None
    timestamp = stem.rsplit("_", 2)
    if len(timestamp) < 2:
        return None
    candidate = "_".join(timestamp[-2:])
    if len(candidate) == 15 and candidate[8] == "_":
        return candidate
    return None


def _load_autostock_validation_detail_map(
    decision_payload: dict,
    *,
    decision_file: Path | None = None,
) -> dict[str, dict]:
    """读取与当前账本关联的 AutoStock 明细审计文件。"""
    audit_path = None
    candidate_timestamps = []
    candidate_dirs = []
    if decision_file is not None:
        decision_file = Path(decision_file)
        file_ts = _extract_timestamp_from_artifact_name(Path(decision_file))
        if file_ts:
            candidate_timestamps.append(file_ts)
        if decision_file.parent.name == "final_screening_decisions":
            candidate_dirs.append(decision_file.parent.parent / "autostock_validation")
        candidate_dirs.append(
            decision_file.parent / "bond_fund_screening" / "output" / ".audit" / "autostock_validation"
        )
        candidate_dirs.append(
            decision_file.parent / ".audit" / "autostock_validation"
        )

    generated_at = str(decision_payload.get("generated_at") or "").strip()
    if generated_at:
        candidate_timestamps.append(
            generated_at.replace("-", "").replace(":", "").replace("T", "_").split(".")[0]
        )

    candidate_dirs.append(
        PROJECT_ROOT
        / "bond_fund_screening"
        / "output"
        / ".audit"
        / "autostock_validation"
    )

    seen_paths = set()
    for compact_ts in candidate_timestamps:
        for candidate_dir in candidate_dirs:
            candidate_path = Path(candidate_dir) / f"bond_autostock_validation_{compact_ts}.json"
            normalized_candidate_path = str(candidate_path.resolve())
            if normalized_candidate_path in seen_paths:
                continue
            seen_paths.add(normalized_candidate_path)
            if candidate_path.exists():
                audit_path = candidate_path
                break
        if audit_path is not None:
            break

    if audit_path is None:
        return {}

    if not audit_path.exists():
        return {}

    payload = json.loads(audit_path.read_text(encoding="utf-8"))
    rows = payload.get("detail_records") or payload.get("rows") or []
    result = {}
    for row in rows:
        fcode = str(row.get("基金代码") or row.get("fcode") or "").strip()
        if fcode:
            result[fcode] = dict(row)
    return result


def _build_type_mismatch_mainlist_reviews(
    mainlist_soft_warning_samples: Sequence[dict],
    autostock_detail_map: dict[str, dict],
) -> list[dict]:
    """专项整理正式主榜中 type_mismatch 样本。"""
    rows = []
    for item in mainlist_soft_warning_samples:
        warning_reasons = list(item.get("warning_reasons") or [])
        if warning_reasons != ["type_mismatch"]:
            continue
        detail_row = autostock_detail_map.get(str(item.get("fcode") or "").strip(), {})
        rows.append(
            {
                "fcode": item.get("fcode"),
                "fund_name": item.get("fund_name"),
                "ftype": item.get("ftype"),
                "top_n_rank": item.get("top_n_rank"),
                "weighted_score": item.get("weighted_score"),
                "validation_status": item.get("validation_status"),
                "local_fund_type": detail_row.get("本地基金类型"),
                "autostock_fund_type": detail_row.get("AutoStock基金类型"),
                "type_match": detail_row.get("类型一致"),
                "type_fields_available": (
                    detail_row.get("本地基金类型") is not None
                    or detail_row.get("AutoStock基金类型") is not None
                    or detail_row.get("类型一致") is not None
                ),
                "nav_match_ok": not bool(detail_row.get("失败原因")),
                "warning_reason_labels": [
                    _format_soft_warning_reason(reason)
                    for reason in warning_reasons
                ],
                "return_1w_diff": detail_row.get("return_1w_diff"),
                "return_1m_diff": detail_row.get("return_1m_diff"),
                "return_3m_diff": detail_row.get("return_3m_diff"),
                "return_6m_diff": detail_row.get("return_6m_diff"),
                "return_1y_diff": detail_row.get("return_1y_diff"),
            }
        )
    return rows


def _build_soft_warning_mainlist_reviews(
    mainlist_soft_warning_samples: Sequence[dict],
    decision_payload: dict,
    autostock_detail_map: dict[str, dict],
) -> list[dict]:
    """逐只整理正式主榜软告警样本，便于人工排查。"""
    top_cutoff_score = float(
        (decision_payload.get("final_screening_summary") or {}).get("top_cutoff_score") or 0.0
    )
    ranked_rows = sorted(
        mainlist_soft_warning_samples,
        key=lambda item: float(item.get("weighted_score", 0.0) or 0.0),
        reverse=True,
    )
    review_rows = []
    for item in ranked_rows:
        warning_reasons = list(item.get("warning_reasons") or [])
        detail_row = autostock_detail_map.get(str(item.get("fcode") or "").strip(), {})
        review_rows.append(
            {
                "fcode": item.get("fcode"),
                "fund_name": item.get("fund_name"),
                "ftype": item.get("ftype"),
                "weighted_score": item.get("weighted_score"),
                "top_n_rank": item.get("top_n_rank"),
                "validation_status": item.get("validation_status"),
                "warning_reasons": warning_reasons,
                "warning_reason_labels": [
                    _format_soft_warning_reason(reason)
                    for reason in warning_reasons
                ],
                "warning_display": item.get("warning_display"),
                "warning_reason_count": int(len(warning_reasons)),
                "local_fund_type": detail_row.get("本地基金类型"),
                "autostock_fund_type": detail_row.get("AutoStock基金类型"),
                "type_match": detail_row.get("类型一致"),
                "type_fields_available": (
                    detail_row.get("本地基金类型") is not None
                    or detail_row.get("AutoStock基金类型") is not None
                    or detail_row.get("类型一致") is not None
                ),
                "gap_above_cutoff": round(
                    max(float(item.get("weighted_score", 0.0) or 0.0) - top_cutoff_score, 0.0),
                    4,
                ),
                "is_type_mismatch_only": warning_reasons == ["type_mismatch"],
                "is_return_1w_soft_mismatch_only": warning_reasons == ["return_1w_soft_mismatch"],
            }
        )
    return review_rows


def _build_soft_warning_replacement_reviews(
    mainlist_soft_warning_samples: Sequence[dict],
    near_miss_rows: Sequence[dict],
    *,
    gap_threshold: float = 1.0,
) -> tuple[dict, list[dict]]:
    """为主榜软告警样本匹配 strict-match 临界候选，供审计替代性复核。"""
    strict_near_rows = [
        row for row in near_miss_rows
        if str(row.get("validation_status") or "").strip() == "strict_match"
        and float(row.get("gap_to_cutoff", 999.0) or 999.0) <= float(gap_threshold)
    ]
    if not mainlist_soft_warning_samples:
        return {
            "available": False,
            "reason": "no_soft_warning_samples",
            "gap_threshold": float(gap_threshold),
            "matching_strategy": "one_to_one_greedy_same_ftype_first",
            "soft_warning_count": 0,
            "strict_near_miss_count": int(len(strict_near_rows)),
            "distinct_replacement_candidate_count": 0,
            "replaceable_count": 0,
            "same_ftype_replaceable_count": 0,
            "cross_ftype_only_count": 0,
            "metric_edge_count": 0,
            "unmatched_count": 0,
            "warnings": [],
        }, []

    review_rows = []
    replaceable_count = 0
    same_ftype_replaceable_count = 0
    cross_ftype_only_count = 0
    metric_edge_count = 0
    unmatched_count = 0
    used_replacement_codes: set[str] = set()
    distinct_replacement_candidates: set[str] = set()
    prioritized_soft_rows = sorted(
        mainlist_soft_warning_samples,
        key=lambda item: (
            0 if "type_mismatch" in (item.get("warning_reasons") or []) else 1,
            int(item.get("top_n_rank") or 999999),
            -float(item.get("weighted_score", 0.0) or 0.0),
            str(item.get("fcode") or ""),
        ),
    )

    for item in prioritized_soft_rows:
        same_ftype_candidates = [
            row for row in strict_near_rows
            if str(row.get("fcode") or "").strip() not in used_replacement_codes
            if str(row.get("ftype") or "").strip()
            and str(row.get("ftype") or "").strip() == str(item.get("ftype") or "").strip()
        ]
        remaining_candidates = [
            row for row in strict_near_rows
            if str(row.get("fcode") or "").strip() not in used_replacement_codes
        ]
        candidate_pool = same_ftype_candidates or remaining_candidates
        replacement_row = None
        if candidate_pool:
            replacement_row = sorted(
                candidate_pool,
                key=lambda row: (
                    float(row.get("gap_to_cutoff", 999.0) or 999.0),
                    -float(row.get("weighted_score", 0.0) or 0.0),
                    str(row.get("fcode") or ""),
                ),
            )[0]
            replacement_fcode = str(replacement_row.get("fcode") or "").strip()
            if replacement_fcode:
                used_replacement_codes.add(replacement_fcode)
                distinct_replacement_candidates.add(replacement_fcode)

        metric_comparison = {}
        replacement_verdict = "no_replacement_candidate"
        replacement_same_ftype = False
        if replacement_row is None:
            unmatched_count += 1
        else:
            replaceable_count += 1
            replacement_same_ftype = (
                str(item.get("ftype") or "").strip()
                == str(replacement_row.get("ftype") or "").strip()
            )
            if replacement_same_ftype:
                same_ftype_replaceable_count += 1
            else:
                cross_ftype_only_count += 1
            metric_comparison = _compare_boundary_pair_metrics(item, replacement_row)
            if (
                metric_comparison.get("near_miss_better_metric_count", 0)
                > metric_comparison.get("bottom_better_metric_count", 0)
            ):
                metric_edge_count += 1
                replacement_verdict = (
                    "same_ftype_metric_edge"
                    if replacement_same_ftype
                    else "cross_ftype_metric_edge"
                )
            else:
                replacement_verdict = (
                    "same_ftype_candidate_available"
                    if replacement_same_ftype
                    else "cross_ftype_candidate_only"
                )

        review_rows.append(
            {
                "fcode": item.get("fcode"),
                "fund_name": item.get("fund_name"),
                "ftype": item.get("ftype"),
                "weighted_score": item.get("weighted_score"),
                "top_n_rank": item.get("top_n_rank"),
                "validation_status": item.get("validation_status"),
                "warning_reasons": list(item.get("warning_reasons") or []),
                "warning_display": item.get("warning_display"),
                "replacement_fcode": replacement_row.get("fcode") if replacement_row else None,
                "replacement_fund_name": replacement_row.get("fund_name") if replacement_row else None,
                "replacement_ftype": replacement_row.get("ftype") if replacement_row else None,
                "replacement_weighted_score": (
                    replacement_row.get("weighted_score") if replacement_row else None
                ),
                "replacement_gap_to_cutoff": (
                    replacement_row.get("gap_to_cutoff") if replacement_row else None
                ),
                "replacement_validation_status": (
                    replacement_row.get("validation_status") if replacement_row else None
                ),
                "replacement_same_ftype": replacement_same_ftype if replacement_row else None,
                "replacement_verdict": replacement_verdict,
                "replacement_verdict_label": _format_replacement_verdict(replacement_verdict),
                "metric_comparison": metric_comparison,
            }
        )

    warnings = []
    if same_ftype_replaceable_count > 0:
        warnings.append(
            "存在 soft warning 主榜样本可由同子类型 strict-match 临界样本承接，正式主榜严格边界仍可继续收紧。"
        )
    elif cross_ftype_only_count > 0:
        warnings.append(
            "存在 soft warning 主榜样本的 cross-type strict-match 临界候选，但暂不足以直接说明可替代。"
        )

    return {
        "available": bool(review_rows),
        "gap_threshold": float(gap_threshold),
        "matching_strategy": "one_to_one_greedy_same_ftype_first",
        "soft_warning_count": int(len(mainlist_soft_warning_samples)),
        "strict_near_miss_count": int(len(strict_near_rows)),
        "distinct_replacement_candidate_count": int(len(distinct_replacement_candidates)),
        "replaceable_count": int(replaceable_count),
        "same_ftype_replaceable_count": int(same_ftype_replaceable_count),
        "cross_ftype_only_count": int(cross_ftype_only_count),
        "metric_edge_count": int(metric_edge_count),
        "unmatched_count": int(unmatched_count),
        "warnings": warnings,
    }, review_rows


def _build_metric_snapshot(rows: Sequence[dict]) -> dict:
    """汇总样本集合的关键指标中位数。"""
    metric_specs = (
        "weighted_score",
        "return_1y",
        "return_3y",
        "annual_return_3y",
        "sharpe_1y",
        "sharpe_3y",
        "mdd_1y",
        "mdd_3y",
    )
    snapshot = {}
    for metric in metric_specs:
        values = [float(item.get(metric)) for item in rows if item.get(metric) is not None]
        if not values:
            continue
        snapshot[metric] = {
            "median": round(float(median(values)), 4),
            "min": round(min(values), 4),
            "max": round(max(values), 4),
        }
    return snapshot


def _build_type_mismatch_tightening_simulation(
    mainlist_records: Sequence[dict],
    record_lookup: dict[str, dict],
    soft_warning_replacement_reviews: Sequence[dict],
) -> dict:
    """模拟将 type_mismatch 主榜样本替换为 strict-match 同子类型临界样本后的指标变化。"""
    current_rows = []
    for record in mainlist_records:
        db_row = record_lookup.get(record.get("fcode"), {})
        current_rows.append(
            {
                "fcode": record.get("fcode"),
                "fund_name": record.get("fund_name"),
                "ftype": db_row.get("ftype"),
                "weighted_score": round(float(record.get("weighted_score", 0.0) or 0.0), 4),
                "validation_status": record.get("autostock_validation_status"),
                "warning_reasons": list(record.get("autostock_warning_reasons") or []),
                "return_1y": db_row.get("return_1y"),
                "return_3y": db_row.get("return_3y"),
                "annual_return_3y": db_row.get("annual_return_3y"),
                "sharpe_1y": db_row.get("sharpe_1y"),
                "sharpe_3y": db_row.get("sharpe_3y"),
                "mdd_1y": db_row.get("mdd_1y"),
                "mdd_3y": db_row.get("mdd_3y"),
            }
        )

    replaceable_rows = [
        row for row in soft_warning_replacement_reviews
        if row.get("replacement_same_ftype")
        and "type_mismatch" in (row.get("warning_reasons") or [])
        and str(row.get("replacement_fcode") or "").strip()
    ]
    if not replaceable_rows:
        return {
            "available": False,
            "reason": "no_replaceable_type_mismatch_rows",
        }

    replacement_lookup = {}
    for row in replaceable_rows:
        replacement_fcode = str(row.get("replacement_fcode") or "").strip()
        db_row = record_lookup.get(replacement_fcode, {})
        replacement_lookup[str(row.get("fcode") or "").strip()] = {
            "fcode": replacement_fcode,
            "fund_name": row.get("replacement_fund_name"),
            "ftype": row.get("replacement_ftype"),
            "weighted_score": row.get("replacement_weighted_score"),
            "validation_status": row.get("replacement_validation_status"),
            "warning_reasons": [],
            "return_1y": db_row.get("return_1y"),
            "return_3y": db_row.get("return_3y"),
            "annual_return_3y": db_row.get("annual_return_3y"),
            "sharpe_1y": db_row.get("sharpe_1y"),
            "sharpe_3y": db_row.get("sharpe_3y"),
            "mdd_1y": db_row.get("mdd_1y"),
            "mdd_3y": db_row.get("mdd_3y"),
        }

    simulated_rows = []
    replaced_out_rows = []
    replaced_in_rows = []
    replaced_out_codes = set(replacement_lookup)
    replaced_in_codes = set()
    for row in current_rows:
        current_fcode = str(row.get("fcode") or "").strip()
        replacement_row = replacement_lookup.get(current_fcode)
        if replacement_row is None:
            simulated_rows.append(dict(row))
            continue
        simulated_rows.append(dict(replacement_row))
        replaced_out_rows.append(dict(row))
        replaced_in_rows.append(dict(replacement_row))
        replaced_in_codes.add(str(replacement_row.get("fcode") or "").strip())

    current_snapshot = _build_metric_snapshot(current_rows)
    simulated_snapshot = _build_metric_snapshot(simulated_rows)
    metric_deltas = []
    for metric in sorted(set(current_snapshot) | set(simulated_snapshot)):
        current_metric = current_snapshot.get(metric) or {}
        simulated_metric = simulated_snapshot.get(metric) or {}
        current_median = current_metric.get("median")
        simulated_median = simulated_metric.get("median")
        median_delta = None
        if current_median is not None and simulated_median is not None:
            median_delta = round(float(simulated_median) - float(current_median), 4)
        metric_deltas.append(
            {
                "metric": metric,
                "current_median": current_median,
                "simulated_median": simulated_median,
                "median_delta": median_delta,
            }
        )

    warnings = [
        "该模拟仅用于审计 type_mismatch 主榜样本的收紧空间，不代表已建议直接替换正式主榜。"
    ]

    return {
        "available": True,
        "scope": "type_mismatch_same_ftype_replacement_only",
        "replaced_out_count": int(len(replaced_out_rows)),
        "replaced_in_count": int(len(replaced_in_rows)),
        "replaced_out_codes": sorted(replaced_out_codes),
        "replaced_in_codes": sorted(replaced_in_codes),
        "current_mainlist_snapshot": current_snapshot,
        "simulated_mainlist_snapshot": simulated_snapshot,
        "metric_deltas": metric_deltas,
        "replaced_out_rows": replaced_out_rows,
        "replaced_in_rows": replaced_in_rows,
        "warnings": warnings,
    }


def _build_weighted_score_conflict_audit(
    soft_warning_replacement_reviews: Sequence[dict],
    *,
    score_gap_warn_threshold: float = 5.0,
) -> dict:
    """审计主榜 soft warning 与 strict-match 临界样本之间的终裁冲突。"""
    type_mismatch_rows = [
        row for row in soft_warning_replacement_reviews
        if "type_mismatch" in (row.get("warning_reasons") or [])
        and row.get("replacement_fcode")
    ]
    if not type_mismatch_rows:
        return {
            "available": False,
            "reason": "no_type_mismatch_conflict_rows",
        }

    rows = []
    same_ftype_count = 0
    weighted_score_soft_higher_count = 0
    metric_edge_count = 0
    large_score_gap_count = 0
    warnings = []

    for row in type_mismatch_rows:
        metric_comparison = row.get("metric_comparison") or {}
        near_better = int(metric_comparison.get("near_miss_better_metric_count", 0) or 0)
        bottom_better = int(metric_comparison.get("bottom_better_metric_count", 0) or 0)
        score_gap = None
        if row.get("weighted_score") is not None and row.get("replacement_weighted_score") is not None:
            score_gap = round(
                float(row.get("weighted_score") or 0.0) - float(row.get("replacement_weighted_score") or 0.0),
                4,
            )
        weighted_score_soft_higher = bool(score_gap is not None and score_gap > 0)
        if weighted_score_soft_higher:
            weighted_score_soft_higher_count += 1
        if row.get("replacement_same_ftype"):
            same_ftype_count += 1
        if near_better > bottom_better:
            metric_edge_count += 1
        if score_gap is not None and score_gap >= float(score_gap_warn_threshold):
            large_score_gap_count += 1

        rows.append(
            {
                "fcode": row.get("fcode"),
                "fund_name": row.get("fund_name"),
                "warning_display": row.get("warning_display"),
                "weighted_score": row.get("weighted_score"),
                "replacement_fcode": row.get("replacement_fcode"),
                "replacement_fund_name": row.get("replacement_fund_name"),
                "replacement_weighted_score": row.get("replacement_weighted_score"),
                "score_gap_soft_minus_strict": score_gap,
                "replacement_same_ftype": row.get("replacement_same_ftype"),
                "near_miss_better_metric_count": near_better,
                "bottom_better_metric_count": bottom_better,
                "tie_metric_count": int(metric_comparison.get("tie_metric_count", 0) or 0),
                "replacement_verdict_label": row.get("replacement_verdict_label"),
            }
        )

    if metric_edge_count > 0 and weighted_score_soft_higher_count > 0:
        warnings.append(
            "存在主榜 soft warning 样本在 weighted_score 上明显领先，但对应 strict-match 临界样本在多项风险收益指标上更优。"
        )
    if large_score_gap_count > 0:
        warnings.append(
            "部分冲突样本的 weighted_score 差距较大，说明当前总分口径对正式主榜终裁具有强支配性。"
        )

    return {
        "available": True,
        "scope": "type_mismatch_soft_warning_vs_strict_near_miss",
        "score_gap_warn_threshold": float(score_gap_warn_threshold),
        "conflict_count": int(len(rows)),
        "same_ftype_conflict_count": int(same_ftype_count),
        "weighted_score_soft_higher_count": int(weighted_score_soft_higher_count),
        "metric_edge_count": int(metric_edge_count),
        "large_score_gap_count": int(large_score_gap_count),
        "warnings": warnings,
        "rows": rows,
    }


def _build_no_topn_simulation(
    candidate_records: Sequence[dict],
    record_lookup: dict[str, dict],
) -> dict:
    """模拟放宽主榜边界到全部 eligible_pool 后的规模与质量变化。"""
    current_main_rows = [
        record for record in candidate_records
        if record.get("candidate_stage") == "final_mainlist"
    ]
    eligible_rows = [
        record for record in candidate_records
        if record.get("candidate_stage") in {"final_mainlist", "eligible_pool"}
    ]
    no_top_rows = [
        record for record in eligible_rows
        if record.get("list_tier") != "watch"
    ]

    if not current_main_rows or not no_top_rows:
        return {
            "available": False,
            "reason": "missing_current_or_eligible_rows",
        }

    def _materialize(records: Sequence[dict]) -> list[dict]:
        rows = []
        for record in records:
            db_row = record_lookup.get(record.get("fcode"), {})
            rows.append(
                {
                    "fcode": record.get("fcode"),
                    "fund_name": record.get("fund_name"),
                    "ftype": db_row.get("ftype"),
                    "weighted_score": round(float(record.get("weighted_score", 0.0) or 0.0), 4),
                    "validation_status": record.get("autostock_validation_status"),
                    "warning_reasons": list(record.get("autostock_warning_reasons") or []),
                    "return_1y": db_row.get("return_1y"),
                    "return_3y": db_row.get("return_3y"),
                    "annual_return_3y": db_row.get("annual_return_3y"),
                    "sharpe_1y": db_row.get("sharpe_1y"),
                    "sharpe_3y": db_row.get("sharpe_3y"),
                    "mdd_1y": db_row.get("mdd_1y"),
                    "mdd_3y": db_row.get("mdd_3y"),
                }
            )
        return rows

    current_materialized = _materialize(current_main_rows)
    no_top_materialized = _materialize(no_top_rows)

    current_snapshot = _build_metric_snapshot(current_materialized)
    no_top_snapshot = _build_metric_snapshot(no_top_materialized)
    metric_deltas = []
    for metric in sorted(set(current_snapshot) | set(no_top_snapshot)):
        current_metric = current_snapshot.get(metric) or {}
        simulated_metric = no_top_snapshot.get(metric) or {}
        current_median = current_metric.get("median")
        simulated_median = simulated_metric.get("median")
        median_delta = None
        if current_median is not None and simulated_median is not None:
            median_delta = round(float(simulated_median) - float(current_median), 4)
        metric_deltas.append(
            {
                "metric": metric,
                "current_median": current_median,
                "simulated_median": simulated_median,
                "median_delta": median_delta,
            }
        )

    current_status = Counter(row.get("validation_status") for row in current_materialized)
    no_top_status = Counter(row.get("validation_status") for row in no_top_materialized)
    warnings = []
    if len(no_top_materialized) >= len(current_materialized) * 5:
        warnings.append(
            "若直接把主榜边界放宽到全部 eligible_pool，正式入榜规模会显著膨胀，当前候选资格本身不足以充当最终发布质量阈值。"
        )
    if (
        (no_top_snapshot.get("sharpe_3y") or {}).get("median") is not None
        and (current_snapshot.get("sharpe_3y") or {}).get("median") is not None
        and float((no_top_snapshot.get("sharpe_3y") or {}).get("median"))
        < float((current_snapshot.get("sharpe_3y") or {}).get("median"))
    ):
        warnings.append(
            "若直接放宽到全部 eligible_pool，3Y Sharpe 中位数明显下降，说明当前正式主榜仍保留了较强的发布收敛价值。"
        )

    return {
        "available": True,
        "scope": "eligible_pool_without_topn_cutoff",
        "current_main_count": int(len(current_materialized)),
        "simulated_count": int(len(no_top_materialized)),
        "count_multiple": round(len(no_top_materialized) / len(current_materialized), 2),
        "current_status_counts": [
            {"status": status, "count": int(count)}
            for status, count in current_status.most_common()
        ],
        "simulated_status_counts": [
            {"status": status, "count": int(count)}
            for status, count in no_top_status.most_common()
        ],
        "current_main_snapshot": current_snapshot,
        "simulated_snapshot": no_top_snapshot,
        "metric_deltas": metric_deltas,
        "warnings": warnings,
    }


def _build_bottom_mainlist_rows(
    decision_payload: dict,
    record_lookup: dict[str, dict],
    *,
    limit: int = 10,
) -> list[dict]:
    """构造正式主榜末位样本，便于与 cutoff 外临界样本直接对照。"""
    top_cutoff_score = float(
        (decision_payload.get("final_screening_summary") or {}).get("top_cutoff_score") or 0.0
    )
    rows = [
        record for record in (decision_payload.get("candidate_records") or [])
        if record.get("candidate_stage") == "final_mainlist"
    ]
    rows.sort(key=lambda item: float(item.get("weighted_score", 0.0) or 0.0))

    result_rows = []
    for record in rows[:limit]:
        db_row = record_lookup.get(record["fcode"], {})
        weighted_score = float(record.get("weighted_score", 0.0) or 0.0)
        result_rows.append(
            {
                "fcode": record["fcode"],
                "fund_name": record.get("fund_name"),
                "ftype": db_row.get("ftype"),
                "weighted_score": round(weighted_score, 4),
                "gap_above_cutoff": round(max(weighted_score - top_cutoff_score, 0.0), 4),
                "return_1y": db_row.get("return_1y"),
                "return_3y": db_row.get("return_3y"),
                "annual_return_3y": db_row.get("annual_return_3y"),
                "sharpe_1y": db_row.get("sharpe_1y"),
                "sharpe_3y": db_row.get("sharpe_3y"),
                "mdd_1y": db_row.get("mdd_1y"),
                "mdd_3y": db_row.get("mdd_3y"),
                "validation_status": record.get("autostock_validation_status"),
                "warning_reasons": list(record.get("autostock_warning_reasons") or []),
            }
        )
    return result_rows


def _compare_boundary_pair_metrics(
    bottom_row: dict,
    near_miss_row: dict,
) -> dict:
    """比较一组主榜末位样本与临界样本的关键指标优劣。"""
    metric_specs = (
        ("return_1y", False),
        ("return_3y", False),
        ("annual_return_3y", False),
        ("sharpe_1y", False),
        ("sharpe_3y", False),
        ("mdd_1y", True),
        ("mdd_3y", True),
    )
    metric_rows = []
    near_miss_better_metric_count = 0
    bottom_better_metric_count = 0
    tie_metric_count = 0

    for metric_key, lower_is_better in metric_specs:
        bottom_value = bottom_row.get(metric_key)
        near_miss_value = near_miss_row.get(metric_key)
        if bottom_value is None or near_miss_value is None:
            continue

        bottom_value = round(float(bottom_value), 4)
        near_miss_value = round(float(near_miss_value), 4)
        delta = round(near_miss_value - bottom_value, 4)

        if lower_is_better:
            if near_miss_value < bottom_value:
                relation = "near_miss_better"
                near_miss_better_metric_count += 1
            elif near_miss_value > bottom_value:
                relation = "bottom_mainlist_better"
                bottom_better_metric_count += 1
            else:
                relation = "tie"
                tie_metric_count += 1
        else:
            if near_miss_value > bottom_value:
                relation = "near_miss_better"
                near_miss_better_metric_count += 1
            elif near_miss_value < bottom_value:
                relation = "bottom_mainlist_better"
                bottom_better_metric_count += 1
            else:
                relation = "tie"
                tie_metric_count += 1

        metric_rows.append(
            {
                "metric": metric_key,
                "bottom_mainlist_value": bottom_value,
                "near_miss_value": near_miss_value,
                "preferred_direction": "lower" if lower_is_better else "higher",
                "delta_near_minus_bottom": delta,
                "relation": relation,
            }
        )

    return {
        "metric_rows": metric_rows,
        "near_miss_better_metric_count": int(near_miss_better_metric_count),
        "bottom_better_metric_count": int(bottom_better_metric_count),
        "tie_metric_count": int(tie_metric_count),
    }


def _build_boundary_pair_reviews(
    bottom_mainlist_rows: Sequence[dict],
    focus_near_miss_rows: Sequence[dict],
    *,
    limit: int = 5,
) -> dict:
    """逐只对照主榜末位样本与临界样本。"""
    if not bottom_mainlist_rows or not focus_near_miss_rows:
        return {
            "pair_count": 0,
            "verdict_counts": [],
            "rows": [],
        }

    sorted_bottom_rows = sorted(
        bottom_mainlist_rows,
        key=lambda item: float(item.get("weighted_score", 0.0) or 0.0),
    )
    sorted_near_miss_rows = sorted(
        focus_near_miss_rows,
        key=lambda item: (
            float(item.get("gap_to_cutoff", 999.0) or 999.0),
            -float(item.get("weighted_score", 0.0) or 0.0),
            str(item.get("fcode") or ""),
        ),
    )

    verdict_counter: Counter[str] = Counter()
    rows = []
    for pair_index, (bottom_row, near_miss_row) in enumerate(
        zip(sorted_bottom_rows, sorted_near_miss_rows),
        start=1,
    ):
        if pair_index > int(limit):
            break

        metric_comparison = _compare_boundary_pair_metrics(bottom_row, near_miss_row)
        score_gap = round(
            float(bottom_row.get("weighted_score", 0.0) or 0.0)
            - float(near_miss_row.get("weighted_score", 0.0) or 0.0),
            4,
        )
        if (
            metric_comparison["near_miss_better_metric_count"]
            > metric_comparison["bottom_better_metric_count"]
        ):
            verdict = "near_miss_metric_edge"
        elif (
            metric_comparison["near_miss_better_metric_count"]
            < metric_comparison["bottom_better_metric_count"]
        ):
            verdict = "bottom_mainlist_metric_edge"
        else:
            verdict = "mixed_or_tie"
        verdict_counter[verdict] += 1

        rows.append(
            {
                "pair_index": int(pair_index),
                "score_gap": score_gap,
                "same_ftype": str(bottom_row.get("ftype") or "") == str(near_miss_row.get("ftype") or ""),
                "verdict": verdict,
                "bottom_mainlist": {
                    "fcode": bottom_row.get("fcode"),
                    "fund_name": bottom_row.get("fund_name"),
                    "ftype": bottom_row.get("ftype"),
                    "weighted_score": bottom_row.get("weighted_score"),
                    "validation_status": bottom_row.get("validation_status"),
                    "warning_reasons": list(bottom_row.get("warning_reasons") or []),
                },
                "near_miss": {
                    "fcode": near_miss_row.get("fcode"),
                    "fund_name": near_miss_row.get("fund_name"),
                    "ftype": near_miss_row.get("ftype"),
                    "weighted_score": near_miss_row.get("weighted_score"),
                    "gap_to_cutoff": near_miss_row.get("gap_to_cutoff"),
                    "validation_status": near_miss_row.get("validation_status"),
                },
                "metric_comparison": metric_comparison,
            }
        )

    return {
        "pair_count": int(len(rows)),
        "verdict_counts": [
            {"verdict": verdict, "count": int(count)}
            for verdict, count in verdict_counter.most_common()
        ],
        "rows": rows,
    }


def _build_cutoff_boundary_comparison(
    bottom_mainlist_rows: Sequence[dict],
    near_miss_rows: Sequence[dict],
    *,
    gap_threshold: float = 1.0,
) -> dict:
    """对比主榜末位样本与 cutoff 外 strict_match 临界样本的指标边界。"""
    focus_near_miss_rows = [
        row for row in near_miss_rows
        if str(row.get("validation_status") or "").strip() == "strict_match"
        and float(row.get("gap_to_cutoff", 999.0) or 999.0) <= float(gap_threshold)
    ]
    if not bottom_mainlist_rows or not focus_near_miss_rows:
        return {
            "available": False,
            "reason": "missing_boundary_samples",
            "gap_threshold": float(gap_threshold),
            "bottom_sample_count": int(len(bottom_mainlist_rows)),
            "near_miss_sample_count": int(len(focus_near_miss_rows)),
        }

    pair_reviews = _build_boundary_pair_reviews(
        bottom_mainlist_rows,
        focus_near_miss_rows,
    )

    metric_specs = (
        ("return_1y", False),
        ("return_3y", False),
        ("annual_return_3y", False),
        ("sharpe_1y", False),
        ("sharpe_3y", False),
        ("mdd_1y", True),
        ("mdd_3y", True),
    )
    metric_rows = []
    near_miss_better_metric_count = 0
    bottom_better_metric_count = 0
    tie_metric_count = 0

    for metric_key, lower_is_better in metric_specs:
        bottom_values = [
            float(item[metric_key]) for item in bottom_mainlist_rows
            if item.get(metric_key) is not None
        ]
        near_values = [
            float(item[metric_key]) for item in focus_near_miss_rows
            if item.get(metric_key) is not None
        ]
        if not bottom_values or not near_values:
            continue

        bottom_median = round(float(median(bottom_values)), 4)
        near_median = round(float(median(near_values)), 4)
        if lower_is_better:
            if near_median < bottom_median:
                relation = "near_miss_better"
                near_miss_better_metric_count += 1
            elif near_median > bottom_median:
                relation = "bottom_mainlist_better"
                bottom_better_metric_count += 1
            else:
                relation = "tie"
                tie_metric_count += 1
        else:
            if near_median > bottom_median:
                relation = "near_miss_better"
                near_miss_better_metric_count += 1
            elif near_median < bottom_median:
                relation = "bottom_mainlist_better"
                bottom_better_metric_count += 1
            else:
                relation = "tie"
                tie_metric_count += 1

        metric_rows.append(
            {
                "metric": metric_key,
                "bottom_mainlist_median": bottom_median,
                "near_miss_median": near_median,
                "preferred_direction": "lower" if lower_is_better else "higher",
                "relation": relation,
            }
        )

    warnings = []
    if near_miss_better_metric_count >= bottom_better_metric_count and near_miss_better_metric_count > 0:
        warnings.append(
            "距 cutoff 1 分内的 strict_match 临界样本，在多项核心指标中并不弱于主榜末位样本。"
        )

    return {
        "available": bool(metric_rows),
        "gap_threshold": float(gap_threshold),
        "bottom_sample_count": int(len(bottom_mainlist_rows)),
        "near_miss_sample_count": int(len(focus_near_miss_rows)),
        "near_miss_codes": [item.get("fcode") for item in focus_near_miss_rows],
        "pair_review_count": int(pair_reviews.get("pair_count", 0) or 0),
        "pair_review_verdict_counts": list(pair_reviews.get("verdict_counts") or []),
        "pair_reviews": list(pair_reviews.get("rows") or []),
        "metric_comparisons": metric_rows,
        "near_miss_better_metric_count": int(near_miss_better_metric_count),
        "bottom_better_metric_count": int(bottom_better_metric_count),
        "tie_metric_count": int(tie_metric_count),
        "warnings": warnings,
    }


def _build_near_miss_rows(
    decision_payload: dict,
    record_lookup: dict[str, dict],
    *,
    limit: int = 20,
) -> tuple[int, int, list[dict]]:
    records = list(decision_payload.get("candidate_records") or [])
    main_codes = {
        record["fcode"]
        for record in records
        if record.get("candidate_stage") == "final_mainlist"
    }
    watch_codes = {
        record["fcode"]
        for record in records
        if record.get("list_tier") == "watch"
    }
    top_cutoff_score = float(
        (decision_payload.get("final_screening_summary") or {}).get("top_cutoff_score") or 0.0
    )
    near_miss = [
        record for record in records
        if record.get("candidate_stage") == "eligible_pool"
        and record.get("fcode") not in main_codes
        and record.get("fcode") not in watch_codes
    ]
    near_miss.sort(key=lambda item: float(item.get("weighted_score", 0.0) or 0.0), reverse=True)

    rows = []
    within_1_score_count = 0
    for record in near_miss[:limit]:
        db_row = record_lookup.get(record["fcode"], {})
        weighted_score = float(record.get("weighted_score", 0.0) or 0.0)
        gap_to_cutoff = round(max(top_cutoff_score - weighted_score, 0.0), 4)
        if gap_to_cutoff <= 1.0:
            within_1_score_count += 1
        rows.append(
            {
                "fcode": record["fcode"],
                "fund_name": record.get("fund_name"),
                "ftype": db_row.get("ftype"),
                "fund_category": record.get("fund_category"),
                "weighted_score": round(weighted_score, 4),
                "gap_to_cutoff": gap_to_cutoff,
                "return_1y": db_row.get("return_1y"),
                "return_3y": db_row.get("return_3y"),
                "sharpe_1y": db_row.get("sharpe_1y"),
                "sharpe_3y": db_row.get("sharpe_3y"),
                "mdd_1y": db_row.get("mdd_1y"),
                "mdd_3y": db_row.get("mdd_3y"),
                "validation_status": record.get("autostock_validation_status"),
            }
        )
    if len(near_miss) > limit:
        for record in near_miss[limit:]:
            weighted_score = float(record.get("weighted_score", 0.0) or 0.0)
            gap_to_cutoff = round(max(top_cutoff_score - weighted_score, 0.0), 4)
            if gap_to_cutoff <= 1.0:
                within_1_score_count += 1
    return len(near_miss), within_1_score_count, rows


def _build_watchlist_rows(
    decision_payload: dict,
    record_lookup: dict[str, dict],
    *,
    limit: int = 20,
) -> list[dict]:
    watch_rows = [
        record for record in (decision_payload.get("candidate_records") or [])
        if record.get("list_tier") == "watch"
    ]
    watch_rows.sort(key=lambda item: float(item.get("weighted_score", 0.0) or 0.0), reverse=True)
    rows = []
    for record in watch_rows[:limit]:
        db_row = record_lookup.get(record["fcode"], {})
        rows.append(
            {
                "fcode": record["fcode"],
                "fund_name": record.get("fund_name"),
                "ftype": db_row.get("ftype"),
                "fund_category": record.get("fund_category"),
                "weighted_score": round(float(record.get("weighted_score", 0.0) or 0.0), 4),
                "watch_only_reason_code": record.get("watch_only_reason_code"),
                "watchlist_gap_to_cutoff": record.get("watchlist_gap_to_cutoff"),
                "return_1y": db_row.get("return_1y"),
                "return_3y": db_row.get("return_3y"),
                "sharpe_1y": db_row.get("sharpe_1y"),
                "sharpe_3y": db_row.get("sharpe_3y"),
                "mdd_1y": db_row.get("mdd_1y"),
                "mdd_3y": db_row.get("mdd_3y"),
                "watchlist_reasons": list(record.get("watchlist_reasons") or []),
            }
        )
    return rows


def _build_weighted_union_samples(
    decision_payload: dict,
    *,
    limit: int = 15,
) -> list[dict]:
    rows = [
        record for record in (decision_payload.get("candidate_records") or [])
        if record.get("candidate_stage") == "weighted_union_candidate"
    ]
    rows.sort(key=lambda item: float(item.get("weighted_score", 0.0) or 0.0), reverse=True)
    return [
        {
            "fcode": record.get("fcode"),
            "fund_name": record.get("fund_name"),
            "fund_category": record.get("fund_category"),
            "weighted_score": round(float(record.get("weighted_score", 0.0) or 0.0), 4),
            "validation_status": record.get("autostock_validation_status"),
        }
        for record in rows[:limit]
    ]


def _format_edge_governance_verdict(verdict: object) -> str:
    """格式化主榜边缘替换对的指标结论。"""
    label_map = {
        "replacement_in_metric_edge": "替换入样本指标占优",
        "replacement_out_metric_edge": "替换出样本指标占优",
        "mixed_or_tie": "指标互有胜负或持平",
    }
    normalized = str(verdict or "").strip()
    return label_map.get(normalized, normalized or "未判定")


def _build_edge_governance_audit(
    decision_payload: dict,
    record_lookup: dict[str, dict],
    mainlist_db_rows: Sequence[dict],
) -> dict:
    """基于真实替换对构造债基主榜边缘替换代价审计。"""
    summary = decision_payload.get("final_screening_summary") or {}
    governance = summary.get("bond_subtype_edge_governance") or {}
    replacement_rows = governance.get("rows") or []
    if not governance.get("applied") or not replacement_rows:
        return {
            "available": False,
            "reason": "no_applied_edge_governance",
            "replacement_count": 0,
        }

    candidate_records = decision_payload.get("candidate_records") or []
    candidate_lookup = {
        str(record.get("fcode") or "").strip(): record
        for record in candidate_records
        if str(record.get("fcode") or "").strip()
    }
    current_ftype_counter = Counter(
        str(row.get("ftype") or "").strip() or "<unknown>"
        for row in mainlist_db_rows
    )
    dominant_after_ftype = None
    dominant_after_count = 0
    dominant_after_share_pct = None
    if current_ftype_counter and mainlist_db_rows:
        dominant_after_ftype, dominant_after_count = current_ftype_counter.most_common(1)[0]
        dominant_after_share_pct = round(
            dominant_after_count / len(mainlist_db_rows) * 100,
            2,
        )

    review_rows = []
    verdict_counter: Counter[str] = Counter()
    score_gaps: list[float] = []
    for pair_index, item in enumerate(replacement_rows, start=1):
        out_fcode = str(item.get("out_fcode") or "").strip()
        in_fcode = str(item.get("in_fcode") or "").strip()
        out_db_row = dict(record_lookup.get(out_fcode) or {})
        in_db_row = dict(record_lookup.get(in_fcode) or {})
        out_record = dict(candidate_lookup.get(out_fcode) or {})
        in_record = dict(candidate_lookup.get(in_fcode) or {})
        out_row = {
            "fcode": out_fcode,
            "fund_name": item.get("out_fund_name") or out_record.get("fund_name"),
            "ftype": out_db_row.get("ftype") or item.get("out_ftype"),
            "weighted_score": item.get("out_weighted_score", out_record.get("weighted_score")),
            "validation_status": out_record.get("autostock_validation_status"),
            "return_1y": out_db_row.get("return_1y"),
            "return_3y": out_db_row.get("return_3y"),
            "annual_return_3y": out_db_row.get("annual_return_3y"),
            "sharpe_1y": out_db_row.get("sharpe_1y"),
            "sharpe_3y": out_db_row.get("sharpe_3y"),
            "mdd_1y": out_db_row.get("mdd_1y"),
            "mdd_3y": out_db_row.get("mdd_3y"),
        }
        in_row = {
            "fcode": in_fcode,
            "fund_name": item.get("in_fund_name") or in_record.get("fund_name"),
            "ftype": in_db_row.get("ftype") or item.get("in_ftype"),
            "weighted_score": item.get("in_weighted_score", in_record.get("weighted_score")),
            "validation_status": in_record.get("autostock_validation_status"),
            "return_1y": in_db_row.get("return_1y"),
            "return_3y": in_db_row.get("return_3y"),
            "annual_return_3y": in_db_row.get("annual_return_3y"),
            "sharpe_1y": in_db_row.get("sharpe_1y"),
            "sharpe_3y": in_db_row.get("sharpe_3y"),
            "mdd_1y": in_db_row.get("mdd_1y"),
            "mdd_3y": in_db_row.get("mdd_3y"),
        }
        metric_comparison = _compare_boundary_pair_metrics(out_row, in_row)
        in_better_count = int(metric_comparison.get("near_miss_better_metric_count", 0) or 0)
        out_better_count = int(metric_comparison.get("bottom_better_metric_count", 0) or 0)
        tie_count = int(metric_comparison.get("tie_metric_count", 0) or 0)
        if in_better_count > out_better_count:
            verdict = "replacement_in_metric_edge"
        elif in_better_count < out_better_count:
            verdict = "replacement_out_metric_edge"
        else:
            verdict = "mixed_or_tie"
        verdict_counter[verdict] += 1
        score_gap = round(float(item.get("score_gap", 0.0) or 0.0), 4)
        score_gaps.append(score_gap)
        review_rows.append(
            {
                "pair_index": int(pair_index),
                "score_gap": score_gap,
                "same_ftype": str(out_row.get("ftype") or "").strip() == str(in_row.get("ftype") or "").strip(),
                "replacement_out": {
                    "fcode": out_row.get("fcode"),
                    "fund_name": out_row.get("fund_name"),
                    "ftype": out_row.get("ftype"),
                    "weighted_score": out_row.get("weighted_score"),
                    "validation_status": out_row.get("validation_status"),
                },
                "replacement_in": {
                    "fcode": in_row.get("fcode"),
                    "fund_name": in_row.get("fund_name"),
                    "ftype": in_row.get("ftype"),
                    "weighted_score": in_row.get("weighted_score"),
                    "validation_status": in_row.get("validation_status"),
                },
                "replacement_in_better_metric_count": in_better_count,
                "replacement_out_better_metric_count": out_better_count,
                "tie_metric_count": tie_count,
                "verdict": verdict,
                "verdict_label": _format_edge_governance_verdict(verdict),
                "metric_comparison": metric_comparison,
            }
        )

    dominant_before_share_pct = (
        round(float(governance.get("dominant_ftype_share_pct") or 0.0), 2)
        if governance.get("dominant_ftype_share_pct") is not None else None
    )
    dominant_share_delta = _safe_float_delta(
        dominant_after_share_pct,
        dominant_before_share_pct,
    )
    replacement_in_metric_edge_count = int(verdict_counter.get("replacement_in_metric_edge", 0))
    replacement_out_metric_edge_count = int(verdict_counter.get("replacement_out_metric_edge", 0))
    mixed_or_tie_count = int(verdict_counter.get("mixed_or_tie", 0))

    if dominant_share_delta is not None and dominant_share_delta < 0:
        if replacement_out_metric_edge_count <= 0:
            status = "structure_improved_with_controlled_tradeoff"
        elif replacement_in_metric_edge_count > 0:
            status = "structure_improved_with_mixed_tradeoff"
        else:
            status = "structure_improved_but_quality_risk"
    else:
        status = "structure_not_improved"

    warnings = []
    warn_share_pct = governance.get("warn_share_pct")
    if (
        warn_share_pct is not None
        and dominant_after_share_pct is not None
        and float(dominant_after_share_pct) >= float(warn_share_pct)
    ):
        warnings.append("替换后主导子类型占比仍高于治理告警线，说明结构改善仍然有限。")
    if replacement_out_metric_edge_count > 0:
        warnings.append("部分替换对显示替换出样本在更多核心指标上占优，继续放宽治理预算前应先复核真实样本。")

    return {
        "available": True,
        "status": status,
        "replacement_count": int(len(review_rows)),
        "dominant_ftype_before": governance.get("dominant_ftype"),
        "dominant_ftype_before_share_pct": dominant_before_share_pct,
        "dominant_ftype_after": dominant_after_ftype,
        "dominant_ftype_after_share_pct": dominant_after_share_pct,
        "dominant_ftype_share_delta_pct": dominant_share_delta,
        "avg_score_gap": round(sum(score_gaps) / len(score_gaps), 4) if score_gaps else None,
        "max_score_gap": round(max(score_gaps), 4) if score_gaps else None,
        "replacement_in_metric_edge_count": replacement_in_metric_edge_count,
        "replacement_out_metric_edge_count": replacement_out_metric_edge_count,
        "mixed_or_tie_count": mixed_or_tie_count,
        "warnings": warnings,
        "rows": review_rows,
    }


def _build_findings(payload: dict) -> list[str]:
    findings: list[str] = []
    summary = payload.get("summary") or {}
    quality_signals = payload.get("quality_signals") or {}
    quality_signal_trend = payload.get("quality_signal_trend") or {}
    subtype_concentration = quality_signals.get("subtype_concentration") or {}
    topn_boundary_pressure = quality_signals.get("topn_boundary_pressure") or {}
    subtype_trend = quality_signal_trend.get("subtype_concentration") or {}
    topn_trend = quality_signal_trend.get("topn_boundary_pressure") or {}
    structure = payload.get("mainlist_structure") or {}
    ftype_rows = structure.get("ftype_counts") or []
    dominant_ftype = ftype_rows[0] if ftype_rows else None
    if subtype_concentration.get("status") == "high_concentration":
        findings.append(
            "正式主榜对子类型存在明显集中，当前主榜更像单一债券风格优选，而不是广义债基统一优选。"
        )
    if int(subtype_trend.get("consecutive_high_concentration_count", 0) or 0) >= 2:
        findings.append(
            "债基子类型集中并非单日偶发，最近连续审计显示主榜长期维持高集中状态。"
        )
    if int(summary.get("long_only_watch_count", 0) or 0) > int(summary.get("short_only_watch_count", 0) or 0):
        findings.append(
            "观察池以长期单边信号为主，说明不少长期质量较强的债基未能获得正式主榜所需的双周期共振。"
        )
    if topn_boundary_pressure.get("status") in {"moderate_pressure", "high_pressure"}:
        findings.append(
            "正式主榜 cutoff 外存在距离分数线很近且校验通过的临界样本，当前主榜边界更像发布收敛而非自然断层。"
        )
    if int(topn_trend.get("consecutive_high_pressure_count", 0) or 0) >= 2:
        findings.append(
            "主榜边界压力并非单日波动，最近连续审计均显示高压力状态。"
        )
    cutoff_boundary_comparison = payload.get("cutoff_boundary_comparison") or {}
    if (
        cutoff_boundary_comparison.get("available")
        and int(cutoff_boundary_comparison.get("near_miss_better_metric_count", 0) or 0)
        >= int(cutoff_boundary_comparison.get("bottom_better_metric_count", 0) or 0)
        and int(cutoff_boundary_comparison.get("near_miss_better_metric_count", 0) or 0) > 0
    ):
        findings.append(
            "主榜末位样本与 cutoff 外 strict_match 临界样本对比显示，后者在多项核心指标上并不处于明显劣势。"
        )
    if int(summary.get("mainlist_soft_warning_count", 0) or 0) > 0:
        findings.append(
            "正式主榜内仍包含软告警通过样本，严格一致样本与软告警样本尚未在正式推荐层完全隔离。"
        )
    replacement_summary = payload.get("soft_warning_replacement_summary") or {}
    if int(replacement_summary.get("same_ftype_replaceable_count", 0) or 0) > 0:
        findings.append(
            "部分主榜软告警样本存在同子类型 strict_match 临界候选，可继续评估是否需要收紧正式主榜边界。"
        )
    edge_governance_audit = payload.get("edge_governance_audit") or {}
    if edge_governance_audit.get("available"):
        findings.append(
            "当前主榜边缘替换已从真实替换对层面被审计，可直接复核结构改善是否建立在可接受的质量代价上。"
        )
        if edge_governance_audit.get("status") == "structure_improved_with_controlled_tradeoff":
            findings.append(
                "边缘替换降低了主导子类型占比，且替换对未显示明显的系统性质量劣化。"
            )
        elif edge_governance_audit.get("status") in {
            "structure_improved_with_mixed_tradeoff",
            "structure_improved_but_quality_risk",
        }:
            findings.append(
                "边缘替换虽然改善了结构集中，但替换对中已出现质量代价信号，当前不宜继续放宽替换预算。"
            )
    if int(summary.get("weighted_union_candidate_count", 0) or 0) > 0:
        findings.append(
            "主榜外存在份额合并类高分排除样本，说明主榜外高分记录并不等同于真实错杀。"
        )
    return findings


def _build_recommendations(payload: dict) -> list[dict]:
    summary = payload.get("summary") or {}
    quality_signals = payload.get("quality_signals") or {}
    quality_signal_trend = payload.get("quality_signal_trend") or {}
    subtype_concentration = quality_signals.get("subtype_concentration") or {}
    topn_boundary_pressure = quality_signals.get("topn_boundary_pressure") or {}
    subtype_trend = quality_signal_trend.get("subtype_concentration") or {}
    topn_trend = quality_signal_trend.get("topn_boundary_pressure") or {}
    recommendations = [
        {
            "topic": "债基主榜语义",
            "recommendation": "将当前正式主榜明确表述为“稳健债基优选”，避免被误解为覆盖全部债券风格的统一最优榜。",
        }
    ]
    if topn_boundary_pressure.get("status") in {"moderate_pressure", "high_pressure"}:
        recommendations.append(
            {
                "topic": "主榜边界复核",
                "recommendation": "持续跟踪 strict-match 临界样本占主榜比例和 cutoff 边界指标对照，先证明当前主榜边界是有效发布阈值而不是机械收敛，再决定是否改成更显式的分数阈值制。",
            }
        )
    if subtype_concentration.get("status") in {"moderate_concentration", "high_concentration"}:
        recommendations.append(
            {
                "topic": "子类型集中度治理",
                "recommendation": "每日跟踪 dominant_ftype 占比与 HHI，若连续高位，应优先复核债基主榜是否被单一子类型长期主导。",
            }
        )
    if (
        int(subtype_trend.get("consecutive_high_concentration_count", 0) or 0) >= 3
        and int(topn_trend.get("consecutive_high_pressure_count", 0) or 0) >= 3
    ):
        recommendations.append(
            {
                "topic": "正式边界重审",
                "recommendation": "若子类型集中与主榜边界压力连续多次同时高位，下一阶段应正式审查债基主榜是否改成更显式的分数阈值制或子类型分层制。",
            }
        )
    if int(summary.get("long_only_watch_count", 0) or 0) > 0:
        recommendations.append(
            {
                "topic": "长期强势债基分流",
                "recommendation": "为长期强势但近期动能不足的债基单独输出中长期观察层，避免它们长期被埋在普通观察池中。",
            }
        )
    if int(summary.get("mainlist_soft_warning_count", 0) or 0) > 0:
        recommendations.append(
            {
                "topic": "正式主榜严格一致边界",
                "recommendation": "将 soft_pass_with_warnings 至少做显式标签，后续再评估是否下调到观察层。",
            }
        )
    replacement_summary = payload.get("soft_warning_replacement_summary") or {}
    if int(replacement_summary.get("same_ftype_replaceable_count", 0) or 0) > 0:
        recommendations.append(
            {
                "topic": "软告警替代性审查",
                "recommendation": "优先复核存在同子类型 strict_match 替代候选的主榜软告警样本，再决定是否调整主榜收录边界。",
            }
        )
    tightening_simulation = payload.get("type_mismatch_tightening_simulation") or {}
    if tightening_simulation.get("available"):
        recommendations.append(
            {
                "topic": "严格一致收紧模拟",
                "recommendation": "先以 type_mismatch 主榜样本为范围运行模拟 diff 审计，再决定是否收紧正式主榜到 strict_match 优先。",
            }
        )
    weighted_score_conflict = payload.get("weighted_score_conflict_audit") or {}
    if weighted_score_conflict.get("available"):
        recommendations.append(
            {
                "topic": "债基终裁冲突复核",
                "recommendation": "进一步审查 weighted_score 与 strict-match 一致性边界的优先级，明确债基正式主榜到底是高分优先还是严格一致优先。",
            }
        )
    no_topn_simulation = payload.get("no_topn_simulation") or {}
    if no_topn_simulation.get("available"):
        recommendations.append(
            {
                "topic": "放宽主榜边界前置审计",
                "recommendation": "不要直接把主榜边界放宽到全部 eligible_pool；应先补正式质量阈值，再决定是否用更显式的阈值制替代当前发布收敛。",
            }
        )
    edge_governance_audit = payload.get("edge_governance_audit") or {}
    if edge_governance_audit.get("available"):
        recommendations.append(
            {
                "topic": "边缘替换预算约束",
                "recommendation": "继续把债基边缘替换保持在小规模预算内，并以真实替换对审计结果作为唯一放宽依据，不要按感觉扩大 budget 或 gap。",
            }
        )
    return recommendations


def _build_strict_near_miss_focus_rows(
    decision_payload: dict,
    record_lookup: dict[str, dict],
    *,
    gap_threshold: float = 1.0,
    limit: int = 20,
) -> tuple[int, list[dict]]:
    records = list(decision_payload.get("candidate_records") or [])
    top_cutoff_score = float(
        (decision_payload.get("final_screening_summary") or {}).get("top_cutoff_score") or 0.0
    )
    candidates = []
    for record in records:
        if record.get("candidate_stage") != "eligible_pool":
            continue
        if record.get("list_tier") in {"main", "watch"}:
            continue
        if str(record.get("autostock_validation_status") or "").strip() != "strict_match":
            continue
        if str(record.get("fund_category") or "").strip() != "交集优选":
            continue
        weighted_score = float(record.get("weighted_score", 0.0) or 0.0)
        gap_to_cutoff = round(max(top_cutoff_score - weighted_score, 0.0), 4)
        if gap_to_cutoff > float(gap_threshold):
            continue
        db_row = record_lookup.get(record["fcode"], {})
        candidates.append(
            {
                "fcode": record["fcode"],
                "fund_name": record.get("fund_name"),
                "ftype": db_row.get("ftype"),
                "weighted_score": round(weighted_score, 4),
                "gap_to_cutoff": gap_to_cutoff,
                "return_1y": db_row.get("return_1y"),
                "return_3y": db_row.get("return_3y"),
                "annual_return_3y": db_row.get("annual_return_3y"),
                "sharpe_1y": db_row.get("sharpe_1y"),
                "sharpe_3y": db_row.get("sharpe_3y"),
                "mdd_1y": db_row.get("mdd_1y"),
                "mdd_3y": db_row.get("mdd_3y"),
                "validation_status": record.get("autostock_validation_status"),
            }
        )
    candidates.sort(key=lambda item: (item["gap_to_cutoff"], -item["weighted_score"], item["fcode"]))
    return len(candidates), candidates[:limit]


def _build_long_only_focus_rows(
    decision_payload: dict,
    record_lookup: dict[str, dict],
    *,
    limit: int = 20,
) -> tuple[int, list[dict]]:
    candidates = []
    for record in (decision_payload.get("candidate_records") or []):
        if record.get("list_tier") != "watch":
            continue
        if str(record.get("watch_only_reason_code") or "").strip() != "LONG_ONLY_SIGNAL":
            continue
        db_row = record_lookup.get(record["fcode"], {})
        candidates.append(
            {
                "fcode": record["fcode"],
                "fund_name": record.get("fund_name"),
                "ftype": db_row.get("ftype"),
                "weighted_score": round(float(record.get("weighted_score", 0.0) or 0.0), 4),
                "watchlist_gap_to_cutoff": record.get("watchlist_gap_to_cutoff"),
                "return_1y": db_row.get("return_1y"),
                "return_3y": db_row.get("return_3y"),
                "annual_return_3y": db_row.get("annual_return_3y"),
                "sharpe_1y": db_row.get("sharpe_1y"),
                "sharpe_3y": db_row.get("sharpe_3y"),
                "mdd_1y": db_row.get("mdd_1y"),
                "mdd_3y": db_row.get("mdd_3y"),
                "watchlist_reasons": list(record.get("watchlist_reasons") or []),
            }
        )
    candidates.sort(key=lambda item: (-item["weighted_score"], item["fcode"]))
    return len(candidates), candidates[:limit]


def build_bond_candidate_layers_payload(
    *,
    db_path: Optional[str] = None,
    decision_path: Optional[str | Path] = None,
    decision_dir: Optional[str | Path] = None,
    near_miss_gap_threshold: float = 1.0,
    near_miss_limit: int = 20,
    long_only_limit: int = 20,
) -> dict:
    """构建债基临界优质副榜与长期强势观察层载荷。"""
    decision_file, decision_payload, decision_source = resolve_bond_decision_payload(
        decision_path=decision_path,
        decision_dir=decision_dir,
    )
    candidate_records = list(decision_payload.get("candidate_records") or [])
    with open_readonly_db(db_path) as conn:
        record_lookup = _query_rows_by_codes(
            conn,
            [record.get("fcode") for record in candidate_records],
        )

    strict_near_miss_count, strict_near_miss_rows = _build_strict_near_miss_focus_rows(
        decision_payload,
        record_lookup,
        gap_threshold=near_miss_gap_threshold,
        limit=near_miss_limit,
    )
    long_only_focus_count, long_only_focus_rows = _build_long_only_focus_rows(
        decision_payload,
        record_lookup,
        limit=long_only_limit,
    )
    summary = {
        "decision_generated_at": decision_payload.get("generated_at"),
        "near_miss_gap_threshold": float(near_miss_gap_threshold),
        "strict_near_miss_count": int(strict_near_miss_count),
        "strict_near_miss_sample_count": int(len(strict_near_miss_rows)),
        "long_only_focus_count": int(long_only_focus_count),
        "long_only_focus_sample_count": int(len(long_only_focus_rows)),
    }
    return {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "db_path": db_path or "database/bond_funds.db",
        "decision_artifact": str(decision_file),
        "decision_source": decision_source,
        "summary": summary,
        "strict_near_miss_candidates": strict_near_miss_rows,
        "long_only_observation_candidates": long_only_focus_rows,
        "notes": [
            "该产物仅用于债基审计与人工复核，不改变正式主榜、正式观察池和正式推荐语义。",
            "临界优质副榜仅承接距离主榜 cutoff 较近且 strict_match 的交集优选样本。",
            "长期强势观察层仅承接 LONG_ONLY_SIGNAL 债基，避免与正式主榜混排。",
        ],
    }


def render_bond_candidate_layers_markdown(payload: dict) -> str:
    """渲染债基临界优质副榜与长期强势观察层 Markdown。"""
    summary = payload.get("summary") or {}
    lines = [
        "# 债基临界优质副榜与长期强势观察层",
        "",
        f"- 生成时间: {payload.get('generated_at', '')}",
        f"- 决策账本: {payload.get('decision_artifact', '')}",
        f"- 账本选择模式: {(payload.get('decision_source') or {}).get('selection_mode', '—')}",
        f"- 输入业务日: {((payload.get('decision_source') or {}).get('input_lineage') or {}).get('today_date', '—')}",
        "",
        "## 摘要",
        "",
        f"- 临界优质 gap 阈值: {summary.get('near_miss_gap_threshold', '—')}",
        f"- 临界优质副榜样本数: {summary.get('strict_near_miss_count', 0)}",
        f"- 临界优质副榜展示数: {summary.get('strict_near_miss_sample_count', 0)}",
        f"- 长期强势观察样本数: {summary.get('long_only_focus_count', 0)}",
        f"- 长期强势观察展示数: {summary.get('long_only_focus_sample_count', 0)}",
        "",
        "## 使用说明",
        "",
    ]
    for note in payload.get("notes") or []:
        lines.append(f"- {note}")

    lines.extend([
        "",
        "## 临界优质副榜",
        "",
    ])
    strict_near_miss_rows = payload.get("strict_near_miss_candidates") or []
    if not strict_near_miss_rows:
        lines.append("- 当前无满足阈值的临界优质债基。")
    else:
        for item in strict_near_miss_rows:
            lines.append(
                f"- {item['fcode']} {item['fund_name']} [{item.get('ftype') or '—'}]: "
                f"weighted={item['weighted_score']}, gap={item['gap_to_cutoff']}, "
                f"1Y={item.get('return_1y')}, 3Y={item.get('return_3y')}, "
                f"3Y_annual={item.get('annual_return_3y')}, "
                f"1Y_sharpe={item.get('sharpe_1y')}, 3Y_sharpe={item.get('sharpe_3y')}"
            )

    lines.extend([
        "",
        "## 长期强势观察层",
        "",
    ])
    long_only_focus_rows = payload.get("long_only_observation_candidates") or []
    if not long_only_focus_rows:
        lines.append("- 当前无 LONG_ONLY_SIGNAL 债基。")
    else:
        for item in long_only_focus_rows:
            lines.append(
                f"- {item['fcode']} {item['fund_name']} [{item.get('ftype') or '—'}]: "
                f"weighted={item['weighted_score']}, gap={item.get('watchlist_gap_to_cutoff')}, "
                f"1Y={item.get('return_1y')}, 3Y={item.get('return_3y')}, "
                f"3Y_annual={item.get('annual_return_3y')}, "
                f"1Y_sharpe={item.get('sharpe_1y')}, 3Y_sharpe={item.get('sharpe_3y')}, "
                f"reasons={json.dumps(item.get('watchlist_reasons') or [], ensure_ascii=False)}"
            )
    return "\n".join(lines).strip() + "\n"


def write_bond_candidate_layers_artifacts(
    payload: dict,
    *,
    output_dir: str = DEFAULT_AUDIT_DIR,
    timestamp: Optional[str] = None,
) -> Tuple[Path, Path]:
    """写出债基临界优质副榜与长期强势观察层产物。"""
    ts = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
    artifact_dir = Path(output_dir)
    if not artifact_dir.is_absolute():
        artifact_dir = (PROJECT_ROOT / artifact_dir).resolve()
    artifact_dir.mkdir(parents=True, exist_ok=True)

    json_path = artifact_dir / f"bond_mainlist_candidate_layers_{ts}.json"
    md_path = artifact_dir / f"bond_mainlist_candidate_layers_{ts}.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(render_bond_candidate_layers_markdown(payload), encoding="utf-8")
    return json_path, md_path


def build_bond_mainlist_audit_payload(
    *,
    db_path: Optional[str] = None,
    decision_path: Optional[str | Path] = None,
    decision_dir: Optional[str | Path] = None,
    previous_audit_dir: Optional[str | Path] = None,
    trend_history_limit: int = 14,
) -> dict:
    """构建债券型正式主榜专项审计载荷。"""
    decision_file, decision_payload, decision_source = resolve_bond_decision_payload(
        decision_path=decision_path,
        decision_dir=decision_dir,
    )
    candidate_records = list(decision_payload.get("candidate_records") or [])
    mainlist_records = [
        record for record in candidate_records
        if record.get("candidate_stage") == "final_mainlist"
    ]
    watch_records = [
        record for record in candidate_records
        if record.get("list_tier") == "watch"
    ]

    with open_readonly_db(db_path) as conn:
        record_lookup = _query_rows_by_codes(
            conn,
            [record.get("fcode") for record in candidate_records],
        )
        universe_rows = _query_bond_universe_rows(conn)

    mainlist_db_rows = [
        record_lookup[record["fcode"]]
        for record in mainlist_records
        if record.get("fcode") in record_lookup
    ]
    autostock_detail_map = _load_autostock_validation_detail_map(
        decision_payload,
        decision_file=decision_file,
    )
    universe_ftype_counter = Counter(
        str(row.get("ftype") or "").strip() or "<unknown>"
        for row in universe_rows
    )

    near_miss_count, near_miss_within_1_score_count, near_miss_rows = _build_near_miss_rows(
        decision_payload,
        record_lookup,
    )
    watchlist_rows = _build_watchlist_rows(decision_payload, record_lookup)
    weighted_union_samples = _build_weighted_union_samples(decision_payload)
    edge_governance_audit = _build_edge_governance_audit(
        decision_payload,
        record_lookup,
        mainlist_db_rows,
    )
    mainlist_soft_warning_samples = _build_mainlist_soft_warning_samples(
        mainlist_records,
        record_lookup,
    )
    soft_warning_reason_counts = _build_soft_warning_reason_counts(
        mainlist_soft_warning_samples
    )
    soft_warning_mainlist_reviews = _build_soft_warning_mainlist_reviews(
        mainlist_soft_warning_samples,
        decision_payload,
        autostock_detail_map,
    )
    type_mismatch_mainlist_reviews = _build_type_mismatch_mainlist_reviews(
        mainlist_soft_warning_samples,
        autostock_detail_map,
    )
    soft_warning_replacement_summary, soft_warning_replacement_reviews = (
        _build_soft_warning_replacement_reviews(
            mainlist_soft_warning_samples,
            near_miss_rows,
            gap_threshold=1.0,
        )
    )
    type_mismatch_tightening_simulation = _build_type_mismatch_tightening_simulation(
        mainlist_records,
        record_lookup,
        soft_warning_replacement_reviews,
    )
    weighted_score_conflict_audit = _build_weighted_score_conflict_audit(
        soft_warning_replacement_reviews,
        score_gap_warn_threshold=5.0,
    )
    no_topn_simulation = _build_no_topn_simulation(
        candidate_records,
        record_lookup,
    )
    bottom_mainlist_rows = _build_bottom_mainlist_rows(
        decision_payload,
        record_lookup,
    )
    cutoff_boundary_comparison = _build_cutoff_boundary_comparison(
        bottom_mainlist_rows,
        near_miss_rows,
        gap_threshold=1.0,
    )
    quality_signals = {
        "subtype_concentration": _build_subtype_concentration_signal(
            mainlist_db_rows,
            universe_rows,
        ),
        "topn_boundary_pressure": _build_topn_boundary_pressure_signal(
            final_mainlist_count=len(mainlist_records),
            near_miss_rows=near_miss_rows,
            cutoff_boundary_comparison=cutoff_boundary_comparison,
            no_topn_simulation=no_topn_simulation,
        ),
    }
    previous_audit_path, previous_audit_payload = resolve_previous_bond_mainlist_audit_payload(
        current_decision_artifact=str(decision_file),
        current_decision_generated_at=decision_payload.get("generated_at"),
        output_dir=previous_audit_dir or DEFAULT_AUDIT_DIR,
    )
    comparison_to_previous = _build_quality_signal_comparison(
        {
            "summary": {
                "decision_generated_at": decision_payload.get("generated_at"),
                "final_mainlist_count": len(mainlist_records),
                "top_n_trimmed": int(
                    (decision_payload.get("final_screening_summary") or {}).get("top_n_trimmed", 0) or 0
                ),
                "mainlist_soft_warning_count": len(mainlist_soft_warning_samples),
                "near_miss_within_1_score_count": near_miss_within_1_score_count,
            },
            "quality_signals": quality_signals,
        },
        previous_audit_payload,
        previous_audit_path,
    )
    recent_history_rows = load_recent_bond_mainlist_audit_history(
        current_decision_artifact=str(decision_file),
        current_decision_generated_at=decision_payload.get("generated_at"),
        output_dir=previous_audit_dir or DEFAULT_AUDIT_DIR,
        limit=trend_history_limit,
    )
    quality_signal_trend = _build_quality_signal_trend_summary(
        {
            "summary": {
                "decision_generated_at": decision_payload.get("generated_at"),
            },
            "quality_signals": quality_signals,
        },
        recent_history_rows,
    )

    summary = {
        "decision_generated_at": decision_payload.get("generated_at"),
        "final_mainlist_count": int(len(mainlist_records)),
        "watch_count": int(len(watch_records)),
        "eligible_pool_count": int(
            (decision_payload.get("final_screening_summary") or {}).get("eligible_pool_count", 0) or 0
        ),
        "weighted_union_candidate_count": int(sum(
            1 for record in candidate_records
            if record.get("candidate_stage") == "weighted_union_candidate"
        )),
        "top_cutoff_score": (
            round(float((decision_payload.get("final_screening_summary") or {}).get("top_cutoff_score") or 0.0), 4)
            if (decision_payload.get("final_screening_summary") or {}).get("top_cutoff_score") is not None else None
        ),
        "top_n_trimmed": int(
            (decision_payload.get("final_screening_summary") or {}).get("top_n_trimmed", 0) or 0
        ),
        "mainlist_soft_warning_count": int(len(mainlist_soft_warning_samples)),
        "long_only_watch_count": int(sum(
            1 for record in watch_records
            if str(record.get("watch_only_reason_code") or "").strip() == "LONG_ONLY_SIGNAL"
        )),
        "short_only_watch_count": int(sum(
            1 for record in watch_records
            if str(record.get("watch_only_reason_code") or "").strip() == "SHORT_ONLY_SIGNAL"
        )),
        "near_miss_count": int(near_miss_count),
        "near_miss_within_1_score_count": int(near_miss_within_1_score_count),
        "bottom_mainlist_sample_count": int(len(bottom_mainlist_rows)),
        "cutoff_boundary_comparison_available": bool(
            cutoff_boundary_comparison.get("available", False)
        ),
    }

    payload = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "db_path": db_path or "database/bond_funds.db",
        "decision_artifact": str(decision_file),
        "decision_source": decision_source,
        "summary": summary,
        "mainlist_structure": {
            "ftype_counts": _normalize_counter(
                Counter(
                    str(row.get("ftype") or "").strip() or "<unknown>"
                    for row in mainlist_db_rows
                ),
                selected_total=len(mainlist_db_rows),
                universe_counter=universe_ftype_counter,
                universe_total=len(universe_rows),
            ),
            "benchmark_family_counts": _normalize_counter(
                Counter(_bond_benchmark_family(row.get("benchmark")) for row in mainlist_db_rows),
                selected_total=len(mainlist_db_rows),
            ),
            "company_counts": _normalize_counter(
                Counter(
                    str(row.get("company_name") or "").strip() or "<unknown>"
                    for row in mainlist_db_rows
                ),
                selected_total=len(mainlist_db_rows),
            ),
        },
        "mainlist_metric_summary": _build_metric_summary(mainlist_db_rows, universe_rows),
        "mainlist_soft_warning_samples": mainlist_soft_warning_samples,
        "soft_warning_reason_counts": soft_warning_reason_counts,
        "soft_warning_mainlist_reviews": soft_warning_mainlist_reviews,
        "type_mismatch_mainlist_reviews": type_mismatch_mainlist_reviews,
        "soft_warning_replacement_summary": soft_warning_replacement_summary,
        "soft_warning_replacement_reviews": soft_warning_replacement_reviews,
        "type_mismatch_tightening_simulation": type_mismatch_tightening_simulation,
        "weighted_score_conflict_audit": weighted_score_conflict_audit,
        "no_topn_simulation": no_topn_simulation,
        "edge_governance_audit": edge_governance_audit,
        "bottom_mainlist_candidates": bottom_mainlist_rows,
        "cutoff_boundary_comparison": cutoff_boundary_comparison,
        "near_miss_candidates": near_miss_rows,
        "watchlist_top_candidates": watchlist_rows,
        "weighted_union_candidate_samples": weighted_union_samples,
        "quality_signals": quality_signals,
        "comparison_to_previous": comparison_to_previous,
        "quality_signal_trend": quality_signal_trend,
    }
    payload["findings"] = _build_findings(payload)
    payload["recommendations"] = _build_recommendations(payload)
    return payload


def render_bond_mainlist_audit_markdown(payload: dict) -> str:
    """渲染债券型主榜专项审计 Markdown。"""
    summary = payload.get("summary") or {}
    metric_summary = payload.get("mainlist_metric_summary") or {}

    lines = [
        "# 债券型正式主榜专项审计",
        "",
        f"- 生成时间: {payload.get('generated_at', '')}",
        f"- 决策账本: {payload.get('decision_artifact', '')}",
        f"- 决策生成时间: {summary.get('decision_generated_at', '')}",
        f"- 账本选择模式: {(payload.get('decision_source') or {}).get('selection_mode', '—')}",
        f"- 账本选择说明: {(payload.get('decision_source') or {}).get('selection_reason', '—')}",
        f"- 输入业务日: {((payload.get('decision_source') or {}).get('input_lineage') or {}).get('today_date', '—')}",
        "",
        "## 摘要",
        "",
        f"- 正式主榜数量: {summary.get('final_mainlist_count', 0)}",
        f"- 观察池数量: {summary.get('watch_count', 0)}",
        f"- 主榜前候选池数量: {summary.get('eligible_pool_count', 0)}",
        f"- 主榜边界截断淘汰: {summary.get('top_n_trimmed', 0)}",
        f"- 主榜末位得分: {summary.get('top_cutoff_score', '—')}",
        f"- 主榜软告警样本数: {summary.get('mainlist_soft_warning_count', 0)}",
        f"- 长期单边观察数: {summary.get('long_only_watch_count', 0)}",
        f"- 近期单边观察数: {summary.get('short_only_watch_count', 0)}",
        f"- 临界优质候选数: {summary.get('near_miss_count', 0)}",
        f"- 临界优质中距 cutoff 1 分内样本数: {summary.get('near_miss_within_1_score_count', 0)}",
        f"- 主榜末位对照样本数: {summary.get('bottom_mainlist_sample_count', 0)}",
        f"- cutoff 边界对照可用: {summary.get('cutoff_boundary_comparison_available', False)}",
        "",
        "## 主要发现",
        "",
    ]

    for finding in payload.get("findings") or []:
        lines.append(f"- {finding}")

    lines.extend([
        "",
        "## 主榜结构",
        "",
        "## 质量信号",
        "",
    ])

    subtype_concentration = (payload.get("quality_signals") or {}).get("subtype_concentration") or {}
    lines.extend([
        "### 子类型集中度",
        "",
        f"- 状态: {subtype_concentration.get('status', '—')}",
        f"- 主导子类型: {subtype_concentration.get('dominant_ftype', '—')}",
        f"- 主导子类型主榜占比: {subtype_concentration.get('dominant_ftype_selected_share_pct', '—')}%",
        f"- 主导子类型底池占比: {subtype_concentration.get('dominant_ftype_universe_share_pct', '—')}%",
        f"- 主导子类型放大倍数: {subtype_concentration.get('dominant_ftype_share_multiple', '—')}",
        f"- 子类型 HHI: {subtype_concentration.get('selected_ftype_hhi', '—')}",
        f"- 等效子类型数量: {subtype_concentration.get('selected_ftype_effective_count', '—')}",
        "",
        "### 主榜边界压力",
        "",
    ])

    topn_boundary_pressure = (payload.get("quality_signals") or {}).get("topn_boundary_pressure") or {}
    lines.extend([
        f"- 状态: {topn_boundary_pressure.get('status', '—')}",
        f"- strict-match 且距 cutoff 1 分内样本数: {topn_boundary_pressure.get('strict_near_miss_within_1_score_count', '—')}",
        f"- 该样本数 / 主榜样本数: {topn_boundary_pressure.get('strict_near_miss_within_1_score_share_vs_mainlist_pct', '—')}%",
        f"- cutoff 配对对照数: {topn_boundary_pressure.get('pair_review_count', '—')}",
        f"- 临界样本指标占优数: {topn_boundary_pressure.get('near_miss_metric_edge_count', '—')}",
        f"- 主榜末位指标占优数: {topn_boundary_pressure.get('bottom_mainlist_metric_edge_count', '—')}",
        f"- 放宽到全部 eligible_pool 的规模膨胀倍数: {topn_boundary_pressure.get('no_topn_count_multiple', '—')}",
        "",
        "### 与上次审计对比",
        "",
    ])

    comparison_to_previous = payload.get("comparison_to_previous") or {}
    if not comparison_to_previous.get("available"):
        lines.append(f"- 不可用: {comparison_to_previous.get('reason', 'no_previous_audit')}")
    else:
        lines.extend([
            f"- 上次审计文件: {comparison_to_previous.get('previous_artifact_path', '—')}",
            f"- 上次决策时间: {comparison_to_previous.get('previous_decision_generated_at', '—')}",
            f"- 主榜数量 delta: {(comparison_to_previous.get('summary_delta') or {}).get('final_mainlist_count_delta', '—')}",
            f"- 主榜边界截断 delta: {(comparison_to_previous.get('summary_delta') or {}).get('top_n_trimmed_delta', '—')}",
            f"- 软告警样本 delta: {(comparison_to_previous.get('summary_delta') or {}).get('mainlist_soft_warning_count_delta', '—')}",
            f"- cutoff 1分内临界样本 delta: {(comparison_to_previous.get('summary_delta') or {}).get('near_miss_within_1_score_count_delta', '—')}",
            f"- 子类型集中度状态: {((comparison_to_previous.get('subtype_concentration_delta') or {}).get('previous_status', '—'))} -> {((comparison_to_previous.get('subtype_concentration_delta') or {}).get('current_status', '—'))}",
            f"- 主导子类型占比 delta: {(comparison_to_previous.get('subtype_concentration_delta') or {}).get('dominant_share_pct_delta', '—')}",
            f"- HHI delta: {(comparison_to_previous.get('subtype_concentration_delta') or {}).get('selected_ftype_hhi_delta', '—')}",
            f"- 主榜边界压力状态: {((comparison_to_previous.get('topn_boundary_pressure_delta') or {}).get('previous_status', '—'))} -> {((comparison_to_previous.get('topn_boundary_pressure_delta') or {}).get('current_status', '—'))}",
            f"- strict-match 临界样本 delta: {(comparison_to_previous.get('topn_boundary_pressure_delta') or {}).get('strict_near_miss_within_1_score_count_delta', '—')}",
            f"- 临界样本占主榜比例 delta: {(comparison_to_previous.get('topn_boundary_pressure_delta') or {}).get('strict_near_miss_share_vs_mainlist_pct_delta', '—')}",
            f"- 放宽主榜边界膨胀倍数 delta: {(comparison_to_previous.get('topn_boundary_pressure_delta') or {}).get('no_topn_count_multiple_delta', '—')}",
        ])
        for warning in comparison_to_previous.get("warnings") or []:
            lines.append(f"- {warning}")

    lines.extend([
        "",
        "### 连续趋势",
        "",
    ])

    quality_signal_trend = payload.get("quality_signal_trend") or {}
    subtype_trend = quality_signal_trend.get("subtype_concentration") or {}
    topn_trend = quality_signal_trend.get("topn_boundary_pressure") or {}
    lines.extend([
        f"- 历史样本数: {quality_signal_trend.get('history_sample_count', '—')}",
        f"- 子类型集中当前状态: {subtype_trend.get('current_status', '—')}",
        f"- 子类型集中连续高位次数: {subtype_trend.get('consecutive_high_concentration_count', '—')}",
        f"- 子类型集中近7次命中率: {((subtype_trend.get('last_7') or {}).get('target_status_hit_rate_pct', '—'))}%",
        f"- 子类型集中近14次命中率: {((subtype_trend.get('last_14') or {}).get('target_status_hit_rate_pct', '—'))}%",
        f"- 主导子类型占比近7次均值: {((subtype_trend.get('last_7') or {}).get('avg_value', '—'))}",
        f"- 主导子类型占比近14次均值: {((subtype_trend.get('last_14') or {}).get('avg_value', '—'))}",
        f"- 主榜边界压力当前状态: {topn_trend.get('current_status', '—')}",
        f"- 主榜边界压力连续高位次数: {topn_trend.get('consecutive_high_pressure_count', '—')}",
        f"- 主榜边界压力近7次命中率: {((topn_trend.get('last_7') or {}).get('target_status_hit_rate_pct', '—'))}%",
        f"- 主榜边界压力近14次命中率: {((topn_trend.get('last_14') or {}).get('target_status_hit_rate_pct', '—'))}%",
        f"- 临界样本占主榜比例近7次均值: {((topn_trend.get('last_7') or {}).get('avg_value', '—'))}",
        f"- 临界样本占主榜比例近14次均值: {((topn_trend.get('last_14') or {}).get('avg_value', '—'))}",
        "",
        "### 主榜边缘替换代价审计",
        "",
    ])

    edge_governance_audit = payload.get("edge_governance_audit") or {}
    if not edge_governance_audit.get("available"):
        lines.append("- 当前无已应用的主榜边缘替换。")
    else:
        lines.extend([
            f"- 状态: {edge_governance_audit.get('status', '—')}",
            f"- 实际替换对数: {edge_governance_audit.get('replacement_count', 0)}",
            f"- 主导子类型: {edge_governance_audit.get('dominant_ftype_before', '—')} -> {edge_governance_audit.get('dominant_ftype_after', '—')}",
            f"- 主导子类型占比: {edge_governance_audit.get('dominant_ftype_before_share_pct', '—')}% -> {edge_governance_audit.get('dominant_ftype_after_share_pct', '—')}% "
            f"(delta {edge_governance_audit.get('dominant_ftype_share_delta_pct', '—')})",
            f"- 替换对平均分差: {edge_governance_audit.get('avg_score_gap', '—')}",
            f"- 替换对最大分差: {edge_governance_audit.get('max_score_gap', '—')}",
            f"- 替换入指标占优对数: {edge_governance_audit.get('replacement_in_metric_edge_count', '—')}",
            f"- 替换出指标占优对数: {edge_governance_audit.get('replacement_out_metric_edge_count', '—')}",
            f"- 指标混合/持平对数: {edge_governance_audit.get('mixed_or_tie_count', '—')}",
        ])
        for warning in edge_governance_audit.get("warnings") or []:
            lines.append(f"- {warning}")
        for item in edge_governance_audit.get("rows") or []:
            replacement_out = item.get("replacement_out") or {}
            replacement_in = item.get("replacement_in") or {}
            lines.append(
                f"- Pair {item.get('pair_index')}: "
                f"out={replacement_out.get('fcode')} {replacement_out.get('fund_name')} "
                f"-> in={replacement_in.get('fcode')} {replacement_in.get('fund_name')} | "
                f"score_gap={item.get('score_gap')} | verdict={item.get('verdict_label')}"
            )

    lines.extend([
        "",
        "### 子类型分布",
        "",
    ])
    for item in (payload.get("mainlist_structure") or {}).get("ftype_counts") or []:
        lines.append(
            f"- {item['name']}: {item['count']} 只，主榜占比 {item['selected_share_pct']}%，"
            f"底池占比 {item.get('universe_share_pct', '—')}%，放大倍数 {item.get('share_multiple', '—')}"
        )

    lines.extend([
        "",
        "### 基准家族分布",
        "",
    ])
    for item in (payload.get("mainlist_structure") or {}).get("benchmark_family_counts") or []:
        lines.append(
            f"- {item['name']}: {item['count']} 只，占比 {item['selected_share_pct']}%"
        )

    lines.extend([
        "",
        "### 公司集中度",
        "",
    ])
    for item in (payload.get("mainlist_structure") or {}).get("company_counts")[:10]:
        lines.append(
            f"- {item['name']}: {item['count']} 只，占比 {item['selected_share_pct']}%"
        )

    lines.extend([
        "",
        "## 主榜指标摘要",
        "",
    ])
    for metric_key, metric_data in metric_summary.items():
        lines.append(
            f"- {metric_key}: min={metric_data['min']}, median={metric_data['median']}, "
            f"max={metric_data['max']}, 主榜中位数相对底池分位={metric_data.get('median_vs_universe_pctile', '—')}"
        )

    lines.extend([
        "",
        "## 主榜软告警样本",
        "",
    ])
    soft_warning_samples = payload.get("mainlist_soft_warning_samples") or []
    if not soft_warning_samples:
        lines.append("- 当前正式主榜样本均为 strict_match。")
    else:
        for item in soft_warning_samples:
            lines.append(
                f"- {item['fcode']} {item['fund_name']}: {item['validation_status']} "
                f"{json.dumps(item.get('warning_reasons') or [], ensure_ascii=False)}"
            )

    lines.extend([
        "",
        "### 软告警原因分布",
        "",
    ])
    reason_rows = payload.get("soft_warning_reason_counts") or []
    if not reason_rows:
        lines.append("- 无")
    else:
        for item in reason_rows:
            lines.append(f"- {item['reason']}: {item['count']}")

    lines.extend([
        "",
        "### 主榜软告警逐只复核",
        "",
    ])
    review_rows = payload.get("soft_warning_mainlist_reviews") or []
    if not review_rows:
        lines.append("- 无")
    else:
        for item in review_rows:
            lines.append(
                f"- {item['fcode']} {item['fund_name']} [{item.get('ftype') or '—'}]: "
                f"rank={item.get('top_n_rank')}, weighted={item.get('weighted_score')}, "
                f"gap_above_cutoff={item.get('gap_above_cutoff')}, "
                f"labels={json.dumps(item.get('warning_reason_labels') or [], ensure_ascii=False)}, "
                f"warning_display={item.get('warning_display')}, "
                f"type_only={item.get('is_type_mismatch_only')}, "
                f"return_1w_only={item.get('is_return_1w_soft_mismatch_only')}"
            )

    lines.extend([
        "",
        "### 软告警替代候选审查",
        "",
    ])
    replacement_summary = payload.get("soft_warning_replacement_summary") or {}
    if not replacement_summary.get("available"):
        lines.append("- 当前无可用的软告警替代候选审查结果。")
    else:
        lines.append(
            f"- 审查范围: {replacement_summary.get('soft_warning_count', 0)} 只主榜软告警 "
            f"vs {replacement_summary.get('strict_near_miss_count', 0)} 只 strict-match 临界样本 "
            f"(gap<={replacement_summary.get('gap_threshold')})"
        )
        lines.append(
            f"- 匹配策略: {_format_matching_strategy(replacement_summary.get('matching_strategy'))}"
        )
        lines.append(
            f"- 可匹配替代候选: {replacement_summary.get('replaceable_count', 0)}，"
            f"去重后替代候选数 {replacement_summary.get('distinct_replacement_candidate_count', 0)}，"
            f"其中同子类型 {replacement_summary.get('same_ftype_replaceable_count', 0)}，"
            f"仅跨子类型 {replacement_summary.get('cross_ftype_only_count', 0)}，"
            f"指标占优 {replacement_summary.get('metric_edge_count', 0)}，"
            f"未匹配 {replacement_summary.get('unmatched_count', 0)}"
        )
        for warning in replacement_summary.get("warnings") or []:
            lines.append(f"- {warning}")
        for item in payload.get("soft_warning_replacement_reviews") or []:
            lines.append(
                f"- {item['fcode']} {item['fund_name']} [{item.get('ftype') or '—'}]: "
                f"warning={item.get('warning_display')}, replacement={item.get('replacement_fcode')} {item.get('replacement_fund_name')}, "
                f"same_ftype={item.get('replacement_same_ftype')}, verdict={item.get('replacement_verdict_label')}, "
                f"replacement_gap={item.get('replacement_gap_to_cutoff')}"
            )

    lines.extend([
        "",
        "### type_mismatch 主榜样本专项复核",
        "",
    ])
    mismatch_rows = payload.get("type_mismatch_mainlist_reviews") or []
    if not mismatch_rows:
        lines.append("- 无")
    else:
        for item in mismatch_rows:
            lines.append(
                f"- {item['fcode']} {item['fund_name']} [{item.get('ftype') or '—'}]: "
                f"rank={item.get('top_n_rank')}, weighted={item.get('weighted_score')}, "
                f"local_type={item.get('local_fund_type')}, autostock_type={item.get('autostock_fund_type')}, "
                f"type_match={item.get('type_match')}, type_fields_available={item.get('type_fields_available')}, "
                f"nav_match_ok={item.get('nav_match_ok')}, "
                f"1w_diff={item.get('return_1w_diff')}, 1m_diff={item.get('return_1m_diff')}, "
                f"3m_diff={item.get('return_3m_diff')}, 6m_diff={item.get('return_6m_diff')}, "
                f"1y_diff={item.get('return_1y_diff')}"
            )

    lines.extend([
        "",
        "### type_mismatch 严格一致收紧模拟",
        "",
    ])
    tightening_simulation = payload.get("type_mismatch_tightening_simulation") or {}
    if not tightening_simulation.get("available"):
        lines.append("- 当前无可用的 type_mismatch 收紧模拟结果。")
    else:
        lines.append(
            f"- 模拟范围: 替换出 {tightening_simulation.get('replaced_out_count', 0)} 只 type_mismatch 主榜样本，"
            f"替换入 {tightening_simulation.get('replaced_in_count', 0)} 只同子类型 strict-match 临界样本"
        )
        for warning in tightening_simulation.get("warnings") or []:
            lines.append(f"- {warning}")
        lines.append("- 指标中位数变化:")
        for item in tightening_simulation.get("metric_deltas") or []:
            lines.append(
                f"  - {item['metric']}: current={item.get('current_median')}, "
                f"simulated={item.get('simulated_median')}, delta={item.get('median_delta')}"
            )
        lines.append("- 替换出样本:")
        for item in tightening_simulation.get("replaced_out_rows") or []:
            lines.append(
                f"  - {item['fcode']} {item['fund_name']} [{item.get('ftype') or '—'}]: "
                f"weighted={item.get('weighted_score')}, warnings={json.dumps(item.get('warning_reasons') or [], ensure_ascii=False)}"
            )
        lines.append("- 替换入样本:")
        for item in tightening_simulation.get("replaced_in_rows") or []:
            lines.append(
                f"  - {item['fcode']} {item['fund_name']} [{item.get('ftype') or '—'}]: "
                f"weighted={item.get('weighted_score')}, validation={item.get('validation_status')}"
            )

    lines.extend([
        "",
        "### weighted_score 终裁冲突审计",
        "",
    ])
    conflict_audit = payload.get("weighted_score_conflict_audit") or {}
    if not conflict_audit.get("available"):
        lines.append("- 当前无可用的 weighted_score 终裁冲突结果。")
    else:
        lines.append(
            f"- 冲突样本数: {conflict_audit.get('conflict_count', 0)}，"
            f"同子类型冲突 {conflict_audit.get('same_ftype_conflict_count', 0)}，"
            f"soft warning 总分更高 {conflict_audit.get('weighted_score_soft_higher_count', 0)}，"
            f"strict-match 指标占优 {conflict_audit.get('metric_edge_count', 0)}，"
            f"大分差样本 {conflict_audit.get('large_score_gap_count', 0)}"
        )
        for warning in conflict_audit.get("warnings") or []:
            lines.append(f"- {warning}")
        for item in conflict_audit.get("rows") or []:
            lines.append(
                f"- {item['fcode']} {item['fund_name']}: warning={item.get('warning_display')}, "
                f"soft_score={item.get('weighted_score')}, strict_match={item.get('replacement_fcode')} {item.get('replacement_fund_name')}, "
                f"strict_score={item.get('replacement_weighted_score')}, "
                f"score_gap={item.get('score_gap_soft_minus_strict')}, "
                f"same_ftype={item.get('replacement_same_ftype')}, "
                f"metric_edge={item.get('near_miss_better_metric_count')}>{item.get('bottom_better_metric_count')}, "
                f"verdict={item.get('replacement_verdict_label')}"
            )

    lines.extend([
        "",
        "### 放宽主榜边界模拟审计",
        "",
    ])
    no_topn_simulation = payload.get("no_topn_simulation") or {}
    if not no_topn_simulation.get("available"):
        lines.append("- 当前无可用的放宽主榜边界模拟结果。")
    else:
        lines.append(
            f"- 当前正式主榜数量: {no_topn_simulation.get('current_main_count', 0)}"
        )
        lines.append(
            f"- 放宽到全部 eligible_pool 后模拟数量: {no_topn_simulation.get('simulated_count', 0)} "
            f"(膨胀倍数 {no_topn_simulation.get('count_multiple')})"
        )
        lines.append(
            f"- 当前主榜校验状态分布: {json.dumps(no_topn_simulation.get('current_status_counts') or [], ensure_ascii=False)}"
        )
        lines.append(
            f"- 模拟后校验状态分布: {json.dumps(no_topn_simulation.get('simulated_status_counts') or [], ensure_ascii=False)}"
        )
        for warning in no_topn_simulation.get("warnings") or []:
            lines.append(f"- {warning}")
        lines.append("- 指标中位数变化:")
        for item in no_topn_simulation.get("metric_deltas") or []:
            lines.append(
                f"  - {item['metric']}: current={item.get('current_median')}, "
                f"simulated={item.get('simulated_median')}, delta={item.get('median_delta')}"
            )

    lines.extend([
        "",
        "## 主榜末位样本",
        "",
    ])
    bottom_mainlist_rows = payload.get("bottom_mainlist_candidates") or []
    if not bottom_mainlist_rows:
        lines.append("- 当前无正式主榜末位样本。")
    else:
        for item in bottom_mainlist_rows:
            lines.append(
                f"- {item['fcode']} {item['fund_name']} [{item.get('ftype') or '—'}]: "
                f"weighted={item['weighted_score']}, gap_above_cutoff={item['gap_above_cutoff']}, "
                f"1Y={item.get('return_1y')}, 3Y={item.get('return_3y')}, "
                f"3Y_annual={item.get('annual_return_3y')}, "
                f"1Y_sharpe={item.get('sharpe_1y')}, 3Y_sharpe={item.get('sharpe_3y')}, "
                f"warnings={json.dumps(item.get('warning_reasons') or [], ensure_ascii=False)}"
            )

    lines.extend([
        "",
        "## 临界优质候选",
        "",
    ])
    for item in payload.get("near_miss_candidates") or []:
        lines.append(
            f"- {item['fcode']} {item['fund_name']} [{item.get('ftype') or '—'}]: "
            f"weighted={item['weighted_score']}, gap={item['gap_to_cutoff']}, "
            f"1Y={item.get('return_1y')}, 3Y={item.get('return_3y')}, "
            f"1Y_sharpe={item.get('sharpe_1y')}, 3Y_sharpe={item.get('sharpe_3y')}, "
            f"validation={item.get('validation_status')}"
        )

    lines.extend([
        "",
        "## cutoff 边界对照",
        "",
    ])
    cutoff_boundary_comparison = payload.get("cutoff_boundary_comparison") or {}
    if not cutoff_boundary_comparison.get("available"):
        lines.append("- 当前样本不足，无法形成主榜末位与临界样本的直接对照。")
    else:
        lines.append(
            f"- 对照范围: 主榜末位 {cutoff_boundary_comparison.get('bottom_sample_count', 0)} 只 "
            f"vs cutoff 外 strict_match 且 gap<={cutoff_boundary_comparison.get('gap_threshold')} 的 "
            f"{cutoff_boundary_comparison.get('near_miss_sample_count', 0)} 只"
        )
        lines.append(
            f"- 逐只配对对照数: {cutoff_boundary_comparison.get('pair_review_count', 0)}"
        )
        lines.append(
            f"- 指标胜出统计: near_miss={cutoff_boundary_comparison.get('near_miss_better_metric_count', 0)}, "
            f"bottom_mainlist={cutoff_boundary_comparison.get('bottom_better_metric_count', 0)}, "
            f"tie={cutoff_boundary_comparison.get('tie_metric_count', 0)}"
        )
        verdict_rows = cutoff_boundary_comparison.get("pair_review_verdict_counts") or []
        if verdict_rows:
            verdict_text = ", ".join(
                f"{item['verdict']}={item['count']}"
                for item in verdict_rows
            )
            lines.append(f"- 配对结论分布: {verdict_text}")
        for warning in cutoff_boundary_comparison.get("warnings") or []:
            lines.append(f"- {warning}")
        for item in cutoff_boundary_comparison.get("metric_comparisons") or []:
            lines.append(
                f"- {item['metric']}: bottom_median={item['bottom_mainlist_median']}, "
                f"near_miss_median={item['near_miss_median']}, "
                f"preferred={item['preferred_direction']}, relation={item['relation']}"
            )

        lines.extend([
            "",
            "### 逐只配对对照",
            "",
        ])
        for item in cutoff_boundary_comparison.get("pair_reviews") or []:
            bottom_row = item.get("bottom_mainlist") or {}
            near_miss_row = item.get("near_miss") or {}
            lines.append(
                f"- Pair {item.get('pair_index')}: "
                f"主榜 {bottom_row.get('fcode')} {bottom_row.get('fund_name')} "
                f"vs 临界 {near_miss_row.get('fcode')} {near_miss_row.get('fund_name')} | "
                f"score_gap={item.get('score_gap')} | same_ftype={item.get('same_ftype')} | verdict={item.get('verdict')}"
            )
            for metric_row in (item.get("metric_comparison") or {}).get("metric_rows") or []:
                lines.append(
                    f"  - {metric_row['metric']}: bottom={metric_row['bottom_mainlist_value']}, "
                    f"near={metric_row['near_miss_value']}, "
                    f"preferred={metric_row['preferred_direction']}, relation={metric_row['relation']}"
                )

    lines.extend([
        "",
        "## 观察池前列",
        "",
    ])
    for item in payload.get("watchlist_top_candidates") or []:
        lines.append(
            f"- {item['fcode']} {item['fund_name']} [{item.get('ftype') or '—'}]: "
            f"weighted={item['weighted_score']}, reason={item.get('watch_only_reason_code')}, "
            f"gap={item.get('watchlist_gap_to_cutoff')}, 1Y={item.get('return_1y')}, 3Y={item.get('return_3y')}"
        )

    lines.extend([
        "",
        "## 份额合并排除样本",
        "",
    ])
    for item in payload.get("weighted_union_candidate_samples") or []:
        lines.append(
            f"- {item['fcode']} {item['fund_name']}: stage=weighted_union_candidate, "
            f"weighted={item['weighted_score']}, category={item.get('fund_category')}"
        )

    lines.extend([
        "",
        "## 建议",
        "",
    ])
    for item in payload.get("recommendations") or []:
        lines.append(f"- {item.get('topic')}: {item.get('recommendation')}")

    return "\n".join(lines).strip() + "\n"


def write_bond_mainlist_audit_artifacts(
    payload: dict,
    *,
    output_dir: str = DEFAULT_AUDIT_DIR,
    timestamp: Optional[str] = None,
) -> Tuple[Path, Path]:
    """写出债券型正式主榜专项审计产物。"""
    ts = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
    artifact_dir = Path(output_dir)
    if not artifact_dir.is_absolute():
        artifact_dir = (PROJECT_ROOT / artifact_dir).resolve()
    artifact_dir.mkdir(parents=True, exist_ok=True)

    json_path = artifact_dir / f"bond_mainlist_quality_audit_{ts}.json"
    md_path = artifact_dir / f"bond_mainlist_quality_audit_{ts}.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(render_bond_mainlist_audit_markdown(payload), encoding="utf-8")
    return json_path, md_path
