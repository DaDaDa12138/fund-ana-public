#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
VENV_PYTHON = PROJECT_ROOT / ".venv" / "bin" / "python"

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.config import load_settings  # noqa: E402
from scripts.audit_runtime_utils import (  # noqa: E402
    build_audit_artifact_path as _build_audit_artifact_path,
    clean_text as _clean_text,
    resolve_artifact_project_path as _resolve_artifact_project_path,
    resolve_run_payload as _resolve_run_payload,
)


_PRIORITY_RANK = {"P0": 0, "P1": 1, "P2": 2, "P3": 3}

_FACTOR_RULES: list[tuple[str, tuple[str, ...]]] = [
    ("annual_return", ("1年年化收益", "below_annual_floor")),
    ("weighted_score", ("综合加权得分", "below_weighted_floor")),
    ("short_term_score", ("近期得分", "below_short_floor")),
    ("long_term_score", ("长期得分", "below_long_floor")),
    ("sharpe", ("1年夏普比率", "below_sharpe_floor")),
    ("sortino", ("1年索提诺比率", "below_sortino_floor")),
    ("calmar", ("1年卡玛比率", "below_calmar_floor")),
    ("positive_ratio", ("1年正收益比率", "below_positive_ratio_floor")),
    ("max_drawdown", ("1年最大回撤", "mdd_too_high")),
    ("volatility", ("1年波动率", "volatility_too_high")),
    ("scale", ("基金规模", "scale_too_small")),
    ("fee", ("费率合计", "fee_too_high")),
]

_FACTOR_LABELS = {
    "annual_return": "1年年化收益",
    "weighted_score": "综合加权得分",
    "short_term_score": "近期得分",
    "long_term_score": "长期得分",
    "sharpe": "1年夏普比率",
    "sortino": "1年索提诺比率",
    "calmar": "1年卡玛比率",
    "positive_ratio": "1年正收益比率",
    "max_drawdown": "1年最大回撤",
    "volatility": "1年波动率",
    "scale": "基金规模",
    "fee": "费率合计",
}

_FACTOR_GROUPS = {
    "annual_return": "收益效率",
    "weighted_score": "综合评分门槛",
    "short_term_score": "综合评分门槛",
    "long_term_score": "综合评分门槛",
    "sharpe": "风险调整收益",
    "sortino": "风险调整收益",
    "calmar": "风险调整收益",
    "positive_ratio": "风险调整收益",
    "max_drawdown": "风险暴露",
    "volatility": "风险暴露",
    "scale": "摩擦与承载",
    "fee": "摩擦与承载",
}

_FOCUS_FACTORS = (
    "annual_return",
    "weighted_score",
    "short_term_score",
    "long_term_score",
    "sharpe",
)

_SCORE_CLUSTER = {"weighted_score", "short_term_score", "long_term_score"}
_RISK_ADJUSTED_CLUSTER = {"sharpe", "sortino", "calmar", "positive_ratio"}


def _ensure_project_venv() -> None:
    if VENV_PYTHON.exists() and Path(sys.executable) != VENV_PYTHON:
        os.execv(str(VENV_PYTHON), [str(VENV_PYTHON), *sys.argv])


def _to_int(value: Any) -> int:
    if value in (None, ""):
        return 0
    try:
        return int(value)
    except (TypeError, ValueError):
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return 0


def _to_float(value: Any) -> float:
    if value in (None, ""):
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _normalize_factor_token(text: Any) -> str:
    normalized = _clean_text(text)
    for factor_key, markers in _FACTOR_RULES:
        if any(marker in normalized for marker in markers):
            return factor_key
    return "other"


def _factor_display_name(factor_key: str) -> str:
    return _FACTOR_LABELS.get(factor_key, factor_key)


def _factor_group_name(factor_key: str) -> str:
    return _FACTOR_GROUPS.get(factor_key, "其他")


def _build_factor_count_map(detail_df: pd.DataFrame) -> dict[str, int]:
    counts: dict[str, int] = {}
    if detail_df.empty:
        return counts
    for _, row in detail_df.iterrows():
        factor_key = _normalize_factor_token(
            row.get("原因代码") or row.get("质量细分因子")
        )
        counts[factor_key] = max(
            counts.get(factor_key, 0),
            _to_int(row.get("命中样本数")),
        )
    return counts


def _parse_combo_factor_keys(combo_text: Any) -> tuple[str, ...]:
    factor_keys: list[str] = []
    seen: set[str] = set()
    for token in str(combo_text or "").split("|"):
        factor_key = _normalize_factor_token(token)
        if factor_key == "other" or factor_key in seen:
            continue
        seen.add(factor_key)
        factor_keys.append(factor_key)
    return tuple(factor_keys)


def _build_pair_rows(pair_df: pd.DataFrame, factor_counts: dict[str, int], quality_total: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if pair_df.empty:
        return rows
    for _, row in pair_df.iterrows():
        factor_a = _normalize_factor_token(row.get("因子A"))
        factor_b = _normalize_factor_token(row.get("因子B"))
        if factor_a == "other" or factor_b == "other":
            continue
        pair_count = _to_int(row.get("共同命中样本数"))
        count_a = factor_counts.get(factor_a, 0)
        count_b = factor_counts.get(factor_b, 0)
        if pair_count <= 0 or count_a <= 0 or count_b <= 0:
            continue
        min_count = min(count_a, count_b)
        max_count = max(count_a, count_b)
        overlap_vs_min = pair_count / min_count if min_count else 0.0
        overlap_vs_max = pair_count / max_count if max_count else 0.0
        rows.append(
            {
                "factor_a": factor_a,
                "factor_b": factor_b,
                "factor_a_label": _factor_display_name(factor_a),
                "factor_b_label": _factor_display_name(factor_b),
                "pair_count": pair_count,
                "quality_share_pct": round(
                    _to_float(row.get("占质量拦截样本比例(%)"))
                    or (pair_count / quality_total * 100.0 if quality_total else 0.0),
                    2,
                ),
                "factor_a_count": count_a,
                "factor_b_count": count_b,
                "overlap_vs_smaller_ratio": round(overlap_vs_min, 4),
                "overlap_vs_larger_ratio": round(overlap_vs_max, 4),
                "overlap_severity": _classify_pair_severity(overlap_vs_min),
                "same_cluster": _same_cluster(factor_a, factor_b),
            }
        )
    rows.sort(
        key=lambda item: (
            -float(item["overlap_vs_smaller_ratio"]),
            -int(item["pair_count"]),
            item["factor_a"],
            item["factor_b"],
        )
    )
    return rows


def _classify_pair_severity(overlap_vs_smaller_ratio: float) -> str:
    if overlap_vs_smaller_ratio >= 0.95:
        return "critical"
    if overlap_vs_smaller_ratio >= 0.9:
        return "high"
    if overlap_vs_smaller_ratio >= 0.8:
        return "medium"
    return "low"


def _same_cluster(factor_a: str, factor_b: str) -> bool:
    pair = {factor_a, factor_b}
    return pair <= _SCORE_CLUSTER or pair <= _RISK_ADJUSTED_CLUSTER


def _build_combo_rows(combo_df: pd.DataFrame, quality_total: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if combo_df.empty:
        return rows
    for _, row in combo_df.iterrows():
        factor_keys = _parse_combo_factor_keys(row.get("重叠组合"))
        if not factor_keys:
            continue
        rows.append(
            {
                "factor_keys": list(factor_keys),
                "factor_labels": [_factor_display_name(item) for item in factor_keys],
                "factor_count": _to_int(row.get("因子数")) or len(factor_keys),
                "sample_count": _to_int(row.get("样本数")),
                "quality_share_pct": round(
                    _to_float(row.get("占质量拦截样本比例(%)"))
                    or (
                        _to_int(row.get("样本数")) / quality_total * 100.0
                        if quality_total
                        else 0.0
                    ),
                    2,
                ),
                "combo_text": _clean_text(row.get("重叠组合")),
                "score_cluster_factor_count": len(_SCORE_CLUSTER.intersection(factor_keys)),
                "risk_adjusted_factor_count": len(_RISK_ADJUSTED_CLUSTER.intersection(factor_keys)),
            }
        )
    rows.sort(
        key=lambda item: (
            -int(item["sample_count"]),
            -int(item["factor_count"]),
            item["combo_text"],
        )
    )
    return rows


def _build_focus_factor_summary(
    factor_counts: dict[str, int],
    pair_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    pair_map: dict[frozenset[str], dict[str, Any]] = {
        frozenset((row["factor_a"], row["factor_b"])): row
        for row in pair_rows
    }
    summary: list[dict[str, Any]] = []
    for factor_key in _FOCUS_FACTORS:
        related = [
            row
            for row in pair_rows
            if factor_key in {row["factor_a"], row["factor_b"]}
        ]
        related.sort(
            key=lambda item: (
                -float(item["overlap_vs_smaller_ratio"]),
                -int(item["pair_count"]),
            )
        )
        top_partner = related[0] if related else None
        factor_count = factor_counts.get(factor_key, 0)
        summary.append(
            {
                "factor_key": factor_key,
                "factor_label": _factor_display_name(factor_key),
                "factor_group": _factor_group_name(factor_key),
                "hit_count": factor_count,
                "top_partner_factor_key": (
                    top_partner["factor_b"]
                    if top_partner and top_partner["factor_a"] == factor_key
                    else (top_partner["factor_a"] if top_partner else None)
                ),
                "top_partner_factor_label": (
                    top_partner["factor_b_label"]
                    if top_partner and top_partner["factor_a"] == factor_key
                    else (top_partner["factor_a_label"] if top_partner else None)
                ),
                "top_partner_overlap_ratio": (
                    top_partner["overlap_vs_smaller_ratio"] if top_partner else 0.0
                ),
                "top_partner_pair_count": (
                    top_partner["pair_count"] if top_partner else 0
                ),
                "review_priority": _factor_review_priority(
                    factor_key=factor_key,
                    factor_count=factor_count,
                    top_partner=top_partner,
                    pair_map=pair_map,
                ),
            }
        )
    summary.sort(
        key=lambda item: (
            _PRIORITY_RANK.get(item["review_priority"], 99),
            -int(item["hit_count"]),
            item["factor_key"],
        )
    )
    return summary


def _factor_review_priority(
    *,
    factor_key: str,
    factor_count: int,
    top_partner: dict[str, Any] | None,
    pair_map: dict[frozenset[str], dict[str, Any]],
) -> str:
    top_ratio = float((top_partner or {}).get("overlap_vs_smaller_ratio") or 0.0)
    annual_pair = pair_map.get(frozenset((factor_key, "annual_return")))
    weighted_pair = pair_map.get(frozenset((factor_key, "weighted_score")))
    if factor_key in {"weighted_score", "annual_return"} and top_ratio >= 0.85 and factor_count >= 2000:
        return "P1"
    if factor_key in {"short_term_score", "long_term_score", "sharpe"} and (
        top_ratio >= 0.9
        or (
            annual_pair is not None
            and float(annual_pair.get("overlap_vs_smaller_ratio") or 0.0) >= 0.85
        )
        or (
            weighted_pair is not None
            and float(weighted_pair.get("overlap_vs_smaller_ratio") or 0.0) >= 0.85
        )
    ):
        return "P1"
    if factor_key in {"sortino", "calmar", "positive_ratio", "scale"} and factor_count >= 1500:
        return "P2"
    return "P3"


def _build_cluster_summary(
    *,
    pair_rows: list[dict[str, Any]],
    combo_rows: list[dict[str, Any]],
    focus_summary: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    cluster_map: dict[str, dict[str, Any]] = {
        "score_cluster": {
            "cluster_key": "score_cluster",
            "cluster_label": "综合评分门槛簇",
            "factor_keys": sorted(_SCORE_CLUSTER),
            "factor_labels": [_factor_display_name(item) for item in sorted(_SCORE_CLUSTER)],
        },
        "return_score_sharpe_cluster": {
            "cluster_key": "return_score_sharpe_cluster",
            "cluster_label": "年化收益/得分/夏普耦合簇",
            "factor_keys": ["annual_return", "weighted_score", "short_term_score", "long_term_score", "sharpe"],
            "factor_labels": [
                _factor_display_name(item)
                for item in ["annual_return", "weighted_score", "short_term_score", "long_term_score", "sharpe"]
            ],
        },
        "risk_adjusted_cluster": {
            "cluster_key": "risk_adjusted_cluster",
            "cluster_label": "风险调整收益簇",
            "factor_keys": sorted(_RISK_ADJUSTED_CLUSTER),
            "factor_labels": [_factor_display_name(item) for item in sorted(_RISK_ADJUSTED_CLUSTER)],
        },
    }

    for cluster in cluster_map.values():
        factor_set = set(cluster["factor_keys"])
        pair_hits = [
            row
            for row in pair_rows
            if {row["factor_a"], row["factor_b"]} <= factor_set
        ]
        combo_hits = [
            row
            for row in combo_rows
            if factor_set.intersection(row["factor_keys"]) == set(row["factor_keys"])
            and len(row["factor_keys"]) >= 2
        ]
        cluster["pair_hit_count"] = len(pair_hits)
        cluster["high_overlap_pair_total"] = sum(
            1 for row in pair_hits if float(row["overlap_vs_smaller_ratio"]) >= 0.9
        )
        cluster["critical_overlap_pair_total"] = sum(
            1 for row in pair_hits if float(row["overlap_vs_smaller_ratio"]) >= 0.95
        )
        cluster["top_pair_preview"] = [
            {
                "factor_a": row["factor_a_label"],
                "factor_b": row["factor_b_label"],
                "pair_count": row["pair_count"],
                "overlap_vs_smaller_ratio": row["overlap_vs_smaller_ratio"],
            }
            for row in pair_hits[:3]
        ]
        cluster["dominant_combo_preview"] = [
            {
                "factor_labels": row["factor_labels"],
                "factor_count": row["factor_count"],
                "sample_count": row["sample_count"],
                "quality_share_pct": row["quality_share_pct"],
            }
            for row in combo_hits[:3]
        ]
        cluster["review_priority"] = _cluster_review_priority(cluster)

    ordered = sorted(
        cluster_map.values(),
        key=lambda item: (
            _PRIORITY_RANK.get(str(item.get("review_priority")), 99),
            -int(item.get("critical_overlap_pair_total") or 0),
            -int(item.get("high_overlap_pair_total") or 0),
            str(item.get("cluster_key")),
        ),
    )
    return ordered


def _cluster_review_priority(cluster: dict[str, Any]) -> str:
    critical_pairs = _to_int(cluster.get("critical_overlap_pair_total"))
    high_pairs = _to_int(cluster.get("high_overlap_pair_total"))
    if critical_pairs >= 2:
        return "P1"
    if high_pairs >= 2:
        return "P1"
    if high_pairs >= 1:
        return "P2"
    return "P3"


def _build_findings(
    *,
    quality_total: int,
    pair_rows: list[dict[str, Any]],
    combo_rows: list[dict[str, Any]],
    focus_summary: list[dict[str, Any]],
    cluster_summary: list[dict[str, Any]],
    high_score_friction_count: int,
    near_review_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    critical_pairs = [row for row in pair_rows if float(row["overlap_vs_smaller_ratio"]) >= 0.95]
    high_pairs = [row for row in pair_rows if float(row["overlap_vs_smaller_ratio"]) >= 0.9]
    if critical_pairs:
        findings.append(
            {
                "severity": "warning",
                "code": "score_cluster_has_near_duplicate_intercepts",
                "summary": "综合加权得分、近期得分、长期得分存在近重复拦截关系，优先复核是否承担了过度重叠的解释责任。",
                "evidence": {
                    "critical_pair_total": len(critical_pairs),
                    "pair_preview": [
                        {
                            "factor_a": row["factor_a_label"],
                            "factor_b": row["factor_b_label"],
                            "pair_count": row["pair_count"],
                            "overlap_vs_smaller_ratio": row["overlap_vs_smaller_ratio"],
                        }
                        for row in critical_pairs[:3]
                    ],
                },
            }
        )
    if any(
        row["factor_key"] == "annual_return" and row["review_priority"] == "P1"
        for row in focus_summary
    ):
        annual_pairs = [
            row
            for row in pair_rows
            if "annual_return" in {row["factor_a"], row["factor_b"]}
        ]
        findings.append(
            {
                "severity": "warning",
                "code": "annual_return_gate_is_strongly_coupled_with_other_quality_gates",
                "summary": "1年年化收益门槛与得分/夏普门槛高度耦合，当前更适合做解释力拆分而不是直接放宽收益阈值。",
                "evidence": {
                    "pair_preview": [
                        {
                            "factor_a": row["factor_a_label"],
                            "factor_b": row["factor_b_label"],
                            "pair_count": row["pair_count"],
                            "overlap_vs_smaller_ratio": row["overlap_vs_smaller_ratio"],
                        }
                        for row in annual_pairs[:3]
                    ],
                },
            }
        )
    dominant_combo = combo_rows[0] if combo_rows else None
    if dominant_combo and _to_int(dominant_combo.get("factor_count")) >= 7:
        findings.append(
            {
                "severity": "info",
                "code": "large_multi_factor_overlap_combos_exist",
                "summary": "质量拦截存在大规模多因子重叠组合，说明当前策略解释已不是单因子问题。",
                "evidence": {
                    "top_combo_factor_count": dominant_combo.get("factor_count"),
                    "top_combo_sample_count": dominant_combo.get("sample_count"),
                    "top_combo_quality_share_pct": dominant_combo.get("quality_share_pct"),
                    "top_combo_factors": dominant_combo.get("factor_labels"),
                },
            }
        )
    if high_score_friction_count > 0:
        pending_verify = next(
            (
                row
                for row in near_review_rows
                if _clean_text(row.get("复审建议")) == "待核实数据/份额结构"
            ),
            None,
        )
        findings.append(
            {
                "severity": "info",
                "code": "high_score_friction_gates_still_need_parallel_review",
                "summary": "高分样本摩擦门槛仍需并行复核，但当前主链优先级仍低于动态质量门槛重叠治理。",
                "evidence": {
                    "high_score_friction_count": high_score_friction_count,
                    "near_review_pending_verify_total": (
                        _to_int((pending_verify or {}).get("样本数"))
                    ),
                },
            }
        )
    findings.sort(key=lambda item: (item["severity"] != "warning", item["code"]))
    return findings


def _build_recommended_actions(
    *,
    focus_summary: list[dict[str, Any]],
    cluster_summary: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    actions: list[dict[str, Any]] = []
    focus_map = {item["factor_key"]: item for item in focus_summary}
    score_cluster = next(
        (item for item in cluster_summary if item.get("cluster_key") == "score_cluster"),
        None,
    )
    if score_cluster is not None and str(score_cluster.get("review_priority")) == "P1":
        actions.append(
            {
                "priority": "P1",
                "action_key": "decompose_score_cluster_explainability",
                "summary": "先复核综合加权得分、近期得分、长期得分三者在项目中的职责边界，确认是否存在重复表达同一质量维度。",
                "target_factors": ["weighted_score", "short_term_score", "long_term_score"],
            }
        )
    if str((focus_map.get("annual_return") or {}).get("review_priority")) == "P1":
        actions.append(
            {
                "priority": "P1",
                "action_key": "audit_annual_return_with_score_and_sharpe_coupling",
                "summary": "单独复核年化收益与综合得分/夏普之间的耦合关系，确认收益门槛承担的是独立约束还是重复拦截。",
                "target_factors": ["annual_return", "weighted_score", "sharpe"],
            }
        )
    risk_adjusted_cluster = next(
        (item for item in cluster_summary if item.get("cluster_key") == "risk_adjusted_cluster"),
        None,
    )
    if risk_adjusted_cluster is not None and _PRIORITY_RANK.get(str(risk_adjusted_cluster.get("review_priority")), 99) <= 1:
        actions.append(
            {
                "priority": "P2",
                "action_key": "review_risk_adjusted_metric_stack",
                "summary": "在主链得分簇复核后，再检查夏普/索提诺/卡玛/正收益比率是否存在可收敛的重复限制。",
                "target_factors": ["sharpe", "sortino", "calmar", "positive_ratio"],
            }
        )
    actions.append(
        {
            "priority": "P2",
            "action_key": "keep_scale_fee_review_parallel_but_not_primary",
            "summary": "规模/费率门槛继续做近阈值复核，但不应抢占当前动态质量门槛重叠治理的主优先级。",
            "target_factors": ["scale", "fee"],
        }
    )
    return sorted(
        actions,
        key=lambda item: (
            _PRIORITY_RANK.get(_clean_text(item.get("priority")), 99),
            _clean_text(item.get("action_key")),
        ),
    )


def _build_judgement(
    *,
    pair_rows: list[dict[str, Any]],
    focus_summary: list[dict[str, Any]],
    cluster_summary: list[dict[str, Any]],
) -> dict[str, Any]:
    critical_pairs = sum(
        1 for row in pair_rows if float(row["overlap_vs_smaller_ratio"]) >= 0.95
    )
    high_pairs = sum(
        1 for row in pair_rows if float(row["overlap_vs_smaller_ratio"]) >= 0.9
    )
    overlap_severity = "moderate"
    if critical_pairs >= 2:
        overlap_severity = "critical"
    elif high_pairs >= 3:
        overlap_severity = "high"

    primary_clusters = [
        item["cluster_key"]
        for item in cluster_summary
        if str(item.get("review_priority")) == "P1"
    ]
    primary_review_factors = [
        item["factor_key"]
        for item in focus_summary
        if str(item.get("review_priority")) == "P1"
    ]
    suspected_redundancy = any(
        row.get("same_cluster") and float(row.get("overlap_vs_smaller_ratio") or 0.0) >= 0.95
        for row in pair_rows
    )
    return {
        "overlap_severity": overlap_severity,
        "suspected_redundancy_present": suspected_redundancy,
        "primary_review_path": "factor_explainability_decomposition",
        "primary_review_clusters": primary_clusters,
        "primary_review_factors": primary_review_factors,
        "direct_threshold_relaxation_recommended_now": False,
        "reason": (
            "当前质量门槛存在高重叠和疑似冗余簇，应先拆清解释职责，再决定是否有任何阈值需要放宽或合并。"
        ),
    }


def _read_sheet(path: Path, sheet_name: str) -> pd.DataFrame:
    try:
        return pd.read_excel(path, sheet_name=sheet_name)
    except Exception:
        return pd.DataFrame()


def _build_dynamic_quality_gate_overlap_payload(
    *,
    latest_payload: dict[str, Any],
    latest_report_file: Path,
    screening_audit_excel: Path,
    replay_precision: dict[str, Any] | None = None,
) -> dict[str, Any]:
    screening_audit_excel_path = (
        screening_audit_excel
        if screening_audit_excel.is_absolute()
        else PROJECT_ROOT / screening_audit_excel
    )
    project_publish_summary = latest_payload.get("project_publish_summary") or {}
    standard_screening_audit = project_publish_summary.get("standard_screening_audit") or {}
    detail_df = _read_sheet(screening_audit_excel_path, "质量细分因子")
    pair_df = _read_sheet(screening_audit_excel_path, "质量重叠Pair")
    combo_df = _read_sheet(screening_audit_excel_path, "质量重叠Combo")
    friction_df = _read_sheet(screening_audit_excel_path, "高分摩擦主因")
    near_review_df = _read_sheet(screening_audit_excel_path, "近门槛复审结论")

    quality_total = 0
    detail_preview = list(standard_screening_audit.get("quality_factor_detail_preview") or [])
    if detail_preview:
        quality_total = max(
            _to_int(item.get("命中样本数"))
            for item in detail_preview
        )
    if quality_total <= 0 and not pair_df.empty:
        quality_total = max(_to_int(item) for item in pair_df.get("共同命中样本数", pd.Series(dtype=int)).tolist())

    factor_counts = _build_factor_count_map(detail_df)
    pair_rows = _build_pair_rows(pair_df, factor_counts, quality_total)
    combo_rows = _build_combo_rows(combo_df, quality_total)
    focus_summary = _build_focus_factor_summary(factor_counts, pair_rows)
    cluster_summary = _build_cluster_summary(
        pair_rows=pair_rows,
        combo_rows=combo_rows,
        focus_summary=focus_summary,
    )
    high_score_friction_count = _to_int(standard_screening_audit.get("high_score_friction_count"))
    near_review_rows = (
        near_review_df.fillna("").to_dict(orient="records")
        if not near_review_df.empty
        else list(standard_screening_audit.get("high_score_scale_gate_near_review_preview") or [])
    )
    findings = _build_findings(
        quality_total=quality_total,
        pair_rows=pair_rows,
        combo_rows=combo_rows,
        focus_summary=focus_summary,
        cluster_summary=cluster_summary,
        high_score_friction_count=high_score_friction_count,
        near_review_rows=near_review_rows,
    )
    actions = _build_recommended_actions(
        focus_summary=focus_summary,
        cluster_summary=cluster_summary,
    )
    judgement = _build_judgement(
        pair_rows=pair_rows,
        focus_summary=focus_summary,
        cluster_summary=cluster_summary,
    )

    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "run_id": latest_payload.get("run_id"),
        "latest_report_file": str(latest_report_file),
        "screening_audit_excel": str(screening_audit_excel),
        "replay_precision": dict(replay_precision or {}),
        "replay_precision_status": _clean_text((replay_precision or {}).get("status")),
        "replay_precision_status_label": _clean_text((replay_precision or {}).get("status_label")),
        "screening_snapshot": {
            "final_count": _to_int((latest_payload.get("counts") or {}).get("final_count")),
            "dynamic_final_count": _to_int((latest_payload.get("counts") or {}).get("dynamic_final_count")),
            "internal_watch_count": _to_int((latest_payload.get("counts") or {}).get("internal_watch_count")),
            "stage_b_candidate_pool_count": _to_int(
                (latest_payload.get("counts") or {}).get("stage_b_candidate_pool_count")
            ),
            "quality_intercept_reference_count": quality_total,
        },
        "focus_factor_summary": focus_summary,
        "cluster_summary": cluster_summary,
        "pair_overlap_preview": pair_rows[:12],
        "combo_overlap_preview": combo_rows[:12],
        "high_score_friction_preview": (
            friction_df.fillna("").to_dict(orient="records")[:5] if not friction_df.empty else []
        ),
        "near_review_preview": near_review_rows[:5],
        "findings": findings,
        "recommended_actions": actions,
        "judgement": judgement,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="审计动态质量门禁的因子重叠与职责边界。")
    parser.add_argument("--run-id", default="", help="指定 run_id；默认读取 latest canonical。")
    args = parser.parse_args()

    settings = load_settings()
    project_root = Path(getattr(settings, "project_root", PROJECT_ROOT))
    try:
        run_id, latest_payload, artifacts, replay_precision = _resolve_run_payload(
            settings,
            run_id=args.run_id,
            project_root=project_root,
        )
    except RuntimeError as exc:
        print(
            json.dumps(
                {
                    "passed": False,
                    "error": str(exc),
                    "latest_report_file": str(settings.latest_report_file),
                    "requested_run_id": str(args.run_id or "").strip(),
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 1

    latest_report_file = settings.latest_report_file.relative_to(project_root)
    screening_audit_value, screening_audit_excel = _resolve_artifact_project_path(
        artifacts,
        "screening_audit_excel",
        project_root=project_root,
    )
    if not screening_audit_value:
        print(
            json.dumps(
                {
                    "passed": False,
                    "error": "screening_audit_excel_missing",
                    "run_id": run_id,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 1

    if screening_audit_excel is None or not screening_audit_excel.exists():
        print(
            json.dumps(
                {
                    "passed": False,
                    "error": "screening_audit_excel_not_found",
                    "screening_audit_excel": screening_audit_value,
                    "run_id": run_id,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 1

    payload = _build_dynamic_quality_gate_overlap_payload(
        latest_payload=latest_payload,
        latest_report_file=latest_report_file,
        screening_audit_excel=screening_audit_excel.relative_to(project_root),
        replay_precision=replay_precision,
    )

    audit_dir = settings.reports_dir / "audit" / "dynamic_quality_gate_overlap"
    artifact_path = _build_audit_artifact_path(
        audit_dir,
        "dynamic_quality_gate_overlap",
        run_id=run_id,
    )
    artifact_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print(
        json.dumps(
            {
                "passed": True,
                "run_id": payload.get("run_id"),
                "artifact_path": str(artifact_path),
                "artifact_project_path": str(artifact_path.relative_to(project_root)),
                "replay_precision_status": payload.get("replay_precision_status"),
                "replay_precision_status_label": payload.get("replay_precision_status_label"),
                "overlap_severity": ((payload.get("judgement") or {}).get("overlap_severity")),
                "suspected_redundancy_present": (
                    (payload.get("judgement") or {}).get("suspected_redundancy_present")
                ),
                "direct_threshold_relaxation_recommended_now": (
                    (payload.get("judgement") or {}).get("direct_threshold_relaxation_recommended_now")
                ),
                "top_action_keys": [
                    item.get("action_key")
                    for item in list(payload.get("recommended_actions") or [])[:3]
                ],
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    _ensure_project_venv()
    raise SystemExit(main())
