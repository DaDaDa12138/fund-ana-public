#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent
VENV_PYTHON = PROJECT_ROOT / ".venv" / "bin" / "python"

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.config import load_settings  # noqa: E402
from backend.high_score_friction import load_high_score_friction_gate_review_from_workbook  # noqa: E402
from backend.risk_residual_warnings import (  # noqa: E402
    build_risk_residual_warning_summary,
)
from backend.risk_coverage_gap_audit import load_latest_risk_coverage_gap_audit_payload  # noqa: E402
from backend.risk_lag_audit import load_latest_risk_lag_audit_payload  # noqa: E402
from backend.risk_sync_audit import load_latest_risk_sync_audit_payload  # noqa: E402
from scripts.audit_runtime_utils import (  # noqa: E402
    build_audit_artifact_path as _build_audit_artifact_path,
    clean_text as _runtime_clean_text,
    resolve_artifact_project_path as _resolve_artifact_project_path,
    resolve_run_payload as _resolve_run_payload,
)


_PRIORITY_RANK = {"P0": 0, "P1": 1, "P2": 2, "P3": 3}
_REASON_FAMILY_RULES: list[tuple[str, tuple[str, ...]]] = [
    ("annual_return", ("below_annual_floor", "年化收益")),
    ("weighted_score", ("below_weighted_floor", "综合加权得分", "评分不足")),
    ("sharpe", ("below_sharpe_floor", "夏普")),
    ("sortino", ("below_sortino_floor", "索提诺")),
    ("calmar", ("below_calmar_floor", "卡玛")),
    ("positive_ratio", ("below_positive_ratio_floor", "正收益比率")),
    ("long_term_score", ("below_long_floor", "长期得分")),
    ("short_term_score", ("below_short_floor", "近期得分")),
    ("max_drawdown", ("mdd_too_high", "最大回撤")),
    ("volatility", ("volatility_too_high", "波动率")),
    ("scale", ("scale_too_small", "基金规模")),
    ("fee", ("fee_too_high", "费率")),
    ("buyable", ("not_buyable", "不可申购")),
]


def _ensure_project_venv() -> None:
    if VENV_PYTHON.exists() and Path(sys.executable) != VENV_PYTHON:
        os.execv(str(VENV_PYTHON), [str(VENV_PYTHON), *sys.argv])


def _load_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        return {}


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


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


def _infer_reason_family(*, code: Any = None, reason: Any = None, display_reason: Any = None) -> str:
    text = " ".join(
        part
        for part in (
            _clean_text(code).lower(),
            _clean_text(reason).lower(),
            _clean_text(display_reason).lower(),
        )
        if part
    )
    for family, markers in _REASON_FAMILY_RULES:
        if any(marker.lower() in text for marker in markers):
            return family
    return "other"


def _reason_entry(
    *,
    code: Any = None,
    reason: Any = None,
    display_reason: Any = None,
    count: Any = None,
) -> dict[str, Any]:
    return {
        "code": _clean_text(code) or None,
        "reason": _clean_text(reason) or None,
        "display_reason": _clean_text(display_reason) or None,
        "count": _to_int(count),
        "reason_family": _infer_reason_family(
            code=code,
            reason=reason,
            display_reason=display_reason,
        ),
    }


def _top_reason_families(rows: list[dict[str, Any]], *, limit: int = 5) -> list[dict[str, Any]]:
    counts: dict[str, int] = {}
    for item in rows:
        family = _clean_text((item or {}).get("reason_family")) or "other"
        counts[family] = counts.get(family, 0) + _to_int((item or {}).get("count"))
    ordered = sorted(counts.items(), key=lambda pair: (-pair[1], pair[0]))
    return [
        {
            "reason_family": family,
            "count": count,
        }
        for family, count in ordered[:limit]
    ]


def _build_publish_focus(summary: dict[str, Any]) -> dict[str, Any]:
    payload = dict(summary or {})
    primary_bottleneck = dict(payload.get("primary_bottleneck") or {})
    top_reasons_raw = list(primary_bottleneck.get("top_reasons") or [])
    if not top_reasons_raw and primary_bottleneck.get("lead_reason"):
        top_reasons_raw = [primary_bottleneck.get("lead_reason") or {}]
    top_reasons = [
        _reason_entry(
            code=item.get("code"),
            reason=item.get("reason"),
            display_reason=item.get("display_reason"),
            count=item.get("count"),
        )
        for item in top_reasons_raw[:5]
    ]
    lead_reason = _reason_entry(
        code=(primary_bottleneck.get("lead_reason") or {}).get("code"),
        reason=(primary_bottleneck.get("lead_reason") or {}).get("reason"),
        display_reason=(primary_bottleneck.get("lead_reason") or {}).get("display_reason"),
        count=(primary_bottleneck.get("lead_reason") or {}).get("count"),
    )
    return {
        "available": bool(payload.get("available")),
        "headline_highlights": list(payload.get("headline_highlights") or [])[:3],
        "primary_bottleneck": {
            "stage_key": _clean_text(primary_bottleneck.get("stage_key")) or None,
            "stage_label": _clean_text(primary_bottleneck.get("stage_label")) or None,
            "input_count": _to_int(primary_bottleneck.get("input_count")),
            "passed_count": _to_int(primary_bottleneck.get("passed_count")),
            "removed_count": _to_int(primary_bottleneck.get("removed_count")),
            "removed_rate_pct": _to_float(primary_bottleneck.get("removed_rate_pct")),
            "lead_reason": lead_reason,
            "top_reasons": top_reasons,
            "reason_family_summary": _top_reason_families(top_reasons, limit=4),
        },
    }


def _build_screening_audit_focus(
    standard_screening_audit: dict[str, Any],
    high_yield_exclusion_ledger: dict[str, Any],
    high_score_friction_gate_review: dict[str, Any] | None = None,
) -> dict[str, Any]:
    audit = dict(standard_screening_audit or {})
    ledger = dict(high_yield_exclusion_ledger or {})
    friction_gate_review = dict(
        high_score_friction_gate_review or audit.get("high_score_friction_gate_review") or {}
    )
    quality_factor_rows = [
        {
            "code": _clean_text(item.get("原因代码")) or None,
            "label": _clean_text(item.get("质量细分因子")) or None,
            "factor_group": _clean_text(item.get("所属因子组")) or None,
            "count": _to_int(item.get("命中样本数")),
            "share_pct": _to_float(item.get("占质量拦截样本比例(%)")),
            "reason_family": _infer_reason_family(
                code=item.get("原因代码"),
                reason=item.get("质量细分因子"),
                display_reason=item.get("质量细分因子"),
            ),
        }
        for item in list(audit.get("quality_factor_detail_preview") or [])[:8]
    ]
    friction_rows = [
        {
            "label": _clean_text(item.get("主排除原因")) or None,
            "count": _to_int(item.get("样本数")),
            "share_pct": _to_float(item.get("占高分摩擦样本比例(%)")),
            "reason_family": _infer_reason_family(
                reason=item.get("主排除原因"),
                display_reason=item.get("主排除原因"),
            ),
        }
        for item in list(audit.get("high_score_friction_reason_preview") or [])[:5]
    ]
    high_yield_thresholds = []
    for item in list(ledger.get("high_yield_thresholds") or [])[:5]:
        top_reasons = [
            _reason_entry(
                reason=reason_item.get("reason"),
                display_reason=reason_item.get("reason"),
                count=reason_item.get("count"),
            )
            for reason_item in list(item.get("top_primary_reasons") or [])[:5]
        ]
        high_yield_thresholds.append(
            {
                "threshold": _to_float(item.get("threshold")),
                "count": _to_int(item.get("count")),
                "top_reasons": top_reasons,
                "reason_family_summary": _top_reason_families(top_reasons, limit=4),
            }
        )
    return {
        "reason_group_preview": list(audit.get("reason_group_preview") or [])[:5],
        "quality_factor_detail_preview": quality_factor_rows,
        "quality_factor_family_summary": _top_reason_families(quality_factor_rows, limit=5),
        "high_score_friction_count": _to_int(audit.get("high_score_friction_count")),
        "high_score_friction_reason_preview": friction_rows,
        "high_score_friction_family_summary": _top_reason_families(friction_rows, limit=4),
        "high_score_friction_gate_review": friction_gate_review,
        "high_yield_thresholds": high_yield_thresholds,
    }


def _build_risk_data_quality_focus(
    risk_lag_summary: dict[str, Any],
    risk_sync_summary: dict[str, Any],
    risk_coverage_gap_summary: dict[str, Any],
) -> dict[str, Any]:
    return build_risk_residual_warning_summary(
        risk_lag_summary,
        risk_sync_summary,
        risk_coverage_gap_summary,
    )


def _build_findings(
    *,
    publish_focus: dict[str, Any],
    screening_focus: dict[str, Any],
    risk_focus: dict[str, Any],
) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []

    risk_status = _clean_text(risk_focus.get("status")) or "healthy"
    if risk_status == "blocking":
        findings.append(
            {
                "severity": "blocking",
                "code": "risk_data_quality_requires_remediation",
                "category": "data_quality",
                "summary": "风险快照对齐或覆盖仍存在阻断项，当前不应直接调整筛选门槛。",
                "evidence": {
                    "blocking_codes": list(risk_focus.get("blocking_codes") or []),
                    "warning_codes": list(risk_focus.get("warning_codes") or []),
                },
            }
        )
    elif risk_status == "warning":
        findings.append(
            {
                "severity": "warning",
                "code": "risk_data_quality_has_residual_warnings",
                "category": "data_quality",
                "summary": "风险数据主链已可运行，但仍有残留 warning，应先完成观察再做策略调参。",
                "evidence": {
                    "warning_codes": list(risk_focus.get("warning_codes") or []),
                },
            }
        )
    else:
        findings.append(
            {
                "severity": "info",
                "code": "risk_data_quality_not_primary_blocker",
                "category": "data_quality",
                "summary": "当前风险数据质量不是主链阻塞，策略解释问题可独立审查。",
                "evidence": {
                    "coverage_ratio_vs_candidate": (
                        (risk_focus.get("risk_lag_summary") or {}).get("coverage_ratio_vs_candidate")
                    ),
                    "actionable_candidate_total": (
                        (risk_focus.get("risk_sync_summary") or {}).get("actionable_candidate_total")
                    ),
                    "actionable_gap_total": (
                        (risk_focus.get("risk_coverage_gap_summary") or {}).get("actionable_gap_total")
                    ),
                },
            }
        )

    publish_families = [
        item.get("reason_family")
        for item in (
            ((publish_focus.get("primary_bottleneck") or {}).get("reason_family_summary")) or []
        )
        if _clean_text(item.get("reason_family"))
    ]
    quality_families = [
        item.get("reason_family")
        for item in list(screening_focus.get("quality_factor_family_summary") or [])
        if _clean_text(item.get("reason_family"))
    ]
    overlap_families = [
        family
        for family in publish_families[:3]
        if family in quality_families[:5]
    ]
    if overlap_families:
        findings.append(
            {
                "severity": "warning" if risk_status == "healthy" else "info",
                "code": "publish_and_screening_reasons_highly_overlapping",
                "category": "strategy_explainability",
                "summary": "发布主链主瓶颈与质量拦截主因高度重叠，应优先复核动态质量门槛叠加解释。",
                "evidence": {
                    "overlap_reason_families": overlap_families,
                    "publish_primary_stage": (
                        (publish_focus.get("primary_bottleneck") or {}).get("stage_key")
                    ),
                    "publish_primary_lead_reason": (
                        (((publish_focus.get("primary_bottleneck") or {}).get("lead_reason")) or {}).get(
                            "code"
                        )
                    ),
                },
            }
        )

    high_score_friction_count = _to_int(screening_focus.get("high_score_friction_count"))
    friction_families = list(screening_focus.get("high_score_friction_family_summary") or [])
    friction_gate_review = dict(screening_focus.get("high_score_friction_gate_review") or {})
    friction_judgement = dict(friction_gate_review.get("judgement") or {})
    actionable_threshold_review_total = _to_int(
        friction_judgement.get("actionable_threshold_review_total")
    )
    pending_data_verify_total = _to_int(friction_judgement.get("pending_data_verify_total"))
    if high_score_friction_count > 0 and friction_families:
        severity = "warning"
        summary = "存在高分候选被费率/规模等摩擦门槛拦截，建议单独核对这些门槛的解释与近阈值治理。"
        if actionable_threshold_review_total <= 0 and pending_data_verify_total > 0:
            severity = "info"
            summary = "高分摩擦门槛当前未显示可直接放宽阈值的候选样本，但仍有少量份额结构或规模数据待核实。"
        elif actionable_threshold_review_total <= 0:
            severity = "info"
            summary = "高分摩擦样本已完成近阈值复核，当前未见支持直接放宽规模/费率门槛的候选样本。"
        findings.append(
            {
                "severity": severity,
                "code": "high_score_candidates_blocked_by_friction_gates",
                "category": "strategy_explainability",
                "summary": summary,
                "evidence": {
                    "high_score_friction_count": high_score_friction_count,
                    "dominant_friction_family": friction_families[0].get("reason_family"),
                    "dominant_friction_count": friction_families[0].get("count"),
                    "friction_gate_status": _clean_text(friction_gate_review.get("status")) or None,
                    "actionable_threshold_review_total": actionable_threshold_review_total,
                    "pending_data_verify_total": pending_data_verify_total,
                },
            }
        )

    threshold_rows = list(screening_focus.get("high_yield_thresholds") or [])
    threshold_5 = next(
        (item for item in threshold_rows if _to_float(item.get("threshold")) == 5.0),
        None,
    )
    if threshold_5 and _to_int(threshold_5.get("count")) > 0:
        family_summary = list(threshold_5.get("reason_family_summary") or [])
        findings.append(
            {
                "severity": "info",
                "code": "high_yield_non_selected_reasons_are_structured",
                "category": "strategy_explainability",
                "summary": "高收益未入榜样本已具备结构化原因，可基于原因家族做定向复核而不是直接放宽收益门槛。",
                "evidence": {
                    "threshold": 5.0,
                    "candidate_count": _to_int(threshold_5.get("count")),
                    "dominant_reason_families": family_summary[:3],
                },
            }
        )

    severity_rank = {"blocking": 0, "warning": 1, "info": 2}
    return sorted(
        findings,
        key=lambda item: (
            severity_rank.get(_clean_text(item.get("severity")), 9),
            _clean_text(item.get("code")),
        ),
    )


def _build_recommended_actions(
    *,
    publish_focus: dict[str, Any],
    screening_focus: dict[str, Any],
    risk_focus: dict[str, Any],
    findings: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    actions: list[dict[str, Any]] = []
    risk_status = _clean_text(risk_focus.get("status")) or "healthy"
    if risk_status == "blocking":
        actions.append(
            {
                "priority": "P0",
                "action_key": "repair_risk_data_pipeline_before_strategy_changes",
                "summary": "先修复风险快照对齐/覆盖阻断，再评估是否调整筛选阈值。",
                "evidence_codes": list(risk_focus.get("blocking_codes") or []),
            }
        )
    elif risk_status == "warning":
        actions.append(
            {
                "priority": "P1",
                "action_key": "observe_residual_risk_data_warnings",
                "summary": "风险主链可运行，但需继续观察 residual warning，不建议立即放宽主链门槛。",
                "evidence_codes": list(risk_focus.get("warning_codes") or []),
            }
        )

    overlap_finding = next(
        (item for item in findings if _clean_text(item.get("code")) == "publish_and_screening_reasons_highly_overlapping"),
        None,
    )
    if overlap_finding is not None:
        actions.append(
            {
                "priority": "P1",
                "action_key": "audit_dynamic_quality_gate_overlap",
                "summary": "复核动态质量门槛中年化收益、综合加权得分、夏普等指标的叠加解释力，优先做重叠与冗余审计，不直接放宽阈值。",
                "evidence_codes": [overlap_finding.get("code")],
            }
        )

    friction_finding = next(
        (item for item in findings if _clean_text(item.get("code")) == "high_score_candidates_blocked_by_friction_gates"),
        None,
    )
    if friction_finding is not None:
        actionable_threshold_review_total = _to_int(
            ((friction_finding.get("evidence") or {}).get("actionable_threshold_review_total"))
        )
        pending_data_verify_total = _to_int(
            ((friction_finding.get("evidence") or {}).get("pending_data_verify_total"))
        )
        actions.append(
            {
                "priority": "P1",
                "action_key": (
                    "review_high_score_friction_gates"
                    if actionable_threshold_review_total > 0
                    else "observe_high_score_friction_gate_audit"
                ),
                "summary": (
                    "对高分候选中的规模/费率摩擦门槛做近阈值分层复核，判断是否存在解释不足而非质量不足的拦截。"
                    if actionable_threshold_review_total > 0
                    else (
                        "当前高分摩擦门槛复核未发现应直接放宽阈值的候选样本，先保留门槛并持续观察。"
                        if pending_data_verify_total <= 0
                        else "当前高分摩擦门槛复核未发现放宽阈值证据，但仍需先核实份额结构或规模数据。"
                    )
                ),
                "evidence_codes": [friction_finding.get("code")],
            }
        )

    high_yield_finding = next(
        (item for item in findings if _clean_text(item.get("code")) == "high_yield_non_selected_reasons_are_structured"),
        None,
    )
    if high_yield_finding is not None:
        actions.append(
            {
                "priority": "P2",
                "action_key": "review_high_yield_non_selected_reason_families",
                "summary": "优先按高收益未入榜原因家族做结构化复盘，不建议用单一收益阈值放宽替代原因审计。",
                "evidence_codes": [high_yield_finding.get("code")],
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
    risk_focus: dict[str, Any],
    findings: list[dict[str, Any]],
    actions: list[dict[str, Any]],
) -> dict[str, Any]:
    risk_status = _clean_text(risk_focus.get("status")) or "healthy"
    primary_action_path = "observe_only"
    reason = "当前未发现需要立即调整的主链策略或数据治理动作。"
    if risk_status == "blocking":
        primary_action_path = "data_quality_first"
        reason = "风险数据存在阻断项，当前不应直接修改筛选阈值。"
    elif any(_clean_text(item.get("category")) == "strategy_explainability" for item in findings):
        primary_action_path = "strategy_review"
        reason = "数据主链基本健康，下一步应优先复核筛选原因重叠、摩擦门槛和解释力，而不是直接放宽门槛。"
    elif risk_status == "warning":
        primary_action_path = "observe_data_quality"
        reason = "风险数据仍有 residual warning，建议先观察再决定是否进入策略调参。"
    return {
        "data_quality_status": risk_status,
        "primary_action_path": primary_action_path,
        "targeted_review_recommended_now": primary_action_path == "strategy_review",
        "direct_threshold_relaxation_recommended_now": False,
        "attention_required": bool(actions),
        "reason": reason,
    }


def _build_strategy_reason_data_quality_payload(
    *,
    latest_payload: dict[str, Any],
    latest_report_file: Path,
    replay_precision: dict[str, Any] | None,
    screening_audit_artifact_path: Path | None,
    risk_lag_summary: dict[str, Any],
    risk_sync_summary: dict[str, Any],
    risk_coverage_gap_summary: dict[str, Any],
    risk_lag_artifact_path: Path | None = None,
    risk_sync_artifact_path: Path | None = None,
    risk_coverage_gap_artifact_path: Path | None = None,
    auxiliary_scope: str = "latest_only",
) -> dict[str, Any]:
    project_publish_summary = (latest_payload.get("project_publish_summary") or {})
    standard_screening_audit = project_publish_summary.get("standard_screening_audit") or {}
    high_score_friction_gate_review = dict(
        standard_screening_audit.get("high_score_friction_gate_review") or {}
    )
    if not high_score_friction_gate_review:
        if screening_audit_artifact_path is not None and screening_audit_artifact_path.exists():
            try:
                high_score_friction_gate_review = (
                    load_high_score_friction_gate_review_from_workbook(
                        screening_audit_artifact_path,
                        high_score_threshold=_to_float(
                            standard_screening_audit.get("high_score_threshold") or 75.0
                        ),
                    )
                )
            except Exception:
                high_score_friction_gate_review = {}
    publish_focus = _build_publish_focus(project_publish_summary.get("publish_reason_summary") or {})
    screening_focus = _build_screening_audit_focus(
        standard_screening_audit,
        project_publish_summary.get("high_yield_exclusion_ledger") or {},
        high_score_friction_gate_review,
    )
    risk_focus = _build_risk_data_quality_focus(
        risk_lag_summary,
        risk_sync_summary,
        risk_coverage_gap_summary,
    )
    findings = _build_findings(
        publish_focus=publish_focus,
        screening_focus=screening_focus,
        risk_focus=risk_focus,
    )
    actions = _build_recommended_actions(
        publish_focus=publish_focus,
        screening_focus=screening_focus,
        risk_focus=risk_focus,
        findings=findings,
    )
    judgement = _build_judgement(
        risk_focus=risk_focus,
        findings=findings,
        actions=actions,
    )

    counts = latest_payload.get("counts") or {}
    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "run_id": latest_payload.get("run_id"),
        "latest_report_file": str(latest_report_file),
        "replay_precision": dict(replay_precision or {}),
        "replay_precision_status": _clean_text((replay_precision or {}).get("status")),
        "replay_precision_status_label": _clean_text((replay_precision or {}).get("status_label")),
        "auxiliary_scope": auxiliary_scope,
        "source_artifacts": {
            "risk_lag_audit": str(risk_lag_artifact_path) if risk_lag_artifact_path else None,
            "risk_sync_audit": str(risk_sync_artifact_path) if risk_sync_artifact_path else None,
            "risk_coverage_gap_audit": (
                str(risk_coverage_gap_artifact_path) if risk_coverage_gap_artifact_path else None
            ),
        },
        "screening_snapshot": {
            "final_count": _to_int(counts.get("final_count")),
            "dynamic_final_count": _to_int(counts.get("dynamic_final_count")),
            "internal_watch_count": _to_int(counts.get("internal_watch_count")),
            "stage_b_candidate_pool_count": _to_int(
                counts.get("stage_b_candidate_pool_count") or counts.get("publishable_pool_count")
            ),
        },
        "publish_reason_focus": publish_focus,
        "screening_audit_focus": screening_focus,
        "risk_data_quality_focus": risk_focus,
        "findings": findings,
        "recommended_actions": actions,
        "judgement": judgement,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="审计发布理由链、筛选门禁与风险数据质量的解释完整性。")
    parser.add_argument("--run-id", default="", help="指定 run_id；默认读取 latest canonical。")
    args = parser.parse_args()

    settings = load_settings()
    project_root = Path(getattr(settings, "project_root", PROJECT_ROOT))
    latest_canonical_payload = _load_json(settings.latest_report_file)
    latest_canonical_run_id = _runtime_clean_text(latest_canonical_payload.get("run_id"))
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
    _, screening_audit_artifact_path = _resolve_artifact_project_path(
        artifacts,
        "screening_audit_excel",
        project_root=project_root,
    )
    risk_lag_artifact_path, risk_lag_payload = load_latest_risk_lag_audit_payload(settings)
    risk_sync_artifact_path, risk_sync_payload = load_latest_risk_sync_audit_payload(settings)
    risk_coverage_gap_artifact_path, risk_coverage_gap_payload = (
        load_latest_risk_coverage_gap_audit_payload(settings)
    )
    auxiliary_scope = "same_run_latest_only" if run_id == latest_canonical_run_id else "cross_run_latest_only"

    payload = _build_strategy_reason_data_quality_payload(
        latest_payload=latest_payload,
        latest_report_file=latest_report_file,
        replay_precision=replay_precision,
        screening_audit_artifact_path=screening_audit_artifact_path,
        risk_lag_summary=risk_lag_payload or {},
        risk_sync_summary=risk_sync_payload or {},
        risk_coverage_gap_summary=risk_coverage_gap_payload or {},
        risk_lag_artifact_path=(
            risk_lag_artifact_path.relative_to(settings.project_root)
            if risk_lag_artifact_path is not None
            else None
        ),
        risk_sync_artifact_path=(
            risk_sync_artifact_path.relative_to(settings.project_root)
            if risk_sync_artifact_path is not None
            else None
        ),
        risk_coverage_gap_artifact_path=(
            risk_coverage_gap_artifact_path.relative_to(project_root)
            if risk_coverage_gap_artifact_path is not None
            else None
        ),
        auxiliary_scope=auxiliary_scope,
    )

    audit_dir = settings.reports_dir / "audit" / "strategy_reason_data_quality"
    artifact_path = _build_audit_artifact_path(
        audit_dir,
        "strategy_reason_data_quality",
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
                "auxiliary_scope": payload.get("auxiliary_scope"),
                "primary_action_path": ((payload.get("judgement") or {}).get("primary_action_path")),
                "targeted_review_recommended_now": (
                    (payload.get("judgement") or {}).get("targeted_review_recommended_now")
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
