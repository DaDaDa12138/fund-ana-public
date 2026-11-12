from __future__ import annotations

from typing import Any

from backend.effective_nav import (
    RISK_EFFECTIVE_COVERAGE_OBSERVATION_RATIO,
    RISK_EFFECTIVE_COVERAGE_RATIO,
)


def _to_float(value: Any, default: float = 0.0) -> float:
    if value in (None, ""):
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def build_risk_coverage_threshold_semantics(
    payload: dict[str, Any] | None = None,
    *,
    coverage_ratio: Any = None,
    coverage_sufficient: bool | None = None,
) -> dict[str, Any]:
    source = dict(payload or {})
    ratio = _to_float(
        coverage_ratio
        if coverage_ratio is not None
        else source.get("coverage_ratio_vs_candidate"),
    )
    publish_minimum = _to_float(
        source.get("publish_minimum_coverage_ratio")
        if source.get("publish_minimum_coverage_ratio") is not None
        else source.get("minimum_coverage_ratio"),
        RISK_EFFECTIVE_COVERAGE_RATIO,
    )
    if publish_minimum <= 0:
        publish_minimum = RISK_EFFECTIVE_COVERAGE_RATIO
    observation_ratio = _to_float(
        source.get("observation_coverage_ratio")
        if source.get("observation_coverage_ratio") is not None
        else source.get("risk_coverage_observation_ratio"),
        RISK_EFFECTIVE_COVERAGE_OBSERVATION_RATIO,
    )
    if observation_ratio <= 0:
        observation_ratio = RISK_EFFECTIVE_COVERAGE_OBSERVATION_RATIO

    if coverage_sufficient is None:
        if source.get("coverage_sufficient") is not None:
            publish_ready = bool(source.get("coverage_sufficient"))
        else:
            publish_ready = bool(ratio >= publish_minimum)
    else:
        publish_ready = bool(coverage_sufficient)
    observation_met = bool(ratio >= observation_ratio)

    if not publish_ready:
        gate_status = "publish_blocking"
        gate_label = "低于发布门槛"
        explanation = (
            f"风险覆盖率 {ratio:.6f} 低于发布最低门槛 {publish_minimum:.6f}，"
            "属于发布阻断。"
        )
    elif not observation_met:
        gate_status = "publish_ready_observation_watch"
        gate_label = "已达发布门槛，低于观察线"
        explanation = (
            f"风险覆盖率 {ratio:.6f} 已达到发布最低门槛 {publish_minimum:.6f}，"
            f"但低于运维观察线 {observation_ratio:.6f}；当前不阻断发布，只需持续监控。"
        )
    else:
        gate_status = "publish_ready"
        gate_label = "已达发布门槛和观察线"
        explanation = (
            f"风险覆盖率 {ratio:.6f} 同时达到发布最低门槛 {publish_minimum:.6f} "
            f"和运维观察线 {observation_ratio:.6f}。"
        )

    return {
        "coverage_ratio_vs_candidate": round(ratio, 6),
        "minimum_coverage_ratio": publish_minimum,
        "publish_minimum_coverage_ratio": publish_minimum,
        "observation_coverage_ratio": observation_ratio,
        "coverage_sufficient": publish_ready,
        "coverage_observation_line_met": observation_met,
        "coverage_gate_status": gate_status,
        "coverage_gate_label": gate_label,
        "coverage_gate_explanation": explanation,
        "coverage_threshold_role": "publish_gate",
        "observation_threshold_role": "operational_watchline",
    }
