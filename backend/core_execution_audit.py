"""Describe this run's recorded child processes without claiming business success."""
from __future__ import annotations

from datetime import datetime
import hashlib
import json
from pathlib import Path
from typing import Any

from backend.calculation_input_capture import CalculationCaptureError, capture_index_reference


# These are executable stages, rather than the architecture's capabilities.
_STAGES = (
    ("collection", "数据采集", (
        "init-db", "collect-funds", "collect-details", "collect-managers", "collect-nav",
        "collect-period-returns", "supplement-holding-period", "collect-companies",
    ), None),
    ("risk", "数据库风险扫描与计算", ("calculate-risk",), None),
    ("screening_master", "核心筛选调度", ("daily-screening-master",), None),
    ("screening_periods", "九周期分析及长短期汇总", (
        "任务1: 长期筛选", "long-1year", "long-2year", "long-3year", "long-4year", "long-5year",
        "任务2: 短期筛选", "short-1week", "short-1month", "short-3month", "short-6month",
    ), "daily-screening-master"),
    ("merge", "长短期最终合并", ("任务3: 最终筛选",), "daily-screening-master"),
    ("core_tracker", "核心累积跟踪", ("任务4: 累积跟踪",), "daily-screening-master"),
)
_LABELS = {
    "succeeded": "已记录进程成功退出",
    "failed": "已记录进程失败",
    "timed_out": "已记录进程超时",
    "not_scheduled": "本轮未安排执行",
    "not_observed": "未记录执行，原因未知",
    "missing_evidence": "已安排但缺少执行记录",
    "invalid_evidence": "执行记录不完整或矛盾",
    "multiple_attempts": "存在多次执行，需逐次核对",
    "unexpected_execution": "执行记录与本轮计划不符",
    "incomplete": "执行记录未完整核实",
}
_SCOPE_NOTE = (
    "仅核对本轮核心子进程的计划与退出记录；成功退出不等于新增数据、实际重算、"
    "指标科学性验证或正式发布。报告补采与后续审计另有各自记录。"
)


def _process_status(item: dict[str, Any]) -> str:
    code, timed_out, success = item.get("return_code"), item.get("timed_out"), item.get("success")
    if type(code) is not int or type(timed_out) is not bool or type(success) is not bool:
        return "invalid_evidence"
    if success != (code == 0 and not timed_out):
        return "invalid_evidence"
    try:
        started = datetime.fromisoformat(item["started_at"])
        finished = datetime.fromisoformat(item["finished_at"])
        if finished < started:
            return "invalid_evidence"
    except (KeyError, TypeError, ValueError):
        return "invalid_evidence"
    if (not isinstance(item.get("command"), list) or not item["command"]
            or any(not isinstance(part, str) or not part for part in item["command"])):
        return "invalid_evidence"
    return "timed_out" if timed_out else "succeeded" if success else "failed"


def _task_summary(name: str, items: list[dict], planned: bool | None) -> dict:
    attempts = []
    for item in items:
        attempts.append({
            "status": _process_status(item),
            **{key: item.get(key) for key in (
                "return_code", "success", "timed_out", "timeout_reason", "started_at", "finished_at",
                "duration_seconds", "command", "meta_path_relative", "stdout_path_relative", "stderr_path_relative",
            )},
        })
    if not attempts:
        status = "not_scheduled" if planned is False else "missing_evidence" if planned is True else "not_observed"
    elif planned is False:
        status = "unexpected_execution"
    elif len(attempts) > 1:
        status = "multiple_attempts"
    else:
        status = attempts[0]["status"]
    return {"task_name": name, "planned": planned, "status": status, "attempts": attempts}


def _stage_status(tasks: list[dict]) -> str:
    statuses = {task["status"] for task in tasks}
    for status in ("timed_out", "failed", "unexpected_execution", "invalid_evidence", "multiple_attempts"):
        if status in statuses:
            return status
    if statuses == {"not_scheduled"}:
        return "not_scheduled"
    if statuses == {"not_observed"}:
        return "not_observed"
    if statuses <= {"succeeded", "not_scheduled"}:
        return "succeeded"
    return "incomplete"


def _summarize_stages(bound: dict) -> tuple[list[dict], list[dict]]:
    items = bound.get("items")
    if not isinstance(items, list) or any(not isinstance(item, dict) for item in items):
        raise ValueError("invalid_bound_process_items")
    plan = bound.get("planned_task_names")
    if plan is not None and (
        not isinstance(plan, list) or any(not isinstance(name, str) or not name for name in plan)
        or len(plan) != len(set(plan))
    ):
        raise ValueError("invalid_bound_process_plan")
    by_name: dict[str, list[dict]] = {}
    for item in items:
        name = item.get("task_name")
        if not isinstance(name, str) or not name:
            raise ValueError("missing_bound_task_name")
        by_name.setdefault(name, []).append(item)
    stages, known = [], set()
    for key, label, names, parent in _STAGES:
        tasks = []
        for name in names:
            known.add(name)
            planned = None if plan is None else name in plan or (parent is not None and parent in plan)
            tasks.append(_task_summary(name, by_name.get(name, []), planned))
        status = _stage_status(tasks)
        stages.append({"key": key, "name": label, "status": status, "status_label": _LABELS[status],
                       "observed_process_count": sum(len(task["attempts"]) for task in tasks), "tasks": tasks})
    other_names = (set(by_name) | set(plan or [])) - known
    others = [_task_summary(name, by_name.get(name, []), None if plan is None else name in plan)
              for name in sorted(other_names)]
    return stages, others


def build_core_execution_audit(
    *, settings: Any, run_id: str, artifacts: dict[str, Any], report_mode: str,
    publication_control: dict[str, Any] | None = None, reused_from_run_id: str | None = None,
) -> dict[str, Any]:
    """Use the immutable pre-report index, never the later mutable audit index."""
    control = publication_control or {}
    result = {
        "schema_version": 2, "scope": "current_run_core_processes", "run_id": run_id,
        "evidence_status": "unavailable", "process_count": None, "plan_recorded": None,
        "report_mode": report_mode, "reused_from_run_id": reused_from_run_id,
        "eligible_for_formal_publication": control.get("eligible_for_formal_publication"),
        "publication_status": "not_eligible" if control.get("eligible_for_formal_publication") is False
                              else "not_committed_at_report_build",
        "business_data_changes": "not_assessed", "scientific_reproduction_status": "not_verified",
        "scope_note": _SCOPE_NOTE, "stages": [], "other_tasks": [], "evidence": None,
    }
    raw_path = artifacts.get("calculation_input_index_file")
    if not raw_path:
        result["reason"] = "current_run_capture_index_missing"
        return result
    try:
        if not isinstance(raw_path, str):
            raise ValueError("invalid_capture_index_path")
        reference = capture_index_reference(settings, raw_path)
        if reference.get("capture_scope") != "workflow" or reference.get("capture_run_id") != run_id:
            raise ValueError("capture_does_not_belong_to_current_workflow")
        path = Path(settings.project_root) / reference["path"]
        accepted_roots = (
            Path(settings.runs_dir) / run_id / "calculation_inputs",
            Path(settings.reports_dir) / "archive/superseded_runs" / run_id / "calculation_inputs",
        )
        if path.parent.resolve() not in {root.resolve() for root in accepted_roots}:
            raise ValueError("capture_outside_current_run")
        raw = path.read_bytes()
        if hashlib.sha256(raw).hexdigest() != reference["sha256"]:
            raise ValueError("capture_index_changed_during_read")
        payload = json.loads(raw)
        bound_path = path.parent / payload["subprocess_index"]
        bound_raw = bound_path.read_bytes()
        if hashlib.sha256(bound_raw).hexdigest() != payload["subprocess_index_sha256"]:
            raise ValueError("bound_process_index_changed_during_read")
        bound = json.loads(bound_raw)
        if bound.get("run_id") != run_id:
            raise ValueError("bound_process_run_mismatch")
        stages, others = _summarize_stages(bound)
        result.update(
            evidence_status="verified", process_count=len(bound["items"]),
            plan_recorded="planned_task_names" in bound,
            stages=stages, other_tasks=others,
            evidence={**reference, "bound_index_path": str(bound_path.relative_to(settings.project_root)),
                      "bound_index_sha256": payload["subprocess_index_sha256"]},
        )
    except (CalculationCaptureError, OSError, ValueError, TypeError, KeyError) as exc:
        result.update(evidence_status="invalid", reason=f"{type(exc).__name__}:{exc}")
    return result


def core_execution_markdown(audit: Any) -> list[str]:
    if not isinstance(audit, dict) or audit.get("scope") != "current_run_core_processes":
        return ["该报告未记录可验证的本轮核心执行范围；历史架构说明不作为执行证明。", ""]
    lines = [audit["scope_note"], ""]
    if audit.get("evidence_status") != "verified":
        return [*lines, "本轮执行证据缺失或未通过绑定核验，执行状态保持未知。", ""]
    lines.extend(["| 核心阶段 | 本轮执行记录 | 子进程数 |", "| --- | --- | --- |"])
    for stage in audit["stages"]:
        lines.append(f"| {stage['name']} | {stage['status_label']} | {stage['observed_process_count']} |")
    mode = "轻量刷新，沿用既有筛选结果" if audit.get("report_mode") == "lightweight_refresh" else "完整报告构建"
    lines.extend(["", f"报告模式：{mode}。本表在报告构建时生成，正式发布状态以发布指针为准。", ""])
    return lines
