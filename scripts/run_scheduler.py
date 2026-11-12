#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import datetime
import json
import os
import signal
import sys
import time
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


PROJECT_ROOT = Path(__file__).resolve().parent.parent
VENV_PYTHON = PROJECT_ROOT / ".venv" / "bin" / "python"
_VENV_BOOTSTRAP_ENV = "BOND_SCREENING_VENV_BOOTSTRAPPED"
_ACTIVE_REQUEST_STATUSES = {"accepted", "running"}
_TERMINAL_REQUEST_STATUSES = {
    "success", "degraded", "warning", "skipped", "failed", "timeout", "rejected", "cancelled",
}


def _ensure_project_venv() -> None:
    if not VENV_PYTHON.exists():
        return
    current_prefix = Path(getattr(sys, "prefix", "") or "").resolve()
    target_prefix = (PROJECT_ROOT / ".venv").resolve()
    if current_prefix == target_prefix:
        return
    attempted_prefix = os.environ.get(_VENV_BOOTSTRAP_ENV, "")
    if attempted_prefix == str(target_prefix):
        return
    if Path(sys.executable) != VENV_PYTHON:
        os.environ[_VENV_BOOTSTRAP_ENV] = str(target_prefix)
        os.execv(str(VENV_PYTHON), [str(VENV_PYTHON), *sys.argv])

sys.path.insert(0, str(PROJECT_ROOT))


def _load_scheduler_runtime_snapshot(settings) -> dict:
    if not getattr(settings, "scheduler_state_file", None):
        return {}
    from backend.scheduler import ProjectScheduler  # noqa: E402
    from backend.scheduler_state import SchedulerStateStore  # noqa: E402

    state_store = SchedulerStateStore(settings.scheduler_state_file)
    payload = state_store.snapshot()
    running_pid = int(payload.get("pid") or 0)
    host_context = str(payload.get("host_context") or "").strip().lower()
    scheduler_running = bool(payload.get("scheduler_running"))
    runtime_evidence = ProjectScheduler._scheduler_runtime_evidence(
        settings,
        state_payload=payload,
    )
    scheduler_alive = bool(runtime_evidence.get("alive"))
    return {
        "host_mode": str(getattr(settings, "scheduler_host_mode", "embedded_web") or "embedded_web").strip().lower(),
        "host_context": host_context,
        "scheduler_running": scheduler_running,
        "scheduler_alive": scheduler_alive,
        "scheduler_runtime_evidence": runtime_evidence,
        "pid": running_pid,
        "web_port_available": bool(getattr(settings, "web_port", None)),
    }


def _safe_load_json_dict(path: str | Path | None) -> dict:
    if not path:
        return {}
    try:
        file_path = Path(path)
    except TypeError:
        return {}
    if not file_path.exists():
        return {}
    try:
        payload = json.loads(file_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _parse_iso_datetime(value: str | None) -> datetime | None:
    normalized = str(value or "").strip()
    if not normalized:
        return None
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


def _load_workflow_runtime_snapshot(settings) -> dict:
    payload = _safe_load_json_dict(getattr(settings, "state_file", None))
    logs = payload.get("logs") or []
    last_log = ""
    if isinstance(logs, list):
        for item in reversed(logs):
            text = str(item or "").strip()
            if text:
                last_log = text
                break
    return {
        "status": str(payload.get("status") or "").strip(),
        "phase": str(payload.get("phase") or "").strip(),
        "run_id": str(payload.get("run_id") or "").strip(),
        "started_at": str(payload.get("started_at") or "").strip(),
        "finished_at": str(payload.get("finished_at") or "").strip(),
        "last_updated_at": str(payload.get("last_updated_at") or "").strip(),
        "last_log": last_log,
    }


def _load_publication_runtime_snapshot(settings) -> dict:
    return _safe_load_json_dict(getattr(settings, "publication_registry_file", None))


def _find_manual_run_entry(snapshot: dict, request_id: str) -> dict | None:
    normalized_request_id = str(request_id or "").strip()
    if not normalized_request_id:
        return None
    for item in snapshot.get("manual_run_history") or []:
        if str((item or {}).get("request_id") or "").strip() == normalized_request_id:
            return dict(item)
    return None


def _emit_once_progress(event: str, **payload) -> None:
    message = {
        "type": "run_scheduler_once_progress",
        "ts": datetime.now().isoformat(timespec="seconds"),
        "event": str(event or "").strip() or "unknown",
    }
    for key, value in payload.items():
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        message[key] = value
    print(json.dumps(message, ensure_ascii=False), file=sys.stderr, flush=True)


def _build_publication_result(settings, workflow_run_id: str) -> dict:
    normalized_run_id = str(workflow_run_id or "").strip()
    if not normalized_run_id:
        return {}
    registry = _load_publication_runtime_snapshot(settings)
    automation_layers = dict(registry.get("automation_layers") or {})
    latest_successful_run = dict(automation_layers.get("latest_successful_run") or {})
    if str(latest_successful_run.get("run_id") or "").strip() != normalized_run_id:
        return {}
    governance = dict(latest_successful_run.get("publish_governance_summary") or {})
    blockers = list(latest_successful_run.get("publish_blockers") or [])
    current_published_run_id = str(registry.get("current_published_run_id") or "").strip()
    latest_is_currently_published = bool(automation_layers.get("latest_successful_is_currently_published"))
    legacy_publish_hold = bool(latest_successful_run.get("publish_hold"))
    governance_status = str(governance.get("status") or "").strip().lower()
    has_blocking_blocker = any(
        isinstance(item, dict)
        and (
            bool(item.get("blocking"))
            or str(item.get("severity") or "").strip().lower() == "blocking"
        )
        for item in blockers
    )
    if latest_is_currently_published or current_published_run_id == normalized_run_id:
        status = "published"
    elif legacy_publish_hold or governance_status in {"held", "blocked"} or has_blocking_blocker:
        status = "blocked"
    elif bool(latest_successful_run.get("publishable")):
        status = "blocked"
    else:
        status = "unknown"
    return {
        "status": status,
        "run_id": normalized_run_id,
        "current_published_run_id": current_published_run_id or None,
        "latest_successful_is_currently_published": latest_is_currently_published,
        "publishable": bool(latest_successful_run.get("publishable")),
        "publish_hold": False,
        "legacy_publish_hold": legacy_publish_hold,
        "publish_hold_reason_codes": list(latest_successful_run.get("publish_hold_reason_codes") or []),
        "primary_blocker_code": str(latest_successful_run.get("primary_blocker_code") or "").strip() or None,
        "primary_blocker_label": str(latest_successful_run.get("primary_blocker_label") or "").strip() or None,
        "primary_blocker_detail": str(latest_successful_run.get("primary_blocker_detail") or "").strip() or None,
        "publish_governance_status": governance_status or None,
        "publish_governance_summary": governance,
        "publish_blockers": blockers,
    }


def _decorate_once_result(
    settings,
    result: dict | None,
    *,
    workflow_snapshot: dict | None = None,
    default_execution_mode: str | None = None,
) -> dict:
    payload = _finalize_cli_result(result, default_execution_mode=default_execution_mode)
    workflow = _linked_workflow_snapshot(payload, workflow_snapshot or {})
    nested_result = payload.get("result") if isinstance(payload.get("result"), dict) else {}
    workflow_run_id = str(
        payload.get("workflow_run_id")
        or payload.get("screening_run_id")
        or payload.get("run_id")
        or nested_result.get("workflow_run_id")
        or nested_result.get("screening_run_id")
        or workflow.get("run_id")
        or ""
    ).strip()
    if workflow_run_id:
        payload["workflow_run_id"] = workflow_run_id
    publication_result = _build_publication_result(settings, workflow_run_id)
    if publication_result:
        payload["publication_result"] = publication_result
        terminal_status = str(payload.get("status") or "").strip().lower()
        if terminal_status in {"success", "degraded"}:
            publication_status = str(publication_result.get("status") or "").strip()
            if publication_status == "blocked":
                payload["status"] = "degraded"
                payload["warning_reason"] = (
                    str(payload.get("warning_reason") or "").strip()
                    or "formal_publication_blocked_unpublished"
                )
                payload["degraded_stage"] = (
                    str(payload.get("degraded_stage") or "").strip()
                    or "formal_publication"
                )
            if publication_status == "published":
                payload["status_detail"] = "success_and_published"
            elif publication_status == "blocked":
                payload["status_detail"] = "publication_blocked_unpublished"
    return payload


def _linked_workflow_snapshot(request: dict, workflow: dict) -> dict:
    """A later start time is not evidence that a workflow belongs to a request."""
    nested = request.get("result") if isinstance(request.get("result"), dict) else {}
    run_id = str(
        request.get("workflow_run_id") or request.get("screening_run_id") or request.get("run_id")
        or nested.get("workflow_run_id") or nested.get("screening_run_id") or ""
    ).strip()
    if run_id and run_id == str(workflow.get("run_id") or "").strip():
        return dict(workflow)
    return {}


def _finalize_cli_result(result: dict | None, *, default_execution_mode: str | None = None) -> dict:
    payload = dict(result or {})
    if default_execution_mode and not str(payload.get("execution_mode") or "").strip():
        payload["execution_mode"] = default_execution_mode
    status = str(payload.get("status") or "").strip().lower()
    error = str(payload.get("error") or "").strip()
    if not payload.get("error_code"):
        if error:
            payload["error_code"] = error
        elif status == "timeout":
            payload["error_code"] = "scheduler_api_timeout"
    return payload


def _should_relay_once_to_embedded_scheduler(settings) -> bool:
    runtime = _load_scheduler_runtime_snapshot(settings)
    return bool(
        runtime.get("host_mode") == "embedded_web"
        and runtime.get("web_port_available")
        and runtime.get("scheduler_alive")
    )


def _should_relay_once_to_external_scheduler(settings) -> bool:
    runtime = _load_scheduler_runtime_snapshot(settings)
    return bool(
        runtime.get("host_mode") == "external_process"
        and runtime.get("web_port_available")
        and runtime.get("scheduler_alive")
        and runtime.get("host_context") == "external_scheduler_process"
    )


def _relay_once_to_embedded_scheduler(settings, job_id: str) -> dict:
    url = f"http://127.0.0.1:{int(settings.web_port)}/api/scheduler/internal-run-once"
    request_payload = json.dumps({"job_id": job_id}, ensure_ascii=False).encode("utf-8")
    request = Request(
        url,
        data=request_payload,
        headers={
            "Content-Type": "application/json",
            "X-Fund-ANA-Internal-Run": "scheduler-cli",
        },
        method="POST",
    )
    try:
        with urlopen(request, timeout=15) as response:
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        payload = exc.read().decode("utf-8", errors="ignore")
        try:
            parsed = json.loads(payload) if payload else {}
        except json.JSONDecodeError:
            parsed = {}
        return {
            "status": parsed.get("status") or "failed",
            "error": parsed.get("error") or f"http_{exc.code}",
            **parsed,
        }
    except URLError as exc:
        return {
            "status": "failed",
            "error": f"embedded_scheduler_unreachable: {exc.reason}",
        }


def _relay_once_to_scheduler_api(settings, job_id: str) -> dict:
    url = f"http://127.0.0.1:{int(settings.web_port)}/api/scheduler/internal-run-once"
    request_payload = json.dumps({"job_id": job_id}, ensure_ascii=False).encode("utf-8")
    request = Request(
        url,
        data=request_payload,
        headers={
            "Content-Type": "application/json",
            "X-Fund-ANA-Internal-Run": "scheduler-cli",
        },
        method="POST",
    )
    try:
        with urlopen(request, timeout=15) as response:
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        payload = exc.read().decode("utf-8", errors="ignore")
        try:
            parsed = json.loads(payload) if payload else {}
        except json.JSONDecodeError:
            parsed = {}
        return {
            "status": parsed.get("status") or "failed",
            "error": parsed.get("error") or f"http_{exc.code}",
            **parsed,
        }
    except URLError as exc:
        return {
            "status": "failed",
            "error": f"scheduler_api_unreachable: {exc.reason}",
        }


def _enqueue_once_to_external_scheduler(scheduler, job_id: str) -> dict:
    try:
        result = scheduler.enqueue_manual_run_request(job_id, requested_by="scheduler_cli")
    except ValueError as exc:
        return {
            "status": "invalid_request",
            "error": str(exc),
            "execution_mode": "external_scheduler_queue",
        }
    payload = dict(result or {})
    payload.setdefault("execution_mode", "external_scheduler_queue")
    return payload


def _wait_for_manual_run_completion(
    settings,
    request_id: str,
    *,
    timeout_seconds: int = 7200,
    execution_mode: str = "scheduler_api_relay",
    emit_progress: bool = False,
    poll_interval_seconds: float = 0.5,
) -> dict:
    from backend.scheduler_state import SchedulerStateStore  # noqa: E402

    state_store = SchedulerStateStore(settings.scheduler_state_file)
    deadline = time.time() + max(1, int(timeout_seconds))
    last_snapshot: dict | None = None
    last_manual_status = ""
    last_current_task_signature: tuple[str, str] | None = None
    last_workflow_signature: tuple[str, str, str, str] | None = None
    waiting_for_claim_emitted = False
    while time.time() < deadline:
        snapshot = state_store.snapshot()
        last_snapshot = snapshot
        item = _find_manual_run_entry(snapshot, request_id)
        status = str((item or {}).get("status") or "").strip().lower()
        current_task = snapshot.get("current_task") or {}
        if not isinstance(current_task, dict):
            current_task = {}
        workflow = _load_workflow_runtime_snapshot(settings)

        if emit_progress:
            if status == "accepted" and not waiting_for_claim_emitted:
                _emit_once_progress(
                    "waiting_for_scheduler_claim",
                    request_id=request_id,
                    execution_mode=execution_mode,
                    job_id=(item or {}).get("job_id"),
                    task_name=(item or {}).get("task_name"),
                )
                waiting_for_claim_emitted = True
            if status and status != last_manual_status:
                _emit_once_progress(
                    "manual_request_status",
                    request_id=request_id,
                    execution_mode=execution_mode,
                    status=status,
                    job_id=(item or {}).get("job_id"),
                    task_name=(item or {}).get("task_name"),
                )
                last_manual_status = status
            current_task_signature = (
                str(current_task.get("task_name") or "").strip(),
                str(current_task.get("started_at") or "").strip(),
            )
            if current_task_signature != last_current_task_signature and current_task_signature[0]:
                _emit_once_progress(
                    "scheduler_task_started",
                    request_id=request_id,
                    execution_mode=execution_mode,
                    task_name=current_task_signature[0],
                    started_at=current_task_signature[1],
                )
                last_current_task_signature = current_task_signature
            workflow_signature = (
                str(workflow.get("status") or "").strip(),
                str(workflow.get("phase") or "").strip(),
                str(workflow.get("run_id") or "").strip(),
                str(workflow.get("last_log") or "").strip(),
            )
            workflow_progress_relevant = (
                workflow_signature[0] == "running" and bool(_linked_workflow_snapshot(item or {}, workflow))
            )
            if (
                workflow_signature != last_workflow_signature
                and any(workflow_signature[:3])
                and workflow_progress_relevant
            ):
                _emit_once_progress(
                    "workflow_state",
                    request_id=request_id,
                    execution_mode=execution_mode,
                    workflow_status=workflow_signature[0],
                    workflow_phase=workflow_signature[1],
                    workflow_run_id=workflow_signature[2],
                    workflow_last_log=workflow_signature[3],
                )
                last_workflow_signature = workflow_signature

        if item:
            if status in _TERMINAL_REQUEST_STATUSES:
                result = dict(item)
                result["task_terminal"] = True
                result["background_continues"] = False
                result.setdefault("execution_mode", execution_mode)
                result = _decorate_once_result(
                    settings,
                    result,
                    workflow_snapshot=workflow,
                    default_execution_mode=execution_mode,
                )
                if emit_progress:
                    terminal_workflow = _linked_workflow_snapshot(result, workflow)
                    result_workflow_run_id = str(result.get("workflow_run_id") or "").strip()
                    latest_workflow = _load_workflow_runtime_snapshot(settings)
                    latest_workflow_run_id = str(latest_workflow.get("run_id") or "").strip()
                    if latest_workflow_run_id and latest_workflow_run_id == result_workflow_run_id:
                        terminal_workflow = latest_workflow
                    workflow_signature = (
                        str(terminal_workflow.get("status") or "").strip(),
                        str(terminal_workflow.get("phase") or "").strip(),
                        str(terminal_workflow.get("run_id") or "").strip(),
                        str(terminal_workflow.get("last_log") or "").strip(),
                    )
                    if (
                        workflow_signature != last_workflow_signature
                        and any(workflow_signature[:3])
                        and workflow_signature[2]
                        and workflow_signature[2] == result_workflow_run_id
                    ):
                        _emit_once_progress(
                            "workflow_state",
                            request_id=request_id,
                            execution_mode=execution_mode,
                            workflow_status=workflow_signature[0],
                            workflow_phase=workflow_signature[1],
                            workflow_run_id=workflow_signature[2],
                            workflow_last_log=workflow_signature[3],
                        )
                        last_workflow_signature = workflow_signature
                    terminal_meta = dict(result.get("result") or {}) if isinstance(result.get("result"), dict) else {}
                    publication_meta = (
                        dict(result.get("publication_result") or {})
                        if isinstance(result.get("publication_result"), dict)
                        else {}
                    )
                    _emit_once_progress(
                        "terminal_status",
                        request_id=request_id,
                        execution_mode=result.get("execution_mode"),
                        status=result.get("status"),
                        status_detail=result.get("status_detail"),
                        error=result.get("error"),
                        error_code=result.get("error_code"),
                        warning_reason=terminal_meta.get("warning_reason"),
                        degraded_stage=terminal_meta.get("degraded_stage"),
                        skip_reason=terminal_meta.get("skip_reason"),
                        publication_status=publication_meta.get("status"),
                        publish_hold=publication_meta.get("publish_hold"),
                        primary_blocker_code=publication_meta.get("primary_blocker_code"),
                    )
                return result
        time.sleep(max(0.1, float(poll_interval_seconds)))
    result = {
        "request_id": request_id,
        "status": "pending",
        "observation_status": "wait_timeout",
        "task_terminal": False,
        "wait_timeout_seconds": timeout_seconds,
        "last_scheduler_state": (last_snapshot or {}).get("current_task"),
        "last_manual_request": next(
            (
                dict(item)
                for item in ((last_snapshot or {}).get("manual_run_history") or [])
                if str(item.get("request_id") or "").strip() == str(request_id or "").strip()
            ),
            None,
        ),
        "execution_mode": execution_mode,
    }
    observed_status = str((result["last_manual_request"] or {}).get("status") or "unknown")
    result["background_task_status"] = observed_status
    result["background_continues"] = True if observed_status in {"accepted", "running"} else None
    result["follow_up"] = {"action": "query_existing_request", "request_id": request_id}
    if emit_progress:
        workflow = _linked_workflow_snapshot(
            result["last_manual_request"] or {}, _load_workflow_runtime_snapshot(settings)
        )
        _emit_once_progress(
            "wait_timeout",
            request_id=request_id,
            execution_mode=execution_mode,
            error=result.get("error"),
            error_code=result.get("error_code"),
            scheduler_task_name=((last_snapshot or {}).get("current_task") or {}).get("task_name"),
            workflow_status=workflow.get("status"),
            workflow_phase=workflow.get("phase"),
            workflow_run_id=workflow.get("run_id"),
        )
    return result


def _result_exit_code(result: dict) -> int:
    status = str(result.get("status") or "").strip().lower()
    if result.get("observation_status") == "interrupted":
        return 130
    if status in {"pending", "accepted", "running"}:
        return 2  # Observation is incomplete; the scheduled task has not failed.
    return 0 if status in {"success", "degraded", "warning", "skipped"} and not result.get("error") else 1


def _query_existing_request(settings, request_id: str) -> dict:
    from backend.scheduler_state import SchedulerStateStore

    snapshot = SchedulerStateStore(settings.scheduler_state_file).snapshot()
    item = _find_manual_run_entry(snapshot, request_id)
    if item is None:
        return {"request_id": request_id, "status": "not_found", "task_terminal": None,
                "error": "request_id_not_in_retained_history", "new_task_submitted": False}
    status = str(item.get("status") or "unknown").strip().lower()
    terminal = (False if status in _ACTIVE_REQUEST_STATUSES else True
                if status in _TERMINAL_REQUEST_STATUSES else None)
    result = {**item, "background_task_status": status, "task_terminal": terminal,
              "background_continues": None if terminal is None else not terminal, "new_task_submitted": False,
              "execution_mode": "existing_scheduler_request"}
    if result["task_terminal"] is True:
        result = _decorate_once_result(settings, result, workflow_snapshot=_load_workflow_runtime_snapshot(settings))
    return result


def _run_once_via_relay(
    settings,
    scheduler,
    *,
    job_id: str,
    execution_mode: str,
    request_payload: dict,
    wait_timeout_seconds: int = 7200,
    wait_for_completion: bool = True,
) -> dict:
    relay_result = dict(request_payload or {})
    relay_status = str(relay_result.get("status") or "").strip().lower()
    if relay_status == "accepted":
        request_id = str(relay_result.get("request_id") or "").strip()
        _emit_once_progress(
            "request_accepted",
            request_id=request_id,
            execution_mode=execution_mode,
            job_id=job_id,
            task_name=relay_result.get("task_name"),
        )
        if not wait_for_completion:
            return {**relay_result, "task_terminal": False, "background_continues": True,
                    "follow_up": {"action": "query_existing_request", "request_id": request_id}}
        return _wait_for_manual_run_completion(
            settings,
            request_id,
            execution_mode=execution_mode,
            emit_progress=True,
            timeout_seconds=wait_timeout_seconds,
        )
    result = _finalize_cli_result(
        {
            **relay_result,
            "execution_mode": execution_mode,
        },
        default_execution_mode=execution_mode,
    )
    _emit_once_progress(
        "request_not_accepted",
        request_id=result.get("request_id"),
        execution_mode=result.get("execution_mode"),
        job_id=job_id,
        status=result.get("status"),
        error=result.get("error"),
        error_code=result.get("error_code"),
    )
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="启动债券基金定时调度器")
    parser.add_argument("--config", default=str(PROJECT_ROOT / "config" / "project.yaml"), help="配置文件路径")
    parser.add_argument(
        "--once",
        choices=["daily_refresh", "intraday_refresh", "weekly_maintenance"],
        help="只执行一次指定调度任务，不进入常驻模式",
    )
    parser.add_argument("--force", action="store_true", help="忽略 scheduler.enabled=false，仍然启动")
    parser.add_argument("--request-id", default="", help="查询既有请求，不再次提交或启动调度器")
    parser.add_argument("--wait", action="store_true", help="配合 --request-id 等待该请求终态")
    parser.add_argument("--no-wait", action="store_true", help="--once 入队后立即返回 request ID；退出码 2 表示仍在执行")
    parser.add_argument("--wait-timeout-seconds", type=int, default=7200, help="CLI 最长观察秒数；超时不取消后台任务")
    args = parser.parse_args()
    if args.request_id and args.once:
        parser.error("--request-id 与 --once 不能同时使用；查询不会重新提交任务")
    if args.wait_timeout_seconds < 1:
        parser.error("--wait-timeout-seconds 必须大于 0")

    from backend.config import load_settings  # noqa: E402

    settings = load_settings(args.config)
    if args.request_id:
        result = _query_existing_request(settings, args.request_id)
        if args.wait and result.get("task_terminal") is False:
            result = _wait_for_manual_run_completion(settings, args.request_id,
                                                     timeout_seconds=args.wait_timeout_seconds, emit_progress=True)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return _result_exit_code(result)
    from backend.schema_bootstrap import ensure_database_schema  # noqa: E402
    from backend.scheduler import ProjectScheduler  # noqa: E402
    # P0-01 / B1: 调度进程进入常驻 / --once 任务循环前先把 schema 迁移跑完。
    ensure_database_schema(settings)
    host_mode = str(getattr(settings, "scheduler_host_mode", "embedded_web") or "embedded_web").strip().lower()
    if not settings.scheduler_enabled and not args.force and not args.once:
        print("scheduler.enabled=false，如需启动请传入 --force 或修改配置。")
        return 1
    if host_mode != "external_process" and not args.force and not args.once:
        print(
            "scheduler.host_mode 当前不是 external_process，独立调度脚本仅作为备用入口。"
            "如需强制启动请传入 --force 或修改配置。"
        )
        return 1

    scheduler = ProjectScheduler(settings=settings)

    if args.once:
        execution_mode = "local_run_once"
        try:
            if _should_relay_once_to_embedded_scheduler(settings):
                execution_mode = "scheduler_api_relay"
                relay_result = _relay_once_to_scheduler_api(settings, args.once)
                result = _run_once_via_relay(
                    settings,
                    scheduler,
                    job_id=args.once,
                    execution_mode=execution_mode,
                    request_payload=relay_result,
                    wait_timeout_seconds=args.wait_timeout_seconds,
                    wait_for_completion=not args.no_wait,
                )
            elif _should_relay_once_to_external_scheduler(settings):
                execution_mode = "external_scheduler_queue"
                relay_result = _enqueue_once_to_external_scheduler(scheduler, args.once)
                result = _run_once_via_relay(
                    settings,
                    scheduler,
                    job_id=args.once,
                    execution_mode=execution_mode,
                    request_payload=relay_result,
                    wait_timeout_seconds=args.wait_timeout_seconds,
                    wait_for_completion=not args.no_wait,
                )
            else:
                _emit_once_progress(
                    "local_run_started",
                    job_id=args.once,
                    execution_mode=execution_mode,
                )
                result = _decorate_once_result(
                    settings,
                    scheduler.run_once(args.once),
                    workflow_snapshot=_load_workflow_runtime_snapshot(settings),
                    default_execution_mode=execution_mode,
                )
        except KeyboardInterrupt:
            request_id = str((locals().get("relay_result") or {}).get("request_id") or "")
            result = (_query_existing_request(settings, request_id) if request_id else
                      {"job_id": args.once, "status": "unknown", "execution_mode": execution_mode})
            result["observation_status"] = "interrupted"
            _emit_once_progress(
                "interrupted",
                job_id=args.once,
                execution_mode=execution_mode,
                request_id=request_id,
            )
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return _result_exit_code(result)

    def _handle_stop(signum, frame) -> None:  # noqa: ARG001
        scheduler.shutdown()
        raise SystemExit(0)

    signal.signal(signal.SIGINT, _handle_stop)
    signal.signal(signal.SIGTERM, _handle_stop)
    scheduler.serve_forever()
    return 0


if __name__ == "__main__":
    _ensure_project_venv()
    raise SystemExit(main())
