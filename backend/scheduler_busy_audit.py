"""Record a skipped scheduler attempt without taking ownership of active work."""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime
import hashlib
from pathlib import Path
import re
from typing import Any, Callable
from uuid import uuid4

from backend.audit_support import build_audit_output_dir, write_audit_payload


def record_scheduler_busy_skip(
    *, settings: Any, state_store: Any, task_recorder: Any,
    task_name: str, trigger_source: str, host_context: str,
    log: Callable[[str], None],
) -> dict[str, Any]:
    """Use independent history/artifacts; never finish the current task or run a shadow."""
    started_at = datetime.now().isoformat(timespec="seconds")
    result: dict[str, Any] = {
        "attempt_id": uuid4().hex,
        "task_name": task_name,
        "status": "skipped",
        "started_at": started_at,
        "finished_at": started_at,
        "skip_reason": "scheduler_busy",
        "trigger_source": trigger_source,
        "host_context": host_context,
        "screening_triggered": False,
        "report_refresh_triggered": False,
        "archive_shadow_observation": {"status": "skipped", "reason": "scheduler_busy"},
        "storage_observation": {"status": "skipped", "reason": "scheduler_busy"},
        "task_history_write_status": "pending",
        "scheduler_state_write_status": "pending",
        "logs_tail": [],
    }
    try:
        state = state_store.snapshot()
        result["busy_task"] = deepcopy(state.get("current_task"))
        result["scheduler_instance_id"] = state.get("scheduler_instance_id")
    except Exception as exc:
        result["busy_task"] = None
        result["busy_task_context_error"] = str(exc)

    task_info = None
    try:
        # The initial SQL row is already skipped, so later I/O failures cannot
        # leave a phantom running task. task_status is never touched.
        task_info = task_recorder.record_skipped_attempt(task_name, result=result)
        result["task_history_id"] = task_info["id"]
    except Exception as exc:
        result["task_history_id"] = None
        result["task_history_write_status"] = "failed"
        result["task_history_write_error"] = str(exc)

    safe_name = re.sub(r"[^A-Za-z0-9_.-]+", "_", task_name)[:80]
    task_token = str(result["task_history_id"] or f"noid_{result['attempt_id']}")
    line = f"[task-skip] task={task_name} trigger_source={trigger_source} reason=scheduler_busy attempt_id={result['attempt_id']}"
    result["logs_tail"] = [line]
    try:
        log_path = Path(settings.project_root) / "logs/tasks" / f"task_{task_token}_{safe_name}.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_path.write_text(f"{started_at} {line}\n", encoding="utf-8")
        result["task_log_path"] = str(log_path.resolve())
        result["task_log_write_status"] = "written"
    except Exception as exc:
        result["task_log_write_status"] = "failed"
        result["task_log_write_error"] = str(exc)

    try:
        # A skip must not displace the latest daily/weekly execution artifact.
        directory = build_audit_output_dir(settings, "scheduler_task_skips", datetime.now().strftime("%Y%m%d"))
        path = directory / f"{result['attempt_id']}_{safe_name}.json"
        write_audit_payload(path, {
            "audit_version": 1,
            "artifact_type": "scheduler_busy_skip",
            "persistence_stage": "before_final_history_and_state",
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "business_runner_executed": False,
            "task": {"task_history_id": result["task_history_id"], "task_name": task_name,
                     "attempt_id": result["attempt_id"], "status": "skipped"},
            "task_result": deepcopy(result),
            "truth_note": "Independent skipped attempt; active workflow and publication are not attributed to it. Final persistence outcomes are recorded in SQL/state after this artifact is written.",
        })
        result["task_run_audit_status"] = "written"
        result["task_run_audit_path"] = str(path.resolve())
        result["task_run_audit_sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
    except Exception as exc:
        result["task_run_audit_status"] = "failed"
        result["task_run_audit_error"] = str(exc)

    result["scheduler_state_write_status"] = "written"
    if task_info is not None:
        result["task_history_write_status"] = "written"
        try:
            task_recorder.finish_task(task_info, status="skipped", extra_data=result, update_task_status=False)
        except Exception as exc:
            result["task_history_write_status"] = "failed"
            result["task_history_write_error"] = str(exc)
    try:
        state_store.record_busy_skip(result)
    except Exception as exc:
        result["scheduler_state_write_status"] = "failed"
        result["scheduler_state_write_error"] = str(exc)
        if task_info is not None:
            try:
                task_recorder.finish_task(task_info, status="skipped", extra_data=result, update_task_status=False)
            except Exception as sql_exc:
                result["task_history_write_status"] = "failed"
                result["task_history_write_error"] = str(sql_exc)
    log(f"[scheduler-busy-skip] task={task_name} attempt_id={result['attempt_id']} "
        f"trigger_source={trigger_source} audit={result['task_run_audit_status']} "
        f"history={result['task_history_write_status']} state={result['scheduler_state_write_status']} "
        f"log={result['task_log_write_status']}")
    return result
