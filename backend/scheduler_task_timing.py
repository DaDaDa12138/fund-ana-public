"""Measured task phases and the completion event shared by runtime projections."""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime
from pathlib import Path
from time import monotonic
from typing import Any

from backend.task_audit_storage import read_task_audit, write_task_audit


TIMING_FIELDS = ("business_started_at", "business_finished_at", "task_timing")


def copy_task_timing_fields(*sources: dict[str, Any]) -> dict[str, Any]:
    """Keep explicit unknowns; legacy records have no invented phase evidence."""
    fields: dict[str, Any] = {}
    for source in sources:
        for key in TIMING_FIELDS:
            if key in source and key not in fields:
                fields[key] = deepcopy(source[key])
    return fields


class TaskTiming:
    def __init__(self, *, measure_total: bool = True):
        self._started = monotonic() if measure_total else None
        self._started_wall = datetime.now() if measure_total else None
        self._phase_starts: dict[str, float] = {}
        self.payload: dict[str, Any] = {
            "schema_version": 1,
            "completion_boundary": "final_task_history_transaction",
            "clock": "host_local_wall_time_with_monotonic_durations",
            "utc_offset": datetime.now().astimezone().strftime("%z"),
            "phases": {},
            "elapsed_seconds": None,
            "wall_elapsed_seconds": None,
        }

    def start_phase(self, name: str) -> None:
        self._phase_starts[name] = monotonic()
        self.payload["phases"][name] = {
            "started_at": datetime.now().isoformat(timespec="microseconds"),
            "finished_at": None, "elapsed_seconds": None,
        }

    def finish_phase(self, name: str, *, at: datetime | None = None) -> None:
        phase = self.payload["phases"][name]
        phase["finished_at"] = (at or datetime.now()).isoformat(timespec="microseconds")
        phase["elapsed_seconds"] = round(monotonic() - self._phase_starts.pop(name), 6)

    def prepare_result(self, result: dict[str, Any], task_info: dict[str, Any]) -> None:
        # The runner's declared dates remain evidence, including explicit NULL.
        result["business_started_at"] = result.get("started_at")
        result["business_finished_at"] = result.get("finished_at")
        result["started_at"] = str(task_info.get("started_at") or "").replace(" ", "T") or None
        result["finished_at"] = None
        self.start_phase("finalization")
        result["task_timing"] = deepcopy(self.payload)

    def complete_result(self, result: dict[str, Any], *, at: datetime) -> None:
        """Called after the recorder acquires its write lock, never with runner time."""
        self.finish_phase("finalization", at=at)
        self.payload["elapsed_seconds"] = (
            round(monotonic() - self._started, 6) if self._started is not None else None
        )
        self.payload["wall_elapsed_seconds"] = (
            (at - self._started_wall).total_seconds() if self._started_wall is not None else None
        )
        result["finished_at"] = at.isoformat(timespec="microseconds")
        result["task_timing"] = deepcopy(self.payload)


def seal_task_run_audit(path: str | Path, result: dict[str, Any]) -> None:
    """Publish completion into the just-created audit, without rerunning its queries.

    Audit collection and runtime-summary refresh precede the SQL completion event.
    This write only propagates that event. Its actual finish is logged separately;
    it is not claimed to have happened at the earlier business return time.
    """
    target = Path(path)
    audit = read_task_audit(target)
    if audit.get("persistence_stage") == "completion_recorded":
        raise ValueError("task_audit_already_sealed")
    task = audit.setdefault("task", {})
    task.update({"started_at": result.get("started_at"), "finished_at": result.get("finished_at"),
                 "status": result.get("status"), **copy_task_timing_fields(result)})
    audit["task_result"] = deepcopy(result)
    scheduler = audit.setdefault("scheduler", {})
    from backend.scheduler_status_support import classify_scheduler_run_semantic_status

    for key in ("task_run_summary", "task_closure"):
        view = scheduler.setdefault(key, {})
        view.update({"started_at": result.get("started_at"), "finished_at": result.get("finished_at"),
                     "status": result.get("status"), **copy_task_timing_fields(result)})
        for field in ("publication_result", "status_detail", "degraded", "degraded_stage",
                     "task_run_audit_status", "task_run_audit_error", "runtime_status_summary_refresh_error"):
            if field in result:
                view[field] = deepcopy(result[field])
        view.update(classify_scheduler_run_semantic_status(
            status=result.get("status"), skip_reason=result.get("skip_reason") or result.get("warning_reason"),
            no_action_category=view.get("no_action_category"),
            degraded=bool(result.get("degraded")) or result.get("status") == "degraded",
        ))
    audit["persistence_stage"] = "completion_recorded"
    audit["completion_recording_started_at"] = datetime.now().isoformat(timespec="microseconds")
    write_task_audit(target, audit, replace_existing=True)
