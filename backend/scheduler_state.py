from __future__ import annotations

import fcntl
import json
import os
import threading
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import uuid4


class SchedulerStateStore:
    STATE_SCHEMA_VERSION = 4

    def __init__(self, state_file: str | Path):
        self.state_file = Path(state_file)
        self._lock = threading.RLock()
        self._lock_file = self.state_file.with_suffix(f"{self.state_file.suffix}.lock")

    @staticmethod
    def _default_state() -> dict[str, Any]:
        return {
            "state_schema_version": SchedulerStateStore.STATE_SCHEMA_VERSION,
            "scheduler_running": False,
            "pid": None,
            "scheduler_instance_id": None,
            "started_at": None,
            "stopped_at": None,
            "last_heartbeat_at": None,
            "host_context": None,
            "jobs": [],
            "current_task": None,
            "last_runs": {},
            "recent_busy_skips": [],
            "last_error": None,
            "manual_run_history": [],
            "pending_manual_run_requests": [],
            "automation_host_mode": None,
            "runtime_api_config": {},
            "effective_nav_snapshot": {},
            "latest_screened_snapshot": {},
            "task_closures": {},
            "child_task_idle_streaks": {},
            "nav_freshness": {},
            "maintenance_summary": {},
            "schedule_stability_summary": {"summary": {}, "tasks": []},
            "automation_closure_digest": {"summary": {}, "alerts": [], "ordered_tasks": []},
            "startup_catch_up_due_jobs": [],
            "startup_catch_up_pending": False,
            "screening_backlog_due": False,
            "screening_backlog_reason": None,
            "configured_job_ids": [],
            "missing_job_ids": [],
            "unexpected_job_ids": [],
            "job_drift_detected": False,
        }

    @staticmethod
    def _normalize_manual_run_entry(entry: Any) -> dict[str, Any] | None:
        if not isinstance(entry, dict):
            return None
        normalized = deepcopy(entry)
        status = str(normalized.get("status") or "").strip().lower()
        result = normalized.get("result") or {}
        if not isinstance(result, dict):
            result = {}
        screening_triggered = bool(result.get("screening_triggered"))
        report_refresh_triggered = bool(result.get("report_refresh_triggered"))
        skip_reason = str(result.get("skip_reason") or "").strip()
        has_terminal_workflow_action = screening_triggered or report_refresh_triggered
        if status == "skipped" and skip_reason and not has_terminal_workflow_action:
            normalized.pop("workflow_run_id", None)
            normalized.pop("publication_result", None)
            normalized.pop("status_detail", None)
            return normalized
        return SchedulerStateStore._normalize_legacy_publication_tracking(normalized)

    @staticmethod
    def _normalize_legacy_publish_blocker(blocker: Any) -> Any:
        if not isinstance(blocker, dict):
            return blocker
        normalized = deepcopy(blocker)
        code = str(normalized.get("code") or "").strip()
        if code != "publish_hold_by_contraction_guard":
            return normalized
        normalized["severity"] = "blocking"
        normalized["blocking"] = True
        if str(normalized.get("label") or "").strip() in {"", "发布暂缓"}:
            normalized["label"] = "发布阻断"
        normalized["governance_stage"] = (
            str(normalized.get("governance_stage") or "").strip()
            or "legacy_publication_guard"
        )
        return normalized

    @staticmethod
    def _safe_int(value: Any, default: int = 0) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _normalize_legacy_publication_result(publication_result: Any) -> tuple[Any, bool]:
        if not isinstance(publication_result, dict):
            return publication_result, False
        normalized = deepcopy(publication_result)
        publication_status = str(normalized.get("status") or "").strip().lower()
        legacy_hold = bool(publication_status == "publish_hold" or normalized.get("publish_hold") is True)
        if not legacy_hold:
            return normalized, False

        normalized["status"] = "blocked"
        normalized["publish_blocked"] = True
        normalized["legacy_publish_hold"] = True
        normalized["publish_hold"] = False
        if str(normalized.get("publish_governance_status") or "").strip().lower() in {"", "held", "publish_hold"}:
            normalized["publish_governance_status"] = "blocked"

        blockers = normalized.get("publish_blockers")
        if isinstance(blockers, list):
            normalized["publish_blockers"] = [
                SchedulerStateStore._normalize_legacy_publish_blocker(item)
                for item in blockers
            ]

        governance = normalized.get("publish_governance_summary")
        if isinstance(governance, dict):
            governance = deepcopy(governance)
            governance["publish_blocked"] = True
            governance["status"] = "blocked"
            governance["highest_severity"] = "blocking"
            governance["blocking_total"] = max(
                1,
                SchedulerStateStore._safe_int(governance.get("blocking_total")),
            )
            governance["warning_total"] = max(
                0,
                SchedulerStateStore._safe_int(governance.get("warning_total")) - 1,
            )
            if str(governance.get("primary_blocker_code") or "").strip() == "publish_hold_by_contraction_guard":
                if str(governance.get("primary_blocker_label") or "").strip() in {"", "发布暂缓"}:
                    governance["primary_blocker_label"] = "发布阻断"
                governance["primary_governance_stage"] = "legacy_publication_guard"
            normalized["publish_governance_summary"] = governance

        if str(normalized.get("primary_blocker_code") or "").strip() == "publish_hold_by_contraction_guard":
            if str(normalized.get("primary_blocker_label") or "").strip() in {"", "发布暂缓"}:
                normalized["primary_blocker_label"] = "发布阻断"
        return normalized, True

    @staticmethod
    def _normalize_legacy_publication_tracking(entry: Any) -> Any:
        if not isinstance(entry, dict):
            return entry
        normalized = deepcopy(entry)
        publication_result, legacy_hold = SchedulerStateStore._normalize_legacy_publication_result(
            normalized.get("publication_result")
        )
        if isinstance(publication_result, dict):
            normalized["publication_result"] = publication_result
        status_detail = str(normalized.get("status_detail") or "").strip()
        legacy_hold = legacy_hold or status_detail == "success_but_publish_hold"
        if not legacy_hold:
            return normalized

        status = str(normalized.get("status") or "").strip().lower()
        if status in {"success", "succeeded"}:
            normalized["status"] = "degraded"
        normalized["status_detail"] = "publication_blocked_unpublished"
        normalized["warning_reason"] = (
            str(normalized.get("warning_reason") or "").strip()
            or "formal_publication_blocked_unpublished"
        )
        normalized["degraded_stage"] = (
            str(normalized.get("degraded_stage") or "").strip()
            or "formal_publication"
        )

        result = normalized.get("result")
        if isinstance(result, dict):
            result = deepcopy(result)
            result_status = str(result.get("status") or "").strip().lower()
            if result_status in {"success", "succeeded"}:
                result["status"] = "degraded"
            result["warning_reason"] = (
                str(result.get("warning_reason") or "").strip()
                or "formal_publication_blocked_unpublished"
            )
            result["degraded_stage"] = (
                str(result.get("degraded_stage") or "").strip()
                or "formal_publication"
            )
            normalized["result"] = result
        return normalized

    @staticmethod
    def _normalize_manual_run_history(history: Any) -> list[dict[str, Any]]:
        if not isinstance(history, list):
            return []
        normalized: list[dict[str, Any]] = []
        for item in history:
            normalized_item = SchedulerStateStore._normalize_manual_run_entry(item)
            if normalized_item is not None:
                normalized.append(normalized_item)
        normalized.sort(key=lambda item: str(item.get("requested_at") or ""), reverse=True)
        return normalized[:20]

    @staticmethod
    def _normalize_pending_manual_run_requests(history: Any) -> list[dict[str, Any]]:
        if not isinstance(history, list):
            return []
        normalized: list[dict[str, Any]] = []
        for item in history:
            if isinstance(item, dict):
                normalized.append(deepcopy(item))
        normalized.sort(
            key=lambda item: (
                str(item.get("requested_at") or ""),
                str(item.get("request_id") or ""),
            )
        )
        return normalized[:20]

    def _load_state(self) -> dict[str, Any]:
        if not self.state_file.exists():
            return self._default_state()
        try:
            payload = json.loads(self.state_file.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError, TypeError, ValueError):
            return self._default_state()
        if not isinstance(payload, dict):
            return self._default_state()
        merged = self._default_state()
        merged.update(payload)
        merged["state_schema_version"] = max(
            self.STATE_SCHEMA_VERSION,
            int(merged.get("state_schema_version") or 0),
        )
        merged["manual_run_history"] = self._normalize_manual_run_history(merged.get("manual_run_history"))
        merged["pending_manual_run_requests"] = self._normalize_pending_manual_run_requests(
            merged.get("pending_manual_run_requests")
        )
        if isinstance(merged.get("last_runs"), dict):
            merged["last_runs"] = {
                key: self._normalize_legacy_publication_tracking(value)
                for key, value in (merged.get("last_runs") or {}).items()
            }
        if isinstance(merged.get("current_task"), dict):
            merged["current_task"] = self._normalize_legacy_publication_tracking(
                merged.get("current_task")
            )
        for key in (
            "jobs",
            "configured_job_ids",
            "missing_job_ids",
            "unexpected_job_ids",
            "startup_catch_up_due_jobs",
        ):
            if not isinstance(merged.get(key), list):
                merged[key] = []
        for key in (
            "last_runs",
            "effective_nav_snapshot",
            "latest_screened_snapshot",
            "task_closures",
            "child_task_idle_streaks",
            "nav_freshness",
            "maintenance_summary",
            "schedule_stability_summary",
            "automation_closure_digest",
            "runtime_api_config",
        ):
            if not isinstance(merged.get(key), dict):
                merged[key] = {}
        return merged

    def snapshot(self, *, include_compat_aliases: bool = True) -> dict[str, Any]:
        with self._lock:
            with self._open_lock_handle():
                merged = self._load_state()
            if include_compat_aliases:
                merged["latest_runs"] = deepcopy(merged.get("last_runs") or {})
            return merged

    def _open_lock_handle(self):
        self._lock_file.parent.mkdir(parents=True, exist_ok=True)
        handle = self._lock_file.open("a+", encoding="utf-8")
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        return handle

    def _persist(self, payload: dict[str, Any]) -> None:
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        temp_file = self.state_file.parent / f"{self.state_file.name}.{uuid4().hex}.tmp"
        temp_file.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        temp_file.replace(self.state_file)

    def _update(self, mutator) -> None:
        with self._lock:
            handle = self._open_lock_handle()
            try:
                payload = self._load_state()
                mutator(payload)
                self._persist(payload)
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
                handle.close()

    def mark_started(
        self,
        *,
        host_context: str | None = None,
        scheduler_instance_id: str | None = None,
        runtime_api_config: dict[str, Any] | None = None,
    ) -> None:
        started_at = datetime.now().isoformat(timespec="seconds")
        runtime_payload = deepcopy(runtime_api_config or {})
        self._update(
            lambda payload: payload.update(
                {
                    "scheduler_running": True,
                    "pid": os.getpid(),
                    "scheduler_instance_id": (
                        str(scheduler_instance_id or "").strip() or None
                    ),
                    "started_at": started_at,
                    "stopped_at": None,
                    "last_heartbeat_at": started_at,
                    "last_error": None,
                    "host_context": str(host_context or "").strip() or None,
                    "runtime_api_config": runtime_payload,
                }
            )
        )

    def mark_stopped(self, *, scheduler_instance_id: str | None = None) -> None:
        stopped_at = datetime.now().isoformat(timespec="seconds")
        normalized_instance_id = str(scheduler_instance_id or "").strip()

        def mutate(payload: dict[str, Any]) -> None:
            persisted_instance_id = str(payload.get("scheduler_instance_id") or "").strip()
            if normalized_instance_id and persisted_instance_id != normalized_instance_id:
                return
            payload.update(
                {
                    "scheduler_running": False,
                    "pid": None,
                    "scheduler_instance_id": None,
                    "stopped_at": stopped_at,
                    "last_heartbeat_at": stopped_at,
                    "current_task": None,
                    "host_context": None,
                    "runtime_api_config": {},
                }
            )

        self._update(mutate)

    def heartbeat(
        self,
        *,
        scheduler_instance_id: str | None = None,
        runtime_api_config: dict[str, Any] | None = None,
        jobs: list[dict[str, Any]] | None = None,
    ) -> None:
        normalized_instance_id = str(scheduler_instance_id or "").strip()

        def mutate(payload: dict[str, Any]) -> None:
            persisted_instance_id = str(payload.get("scheduler_instance_id") or "").strip()
            if normalized_instance_id and persisted_instance_id != normalized_instance_id:
                return
            payload["last_heartbeat_at"] = datetime.now().isoformat(timespec="seconds")
            if runtime_api_config is not None:
                payload["runtime_api_config"] = deepcopy(runtime_api_config)
            if jobs is not None:
                payload["jobs"] = deepcopy(jobs)

        self._update(mutate)

    def set_runtime_api_config(self, runtime_api_config: dict[str, Any] | None) -> None:
        self._update(
            lambda payload: payload.__setitem__(
                "runtime_api_config",
                deepcopy(runtime_api_config or {}),
            )
        )

    def set_jobs(self, jobs: list[dict[str, Any]]) -> None:
        self._update(lambda payload: payload.__setitem__("jobs", deepcopy(jobs)))

    def start_task(self, task_name: str) -> None:
        self._update(
            lambda payload: payload.__setitem__(
                "current_task",
                {
                    "task_name": task_name,
                    "started_at": datetime.now().isoformat(timespec="seconds"),
                },
            )
        )

    def finish_task(self, task_name: str, result: dict[str, Any]) -> None:
        def mutate(payload: dict[str, Any]) -> None:
            last_runs = payload.get("last_runs") or {}
            if not isinstance(last_runs, dict):
                last_runs = {}
            last_runs[str(task_name)] = deepcopy(result)
            payload["last_runs"] = last_runs
            payload["current_task"] = None
            payload["last_error"] = result.get("error") if result.get("status") == "failed" else None
            # Heavy runtime digests were prepared while this task still owned
            # current_task. Complete their same-run time projections atomically.
            if "task_timing" in result:
                from backend.scheduler_task_timing import copy_task_timing_fields

                fields = {"finished_at": result.get("finished_at"), **copy_task_timing_fields(result)}
                for field in ("task_run_audit_status", "task_run_audit_error", "runtime_status_summary_refresh_error"):
                    if field in result:
                        fields[field] = deepcopy(result[field])
                task_key = None
                for key, closure in (payload.get("task_closures") or {}).items():
                    if (isinstance(closure, dict) and closure.get("task_name") == task_name
                            and closure.get("started_at") == result.get("started_at")):
                        closure.update(deepcopy(fields))
                        task_key = key
                digest = payload.get("automation_closure_digest") or {}
                views = [*(digest.get("ordered_tasks") or []), *(digest.get("task_map") or {}).values()]
                for view in views:
                    if (isinstance(view, dict) and task_key and view.get("task_key") == task_key
                            and view.get("started_at") == result.get("started_at")):
                        view.update(deepcopy(fields))

        self._update(mutate)

    def stage_task_result(self, task_name: str, result: dict[str, Any]) -> None:
        """Make the business outcome available to audit without releasing ownership."""
        def mutate(payload: dict[str, Any]) -> None:
            last_runs = payload.get("last_runs") or {}
            last_runs[str(task_name)] = deepcopy(result)
            payload["last_runs"] = last_runs
            current = payload.get("current_task") or {}
            if current.get("task_name") == task_name:
                current["phase"] = "finalizing"

        self._update(mutate)

    def record_busy_skip(self, result: dict[str, Any]) -> None:
        """Keep a bounded attempt history without changing execution ownership."""
        if result.get("status") != "skipped" or result.get("skip_reason") != "scheduler_busy":
            raise ValueError("busy_skip_history_requires_scheduler_busy_result")

        def mutate(payload: dict[str, Any]) -> None:
            history = payload.get("recent_busy_skips") or []
            if not isinstance(history, list):
                raise ValueError("invalid_busy_skip_history")
            payload["recent_busy_skips"] = [deepcopy(result), *history[:19]]

        self._update(mutate)

    def recover_stale_run(self, reason: str) -> None:
        recovered_at = datetime.now().isoformat(timespec="seconds")

        def mutate(payload: dict[str, Any]) -> None:
            if not payload.get("scheduler_running"):
                return
            payload.update(
                {
                    "scheduler_running": False,
                    "pid": None,
                    "scheduler_instance_id": None,
                    "stopped_at": recovered_at,
                    "last_heartbeat_at": recovered_at,
                    "current_task": None,
                    "last_error": reason,
                    "host_context": None,
                    "runtime_api_config": {},
                }
            )

        self._update(mutate)

    @staticmethod
    def _parse_iso_datetime(value: Any) -> datetime | None:
        normalized = str(value or "").strip()
        if not normalized:
            return None
        try:
            return datetime.fromisoformat(normalized)
        except ValueError:
            return None

    def recover_orphan_manual_run_entries(
        self,
        reason: str,
        *,
        stale_after_seconds: int = 120,
    ) -> dict[str, Any]:
        normalized_reason = str(reason or "").strip() or "scheduler_process_missing"
        recovered_dt = datetime.now()
        recovered_at = recovered_dt.isoformat(timespec="seconds")
        recoverable_active_statuses = {"running", "started"}

        with self._lock:
            handle = self._open_lock_handle()
            try:
                payload = self._load_state()
                queue = self._normalize_pending_manual_run_requests(payload.get("pending_manual_run_requests"))
                pending_request_ids = {
                    str((item or {}).get("request_id") or "").strip()
                    for item in queue
                    if str((item or {}).get("request_id") or "").strip()
                }
                current_task = (
                    deepcopy(payload.get("current_task") or {})
                    if isinstance(payload.get("current_task"), dict)
                    else {}
                )
                current_task_name = str(current_task.get("task_name") or "").strip()
                history = self._normalize_manual_run_history(payload.get("manual_run_history"))
                next_history: list[dict[str, Any]] = []
                recovered_request_ids: list[str] = []
                changed = False

                for item in history:
                    request_id = str(item.get("request_id") or "").strip()
                    status = str(item.get("status") or "").strip().lower()
                    task_name = str(item.get("task_name") or "").strip()
                    started_dt = self._parse_iso_datetime(item.get("started_at"))
                    requested_dt = self._parse_iso_datetime(item.get("requested_at"))
                    reference_dt = started_dt or requested_dt
                    stale_enough = bool(
                        reference_dt is not None
                        and int((recovered_dt - reference_dt).total_seconds()) >= max(0, int(stale_after_seconds))
                    )
                    if (
                        status in recoverable_active_statuses
                        and request_id not in pending_request_ids
                        and (not current_task_name or task_name != current_task_name)
                        and stale_enough
                    ):
                        merged = deepcopy(item)
                        merged["status"] = "failed"
                        merged["finished_at"] = str(merged.get("finished_at") or "").strip() or recovered_at
                        merged["error"] = str(merged.get("error") or "").strip() or normalized_reason
                        next_history.append(merged)
                        if request_id:
                            recovered_request_ids.append(request_id)
                        changed = True
                        continue
                    next_history.append(deepcopy(item))

                if changed:
                    payload["manual_run_history"] = self._normalize_manual_run_history(next_history)
                    self._persist(payload)

                return {
                    "recovered_total": len(recovered_request_ids),
                    "recovered_request_ids": recovered_request_ids,
                }
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
                handle.close()

    def append_manual_run_entry(self, entry: dict[str, Any]) -> None:
        def mutate(payload: dict[str, Any]) -> None:
            history = self._normalize_manual_run_history(payload.get("manual_run_history"))
            history.insert(0, deepcopy(entry))
            payload["manual_run_history"] = self._normalize_manual_run_history(history)

        self._update(mutate)

    def update_manual_run_entry(self, request_id: str, updates: dict[str, Any]) -> None:
        normalized_request_id = str(request_id or "").strip()
        if not normalized_request_id:
            return

        def mutate(payload: dict[str, Any]) -> None:
            history = self._normalize_manual_run_history(payload.get("manual_run_history"))
            next_history: list[dict[str, Any]] = []
            updated = False
            for item in history:
                if not updated and str(item.get("request_id") or "").strip() == normalized_request_id:
                    merged = deepcopy(item)
                    merged.update(deepcopy(updates))
                    next_history.append(merged)
                    updated = True
                else:
                    next_history.append(deepcopy(item))
            payload["manual_run_history"] = self._normalize_manual_run_history(next_history)

        self._update(mutate)

    def enqueue_pending_manual_run_request(self, entry: dict[str, Any]) -> None:
        request_id = str((entry or {}).get("request_id") or "").strip()
        if not request_id:
            return

        def mutate(payload: dict[str, Any]) -> None:
            queue = self._normalize_pending_manual_run_requests(payload.get("pending_manual_run_requests"))
            for item in queue:
                if str(item.get("request_id") or "").strip() == request_id:
                    payload["pending_manual_run_requests"] = queue
                    return
            queue.append(deepcopy(entry))
            payload["pending_manual_run_requests"] = self._normalize_pending_manual_run_requests(queue)

        self._update(mutate)

    def enqueue_unique_manual_run_request(self, entry: dict[str, Any]) -> dict[str, Any]:
        request_id = str((entry or {}).get("request_id") or "").strip()
        job_id = str((entry or {}).get("job_id") or "").strip()
        requested_at = str((entry or {}).get("requested_at") or "").strip()
        if not request_id or not job_id:
            return {
                "accepted": False,
                "reason": "invalid_request",
                "entry": {},
            }

        with self._lock:
            handle = self._open_lock_handle()
            try:
                payload = self._load_state()
                current_task = deepcopy((payload.get("current_task") or {})) if isinstance(payload.get("current_task"), dict) else {}
                queue = self._normalize_pending_manual_run_requests(payload.get("pending_manual_run_requests"))
                history = self._normalize_manual_run_history(payload.get("manual_run_history"))

                if current_task:
                    rejected_entry = deepcopy(entry)
                    rejected_entry.update(
                        {
                            "status": "rejected",
                            "finished_at": requested_at or datetime.now().isoformat(timespec="seconds"),
                            "error": "scheduler_busy",
                            "current_task": current_task,
                        }
                    )
                    history.insert(0, rejected_entry)
                    payload["manual_run_history"] = self._normalize_manual_run_history(history)
                    self._persist(payload)
                    return {
                        "accepted": False,
                        "reason": "scheduler_busy",
                        "entry": rejected_entry,
                    }

                for item in queue:
                    if str((item or {}).get("job_id") or "").strip() != job_id:
                        continue
                    rejected_entry = deepcopy(entry)
                    rejected_entry.update(
                        {
                            "status": "rejected",
                            "finished_at": requested_at or datetime.now().isoformat(timespec="seconds"),
                            "error": "scheduler_request_already_pending",
                        }
                    )
                    history.insert(0, rejected_entry)
                    payload["manual_run_history"] = self._normalize_manual_run_history(history)
                    self._persist(payload)
                    return {
                        "accepted": False,
                        "reason": "scheduler_request_already_pending",
                        "entry": rejected_entry,
                    }

                accepted_entry = deepcopy(entry)
                accepted_entry["status"] = str(accepted_entry.get("status") or "accepted").strip() or "accepted"
                history.insert(0, accepted_entry)
                queue.append(deepcopy(accepted_entry))
                payload["manual_run_history"] = self._normalize_manual_run_history(history)
                payload["pending_manual_run_requests"] = self._normalize_pending_manual_run_requests(queue)
                self._persist(payload)
                return {
                    "accepted": True,
                    "reason": None,
                    "entry": accepted_entry,
                }
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
                handle.close()

    def claim_next_pending_manual_run_request(self) -> dict[str, Any] | None:
        with self._lock:
            handle = self._open_lock_handle()
            try:
                payload = self._load_state()
                queue = self._normalize_pending_manual_run_requests(payload.get("pending_manual_run_requests"))
                if not queue:
                    return None
                claimed = deepcopy(queue[0])
                payload["pending_manual_run_requests"] = self._normalize_pending_manual_run_requests(queue[1:])
                self._persist(payload)
                return claimed
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
                handle.close()

    def set_runtime_status_summary(self, summary: dict[str, Any]) -> None:
        normalized = {
            "state_schema_version": self.STATE_SCHEMA_VERSION,
            "automation_host_mode": summary.get("automation_host_mode"),
            "host_context": summary.get("host_context"),
            "configured_host_context": summary.get("configured_host_context"),
            "host_context_mismatch": bool(summary.get("host_context_mismatch")),
            "effective_nav_snapshot": deepcopy(summary.get("effective_nav_snapshot") or {}),
            "latest_screened_snapshot": deepcopy(summary.get("latest_screened_snapshot") or {}),
            "task_closures": deepcopy(summary.get("task_closures") or {}),
            "child_task_idle_streaks": deepcopy(summary.get("child_task_idle_streaks") or {}),
            "nav_freshness": deepcopy(summary.get("nav_freshness") or {}),
            "maintenance_summary": deepcopy(summary.get("maintenance_summary") or {}),
            "schedule_stability_summary": deepcopy(summary.get("schedule_stability_summary") or {}),
            "automation_closure_digest": deepcopy(summary.get("automation_closure_digest") or {}),
            "recent_failure_recovery_summary": deepcopy(
                summary.get("recent_failure_recovery_summary") or {}
            ),
            "startup_catch_up_due_jobs": list(summary.get("startup_catch_up_due_jobs") or []),
            "startup_catch_up_pending": bool(summary.get("startup_catch_up_pending")),
            "screening_backlog_due": bool(summary.get("screening_backlog_due")),
            "screening_backlog_reason": summary.get("screening_backlog_reason"),
            "configured_job_ids": list(summary.get("configured_job_ids") or []),
            "missing_job_ids": list(summary.get("missing_job_ids") or []),
            "unexpected_job_ids": list(summary.get("unexpected_job_ids") or []),
            "job_drift_detected": bool(summary.get("job_drift_detected")),
        }
        with self._lock:
            payload = self._load_state()
            changed = False
            for key, value in normalized.items():
                if payload.get(key) != value:
                    payload[key] = value
                    changed = True
            if changed:
                self._persist(payload)
