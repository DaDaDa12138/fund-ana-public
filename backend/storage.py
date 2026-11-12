from __future__ import annotations

import fcntl
import json
import os
import socket
import threading
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from backend.state_normalizer import normalize_snapshot_payload


class WorkflowLockBusyError(RuntimeError):
    """workflow 执行锁已被其他实例占用。"""


class WorkflowOwnershipError(RuntimeError):
    """当前 store 已失去对运行中 workflow 的写入 ownership。"""


_PROCESS_WORKFLOW_LOCK_GUARD = threading.Lock()
_PROCESS_WORKFLOW_LOCKS: set[str] = set()


class WorkflowStore:
    def __init__(self, state_file: Path):
        self.state_file = Path(state_file)
        self.lock_file = self.state_file.with_suffix(f"{self.state_file.suffix}.lock")
        self._lock_key = str(self.lock_file.resolve())
        self._lock = threading.Lock()
        self._run_lock_handle = None
        self._active_run_id: str | None = None
        self.state = self._load_state()

    def _default_state(self) -> dict[str, Any]:
        return {
            "status": "idle",
            "phase": "waiting",
            "run_id": None,
            "pid": None,
            "runtime_host": None,
            "started_at": None,
            "finished_at": None,
            "last_updated_at": None,
            "error": None,
            "options": {},
            "logs": [],
            "report": None,
            "current_run_report": None,
            "published_report": None,
        }

    @staticmethod
    def _timestamp() -> str:
        return datetime.now().isoformat(timespec="seconds")

    @staticmethod
    def _runtime_host() -> str:
        try:
            return socket.gethostname()
        except OSError:
            return ""

    def _load_state(self) -> dict[str, Any]:
        if not self.state_file.exists():
            return self._default_state()
        try:
            payload = json.loads(self.state_file.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError, TypeError, ValueError):
            return self._default_state()
        if not isinstance(payload, dict):
            return self._default_state()
        payload, _ = normalize_snapshot_payload(payload)
        merged = self._default_state()
        merged.update(payload)
        return merged

    def _persist(self) -> None:
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        normalized_state, _ = normalize_snapshot_payload(self.state)
        temp_file = self.state_file.parent / f"{self.state_file.name}.{uuid4().hex}.tmp"
        temp_file.write_text(
            json.dumps(normalized_state, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        temp_file.replace(self.state_file)

    def _acquire_process_lock_guard_locked(self) -> None:
        with _PROCESS_WORKFLOW_LOCK_GUARD:
            if self._lock_key in _PROCESS_WORKFLOW_LOCKS:
                raise WorkflowLockBusyError("workflow_lock_busy")
            _PROCESS_WORKFLOW_LOCKS.add(self._lock_key)

    def _release_process_lock_guard_locked(self) -> None:
        with _PROCESS_WORKFLOW_LOCK_GUARD:
            _PROCESS_WORKFLOW_LOCKS.discard(self._lock_key)

    def _acquire_run_lock_locked(self, run_id: str) -> None:
        normalized_run_id = str(run_id or "").strip()
        if not normalized_run_id:
            raise WorkflowOwnershipError("workflow_run_id_missing")
        if self._run_lock_handle is not None:
            if self._active_run_id == normalized_run_id:
                return
            raise WorkflowLockBusyError("workflow_lock_busy")

        self.lock_file.parent.mkdir(parents=True, exist_ok=True)
        self._acquire_process_lock_guard_locked()
        handle = self.lock_file.open("a+", encoding="utf-8")
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            handle.close()
            self._release_process_lock_guard_locked()
            raise WorkflowLockBusyError("workflow_lock_busy") from exc
        except BaseException:
            handle.close()
            self._release_process_lock_guard_locked()
            raise

        lock_payload = {
            "run_id": normalized_run_id,
            "pid": os.getpid(),
            "state_file": str(self.state_file),
            "acquired_at": self._timestamp(),
        }
        handle.seek(0)
        handle.truncate()
        handle.write(json.dumps(lock_payload, ensure_ascii=False, indent=2))
        handle.flush()
        os.fsync(handle.fileno())
        self._run_lock_handle = handle
        self._active_run_id = normalized_run_id

    def _release_run_lock_locked(self) -> None:
        handle = self._run_lock_handle
        if handle is None:
            self._active_run_id = None
            self._release_process_lock_guard_locked()
            return
        try:
            try:
                handle.seek(0)
                handle.truncate()
                handle.flush()
                os.fsync(handle.fileno())
            except OSError:
                pass
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            try:
                handle.close()
            finally:
                self._run_lock_handle = None
                self._active_run_id = None
                self._release_process_lock_guard_locked()

    def has_active_lock(self) -> bool:
        with self._lock:
            if self._run_lock_handle is not None:
                return True
            with _PROCESS_WORKFLOW_LOCK_GUARD:
                if self._lock_key in _PROCESS_WORKFLOW_LOCKS:
                    return True

        self.lock_file.parent.mkdir(parents=True, exist_ok=True)
        handle = self.lock_file.open("a+", encoding="utf-8")
        try:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return True
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            return False
        finally:
            handle.close()

    def _assert_run_ownership_locked(self) -> str:
        normalized_run_id = str(self._active_run_id or "").strip()
        if not normalized_run_id:
            raise WorkflowOwnershipError("workflow_lock_not_held")
        if self._run_lock_handle is None:
            raise WorkflowOwnershipError("workflow_lock_not_held")

        self._reload_from_disk_locked()
        disk_run_id = str(self.state.get("run_id") or "").strip()
        disk_status = str(self.state.get("status") or "").strip().lower()
        if disk_status == "running" and disk_run_id and disk_run_id != normalized_run_id:
            raise WorkflowOwnershipError("workflow_run_ownership_lost")
        return normalized_run_id

    def _reload_from_disk_locked(self) -> None:
        self.state = self._load_state()

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            self._reload_from_disk_locked()
            return deepcopy(self.state)

    def snapshot_summary(self) -> dict[str, Any]:
        with self._lock:
            self._reload_from_disk_locked()
            return {
                "status": self.state.get("status"),
                "phase": self.state.get("phase"),
                "run_id": self.state.get("run_id"),
                "pid": self.state.get("pid"),
                "started_at": self.state.get("started_at"),
                "finished_at": self.state.get("finished_at"),
                "error": self.state.get("error"),
                "options": deepcopy(self.state.get("options") or {}),
                "logs_tail": list((self.state.get("logs") or [])[-10:]),
            }

    def start(self, run_id: str, options: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            self._acquire_run_lock_locked(run_id)
            published_report = self._load_state().get("published_report")
            self.state = self._default_state()
            self.state.update(
                {
                    "status": "running",
                    "phase": "initializing",
                    "run_id": run_id,
                    "pid": os.getpid(),
                    "runtime_host": self._runtime_host(),
                    "started_at": self._timestamp(),
                    "last_updated_at": self._timestamp(),
                    "options": deepcopy(options),
                    "published_report": deepcopy(published_report),
                }
            )
            self._persist()
            return deepcopy(self.state)

    def set_phase(self, phase: str) -> None:
        with self._lock:
            self._assert_run_ownership_locked()
            self.state["phase"] = phase
            self.state["last_updated_at"] = self._timestamp()
            self._persist()

    def append_log(self, message: str) -> None:
        with self._lock:
            self._assert_run_ownership_locked()
            logs = self.state.setdefault("logs", [])
            logs.append(message.rstrip())
            self.state["logs"] = logs[-400:]
            self.state["last_updated_at"] = self._timestamp()
            self._persist()

    def touch(self) -> None:
        with self._lock:
            self._assert_run_ownership_locked()
            self.state["last_updated_at"] = self._timestamp()
            self._persist()

    def finish_success(self, report: dict[str, Any]) -> None:
        with self._lock:
            self._assert_run_ownership_locked()
            report_run_id = str((report or {}).get("run_id") or "").strip()
            try:
                self.state["status"] = "succeeded"
                self.state["phase"] = "completed"
                if report_run_id:
                    self.state["run_id"] = report_run_id
                self.state["pid"] = None
                self.state["finished_at"] = self._timestamp()
                self.state["last_updated_at"] = self._timestamp()
                self.state["report"] = deepcopy(report)
                self.state["current_run_report"] = deepcopy(report)
                self.state["error"] = None
                self._persist()
            finally:
                self._release_run_lock_locked()

    def set_published_report(self, report: dict[str, Any] | None) -> None:
        with self._lock:
            self._reload_from_disk_locked()
            disk_run_id = str(self.state.get("run_id") or "").strip()
            disk_status = str(self.state.get("status") or "").strip().lower()
            active_run_id = str(self._active_run_id or "").strip()
            if active_run_id and disk_status == "running" and disk_run_id and disk_run_id != active_run_id:
                raise WorkflowOwnershipError("workflow_run_ownership_lost")
            self.state["published_report"] = deepcopy(report) if report else None
            self.state["last_updated_at"] = self._timestamp()
            self._persist()

    def finish_failure(self, error: str) -> None:
        with self._lock:
            self._assert_run_ownership_locked()
            try:
                self.state["status"] = "failed"
                self.state["phase"] = "failed"
                self.state["pid"] = None
                self.state["finished_at"] = self._timestamp()
                self.state["last_updated_at"] = self._timestamp()
                self.state["error"] = error
                self._persist()
            finally:
                self._release_run_lock_locked()

    def recover_stale_run(self, reason: str) -> None:
        with self._lock:
            if self.state.get("status") != "running":
                return
            logs = self.state.setdefault("logs", [])
            logs.append(f"检测到运行态残留，已自动回收: {reason}")
            self.state["logs"] = logs[-400:]
            self.state["status"] = "failed"
            self.state["phase"] = "failed"
            self.state["pid"] = None
            self.state["finished_at"] = self._timestamp()
            self.state["last_updated_at"] = self._timestamp()
            self.state["error"] = reason
            self._persist()

    def close(self) -> None:
        with self._lock:
            self._release_run_lock_locked()
