"""Retain collection inputs under their own attempt, then reference them from workflows."""
from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from copy import deepcopy
from datetime import datetime
import json
from pathlib import Path
import re
from typing import Any
from uuid import uuid4

from backend.calculation_input_capture import (
    CalculationCaptureError, _safe, _sha, _write_immutable,
    build_calculation_input_index, capture_index_reference,
)

_TASK = ContextVar("fund_ana_collection_calculation_task", default=None)
_ID = re.compile(r"collect_[0-9]{8}_[0-9]{6}_[0-9a-f]{32}")


@contextmanager
def collection_task_context(*, task_id, task_name: str, started_at: str, trigger_source: str):
    state = {"task": {
        "task_history_id": task_id if type(task_id) is int and task_id > 0 else None,
        "task_name": task_name, "started_at": started_at, "trigger_source": trigger_source,
    }, "attempts": []}
    token = _TASK.set(state)
    try:
        yield state
    finally:
        _TASK.reset(token)


def validate_execution(execution: dict) -> None:
    if (not isinstance(execution, dict) or execution.get("schema_version") != 1
            or execution.get("capture_scope") != "collection"
            or execution.get("workflow_run_id") is not None
            or not isinstance(execution.get("capture_run_id"), str)
            or not _ID.fullmatch(execution["capture_run_id"])):
        raise CalculationCaptureError("invalid_collection_execution_identity")
    created = datetime.fromisoformat(execution["created_at"])
    if created.tzinfo is None or created.strftime("%Y%m%d_%H%M%S") != execution["capture_run_id"][8:23]:
        raise CalculationCaptureError("collection_execution_time_mismatch")
    task = execution.get("task")
    if task is not None:
        if not isinstance(task, dict) or not isinstance(task.get("task_name"), str) or not task["task_name"]:
            raise CalculationCaptureError("invalid_collection_task_identity")
        task_id = task.get("task_history_id")
        if task_id is not None and (type(task_id) is not int or task_id <= 0):
            raise CalculationCaptureError("invalid_collection_task_history_id")
        started = datetime.fromisoformat(task["started_at"])
        if started.tzinfo is None:
            started = started.replace(tzinfo=created.tzinfo)
        if started > created:
            raise CalculationCaptureError("collection_started_before_parent_task")


def collection_execution_root(settings: Any, execution: dict) -> Path:
    validate_execution(execution)
    reports = Path(getattr(settings, "reports_dir", Path(settings.project_root) / "reports"))
    return _safe(reports / ".audit/collection_calculations" / execution["capture_run_id"][8:16]
                 / execution["capture_run_id"])


def prepare_collection_capture(settings: Any, options: dict) -> dict:
    now = datetime.now().astimezone()
    state = _TASK.get()
    execution = {
        "schema_version": 1, "capture_scope": "collection", "workflow_run_id": None,
        "capture_run_id": "collect_" + now.strftime("%Y%m%d_%H%M%S_") + uuid4().hex,
        "created_at": now.isoformat(), "task": deepcopy(state["task"]) if state else None,
        "pipeline_options": {name: options.get(name) for name in (
            "run_collection", "run_screening", "collection_profile", "skip_collect_funds",
            "skip_risk_calculation", "force_calculate_risk", "risk_refresh_changed_nav_only",
        )},
    }
    root = collection_execution_root(settings, execution)
    root.mkdir(parents=True, exist_ok=False)
    directory = root / "calculation_inputs"
    directory.mkdir()
    raw = json.dumps(execution, ensure_ascii=False, sort_keys=True, allow_nan=False).encode() + b"\n"
    _write_immutable(root / "execution.json", raw)
    record = {
        "capture_run_id": execution["capture_run_id"], "capture_scope": "collection",
        "source_workflow_run_id": None, "task": execution["task"],
        "execution_path": str((root / "execution.json").relative_to(settings.project_root)),
        "execution_sha256": _sha(root / "execution.json"),
        "pipeline_outcome": "running", "capture_complete": False,
    }
    if state is not None:
        state["attempts"].append(record)
    return {"execution": execution, "root": root, "directory": directory, "record": record}


def finish_collection_capture(
    settings: Any, attempt: dict, *, planned_tasks: list[str], subprocess_index: dict,
    pipeline_error: BaseException | None = None,
) -> Path | None:
    record, root = attempt["record"], attempt["root"]
    index, capture_error = None, None
    try:
        index = build_calculation_input_index(
            settings, attempt["directory"], run_id=attempt["execution"]["capture_run_id"],
            planned_tasks=planned_tasks, subprocess_index=subprocess_index,
            collection_execution=attempt["execution"],
        )
    except Exception as exc:
        capture_error = exc
    record.update(pipeline_outcome="failed" if pipeline_error or capture_error else "succeeded",
                  capture_complete=capture_error is None,
                  pipeline_error=f"{type(pipeline_error).__name__}:{pipeline_error}" if pipeline_error else None,
                  capture_error=f"{type(capture_error).__name__}:{capture_error}" if capture_error else None)
    if index is not None:
        record.update(index_path=str(index.relative_to(settings.project_root)), index_sha256=_sha(index))
    terminal = {"schema_version": 1, **record, "finished_at": datetime.now().astimezone().isoformat()}
    terminal_path = root / "terminal.json"
    _write_immutable(terminal_path, json.dumps(terminal, ensure_ascii=False, sort_keys=True,
                                            allow_nan=False).encode() + b"\n")
    record.update(terminal_path=str(terminal_path.relative_to(settings.project_root)),
                  terminal_sha256=_sha(terminal_path))
    if capture_error is not None and pipeline_error is None:
        raise CalculationCaptureError(f"collection_calculation_capture_incomplete:{capture_error}") from capture_error
    return index


def completed_collection_captures() -> list[dict]:
    state = _TASK.get()
    return [deepcopy(item) for item in (state or {}).get("attempts", [])
            if item.get("capture_complete") is True and item.get("terminal_path")]


def validate_collection_capture_reference(settings: Any, reference: dict) -> dict:
    """Bind a completed earlier attempt; never claim it computed this workflow."""
    if (not isinstance(reference, dict) or reference.get("capture_complete") is not True
            or reference.get("capture_scope") != "collection"
            or reference.get("source_workflow_run_id") is not None):
        raise CalculationCaptureError("invalid_collection_capture_reference")
    paths = {}
    reports = _safe(Path(settings.reports_dir))
    for key in ("execution", "terminal", "index"):
        raw = reference.get(key + "_path")
        if not isinstance(raw, str) or Path(raw).is_absolute() or ".." in Path(raw).parts:
            raise CalculationCaptureError("invalid_collection_reference_path")
        path = _safe(Path(settings.project_root) / raw)
        if (not path.is_relative_to(reports / ".audit/collection_calculations")
                or path.stat().st_size > 8 * 1024**2
                or _sha(path) != reference.get(key + "_sha256")):
            raise CalculationCaptureError("collection_reference_file_changed")
        paths[key] = path
    execution = json.loads(paths["execution"].read_text())
    root = collection_execution_root(settings, execution)
    run_id = execution["capture_run_id"]
    if (reference.get("capture_run_id") != run_id or reference.get("task") != execution["task"]
            or paths["execution"] != root / "execution.json"
            or paths["terminal"] != root / "terminal.json"
            or paths["index"] != root / "calculation_inputs" / f"calculation_input_index_{run_id}.json"):
        raise CalculationCaptureError("collection_reference_identity_mismatch")
    terminal = json.loads(paths["terminal"].read_text())
    if any(terminal.get(key) != reference.get(key) for key in (
        "capture_run_id", "capture_scope", "source_workflow_run_id", "task",
        "execution_path", "execution_sha256", "pipeline_outcome", "capture_complete",
        "pipeline_error", "capture_error", "index_path", "index_sha256",
    )):
        raise CalculationCaptureError("collection_reference_terminal_disagree")
    index_reference = capture_index_reference(settings, str(paths["index"]))
    index_payload = json.loads(paths["index"].read_text())
    if (index_reference["capture_scope"] != "collection" or index_reference["capture_run_id"] != run_id
            or index_reference["sha256"] != reference["index_sha256"]
            or index_payload.get("collection_execution_sha256") != reference["execution_sha256"]):
        raise CalculationCaptureError("collection_reference_index_mismatch")
    return {**deepcopy(reference), "scientific_reproduction_status": "not_verified",
            "scopes": index_reference["scopes"],
            "scope": "Earlier collection attempt; does not replace this workflow's inputs or prove that later database values are unchanged."}
