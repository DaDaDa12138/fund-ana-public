"""Maintenance counters retain their units, missing values and task identity."""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime
import hashlib
import json
from pathlib import Path
import re
import sqlite3
from typing import Any

from backend.task_audit_storage import read_task_audit_bytes

MAINTENANCE_CHILD_KEYS = ("nav_history_maintenance", "long_term_history_maintenance")
MAINTENANCE_COUNTER_FIELDS = (
    "candidate_total", "processed_total", "attempted_total", "changed_total",
    "recovered_total", "failed_total", "nav_records_inserted", "annual_success_total",
    "rank_success_total", "annual_failed_total", "rank_failed_total",
)


def optional_count(value: Any) -> int | None:
    # bool, fractional values and negative counters are not measurements.
    if type(value) is int and value >= 0:
        return value
    if isinstance(value, str) and len(value) <= 20 and re.fullmatch(r"[0-9]+", value.strip()):
        return int(value)
    return None


def maintenance_metrics(child: dict[str, Any] | None, *, legacy_summary: bool = False) -> dict[str, Any]:
    source = child if isinstance(child, dict) else {}
    trusted = not legacy_summary or (
        source.get("maintenance_metrics_schema_version") == 1
        and source.get("maintenance_metrics_status") == "reported"
    )
    nested = source.get("maintenance_result")
    nested = nested if isinstance(nested, dict) else {}
    counters = {}
    for name in MAINTENANCE_COUNTER_FIELDS:
        # An explicitly reported NULL/0 must not fall through to another count.
        value = source[name] if name in source else nested.get(name)
        if name == "candidate_total" and name not in source and name not in nested:
            selection = source.get("selection")
            value = selection.get(name) if isinstance(selection, dict) else None
        counters[name] = optional_count(value) if trusted else None
    activities = [counters[k] for k in ("changed_total", "recovered_total", "nav_records_inserted",
                                       "annual_success_total", "rank_success_total")]
    activity = True if any(v is not None and v > 0 for v in activities) else None
    if activity is None and ((
        counters["annual_success_total"] == counters["rank_success_total"] == 0
    ) or (counters["recovered_total"] == counters["nav_records_inserted"] == 0)):
        activity = False
    return {**counters, "maintenance_metrics_schema_version": 1,
            "maintenance_metrics_status": "reported" if trusted else "legacy_unverified",
            "activity_observed": activity}


def _timestamp(value: Any) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("maintenance_task_time_missing")
    return datetime.fromisoformat(value)


def _audit_path(value: Any, root: Path) -> Path:
    raw = str(value or "")
    if raw.startswith("/app/"):
        path = root / raw.removeprefix("/app/")
    else:
        path = Path(raw)
        if not path.is_absolute():
            path = root / path
    if ".." in path.parts or any(p.is_symlink() for p in (path, *path.parents)):
        raise ValueError("maintenance_audit_unsafe_path")
    allowed = root / "reports/.audit/scheduler_task_runs"
    if not path.is_relative_to(allowed) or not path.name.endswith((".json", ".json.gz")):
        raise ValueError("maintenance_audit_outside_task_directory")
    return path


def _bound_weekly_audit(run: dict[str, Any], settings: Any) -> tuple[dict, dict]:
    root = Path(settings.project_root).resolve()
    task_name = "scheduled_weekly_bond_maintenance"
    if run.get("task_name") not in (None, task_name):
        raise ValueError("maintenance_task_name_mismatch")
    task_id = optional_count(run.get("task_history_id"))
    log = str(run.get("task_log_file") or run.get("task_log_path") or "")
    match = re.fullmatch(r"task_([0-9]+)_scheduled_weekly_bond_maintenance\.log", Path(log).name)
    if match:
        log_id = int(match[1])
        if task_id is not None and task_id != log_id:
            raise ValueError("maintenance_task_id_mismatch")
        task_id = log_id
    if not task_id:
        raise ValueError("maintenance_task_id_missing")
    if run.get("status") not in {"success", "degraded", "failed", "skipped"}:
        raise ValueError("maintenance_task_not_terminal")
    artifact = run.get("task_run_audit_json_project_path") or run.get("task_run_audit_json") or run.get("task_run_audit_path")
    path = _audit_path(artifact, root)
    before = path.stat()
    raw = read_task_audit_bytes(path)
    audit = json.loads(raw)
    task, result = audit.get("task"), audit.get("task_result")
    if not isinstance(task, dict) or not isinstance(result, dict):
        raise ValueError("maintenance_audit_payload_missing")
    if (audit.get("persistence_stage") != "completion_recorded"
            or task.get("task_history_id") != task_id or task.get("task_name") != task_name
            or task.get("task_key") != "weekly_maintenance"
            or task.get("status") != run.get("status") or result.get("status") != run.get("status")):
        raise ValueError("maintenance_audit_task_mismatch")
    for name in ("started_at", "finished_at"):
        if _timestamp(task.get(name)) != _timestamp(run.get(name)) or _timestamp(result.get(name)) != _timestamp(run.get(name)):
            raise ValueError("maintenance_audit_time_mismatch")
    database = Path(settings.database_path).resolve()
    with sqlite3.connect(database.as_uri() + "?mode=ro", uri=True, timeout=5) as connection:
        row = connection.execute(
            "SELECT task_name,status,started_at,completed_at,extra_data FROM task_history WHERE id=?", (task_id,)
        ).fetchone()
    if (row is None or row[0] != task_name or row[1] != task["status"]
            or _timestamp(row[2]) != _timestamp(task["started_at"])
            or _timestamp(row[3]) != _timestamp(task["finished_at"])):
        raise ValueError("maintenance_sql_task_mismatch")
    compact = json.loads(row[4])
    bound_path = compact.get("task_run_audit_path") or compact.get("task_run_audit_json_project_path") or compact.get("task_run_audit_json")
    if _audit_path(bound_path, root) != path:
        raise ValueError("maintenance_sql_audit_path_mismatch")
    binding = compact.get("task_history_full_payload") or {}
    encoded = json.dumps(result, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode()
    if binding.get("full_payload_sha256") is not None:
        if (binding.get("source") != "scheduler_task_run_audit"
                or binding.get("full_payload_bytes") != len(encoded)
                or binding.get("full_payload_sha256") != hashlib.sha256(encoded).hexdigest()):
            raise ValueError("maintenance_sql_payload_sha_mismatch")
    after = path.stat()
    identity = lambda value: (value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns)
    if identity(before) != identity(after):
        raise ValueError("maintenance_audit_changed_during_read")
    return result, {"status": "bound_task_audit", "task_history_id": task_id,
                    "task_name": task_name, "started_at": task["started_at"], "finished_at": task["finished_at"],
                    "task_status": task["status"], "audit_path": path.relative_to(root).as_posix(),
                    "audit_payload_sha256": hashlib.sha256(raw).hexdigest(),
                    "full_result_sha256": hashlib.sha256(encoded).hexdigest(), "sql_identity_verified": True}


def restore_weekly_maintenance_metrics(run: dict[str, Any] | None, settings: Any) -> dict[str, Any]:
    """Read one bound terminal audit for legacy counters; never rewrite its outcome."""
    payload = deepcopy(run or {})
    if not payload:
        return payload
    children = payload.get("child_tasks") or {}
    # Current full results and explicitly versioned summaries already carry the contract.
    if any(isinstance(payload.get(k), dict) for k in MAINTENANCE_CHILD_KEYS):
        payload.setdefault("maintenance_metrics_source", {"status": "task_result"})
        return payload
    if any(k in children for k in MAINTENANCE_CHILD_KEYS) and all(
        k not in children or (children[k].get("maintenance_metrics_schema_version") == 1
                              and children[k].get("maintenance_metrics_status") == "reported")
        for k in MAINTENANCE_CHILD_KEYS
    ):
        payload.setdefault("maintenance_metrics_source", {"status": "task_summary"})
        return payload
    try:
        full, provenance = _bound_weekly_audit(payload, settings)
    except (OSError, ValueError, TypeError, KeyError, AttributeError, sqlite3.Error) as exc:
        payload["maintenance_metrics_source"] = {"status": "unavailable", "reason": str(exc)}
        return payload
    for key in MAINTENANCE_CHILD_KEYS:
        if isinstance(full.get(key), dict):
            payload[key] = deepcopy(full[key])
    payload["maintenance_metrics_source"] = provenance
    payload["task_history_id"] = provenance["task_history_id"]
    return payload
