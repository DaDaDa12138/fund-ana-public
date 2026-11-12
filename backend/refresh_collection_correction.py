"""Reproducible historical collection outcomes; original task records stay intact.

The existing independent phase auditor is reused for one explicit task identity.
No upstream requests, workflow execution, or database writes occur here. HTTP
callers verify after their ordinary response cache, never cache a verified claim.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
from typing import Any
from uuid import uuid4

from backend.task_audit_storage import artifact_exists, read_original_artifact_bytes, read_task_audit_bytes

SCHEMA_VERSION = 1
RULE_ID = "historical_refresh_collection_phases_v1"
TASK_NAMES = {"scheduled_daily_bond_refresh", "scheduled_intraday_bond_refresh"}
MAX_RECORD_BYTES = 2 * 1024 * 1024
MAX_SOURCE_BYTES = 64 * 1024 * 1024
MAX_SOURCE_FILES = 128
RECENT_LIMIT = 20
FIELD = "historical_collection_outcome"


def object_hash(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True, allow_nan=False).encode()).hexdigest()


def _task_id(value: Any) -> int:
    if type(value) is not int or value < 1:
        raise ValueError("refresh_correction_task_id_invalid")
    return value


def _object(data: bytes | str) -> dict[str, Any]:
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("refresh_correction_duplicate_json_key")
            result[key] = value
        return result

    def invalid(value):
        raise ValueError("refresh_correction_nonfinite_json")

    result = json.loads(data, object_pairs_hook=pairs, parse_constant=invalid)
    if not isinstance(result, dict):
        raise ValueError("refresh_correction_object_required")
    return result


def _read(path: Path, limit: int = MAX_SOURCE_BYTES) -> bytes:
    return read_original_artifact_bytes(path, max_bytes=limit)


def _resolve(root: Path, value: Any) -> Path:
    raw = str(value or "")
    if raw.startswith("/app/"):
        raw = raw[5:]
    path = (root / raw).resolve()
    if not raw or not path.is_relative_to(root) or not artifact_exists(path):
        raise ValueError("refresh_correction_source_missing_or_outside_project")
    return path


def _portable(root: Path, value: Any) -> Any:
    if isinstance(value, dict):
        return {_portable(root, key): _portable(root, item) for key, item in value.items()}
    if isinstance(value, list):
        return [_portable(root, item) for item in value]
    if isinstance(value, str):
        for prefix in (str(root) + "/", "/app/"):
            if value.startswith(prefix):
                return value[len(prefix):]
    return value


def _read_task(settings, task_id: int) -> tuple[dict[str, Any], dict[str, Any]]:
    _task_id(task_id)
    database = Path(settings.database_path).resolve()
    with sqlite3.connect(database.as_uri() + "?mode=ro", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM task_history WHERE id = ?", (task_id,)).fetchone()
    if row is None or row["task_name"] not in TASK_NAMES:
        raise ValueError("refresh_correction_task_identity_mismatch")
    task = dict(row)
    if task.get("status") not in {"success", "degraded"} or not task.get("completed_at"):
        raise ValueError("refresh_correction_task_not_completed")
    return task, _object(task.get("extra_data") or "{}")


def _reproduce(settings, task_id: int) -> dict[str, Any]:
    # This calls the same independently tested reconciliation as the CLI audit,
    # without its project scan, latest-run lookup, or live API audit entry point.
    from scripts import audit_data_truthfulness as auditor

    root = Path(settings.project_root).resolve()
    task, extra = _read_task(settings, task_id)
    audit_path = _resolve(root, extra.get("task_run_audit_path"))
    task_audit = _object(read_task_audit_bytes(audit_path))
    audit_task = task_audit.get("task") or {}
    if (audit_task.get("task_history_id") != task_id or audit_task.get("task_name") != task["task_name"]
            or datetime.fromisoformat(audit_task["started_at"]) != datetime.fromisoformat(task["started_at"])
            or datetime.fromisoformat(audit_task["finished_at"]) != datetime.fromisoformat(task["completed_at"])):
        raise ValueError("refresh_correction_audit_task_identity_mismatch")
    run_id = extra.get("screening_run_id")
    if not isinstance(run_id, str) or not re.fullmatch(r"[A-Za-z0-9_-]+", run_id):
        raise ValueError("refresh_correction_run_identity_missing")
    log_path = _resolve(root, extra.get("task_log_path"))
    log = _read(log_path).decode("utf-8")
    identities = re.findall(r"\[task-start\] task_id=(\d+) task_name=(\S+) started_at=(\S+)", log)
    if (len(identities) != 1 or int(identities[0][0]) != task_id or identities[0][1] != task["task_name"]
            or datetime.fromisoformat(identities[0][2]) != datetime.fromisoformat(task["started_at"])):
        raise ValueError("refresh_correction_parent_log_identity_mismatch")
    required = (auditor.AUTOMATION_REQUIRED_INTRADAY_SUBPROCESS_TASKS
                if task["task_name"] == "scheduled_intraday_bond_refresh"
                else auditor.AUTOMATION_REQUIRED_REFRESH_SUBPROCESS_TASKS)
    _, _, index, index_errors, _ = auditor._refresh_collection_index_from_task_log(
        settings, task_log_path_raw=str(log_path), run_id=run_id, required_tasks=required)
    reconciliation, _, errors, _ = auditor._refresh_collection_phase_reconciliation(
        settings, row={key: value for key, value in task.items() if key != "extra_data"},
        extra=extra, task_audit=task_audit, collection_index=index, collection_blockers=index_errors,
        task_log=auditor._file_evidence(settings, str(log_path)), state_payload={})
    if (reconciliation.get("source_evidence_verified") is not True
            or reconciliation.get("parent_payload_binding", {}).get("verified") is not True
            or not reconciliation.get("derived") or not reconciliation.get("current_task_phase_count")):
        raise ValueError("refresh_correction_source_reconciliation_incomplete")
    if reconciliation.get("parent_matches_derived") is not False:
        raise ValueError("refresh_correction_no_historical_discrepancy")
    expected_prefixes = ("scheduled_refresh_collection_parent_mismatch:",
                         "scheduled_refresh_collection_parent_log_phase_mismatch")
    if not errors or any(not value.startswith(expected_prefixes) for value in errors):
        raise ValueError("refresh_correction_unexplained_source_error")
    sources = reconciliation["source_files"]
    if not isinstance(sources, dict) or not 1 <= len(sources) <= MAX_SOURCE_FILES:
        raise ValueError("refresh_correction_source_list_invalid")
    if any(str(path) not in sources for path in (audit_path, log_path)):
        raise ValueError("refresh_correction_required_source_missing")
    for value, digest in sources.items():
        if hashlib.sha256(_read(_resolve(root, value))).hexdigest() != digest:
            raise ValueError("refresh_correction_source_changed_during_reproduction")
    if _read_task(settings, task_id)[0] != task:
        raise ValueError("refresh_correction_task_changed_during_reproduction")
    recorded_result = task_audit.get("task_result") or {}
    period = recorded_result.get("period_return_collection")
    original = {key: recorded_result.get(key) for key in ("status", "degraded", "degraded_stage", "warning_reason")}
    original["pending_count"] = period.get("pending_count") if isinstance(period, dict) else None
    return _portable(root, {
        "task_identity": {key: task[key] for key in ("id", "task_name", "status", "started_at", "completed_at")},
        "run_id": run_id, "original_task_sha256": object_hash(task),
        "recorded": original, "task_audit_path": str(audit_path), "task_log_path": str(log_path),
        "reconciliation": reconciliation,
        "scope": "historical_period_return_collection_in_this_task",
        "scheduler_state_basis": "sealed_scheduler_views_inside_the_original_task_audit",
        "underlying_collection_issue_resolved": False,
        "truth_policy": "append_only_explanation_original_sql_logs_audit_and_business_data_unchanged",
    })


def correction_path(settings, task_id: int) -> Path:
    _task_id(task_id)
    reports = getattr(settings, "reports_dir", None) or Path(settings.project_root) / "reports"
    return Path(reports) / "audit/refresh_collection_corrections" / f"task_{task_id}.json"


def build_correction(settings, task_id: int) -> dict[str, Any]:
    record = {"schema_version": SCHEMA_VERSION, "rule_id": RULE_ID,
              "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
              "proof": _reproduce(settings, task_id)}
    record["record_sha256"] = object_hash(record)
    return record


def _verify_record(settings, record: dict[str, Any], task_id: int) -> dict[str, Any]:
    unsigned = {key: value for key, value in record.items() if key != "record_sha256"}
    if (record.get("schema_version") != SCHEMA_VERSION or record.get("rule_id") != RULE_ID
            or record["proof"]["task_identity"]["id"] != _task_id(task_id)
            or object_hash(unsigned) != record.get("record_sha256")):
        raise ValueError("refresh_correction_record_identity_or_hash_mismatch")
    proof = _reproduce(settings, task_id)
    if proof != record["proof"]:
        raise ValueError("refresh_correction_original_or_reproduction_changed")
    return proof


def write_correction(settings, record: dict[str, Any]) -> Path:
    task_id = _task_id(record["proof"]["task_identity"]["id"])
    _verify_record(settings, record, task_id)
    path = correction_path(settings, task_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        data = (json.dumps(record, ensure_ascii=False, indent=2, allow_nan=False) + "\n").encode()
        if len(data) > MAX_RECORD_BYTES:
            raise ValueError("refresh_correction_record_too_large")
        with temporary.open("xb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.link(temporary, path)
        descriptor = os.open(path.parent, os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    finally:
        temporary.unlink(missing_ok=True)
    return path


def load_verified_correction(settings, task_id: int) -> dict[str, Any]:
    try:
        path = correction_path(settings, task_id)
        if not path.exists():
            return {"status": "absent", "verified": False, "task_id": task_id}
        record = _object(_read(path, MAX_RECORD_BYTES))
        proof = _verify_record(settings, record, task_id)
        reconciliation = proof["reconciliation"]
        return {"status": "verified", "verified": True, "task_id": task_id,
                "task_identity": proof["task_identity"], "run_id": proof["run_id"],
                "generated_at": record["generated_at"], "record_sha256": record["record_sha256"],
                "artifact_path": str(path.relative_to(Path(settings.project_root).resolve())),
                "recorded": proof["recorded"], "derived": reconciliation["derived"],
                "phases": reconciliation["phases"], "differences": reconciliation["differences"],
                "parent_log_bound_phase_count": reconciliation["parent_log_bound_phase_count"],
                "source_phase_count": reconciliation["source_phase_count"],
                "source_files": reconciliation["source_files"], "scope": proof["scope"],
                "original_task_status_preserved": True, "underlying_collection_issue_resolved": False,
                "truth_policy": proof["truth_policy"]}
    except (OSError, ValueError, TypeError, KeyError, AttributeError, sqlite3.Error) as exc:
        return {"status": "invalid", "verified": False, "task_id": task_id, "reason": str(exc)}


def annotate_scheduler_payload(settings, payload: dict[str, Any]) -> dict[str, Any]:
    """Attach freshly verified explanations without changing the cached input."""
    current = deepcopy(payload)
    current.pop("historical_collection_outcomes", None)
    directory = correction_path(settings, 1).parent
    identities = sorted((int(path.stem[5:]) for path in directory.glob("task_*.json")
                         if re.fullmatch(r"task_[1-9][0-9]*", path.stem)), reverse=True) if directory.is_dir() else []
    verified = {}

    def load(task_id):
        if task_id not in verified:
            verified[task_id] = load_verified_correction(settings, task_id)
        return verified[task_id]

    def attach(run):
        if not isinstance(run, dict):
            return
        run.pop(FIELD, None)
        if run.get("task_name") not in TASK_NAMES or not run.get("started_at"):
            return
        details = run.get("degradation_summary") or {}
        path = (run.get("task_run_audit_json_project_path") or run.get("task_run_audit_path")
                or run.get("task_run_audit_json_relative") or run.get("task_run_audit_json")
                or details.get("task_run_audit_json_project_path"))
        if not path or not identities:
            return
        try:
            task_audit = _object(read_task_audit_bytes(_resolve(Path(settings.project_root).resolve(), path)))
            task = task_audit.get("task") or {}
            task_id = _task_id(task["task_history_id"])
            if task_id not in identities:
                return
            evidence = load(task_id)
            if (task.get("task_name") != run["task_name"]
                    or datetime.fromisoformat(task["started_at"]) != datetime.fromisoformat(run["started_at"])
                    or task.get("status") != run.get("status")):
                raise ValueError("refresh_correction_payload_task_identity_mismatch")
            if evidence.get("verified"):
                identity = evidence["task_identity"]
                if (identity["task_name"] != run["task_name"]
                        or datetime.fromisoformat(identity["started_at"]) != datetime.fromisoformat(run["started_at"])
                        or run.get("screening_run_id", run.get("workflow_run_id")) not in (None, evidence["run_id"])):
                    raise ValueError("refresh_correction_payload_run_identity_mismatch")
            if evidence["status"] != "absent":
                run[FIELD] = {key: value for key, value in evidence.items() if key != "source_files"}
        except (OSError, ValueError, TypeError, KeyError, AttributeError) as exc:
            run[FIELD] = {"status": "invalid", "verified": False, "reason": str(exc)}

    def annotate_section(section):
        if not isinstance(section, dict):
            return
        for key in ("daily_run", "intraday_run"):
            attach(section.get(key))
        for key in ("last_runs", "latest_runs", "task_closures"):
            for run in (section.get(key) or {}).values():
                attach(run)
        for run in (section.get("task_run_artifacts") or {}).get("ordered_tasks") or []:
            attach(run)

    annotate_section(current)
    for key in ("scheduler_overview", "scheduler", "health"):
        annotate_section(current.get(key))
    records = [load(task_id) for task_id in identities[:RECENT_LIMIT]]
    if records:
        current["historical_collection_outcomes"] = {
            "records": [{key: value for key, value in record.items() if key != "source_files"} for record in records],
            "available_count": len(identities), "returned_count": len(records),
            "limit": RECENT_LIMIT, "truncated": len(identities) > len(records),
            "scope": "historical_explanations_not_current_collection_backlog",
        }
    return current
