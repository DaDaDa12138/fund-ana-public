"""Append-only explanations for historical collection-warning contract defects.

These records explain missing warning metadata. They never change task outcomes,
fund data, original logs, or whether the underlying collection issue is resolved.
"""
from __future__ import annotations

from datetime import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
from typing import Any
from uuid import uuid4

from backend.collection_log_summary import classify_collection_log_severity
from backend.replay_evidence import content_hash
from backend.task_audit_storage import artifact_exists, read_original_artifact_bytes, read_task_audit_bytes

TASK_NAME = "scheduled_weekly_bond_maintenance"
SCHEMA_VERSION = 1


def object_hash(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False).encode()).hexdigest()


def _resolve(root: Path, value: Any) -> Path:
    raw = str(value or "").strip()
    if raw.startswith("/app/"):
        raw = raw[len("/app/"):]
    path = (root / raw).resolve()
    if not raw or not path.is_relative_to(root.resolve()) or not artifact_exists(path):
        raise ValueError("weekly_correction_source_missing_or_outside_project")
    return path


def _read_object(path: Path) -> dict[str, Any]:
    value = json.loads(read_task_audit_bytes(path).decode("utf-8"))
    if not isinstance(value, dict):
        raise ValueError("weekly_correction_source_not_object")
    return value


def _read_task(settings, task_id: int) -> tuple[dict[str, Any], dict[str, Any]]:
    with sqlite3.connect(f"{Path(settings.database_path).resolve().as_uri()}?mode=ro", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM task_history WHERE id = ?", (task_id,)).fetchone()
    if row is None or row["task_name"] != TASK_NAME:
        raise ValueError("weekly_correction_task_identity_mismatch")
    record = dict(row)
    extra = json.loads(record.get("extra_data") or "{}")
    if not isinstance(extra, dict):
        raise ValueError("weekly_correction_extra_data_invalid")
    return record, extra


def _warning_fields(value: dict[str, Any]) -> dict[str, Any]:
    return {key: value.get(key) for key in ("status", "started_at", "warning_reason", "degraded_stage")}


def _collection_logs(root: Path, index_path: Path) -> tuple[list[str], dict[str, str]]:
    index = _read_object(index_path)
    lines: list[str] = []
    sources = {str(index_path.relative_to(root)): content_hash(index_path)}
    found: set[str] = set()
    for item in index.get("items") or []:
        stage = item.get("task_name")
        if stage not in {"collect-details", "collect-managers"}:
            continue
        found.add(stage)
        lines.append(f"[{stage}] $ " + " ".join(item.get("command") or []))
        for key in ("stdout_path_relative", "stderr_path_relative"):
            source = _resolve(root, item.get(key))
            sources[str(source.relative_to(root))] = content_hash(source)
            lines.extend(source.read_text(encoding="utf-8").splitlines())
    if "collect-managers" not in found:
        raise ValueError("weekly_correction_manager_source_missing")
    return lines, sources


def correction_path(settings, task_id: int) -> Path:
    reports_dir = getattr(settings, "reports_dir", None) or Path(settings.project_root) / "reports"
    return Path(reports_dir) / "audit/weekly_warning_corrections" / f"task_{int(task_id)}.json"


def build_correction(settings, task_id: int) -> dict[str, Any]:
    root = Path(settings.project_root).resolve()
    task, extra = _read_task(settings, task_id)
    if task.get("status") != "degraded" or extra.get("warning_reason"):
        raise ValueError("weekly_correction_not_a_missing_reason_on_degraded_task")
    audit_path = _resolve(root, extra.get("task_run_audit_path") or extra.get("task_run_audit_json_project_path"))
    audit = _read_object(audit_path)
    audit_task = audit.get("task") or {}
    if audit_task.get("task_history_id") != task_id or audit_task.get("task_name") != TASK_NAME:
        raise ValueError("weekly_correction_audit_identity_mismatch")
    task_log = _resolve(root, extra.get("task_log_path") or extra.get("task_log_file_project_path"))
    matches = re.findall(r"subprocess_index=(\S+)", read_original_artifact_bytes(task_log).decode("utf-8"))
    index_paths = list(dict.fromkeys(_resolve(root, match) for match in matches))
    selected = []
    for path in index_paths:
        items = _read_object(path).get("items") or []
        if any(item.get("task_name") == "collect-managers" for item in items):
            selected.append(path)
    if len(selected) != 1:
        raise ValueError("weekly_correction_collection_index_ambiguous")
    lines, sources = _collection_logs(root, selected[0])
    summary = classify_collection_log_severity(lines)
    if (not summary.get("degraded") or summary.get("reason") != "manager_collection_failed"
            or summary.get("manager_summary_source") != "api_source_evidence"
            or summary.get("degraded_stage") != extra.get("degraded_stage")):
        raise ValueError("weekly_correction_structured_source_does_not_prove_missing_reason")
    state = _read_object(Path(settings.scheduler_state_file))
    state_sources = {
        "last_runs.scheduled_weekly_bond_maintenance": (state.get("last_runs") or {}).get(TASK_NAME) or {},
        "task_closures.weekly_maintenance": (state.get("task_closures") or {}).get("weekly_maintenance") or {},
    }
    for value in state_sources.values():
        if (str(value.get("started_at") or "").replace("T", " ") != task["started_at"]
                or value.get("status") != task["status"]):
            raise ValueError("weekly_correction_state_not_same_historical_task")
    originals = {
        "task_history": {"sha256": object_hash(task), "values": _warning_fields(extra)},
        "task_run_audit": {"path": str(audit_path.relative_to(root)), "values": _warning_fields(audit.get("task_result") or {})},
        "scheduler_state": {key: {"sha256": object_hash(value), "snapshot": value, "values": _warning_fields(value)}
                            for key, value in state_sources.items()},
    }
    if any(value.get("warning_reason") for value in [
        originals["task_run_audit"]["values"], *[value["values"] for value in originals["scheduler_state"].values()],
    ]):
        raise ValueError("weekly_correction_original_reason_is_not_missing")
    for source in (audit_path, task_log):
        sources[str(source.relative_to(root))] = content_hash(source)
    record = {
        "schema_version": SCHEMA_VERSION, "generated_at": datetime.now().isoformat(timespec="seconds"),
        "task_id": task_id, "task_name": TASK_NAME, "started_at": task["started_at"],
        "originals": originals, "source_files": sources,
        "subprocess_index_path": str(selected[0].relative_to(root)),
        "recomputed_classification": summary,
        "correction": {"warning_reason": summary["reason"], "degraded_stage": summary["degraded_stage"],
                       "task_status": task["status"], "underlying_collection_issue_resolved": False},
        "rule": {"id": "structured_manager_source_counts_override_zero_target_detail_summary_v1",
                 "classifier_source_sha256": content_hash(Path(__file__).with_name("collection_log_summary.py"))},
        "truth_policy": "append_only_historical_reason_correction_original_state_and_business_data_unchanged",
    }
    record["record_sha256"] = object_hash(record)
    return record


def write_correction(settings, record: dict[str, Any]) -> Path:
    path = correction_path(settings, record["task_id"])
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        with temporary.open("x", encoding="utf-8") as stream:
            json.dump(record, stream, ensure_ascii=False, indent=2, allow_nan=False)
            stream.flush()
            os.fsync(stream.fileno())
        os.link(temporary, path)  # Atomic create, refusing to replace existing evidence.
        directory_fd = os.open(path.parent, os.O_DIRECTORY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        temporary.unlink(missing_ok=True)
    return path


def load_verified_correction(settings, task_id: int) -> dict[str, Any]:
    try:
        path = correction_path(settings, task_id)
        if not path.is_file():
            return {"status": "absent", "verified": False}
        root = Path(settings.project_root).resolve()
        record = _read_object(path)
        unsigned = {key: value for key, value in record.items() if key != "record_sha256"}
        if (record.get("schema_version") != SCHEMA_VERSION or record.get("task_id") != task_id
                or record.get("task_name") != TASK_NAME or object_hash(unsigned) != record.get("record_sha256")):
            raise ValueError("weekly_correction_record_identity_or_hash_mismatch")
        task, _ = _read_task(settings, task_id)
        if object_hash(task) != record["originals"]["task_history"]["sha256"]:
            raise ValueError("weekly_correction_original_task_changed")
        if not isinstance(record.get("source_files"), dict) or not record["source_files"]:
            raise ValueError("weekly_correction_sources_missing")
        for name, signature in record["source_files"].items():
            if content_hash(_resolve(root, name)) != signature:
                raise ValueError("weekly_correction_source_hash_mismatch")
        for value in record["originals"]["scheduler_state"].values():
            if object_hash(value["snapshot"]) != value["sha256"]:
                raise ValueError("weekly_correction_state_snapshot_hash_mismatch")
        lines, log_sources = _collection_logs(root, _resolve(root, record["subprocess_index_path"]))
        if (not set(log_sources).issubset(record["source_files"])
                or record["originals"]["task_run_audit"]["path"] not in record["source_files"]):
            raise ValueError("weekly_correction_required_sources_missing")
        summary = classify_collection_log_severity(lines)
        if summary != record["recomputed_classification"]:
            raise ValueError("weekly_correction_log_reproduction_mismatch")
        correction = record["correction"]
        if (correction.get("warning_reason") != summary.get("reason")
                or correction.get("degraded_stage") != summary.get("degraded_stage")
                or correction.get("task_status") != task["status"]
                or correction.get("underlying_collection_issue_resolved") is not False):
            raise ValueError("weekly_correction_claim_mismatch")
        return {"status": "verified", "verified": True, "task_id": task_id,
                "artifact_path": str(path.relative_to(root)), "record_sha256": record["record_sha256"],
                "correction": correction, "recomputed_classification": summary,
                "original_warning_reason_preserved": True,
                "underlying_collection_issue_resolved": False, "truth_policy": record["truth_policy"]}
    except (OSError, ValueError, TypeError, KeyError, AttributeError, sqlite3.Error) as exc:
        return {"status": "invalid", "verified": False, "reason": str(exc)}


def correction_for_weekly_payload(settings, payload: dict[str, Any]) -> dict[str, Any]:
    """Resolve through the task's own audit, never through a newest-file guess."""
    try:
        details = payload.get("degradation_summary") or {}
        path_value = payload.get("task_run_audit_json_project_path") or details.get("task_run_audit_json_project_path")
        if not path_value:
            return {"status": "absent", "verified": False}
        audit = _read_object(_resolve(Path(settings.project_root), path_value))
        task = audit.get("task") or {}
        if task.get("task_name") != TASK_NAME:
            raise ValueError("weekly_correction_payload_identity_mismatch")
        return load_verified_correction(settings, int(task["task_history_id"]))
    except (OSError, ValueError, TypeError, KeyError) as exc:
        return {"status": "invalid", "verified": False, "reason": str(exc)}


def apply_historical_warning_correction(settings, payload: dict[str, Any]) -> dict[str, Any]:
    current = dict(payload or {})
    if current.get("status") != "degraded" or current.get("warning_reason"):
        return current
    evidence = correction_for_weekly_payload(settings, current)
    if evidence.get("status") != "absent":
        current["historical_warning_correction"] = evidence
    if evidence.get("verified"):
        current["corrected_warning_reason"] = evidence["correction"]["warning_reason"]
    else:
        current.pop("corrected_warning_reason", None)
    return current
