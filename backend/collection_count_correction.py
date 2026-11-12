"""Append-only, reproducible counts for completed historical weekly collections.

The correction is a separate view of logged outcomes, not a database repair.
It never totals different entity types or silently chooses between conflicting
collector and executor labels. The existing warning-correction v1 stays intact.
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

from backend.task_audit_storage import (
    artifact_exists, decode_task_audit_bytes, read_original_artifact_bytes, read_task_audit_bytes,
)

SCHEMA_VERSION = 1
RULE_ID = "weekly_collection_stage_counts_v1"
TASK_NAME = "scheduled_weekly_bond_maintenance"
MAX_SOURCE_BYTES = 32 * 1024 * 1024
COUNT_FIELDS = ("records_processed", "records_inserted", "records_updated", "records_failed")
OUTCOME_FIELDS = ("target_count", "processed_count", "success_count", "failed_count", "skipped_count")
SOURCES = {
    "fundMNDetailInformation": ("collect-details", "fund_detail", "fund", "基金详情", "只基金"),
    "fundMNMangerList": ("collect-managers", "manager_list", "fund", "基金经理名单", "只基金"),
    "fundMSNMangerInfo": ("collect-managers", "manager_detail", "manager", "经理详情", "位经理"),
}


def _hash(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True, allow_nan=False).encode()).hexdigest()


def _resolve(root: Path, value: Any) -> Path:
    raw = str(value or "")
    if raw.startswith("/app/"):
        raw = raw[5:]
    path = (root / raw).resolve()
    if not raw or not path.is_relative_to(root) or not artifact_exists(path):
        raise ValueError("collection_counts_source_missing_or_outside_project")
    return path


def _read(path: Path) -> bytes:
    return read_original_artifact_bytes(path, max_bytes=MAX_SOURCE_BYTES)


def _object(data: bytes | str) -> dict[str, Any]:
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("collection_counts_duplicate_json_key")
            result[key] = value
        return result

    def invalid_constant(value):
        raise ValueError("collection_counts_nonfinite_json")

    result = json.loads(data, object_pairs_hook=pairs, parse_constant=invalid_constant)
    if not isinstance(result, dict):
        raise ValueError("collection_counts_source_not_object")
    return result


def _count(value: Any) -> int:
    if type(value) is not int or value < 0:
        raise ValueError("collection_counts_nonnegative_integer_required")
    return value


def _outcomes(total: int, success: int, failed: int, skipped: int) -> dict[str, int]:
    for value in (total, success, failed, skipped):
        _count(value)
    if success + failed + skipped != total:
        raise ValueError("collection_counts_outcomes_do_not_balance")
    return dict(zip(OUTCOME_FIELDS, (total, total, success, failed, skipped)))


def _execution_summaries(lines: list[str]) -> list[dict[str, Any]]:
    """Only complete ProgressMonitor blocks, never the trailing collector totals."""
    blocks = []
    current: dict[str, Any] | None = None
    fields = {"总任务数": "target_count", "已处理": "processed_count", "成功": "success_count",
              "失败": "failed_count", "跳过": "skipped_count"}
    for number, raw in enumerate(lines, 1):
        line = raw.strip()
        if line == "任务完成统计":
            if current is not None:
                raise ValueError("collection_counts_incomplete_execution_summary")
            current = {"line_number": number}
        elif current is not None:
            match = re.fullmatch(r"(总任务数|已处理|成功|失败|跳过):\s*([0-9,]+)", line)
            if match:
                key = fields[match[1]]
                if key in current:
                    raise ValueError("collection_counts_duplicate_execution_field")
                current[key] = int(match[2].replace(",", ""))
            elif line.startswith("平均速度:"):
                unit = next((value for value in ("只基金", "位经理") if value + "/秒" in line), None)
                if unit is None or any(key not in current for key in OUTCOME_FIELDS):
                    raise ValueError("collection_counts_execution_summary_fields_missing")
                expected = _outcomes(current["target_count"], current["success_count"],
                                     current["failed_count"], current["skipped_count"])
                if {key: current[key] for key in OUTCOME_FIELDS} != expected:
                    raise ValueError("collection_counts_execution_summary_incomplete")
                blocks.append({"line_number": current["line_number"], "unit": unit, "outcomes": expected})
                current = None
    if current is not None:
        raise ValueError("collection_counts_incomplete_execution_summary")
    return blocks


def _stage_counts(evidence: dict[str, Any], block: dict[str, Any] | None) -> tuple[dict[str, int], str]:
    """Legacy missing outcome fields only support a proven zero or full write."""
    total = _count(evidence.get("target_count"))
    io_keys = ("request_count", "response_count", "accepted_response_count", "failed_response_count",
               "has_data_response_count", "empty_response_count", "transport_exception_count",
               "returned_record_count", "saved_fund_count", "written_record_count", "save_failed_count")
    io = {key: _count(evidence.get(key)) for key in io_keys}
    if total == 0:
        if any(io.values()) or any(_count(evidence[key]) != 0 for key in ("success_count", "failed_count", "skipped_count") if key in evidence):
            raise ValueError("collection_counts_zero_target_has_activity")
        return _outcomes(0, 0, 0, 0), "explicit_zero_targets_and_no_activity"
    if block is None:
        raise ValueError("collection_counts_execution_summary_missing")
    if "success_count" in evidence and "failed_count" in evidence:
        # Historical manager-list snapshots omit skipped_count. Its explicit
        # executor summary is the evidence; a missing value is not defaulted to 0.
        skipped = evidence.get("skipped_count", block["outcomes"]["skipped_count"])
        counts = _outcomes(total, _count(evidence["success_count"]), _count(evidence["failed_count"]), _count(skipped))
        if io["saved_fund_count"] != counts["success_count"] or io["written_record_count"] != counts["success_count"]:
            raise ValueError("collection_counts_success_not_supported_by_logged_writes")
        if io["save_failed_count"] > counts["failed_count"]:
            raise ValueError("collection_counts_write_failures_not_accounted_for")
        if (io["request_count"] != io["response_count"] + io["transport_exception_count"]
                or io["request_count"] != io["accepted_response_count"] + io["failed_response_count"]
                or io["response_count"] != io["has_data_response_count"] + io["empty_response_count"]
                or io["accepted_response_count"] < counts["success_count"]
                or io["has_data_response_count"] < counts["success_count"]):
            raise ValueError("collection_counts_response_evidence_inconsistent")
        return counts, "explicit_collector_outcomes_and_logged_writes"
    if any(key in evidence for key in ("success_count", "failed_count", "skipped_count")):
        raise ValueError("collection_counts_partial_outcome_schema")
    complete = ("request_count", "response_count", "accepted_response_count", "has_data_response_count",
                "returned_record_count", "saved_fund_count", "written_record_count")
    zero = ("failed_response_count", "empty_response_count", "transport_exception_count", "save_failed_count")
    if evidence["source"] != "fundMSNMangerInfo" or any(io[key] != total for key in complete) or any(io[key] != 0 for key in zero):
        raise ValueError("collection_counts_legacy_detail_outcomes_unproven")
    counts = _outcomes(total, total, 0, 0)
    if block["outcomes"] != counts:
        raise ValueError("collection_counts_legacy_detail_execution_disagrees")
    return counts, "legacy_complete_detail_writes_and_execution_summary"


def _read_task(settings, task_id: int) -> tuple[dict[str, Any], dict[str, Any]]:
    _count(task_id)
    with sqlite3.connect(Path(settings.database_path).resolve().as_uri() + "?mode=ro", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM task_history WHERE id = ?", (task_id,)).fetchone()
    if row is None or row["task_name"] != TASK_NAME:
        raise ValueError("collection_counts_task_identity_mismatch")
    task = dict(row)
    if task["status"] not in {"success", "degraded", "failed", "warning"} or not task.get("completed_at"):
        raise ValueError("collection_counts_task_not_terminal")
    return task, _object(task.get("extra_data") or "{}")


def _build_proof(settings, task_id: int) -> dict[str, Any]:
    root = Path(settings.project_root).resolve()
    task, extra = _read_task(settings, task_id)
    sources: dict[str, str] = {}

    def source(value):
        path = _resolve(root, value)
        data = _read(path)
        name = path.relative_to(root).as_posix()
        digest = hashlib.sha256(data).hexdigest()
        if name in sources and sources[name] != digest:
            raise ValueError("collection_counts_source_changed_during_read")
        sources[name] = digest
        return name, data

    audit_name, audit_data = source(extra.get("task_run_audit_path") or extra.get("task_run_audit_json_project_path"))
    audit = _object(decode_task_audit_bytes(
        audit_data, compressed=audit_name.endswith(".json.gz"), max_bytes=MAX_SOURCE_BYTES,
    ))
    audit_task = audit.get("task") or {}
    started, completed = datetime.fromisoformat(task["started_at"]), datetime.fromisoformat(task["completed_at"])
    if (audit_task.get("task_history_id") != task_id or audit_task.get("task_name") != TASK_NAME
            or audit_task.get("status") != task["status"]
            or datetime.fromisoformat(audit_task["started_at"]) != started
            or not started <= datetime.fromisoformat(audit_task["finished_at"]) <= completed):
        raise ValueError("collection_counts_audit_identity_mismatch")
    log_name, log_data = source(extra.get("task_log_path") or extra.get("task_log_file_project_path"))
    log = log_data.decode("utf-8")
    identities = re.findall(r"\[task-start\] task_id=(\d+) task_name=(\S+) started_at=(\S+)", log)
    if (len(identities) != 1 or int(identities[0][0]) != task_id or identities[0][1] != TASK_NAME
            or datetime.fromisoformat(identities[0][2]) != started):
        raise ValueError("collection_counts_task_log_identity_mismatch")
    indices = list(dict.fromkeys(re.findall(r"subprocess_index=(\S+)", log)))
    selected = []
    for name in indices:
        path = _resolve(root, name)
        index = _object(_read(path))
        if any(item.get("task_name") == "collect-managers" for item in index.get("items", [])):
            selected.append(path)
    if len(selected) != 1:
        raise ValueError("collection_counts_subprocess_index_ambiguous")
    index_name, index_data = source(selected[0])
    index = _object(index_data)
    items = index.get("items") or []
    stages = []
    seen_tasks = set()
    for item in items:
        task_name = item.get("task_name")
        if task_name not in {entry[0] for entry in SOURCES.values()}:
            continue
        if task_name in seen_tasks:
            raise ValueError("collection_counts_subprocess_attempts_ambiguous")
        seen_tasks.add(task_name)
        meta_name, meta_data = source(item.get("meta_path_relative"))
        meta = _object(meta_data)
        for key in ("task_name", "command", "started_at", "finished_at", "return_code", "timed_out", "success"):
            if key not in item or meta.get(key) != item[key]:
                raise ValueError("collection_counts_subprocess_meta_mismatch")
        if (item["timed_out"] is not False or item["return_code"] not in (0, 2)
                or not started <= datetime.fromisoformat(item["started_at"]) <= datetime.fromisoformat(item["finished_at"]) <= completed):
            raise ValueError("collection_counts_subprocess_not_complete_in_task")
        names = {}
        texts = {}
        for stream in ("stdout", "stderr"):
            name, data = source(item.get(stream + "_path_relative"))
            if _resolve(root, meta.get(stream + "_path")) != root / name:
                raise ValueError("collection_counts_subprocess_stream_mismatch")
            names[stream] = name
            texts[stream] = data.decode("utf-8")
        lines = texts["stdout"].splitlines()
        blocks = _execution_summaries(lines)
        evidence_by_source = {}
        for number, line in enumerate(lines, 1):
            if line.startswith("api_source_evidence="):
                evidence = _object(line.split("=", 1)[1])
                api = evidence.get("source")
                if api not in SOURCES or SOURCES[api][0] != task_name or api in evidence_by_source:
                    raise ValueError("collection_counts_source_identity_or_duplicates")
                if evidence.get("schema_version") != 1 or evidence.get("task") != task_name:
                    raise ValueError("collection_counts_unsupported_source_schema")
                stage = SOURCES[api][1]
                if evidence.get("stage") not in (None, stage) or (task_name == "collect-managers" and evidence.get("stage") != stage):
                    raise ValueError("collection_counts_source_stage_mismatch")
                evidence_by_source[api] = (number, evidence)
        expected_sources = {key for key, value in SOURCES.items() if value[0] == task_name}
        if set(evidence_by_source) != expected_sources:
            raise ValueError("collection_counts_required_source_missing")
        used_blocks = set()
        for api, (number, evidence) in evidence_by_source.items():
            _, stage, entity, label, unit = SOURCES[api]
            matching = [block for block in blocks if block["unit"] == unit and block["outcomes"]["target_count"] == evidence.get("target_count")]
            if len(matching) > 1:
                raise ValueError("collection_counts_execution_stage_ambiguous")
            block = matching[0] if matching else None
            counts, basis = _stage_counts(evidence, block)
            if block:
                used_blocks.add(block["line_number"])
                if counts["target_count"] != block["outcomes"]["target_count"] or counts["success_count"] != block["outcomes"]["success_count"]:
                    raise ValueError("collection_counts_execution_success_disagrees")
                if task_name == "collect-managers" and counts != block["outcomes"]:
                    raise ValueError("collection_counts_manager_outcomes_disagree")
            stages.append({
                "task_name": task_name, "stage": stage, "source": api, "entity_type": entity, "label": label, "unit": unit,
                "collector_outcomes": counts, "execution_outcomes": block["outcomes"] if block else None,
                "outcome_labels_match": counts == block["outcomes"] if block else None,
                "basis": basis, "logged_written_record_count": evidence["written_record_count"],
                "source_path": names["stdout"], "source_line": number,
                "execution_summary_line": block["line_number"] if block else None,
                "source_evidence": evidence, "subprocess_meta_path": meta_name,
            })
        if used_blocks != {block["line_number"] for block in blocks}:
            raise ValueError("collection_counts_unbound_execution_summary")
    if "collect-managers" not in seen_tasks:
        raise ValueError("collection_counts_manager_stage_missing")
    # Bind the complete persisted row, including its existing compaction metadata.
    # A future archive/mutation invalidates this proof instead of silently rebasing it.
    final_task, _ = _read_task(settings, task_id)
    if task != final_task or any(hashlib.sha256(_read(root / name)).hexdigest() != digest for name, digest in sources.items()):
        raise ValueError("collection_counts_original_changed_during_build")
    return {
        "task_identity": {key: task[key] for key in ("id", "task_name", "status", "started_at", "completed_at")},
        "original_task_sha256": _hash(task),
        "original_sql_counts": {key: task.get(key) for key in COUNT_FIELDS},
        "original_summary_counts": {key: extra.get(key) for key in (*COUNT_FIELDS, "records_succeeded")},
        "task_audit_path": audit_name, "task_log_path": log_name, "subprocess_index_path": index_name,
        "source_files": sources, "stages": sorted(stages, key=lambda row: row["stage"]),
        "unreconstructed_subprocesses": sorted({item["task_name"] for item in items} - seen_tasks),
        "unique_fund_total": None, "whole_task_totals": None,
        "aggregation_note": "Stages can overlap and include different entities; whole-task and unique-fund totals are unknown.",
        "underlying_collection_issue_resolved": False,
        "truth_policy": "append_only_logged_stage_counts_original_sql_state_logs_and_business_data_unchanged",
    }


def correction_path(settings, task_id: int) -> Path:
    _count(task_id)
    reports_dir = getattr(settings, "reports_dir", None) or Path(settings.project_root) / "reports"
    return Path(reports_dir) / "audit/collection_count_corrections" / f"task_{task_id}.json"


def build_correction(settings, task_id: int) -> dict[str, Any]:
    record = {"schema_version": SCHEMA_VERSION, "rule_id": RULE_ID,
              "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
              "proof": _build_proof(settings, task_id)}
    record["record_sha256"] = _hash(record)
    return record


def write_correction(settings, record: dict[str, Any]) -> Path:
    """Validate again and create atomically; never replace an existing explanation."""
    _verify_record(settings, record)
    path = correction_path(settings, record["proof"]["task_identity"]["id"])
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        with temporary.open("x", encoding="utf-8") as stream:
            json.dump(record, stream, ensure_ascii=False, indent=2, allow_nan=False)
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


def _verify_record(settings, record: dict[str, Any]) -> dict[str, Any]:
    unsigned = {key: value for key, value in record.items() if key != "record_sha256"}
    if (record.get("schema_version") != SCHEMA_VERSION or record.get("rule_id") != RULE_ID
            or _hash(unsigned) != record.get("record_sha256")):
        raise ValueError("collection_counts_record_contract_or_hash_mismatch")
    proof = _build_proof(settings, record["proof"]["task_identity"]["id"])
    if proof != record["proof"]:
        raise ValueError("collection_counts_original_or_reproduction_mismatch")
    return proof


def load_verified_correction(settings, task_id: int) -> dict[str, Any]:
    try:
        path = correction_path(settings, task_id)
        if not path.exists():
            return {"status": "absent", "verified": False, "task_id": task_id}
        record = _object(_read(path))
        if record["proof"]["task_identity"]["id"] != task_id:
            raise ValueError("collection_counts_record_task_mismatch")
        proof = _verify_record(settings, record)
        # HTTP summaries omit raw source payloads; the immutable artifact retains them.
        summary = {key: value for key, value in proof.items() if key not in {"source_files", "stages"}}
        summary["stages"] = [{key: value for key, value in stage.items() if key != "source_evidence"} for stage in proof["stages"]]
        return {"status": "verified", "verified": True, "task_id": task_id,
                "artifact_path": str(path.relative_to(Path(settings.project_root).resolve())),
                "record_sha256": record["record_sha256"], **summary}
    except (OSError, ValueError, TypeError, KeyError, AttributeError, sqlite3.Error) as exc:
        return {"status": "invalid", "verified": False, "task_id": task_id, "reason": str(exc)}


def apply_to_weekly_payload(settings, payload: dict[str, Any]) -> dict[str, Any]:
    current = dict(payload or {})
    current.pop("historical_collection_counts", None)
    if current.get("task_name") not in (None, TASK_NAME) or not current.get("started_at"):
        return current
    details = current.get("degradation_summary") or {}
    path_value = current.get("task_run_audit_json_project_path") or details.get("task_run_audit_json_project_path")
    if not path_value:
        return current
    try:
        root = Path(settings.project_root).resolve()
        task = _object(read_task_audit_bytes(_resolve(root, path_value), max_bytes=MAX_SOURCE_BYTES)).get("task") or {}
        if task.get("task_name") != TASK_NAME:
            return current
        if datetime.fromisoformat(task["started_at"]) != datetime.fromisoformat(current["started_at"]):
            raise ValueError("collection_counts_payload_task_identity_mismatch")
        result = load_verified_correction(settings, _count(task["task_history_id"]))
        if result["status"] != "absent":
            current["historical_collection_counts"] = result
    except (OSError, ValueError, TypeError, KeyError, AttributeError) as exc:
        current["historical_collection_counts"] = {"status": "invalid", "verified": False, "reason": str(exc)}
    return current
