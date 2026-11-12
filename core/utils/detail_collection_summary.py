"""Separate fetched update payloads from committed fund-detail outcomes."""
from __future__ import annotations

from collections.abc import Mapping
import json
from typing import Any


PREFIX = "detail_collection_summary="
_NOT_PROVIDED = object()
INPUT_COUNTS = ("target_count", "fetch_ready_count", "fetch_failed_count", "unavailable_count",
                "write_success_count", "write_failed_count")


def build_detail_collection_summary(counts: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(counts, Mapping):
        raise ValueError("detail_collection_counts_missing")
    values = {}
    for key in INPUT_COUNTS:
        value = counts.get(key)
        if type(value) is not int or value < 0:
            raise ValueError(f"detail_collection_count_invalid:{key}")
        values[key] = value
    if values["fetch_ready_count"] + values["fetch_failed_count"] + values["unavailable_count"] != values["target_count"]:
        raise ValueError("detail_collection_fetch_outcomes_unbalanced")
    if values["write_success_count"] + values["write_failed_count"] != values["fetch_ready_count"]:
        raise ValueError("detail_collection_persistence_outcomes_unbalanced")
    failed = values["fetch_failed_count"] + values["write_failed_count"]
    status, reason = "success", None
    if failed:
        status = "degraded"
        reason = "detail_persistence_failed" if values["write_failed_count"] else "detail_fetch_failed"
    elif values["unavailable_count"]:
        status, reason = "completed_with_unavailable", "detail_no_usable_update_payload"
    elif values["target_count"] == 0:
        status, reason = "skipped", "no_targets"
    return {**values, "schema_version": 1, "task": "collect-details", "entity_type": "fund",
            "status": status, "reason": reason, "processed_count": values["target_count"],
            "success_count": values["write_success_count"], "failed_count": failed,
            "skipped_count": values["unavailable_count"],
            "success_basis": "committed_fund_detail_write",
            "unavailable_basis": "no_detail_update_payload_not_a_database_write_failure",
            "stop_watch_fallback_scope": "not_counted_as_detail_success"}


def validate_detail_collection_summary(summary: Any, *, returned_count: Any = _NOT_PROVIDED) -> dict[str, Any]:
    if not isinstance(summary, Mapping):
        raise ValueError("detail_collection_summary_missing")
    expected = build_detail_collection_summary(summary)
    for key, value in expected.items():
        if key not in summary or type(summary[key]) is not type(value) or summary[key] != value:
            raise ValueError(f"detail_collection_summary_mismatch:{key}")
    if returned_count is not _NOT_PROVIDED and (type(returned_count) is not int or returned_count != expected["success_count"]):
        raise ValueError("detail_collection_returned_count_mismatch")
    return expected


def detail_collection_source_errors(summary: dict, evidence: Any) -> list[str]:
    errors = []
    if not isinstance(evidence, Mapping) or evidence.get("source") != "fundMNDetailInformation":
        return ["detail_collection_source_evidence_missing"]
    if evidence.get("task") != "collect-details":
        errors.append("detail_collection_source_task_mismatch")
    for field, expected in {
        "target_count": summary["target_count"], "success_count": summary["success_count"],
        "failed_count": summary["failed_count"], "skipped_count": summary["skipped_count"],
        "saved_fund_count": summary["write_success_count"], "written_record_count": summary["write_success_count"],
        "save_failed_count": summary["write_failed_count"],
    }.items():
        if type(evidence.get(field)) is not int or evidence[field] != expected:
            errors.append(f"detail_collection_source_mismatch:{field}")
    accepted = evidence.get("accepted_response_count")
    if type(accepted) is not int or accepted < summary["fetch_ready_count"]:
        errors.append("detail_collection_fetch_not_supported_by_accepted_responses")
    return errors


def parse_detail_collection_summary(lines: list[str]) -> tuple[dict | None, list[str]]:
    observed = [str(line).split(PREFIX, 1)[1] for line in lines if PREFIX in str(line)]
    if not observed:
        return None, []
    if len(observed) != 1:
        return None, ["detail_collection_summary_ambiguous"]
    try:
        return validate_detail_collection_summary(json.loads(observed[0])), []
    except (ValueError, TypeError) as exc:
        return None, [str(exc)]
