"""Manager collection outcomes: final business counts, independent of HTTP retries."""
from __future__ import annotations

from collections.abc import Iterable, Mapping
import json
from typing import Any

_NOT_PROVIDED = object()


def _count(value: Any, field: str) -> int:
    if type(value) is not int or value < 0:
        raise ValueError(f"manager_collection_count_invalid:{field}")
    return value


def build_manager_collection_summary(stages: Any) -> dict[str, Any]:
    """Validate both stages before deriving a terminal state; absent counts are unknown."""
    if not isinstance(stages, Mapping):
        raise ValueError("manager_collection_stage_summary_missing")
    counts: dict[str, dict[str, int]] = {}
    for stage, fields in {
        "fund_manager_list": ("total_funds", "success_funds", "failed_funds", "empty_funds"),
        "manager_details": ("requested_manager_count", "updated_manager_count",
                            "failed_manager_count", "skipped_manager_count"),
    }.items():
        payload = stages.get(stage)
        if not isinstance(payload, Mapping):
            raise ValueError(f"manager_collection_stage_missing:{stage}")
        counts[stage] = {field: _count(payload.get(field), f"{stage}.{field}") for field in fields}

    listing, details = counts["fund_manager_list"], counts["manager_details"]
    if listing["success_funds"] + listing["failed_funds"] != listing["total_funds"]:
        raise ValueError("manager_collection_list_outcomes_unbalanced")
    if listing["empty_funds"] > listing["total_funds"]:
        raise ValueError("manager_collection_list_empty_gt_targets")
    if (details["updated_manager_count"] + details["failed_manager_count"]
            + details["skipped_manager_count"] != details["requested_manager_count"]):
        raise ValueError("manager_collection_detail_outcomes_unbalanced")

    total = listing["total_funds"] + details["requested_manager_count"]
    succeeded = listing["success_funds"] + details["updated_manager_count"]
    failed = listing["failed_funds"] + details["failed_manager_count"]
    skipped = details["skipped_manager_count"]
    empty = listing["empty_funds"]
    listing_payload = stages["fund_manager_list"]
    if "cooldown_count" in stages and "cooldown_funds" not in listing_payload:
        raise ValueError("manager_collection_cooldown_without_stage_evidence")
    cooldown = _count(listing_payload.get("cooldown_funds", 0), "fund_manager_list.cooldown_funds")
    if failed or empty:
        status = "degraded"
        reason = "manager_collection_failed" if failed else "manager_collection_empty_responses"
    elif cooldown:
        status, reason = "degraded", "manager_collection_retry_cooldown"
    elif total == skipped:
        status = "skipped"
        reason = "no_targets" if total == 0 else "all_targets_skipped"
    else:
        status, reason = "success", None
    result = {
        **stages, "schema_version": 1, "status": status, "reason": reason,
        "target_count": total, "success_count": succeeded, "failed_count": failed,
        "skipped_count": skipped, "empty_count": empty,
    }
    # Additive contract: old immutable summaries predate attempt tracking.
    # Do not claim their unobserved cooldown count was measured as zero.
    if "cooldown_funds" in listing_payload:
        result["cooldown_count"] = cooldown
    return result


def validate_manager_collection_summary(
    summary: Any, *, returned_manager_count: Any = _NOT_PROVIDED,
) -> dict[str, Any]:
    expected = build_manager_collection_summary(summary)
    for field in ("schema_version", "status", "reason", "target_count", "success_count",
                  "failed_count", "skipped_count", "empty_count"):
        if field not in summary or type(summary[field]) is not type(expected[field]) or summary[field] != expected[field]:
            raise ValueError(f"manager_collection_summary_mismatch:{field}")
    if "cooldown_count" in expected:
        if type(summary.get("cooldown_count")) is not int or summary["cooldown_count"] != expected["cooldown_count"]:
            raise ValueError("manager_collection_summary_mismatch:cooldown_count")
    elif "cooldown_count" in summary:
        raise ValueError("manager_collection_cooldown_without_stage_evidence")
    if returned_manager_count is not _NOT_PROVIDED:
        count = _count(returned_manager_count, "returned_manager_count")
        if count != summary["manager_details"]["updated_manager_count"]:
            raise ValueError("manager_collection_returned_count_mismatch")
    return expected


def parse_manager_collection_summary(lines: Iterable[str]) -> tuple[dict[str, Any] | None, list[str]]:
    """Old immutable logs may lack the marker; a present but bad marker must fail."""
    payloads = [str(line).split("manager_collection_summary=", 1)[1]
                for line in lines if "manager_collection_summary=" in str(line)]
    if not payloads:
        return None, []
    if len(payloads) != 1:
        return None, ["manager_collection_summary_not_unique"]
    try:
        return validate_manager_collection_summary(json.loads(payloads[0])), []
    except (ValueError, TypeError) as exc:
        return None, [f"manager_collection_summary_invalid:{exc}"]


def manager_collection_source_errors(
    summary: dict[str, Any], evidence_entries: Iterable[dict[str, Any]],
) -> list[str]:
    """Compare final counts, not failed HTTP attempts that may recover on retry."""
    errors = []
    entries = list(evidence_entries)
    fields = {
        "fundMNMangerList": ("fund_manager_list", {
            "target_count": "total_funds", "success_count": "success_funds",
            "failed_count": "failed_funds", "empty_count": "empty_funds",
        }),
        "fundMSNMangerInfo": ("manager_details", {
            "target_count": "requested_manager_count", "success_count": "updated_manager_count",
            "failed_count": "failed_manager_count", "skipped_count": "skipped_manager_count",
        }),
    }
    if "cooldown_funds" in summary["fund_manager_list"]:
        fields["fundMNMangerList"][1]["cooldown_count"] = "cooldown_funds"
    if len(entries) != 2:
        errors.append("manager_collection_two_source_records_required")
    for source, (stage, mapping) in fields.items():
        matches = [entry for entry in entries if entry.get("source") == source]
        if len(matches) != 1:
            errors.append(f"manager_collection_source_not_unique:{source}")
            continue
        evidence = matches[0]
        if evidence.get("task") != "collect-managers":
            errors.append(f"manager_collection_source_task_mismatch:{source}")
        no_requests = (type(evidence.get("target_count")) is int and evidence["target_count"] == 0
                       and type(evidence.get("request_count")) is int and evidence["request_count"] == 0)
        for source_field, stage_field in mapping.items():
            # Legacy zero-target detail evidence omitted the outcome fields.
            value = evidence.get(source_field, 0 if no_requests and source_field != "cooldown_count" else None)
            if type(value) is not int or value != summary[stage][stage_field]:
                errors.append(f"manager_collection_source_count_mismatch:{source}:{source_field}")
    return errors
