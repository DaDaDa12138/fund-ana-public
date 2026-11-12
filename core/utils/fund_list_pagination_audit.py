"""Record observed pagination coverage without changing accepted fund records."""
from __future__ import annotations

from collections import Counter
from copy import deepcopy
from datetime import datetime
import hashlib
import json
from typing import Any

from api.response_observation import rank_response_observation


def observed_total(value: Any) -> int | None:
    """A missing, negative or nonintegral total is unknown, not an empty universe."""
    if isinstance(value, bool) or value is None:
        return None
    text = str(value).strip()
    return int(text) if text.isascii() and text.isdigit() else None


def _code_digest(codes: list[str]) -> str:
    body = json.dumps(codes, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(body).hexdigest()


def record_rank_page_observation(
    evidence: dict[str, Any],
    records: list,
    *,
    page: int,
    previous_codes: set[str],
    declared_total: Any,
    sort_column: str,
    response_payload: Any = None,
) -> None:
    codes = [str(row.get("FCODE") or "").strip() for row in records if isinstance(row, dict)]
    codes = [code for code in codes if code]
    counts = Counter(codes)
    unique = set(codes)
    ordered_digest = _code_digest(codes)
    membership_digest = _code_digest(sorted(unique))
    prior = evidence.setdefault("page_observations", [])
    same_membership_pages = [item["page"] for item in prior
                             if unique and item["code_set_sha256"] == membership_digest]
    prior.append({
        "page_observation_schema_version": 2,
        "page": page, "observed_at": datetime.now().isoformat(timespec="seconds"),
        "declared_total_count": observed_total(declared_total), "record_count": len(records),
        "identified_record_count": len(codes), "unique_code_count": len(unique),
        "ordered_codes_sha256": ordered_digest, "code_set_sha256": membership_digest,
        # Preserve all observed positions and original field types, including
        # missing fields, explicit nulls and non-object records. This projection
        # is sufficient to reconstruct membership and ties, not other metrics.
        "identity_sort_fields": ["FCODE", "SHORTNAME", sort_column],
        "identity_sort_records": [
            {key: deepcopy(row[key]) for key in ("FCODE", "SHORTNAME", sort_column) if key in row}
            if isinstance(row, dict) else deepcopy(row) for row in records
        ],
        "response_observation": rank_response_observation(response_payload),
        "first_code": codes[0] if codes else None, "last_code": codes[-1] if codes else None,
        "within_page_duplicate_codes": sorted(code for code, count in counts.items() if count > 1),
        "previous_pages_overlap_codes": sorted(unique & previous_codes),
        "same_membership_pages": same_membership_pages,
        "first_sort_value": records[0].get(sort_column) if records and isinstance(records[0], dict) else None,
        "last_sort_value": records[-1].get(sort_column) if records and isinstance(records[-1], dict) else None,
    })


def finalize_rank_pagination_evidence(evidence: dict[str, Any], seen_codes: set[str]) -> None:
    pages = evidence.get("page_observations") or []
    declared = observed_total(evidence.get("first_page_declared_total_count"))
    unique_count = len(seen_codes)
    evidence["unique_raw_code_count"] = unique_count
    evidence["declared_unique_code_shortfall"] = max(declared - unique_count, 0) if declared is not None else None
    evidence["declared_unique_code_overflow"] = max(unique_count - declared, 0) if declared is not None else None
    totals = sorted({item["declared_total_count"] for item in pages if item["declared_total_count"] is not None})
    evidence["observed_declared_totals"] = totals
    warnings = []
    if declared is None:
        warnings.append("declared_total_unknown")
    if evidence.get("duplicate_code_count", 0) > 0:
        warnings.append("intra_type_duplicate_codes")
    if any(item["same_membership_pages"] for item in pages):
        warnings.append("repeated_page_membership")
    if len(totals) > 1:
        warnings.append("declared_total_changed")
    if evidence["declared_unique_code_shortfall"]:
        warnings.append("declared_unique_code_shortfall")
    if evidence["declared_unique_code_overflow"]:
        warnings.append("declared_unique_code_overflow")
    if evidence.get("early_stop_page") is not None:
        warnings.append("pagination_ended_early")
    evidence["pagination_warning_codes"] = warnings
    evidence["unique_coverage_status"] = (
        "unknown_declared_total" if declared is None else "coverage_not_proven" if warnings else "matched_declared_total"
    )
    evidence["unique_coverage_basis"] = "raw_FCODE_within_each_requested_fund_type_not_cross_type_union_or_database_universe"
