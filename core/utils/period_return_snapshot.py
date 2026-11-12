"""Dated API period returns are one snapshot, independent of the latest NAV.

Legacy rows have no recoverable return date. A NAV write must never certify
their period values; only an observed, dated API response can do that.
"""
from __future__ import annotations

from datetime import date, datetime
import hashlib
import json
import math
from typing import Any, Mapping
from zoneinfo import ZoneInfo


RETURN_FIELDS = (
    "return_1w", "return_1m", "return_3m", "return_6m", "return_1y",
    "return_2y", "return_3y", "return_5y", "return_ytd", "return_since",
)
RETURN_PROVENANCE_FIELDS = (
    "period_return_date", "period_return_observed_at", "period_return_source",
    "period_return_source_sha256", "period_return_source_json",
)
RETURN_REJECTION_FIELDS = ("period_return_rejection_json", "period_return_rejection_sha256")
RETURN_VALIDATION_FIELDS = ("period_return_last_status", *RETURN_REJECTION_FIELDS)
INITIALIZATION_QUARANTINE = "initialization_placeholder_quarantined"
RETURN_SNAPSHOT_FIELDS = (*RETURN_FIELDS, *RETURN_PROVENANCE_FIELDS)
NAV_FIELDS = ("latest_nav", "latest_nav_date", "daily_growth")
RETURN_PUBLIC_FIELDS = {
    "period_return_date": "区间收益基准日",
    "period_return_observed_at": "区间收益获取时间",
    "period_return_source": "区间收益来源",
    "period_return_source_sha256": "区间收益来源SHA256",
}


def snapshot_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        try:
            return date(value.year, value.month, value.day)
        except (TypeError, ValueError):
            return None
    if isinstance(value, date):
        return value
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        return datetime.fromisoformat(value.strip().replace("Z", "+00:00")).date()
    except ValueError:
        return None


def snapshot_time(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if snapshot_date(value) is not None else None
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        return datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None


def local_observation_time(value: Any) -> datetime | None:
    parsed = snapshot_time(value)
    if parsed is not None and parsed.tzinfo is not None:
        return parsed.astimezone(ZoneInfo("Asia/Shanghai")).replace(tzinfo=None)
    return parsed


def period_number(value: Any) -> float | None:
    if value is None or (isinstance(value, str) and value.strip().lower() in {
        "", "-", "--", "---", "暂无", "n/a", "null", "none",
    }):
        return None
    if isinstance(value, bool):
        raise ValueError("period_return_boolean_is_not_a_number")
    number = float(value)
    if not math.isfinite(number):
        raise ValueError("period_return_nonfinite")
    return number


def build_period_snapshot(
    values: Mapping[str, Any], *, as_of_date: Any, observed_at: datetime,
    source: str, source_payload: Any,
) -> dict[str, Any]:
    """Freeze all ten fields, including explicit NULLs, from one API response."""
    observed_at = local_observation_time(observed_at)
    parsed_date = snapshot_date(as_of_date)
    if observed_at is None or parsed_date is None or parsed_date > observed_at.date():
        raise ValueError("period_return_date_missing_or_future")
    if source not in {"fundMNRank", "fundMNPeriodIncrease"}:
        raise ValueError("period_return_source_unknown")
    numbers = {field: period_number(values.get(field)) for field in RETURN_FIELDS}
    if not any(value is not None for value in numbers.values()):
        raise ValueError("period_return_values_missing")
    encoded = json.dumps(source_payload, ensure_ascii=False, sort_keys=True,
                         separators=(",", ":"), allow_nan=False)
    if len(encoded.encode("utf-8")) > 65536:
        raise ValueError("period_return_source_payload_too_large")
    return {
        **numbers,
        "period_return_date": parsed_date,
        "period_return_observed_at": observed_at,
        "period_return_source": source,
        "period_return_source_sha256": hashlib.sha256(encoded.encode("utf-8")).hexdigest(),
        "period_return_source_json": encoded,
    }


def has_dated_period_snapshot(row: Mapping[str, Any], *, verify_payload: bool = False) -> bool:
    as_of_date = snapshot_date(row.get("period_return_date"))
    observed_at = local_observation_time(row.get("period_return_observed_at"))
    if (as_of_date is None or observed_at is None or as_of_date > observed_at.date()
            or row.get("period_return_source") not in {"fundMNRank", "fundMNPeriodIncrease"}):
        return False
    digest = row.get("period_return_source_sha256")
    if not isinstance(digest, str) or len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
        return False
    if verify_payload:
        payload = row.get("period_return_source_json")
        if not isinstance(payload, str) or hashlib.sha256(payload.encode("utf-8")).hexdigest() != digest:
            return False
    return True


def verified_current_rank_snapshot(record: Mapping[str, Any], *, now: datetime | None = None) -> bool:
    """Reproject the newer stored response before isolating an old refresh.

    A digest alone proves the payload was retained, not that the database
    values still correspond to it. This exemption needs the rank response's
    identity, NAV, date and all ten return fields to match the current row.
    """
    now = now or datetime.now()
    if (record.get("period_return_source") != "fundMNRank"
            or not has_dated_period_snapshot(record, verify_payload=True)
            or not any(record.get(field) is not None for field in RETURN_FIELDS)):
        return False
    nav_date = snapshot_date(record.get("latest_nav_date"))
    observed_at = local_observation_time(record.get("period_return_observed_at"))
    if (nav_date is None or nav_date > now.date()
            or snapshot_date(record.get("period_return_date")) != nav_date
            or observed_at is None or observed_at > now):
        return False
    rank_fields = ("SYL_Z", "SYL_Y", "SYL_3Y", "SYL_6Y", "SYL_1N",
                   "SYL_2N", "SYL_3N", "SYL_5N", "SYL_JN", "SYL_LN")
    try:
        payload = json.loads(record["period_return_source_json"])
        return bool(
            isinstance(payload, dict) and payload.get("FCODE") == record.get("fcode")
            and snapshot_date(payload.get("FSRQ")) == nav_date
            and period_number(payload.get("DWJZ")) is not None
            and period_number(payload["DWJZ"]) > 0
            and period_number(payload["DWJZ"]) == record.get("latest_nav")
            and all(source in payload and period_number(payload[source]) == record.get(field)
                    for field, source in zip(RETURN_FIELDS, rank_fields))
        )
    except (TypeError, ValueError, KeyError):
        return False



def verified_superseded_date_proof(proof: Mapping[str, Any], publication_nav_date: Any) -> bool:
    """Bind an excluded fund's own NAV to the original collection age window."""
    fund = snapshot_date(proof.get("nav_date"))
    eligible = snapshot_date(proof.get("eligibility_date"))
    published = snapshot_date(publication_nav_date)
    limit = proof.get("max_nav_age_days")
    return bool(fund is not None and eligible is not None and published is not None
                and type(limit) is int and limit > 0
                and fund <= published <= eligible and (eligible - fund).days <= limit)


def initialization_placeholder(response: Any, *, estab_date: Any, nav_date: Any) -> bool:
    """Recognize the provider's reset-to-inception sentinel, not a real zero return.

    Only a successful, complete ten-period response with nine explicit empty
    values and LN=0 at a corroborated inception date older than NAV qualifies.
    Other date conflicts and transport/business failures remain blocking.
    """
    # Initialize the API package before its quality module: api.__init__ also
    # loads endpoints, which import quality inspectors during initialization.
    # This keeps standalone proof verification independent of caller imports.
    from api.client import APIFailureResult
    from utils.tiantian_data_quality import inspect_tiantian_period_increase_payload

    if isinstance(response, APIFailureResult):
        return False

    try:
        data, quality = inspect_tiantian_period_increase_payload(response)
        inception, nav = snapshot_date(estab_date), snapshot_date(nav_date)
        expansion = response.get("Expansion") or {}
        titles = ("Z", "Y", "3Y", "6Y", "1N", "2N", "3N", "5N", "JN", "LN")
        if (response.get("Success") is not True or not quality.accepted or not quality.has_data or inception is None or nav is None
                or not inception < nav or expansion.get("ISUPDATING") is not False
                or snapshot_date(expansion.get("TIME")) != inception
                or snapshot_date(expansion.get("ESTABDATE")) != inception
                or not isinstance(data, list) or len(data) != len(titles)
                or any(not isinstance(item, dict) or "syl" not in item for item in data)):
            return False
        values = {item.get("title"): period_number(item["syl"]) for item in data}
        return (set(values) == set(titles) and values["LN"] == 0
                and all(values[title] is None for title in titles[:-1]))
    except (AttributeError, KeyError, TypeError, ValueError):
        return False


def verified_initialization_quarantine(row: Mapping[str, Any]) -> bool:
    """A durable rejection masks this fund while leaving accepted facts intact."""
    try:
        raw, digest = (row.get(field) for field in RETURN_REJECTION_FIELDS)
        if not isinstance(raw, str) or hashlib.sha256(raw.encode()).hexdigest() != digest:
            return False
        proof = json.loads(raw)
        observed = local_observation_time(proof["observed_at"])
        return bool(
            proof["contract"] == "period-initialization-rejection-v1"
            and proof["fcode"] == row.get("fcode")
            and has_dated_period_snapshot(row, verify_payload=True)
            and observed is not None and observed <= datetime.now()
            and snapshot_date(proof["nav_date"]) == snapshot_date(row.get("latest_nav_date"))
            and proof["nav_value"] == period_number(row.get("latest_nav"))
            and period_number(proof["nav_value"]) is not None and proof["nav_value"] > 0
            and proof["accepted_source_sha256"] == row.get("period_return_source_sha256")
            and snapshot_date(row.get("period_return_date")) == snapshot_date(row.get("latest_nav_date"))
            and initialization_placeholder(proof["response"], estab_date=proof["estab_date"],
                                           nav_date=proof["nav_date"])
        )
    except (KeyError, TypeError, ValueError):
        return False


def select_return_update(
    existing: Mapping[str, Any] | None, incoming: Mapping[str, Any],
) -> dict[str, Any]:
    """Choose NAV and period snapshots separately; never merge period fields.

    Undated legacy mappings may update NAV, but cannot overwrite or certify
    period returns. Partial dated mappings fail before any database mutation.
    """
    previous = existing or {}
    result = {key: incoming[key] for key in ("fcode", "last_updated_at") if key in incoming}
    nav_date = snapshot_date(incoming.get("latest_nav_date"))
    previous_nav_date = snapshot_date(previous.get("latest_nav_date"))
    if nav_date is not None and (previous_nav_date is None or nav_date >= previous_nav_date):
        # A full NAV value/date observation supersedes its previous daily
        # change too. An absent daily change belongs to this new snapshot.
        if incoming.get("latest_nav") is not None:
            result.update({key: incoming.get(key) for key in NAV_FIELDS})
            result["latest_nav_date"] = nav_date

    if any(key in incoming for key in RETURN_PROVENANCE_FIELDS):
        if not has_dated_period_snapshot(incoming, verify_payload=True):
            raise ValueError("period_return_snapshot_provenance_invalid")
        if any(field not in incoming for field in RETURN_FIELDS):
            raise ValueError("period_return_snapshot_partial_mapping")
        current_date = snapshot_date(incoming["period_return_date"])
        previous_date = snapshot_date(previous.get("period_return_date"))
        accept = previous_date is None or current_date > previous_date
        if current_date == previous_date:
            old_time = local_observation_time(previous.get("period_return_observed_at"))
            new_time = local_observation_time(incoming["period_return_observed_at"])
            accept = old_time is None or new_time >= old_time
        if accept:
            result.update({field: incoming[field] for field in RETURN_SNAPSHOT_FIELDS})
            # Only a newly accepted dated snapshot retires a rejection. An
            # unrelated NAV write or failed retry cannot silently clear it.
            if any(previous.get(field) for field in RETURN_REJECTION_FIELDS):
                result.update({field: None for field in RETURN_REJECTION_FIELDS})
    return result


def attach_period_return_status(frame, *, mask_unknown: bool = False):
    """Annotate a database projection; raw unknown values remain auditable."""
    if frame.empty:
        frame["period_return_status"] = "unknown"
        return frame
    dated = frame.apply(lambda row: has_dated_period_snapshot(row), axis=1)
    frame["period_return_status"] = dated.map({True: "dated", False: "unknown"})
    misaligned = frame.apply(lambda row: (
        snapshot_date(row.get("latest_nav_date")) is not None
        and snapshot_date(row.get("period_return_date")) != snapshot_date(row.get("latest_nav_date"))
    ), axis=1) & dated
    frame.loc[misaligned, "period_return_status"] = "date_mismatch"
    rejected = frame.apply(lambda row: any(
        isinstance(row.get(field), str) and bool(row.get(field)) for field in RETURN_REJECTION_FIELDS
    ), axis=1)
    frame.loc[rejected, "period_return_status"] = "upstream_response_quarantined"
    if mask_unknown:
        for field in RETURN_FIELDS:
            if field in frame:
                frame.loc[~dated | misaligned | rejected, field] = None
    return frame


def period_collection_blocking_count(summary: Mapping[str, Any]) -> int:
    """Keep data unavailability distinct from a failure that blocks screening.

    Legacy summaries have no evidence for an exemption. The only new
    exemptions require explicit missing data in an empty row, or a dated
    response that exactly reproduces a stored, lagging snapshot. A separately
    verified current rank snapshot can also survive a strictly older refresh;
    all per-fund publication gates still apply to the retained snapshot.
    """
    failed, cooldown = summary["failed_count"], summary["cooldown_count"]
    if any(type(value) is not int or value < 0 for value in (failed, cooldown)):
        raise ValueError("period_return_failure_counts_invalid")
    if summary.get("schema_version") == 1:
        return failed + cooldown
    if summary.get("schema_version") != 2:
        raise ValueError("period_return_summary_version_invalid")
    outcomes, cooling = summary["outcomes"], summary["cooldown_outcomes"]
    if not isinstance(outcomes, list) or not isinstance(cooling, list) or len(cooling) != cooldown:
        raise ValueError("period_return_cooldown_outcomes_invalid")
    failures = [item for item in outcomes if item["status"] != "ok"]
    if len(failures) != failed:
        raise ValueError("period_return_failed_outcomes_invalid")
    all_codes = [item["fcode"] for item in [*outcomes, *cooling]]
    if len(set(all_codes)) != len(all_codes):
        raise ValueError("period_return_outcomes_duplicated")
    excluded = []
    lagging = []
    superseded = []
    placeholders = []
    for item in [*failures, *cooling]:
        if item.get("unavailable_without_returns") is True:
            if item["status"] != "missing":
                raise ValueError("period_return_nonmissing_failure_cannot_be_exempt")
            excluded.append(item["fcode"])
        if item.get("quarantined_date_lag") is True:
            if item["status"] != "date_mismatch_quarantined":
                raise ValueError("period_return_unverified_date_lag_cannot_be_exempt")
            lagging.append(item["fcode"])
        if item.get("superseded_refresh_rejected") is True:
            if item["status"] != "superseded_refresh_rejected":
                raise ValueError("period_return_unverified_superseded_refresh_cannot_be_exempt")
            superseded.append(item["fcode"])
        if item.get("initialization_placeholder_quarantined") is True:
            proof = item.get("rejection_sha256")
            if (item["status"] != INITIALIZATION_QUARANTINE or not isinstance(proof, str)
                    or len(proof) != 64 or any(c not in "0123456789abcdef" for c in proof)):
                raise ValueError("period_return_unverified_initialization_placeholder_cannot_be_exempt")
            placeholders.append(item["fcode"])
    if summary["nonblocking_missing_fcodes"] != sorted(excluded):
        raise ValueError("period_return_nonblocking_codes_mismatch")
    if summary.get("quarantined_date_lag_fcodes", []) != sorted(lagging):
        raise ValueError("period_return_quarantined_date_lag_codes_mismatch")
    if summary.get("superseded_refresh_fcodes", []) != sorted(superseded):
        raise ValueError("period_return_superseded_refresh_codes_mismatch")
    if summary.get("initialization_placeholder_fcodes", []) != sorted(placeholders):
        raise ValueError("period_return_initialization_placeholder_codes_mismatch")
    exemptions = excluded + lagging + superseded + placeholders
    if len(exemptions) != len(set(exemptions)):
        raise ValueError("period_return_exemptions_overlap")
    blocking = failed + cooldown - len(exemptions)
    if "blocking_count" in summary and summary["blocking_count"] != blocking:
        raise ValueError("period_return_blocking_count_mismatch")
    if "screening_blocked" in summary and summary["screening_blocked"] is not (blocking > 0):
        raise ValueError("period_return_screening_block_mismatch")
    return blocking
