"""Separate safe publication completion from explicitly verified data exclusions."""
from __future__ import annotations

from contextlib import closing
import csv
from pathlib import Path
import re
import sqlite3

from backend.github_reports import ReportExportError, task_outcome, verify_bundle
from core.utils.period_return_snapshot import (
    RETURN_FIELDS, RETURN_PROVENANCE_FIELDS, RETURN_REJECTION_FIELDS,
    period_collection_blocking_count, verified_initialization_quarantine, verified_current_rank_snapshot, verified_superseded_date_proof,
)


def hosted_task_outcome(result: dict, return_code: int, before: dict, after: dict,
                        bundle_dir: Path, database: Path) -> tuple[str, dict | None]:
    """Unknown degradation still fails. No scheduler status or gate is changed.

    The sole exception is a completed, committed publication with a
    verified initializer quarantine, genuinely empty excluded returns, or a
    reprojected current rank snapshot retained after an older refresh, also excluded.
    It requires fresh DB rejection proof, reconciled collection counts and
    zero overlap with the SHA-verified formal CSV. An unchanged snapshot also
    requires exact ready-signature and completed-maintenance evidence. Busy,
    generic API errors and publication blocks cannot use this exception.
    """
    try:
        return task_outcome(result, return_code, before, after), None
    except ReportExportError:
        if result.get("status") != "degraded":
            raise
    reason = result.get("warning_reason")
    if (return_code != 0 or result.get("error") or result.get("task_terminal") is not True
            or result.get("background_continues") is not False
            or reason not in {"period_return_initialization_placeholder_quarantined",
                              "period_return_unavailable_without_returns", "period_return_refresh_superseded"}
            or result.get("degraded_stage") != "collect-period-returns"):
        raise ReportExportError("degraded_completion_not_verified")
    for key, value in result.items():
        if ((key.endswith("_error") or key in {"records_failed", "collection_step_failures"}) and value):
            raise ReportExportError("additional_task_failure")
    publication = result.get("publication_result") or {}
    governance = publication.get("publish_governance_summary") or {}
    changed = before != after
    if changed and (not result.get("screening_triggered") or result.get("screening_run_id") != after.get("run_id")):
        raise ReportExportError("warning_new_publication_not_bound")
    if changed and (publication.get("status") != "published" or publication.get("run_id") != after.get("run_id")
            or publication.get("current_published_run_id") != after.get("run_id")
            or publication.get("publishable") is not True or publication.get("publish_hold") is not False
            or publication.get("legacy_publish_hold") is not False
            or governance.get("publishable") is not True or governance.get("risk_publish_ready") is not True
            or governance.get("publish_blocked") is not False or governance.get("blocking_total") != 0
            or any(x.get("blocking") or x.get("severity") == "blocking" for x in publication.get("publish_blockers", []))):
        raise ReportExportError("warning_publication_gate_not_verified")
    if not changed:
        current = result.get("after_effective_nav_snapshot") or {}
        previous = result.get("latest_screened_snapshot") or {}
        if (result.get("screening_triggered") is not False or result.get("report_refresh_triggered") is not False
                or result.get("collection_skipped") is not False
                or result.get("skip_reason") != "effective_nav_snapshot_already_screened"
                or current.get("ready") is not True or previous.get("ready") is not True
                or not current.get("signature") or current.get("signature") != previous.get("signature")
                or not current.get("candidate_date") or current.get("candidate_date") != previous.get("candidate_date")
                or (result.get("maintenance_data_change_summary") or {}).get("changed") is not False
                or (result.get("lineage_rescreen_requirement") or {}).get("required") is not False
                or publication.get("status") in {"unknown", "blocked"}):
            raise ReportExportError("warning_unchanged_snapshot_not_verified")
    summary = result.get("period_return_collection") or {}
    try:
        if summary.get("schema_version") != 2 or period_collection_blocking_count(summary) != 0:
            raise ValueError("not_nonblocking")
        if summary.get("screening_blocked") is not False or summary.get("blocking_count") != 0:
            raise ValueError("missing_explicit_nonblocking_evidence")
        if summary.get("quarantined_date_lag_fcodes"):
            raise ValueError("unsupported_warning")
        superseded = summary.get("superseded_refresh_fcodes", [])
        expected_reason = ("period_return_refresh_superseded" if superseded else
                           "period_return_initialization_placeholder_quarantined" if summary.get("initialization_placeholder_fcodes")
                           else "period_return_unavailable_without_returns")
        if reason != expected_reason:
            raise ValueError("warning_reason_exclusion_mismatch")
        failed, cooldown = summary["failed_count"], summary["cooldown_count"]
        if (summary["target_count"] != summary["saved_fund_count"] + failed
                or len(summary["outcomes"]) != summary["target_count"]
                or type(summary.get("unselected_count")) is not int or summary["unselected_count"] < 0
                or summary["pending_count"] != failed + cooldown + summary["unselected_count"]):
            raise ValueError("unbalanced_counts")
        codes = sorted(summary["initialization_placeholder_fcodes"] + summary["nonblocking_missing_fcodes"] + superseded)
        if not codes or any(not re.fullmatch(r"[0-9]{6}", code) for code in codes):
            raise ValueError("invalid_codes")
    except (KeyError, TypeError, ValueError) as exc:
        raise ReportExportError("nonblocking_collection_evidence_invalid") from exc
    bundle = verify_bundle(bundle_dir)
    if any(bundle["publication"].get(k) != after.get(k) for k in ("run_id", "revision", "generation")):
        raise ReportExportError("warning_bundle_identity_mismatch")
    if not changed and bundle["publication"].get("nav_date") != result["after_effective_nav_snapshot"]["candidate_date"]:
        raise ReportExportError("warning_unchanged_nav_date_mismatch")
    with (bundle_dir / "formal.csv").open(encoding="utf-8-sig", newline="") as stream:
        formal = list(csv.DictReader(stream))
    formal_codes = [row.get("基金代码", "") for row in formal]
    if (len(formal) != bundle["publication"]["counts"]["final_count"]
            or len(set(formal_codes)) != len(formal_codes) or set(codes) & set(formal_codes)):
        raise ReportExportError("quarantined_fund_in_formal_report")
    proofs = []
    missing_proofs = []
    superseded_proofs = []
    with closing(sqlite3.connect(database.resolve().as_uri() + "?mode=ro", uri=True)) as conn:
        conn.row_factory = sqlite3.Row
        for code in codes:
            record = conn.execute("SELECT * FROM fund_returns WHERE fcode=?", (code,)).fetchone()
            row = dict(record) if record else {}
            if code in summary["initialization_placeholder_fcodes"]:
                if not verified_initialization_quarantine(row):
                    raise ReportExportError("quarantine_database_proof_invalid")
                observed = next(x for x in [*summary["outcomes"], *summary["cooldown_outcomes"]] if x["fcode"] == code)
                if observed.get("rejection_sha256") != row.get("period_return_rejection_sha256"):
                    raise ReportExportError("quarantine_rejection_binding_mismatch")
                proofs.append({"fcode": code, "rejection_sha256": row["period_return_rejection_sha256"]})
            elif code in superseded:
                date_proof = {"nav_date": str(row.get("latest_nav_date") or "")[:10],
                              "eligibility_date": str(summary.get("started_at") or "")[:10],
                              "max_nav_age_days": summary.get("max_nav_age_days")}
                if (row.get("period_return_last_status") != "superseded_refresh_rejected"
                        or any(row.get(field) is not None for field in RETURN_REJECTION_FIELDS)
                        or not verified_superseded_date_proof(date_proof, bundle["publication"].get("nav_date"))
                        or not verified_current_rank_snapshot(row)):
                    raise ReportExportError("superseded_snapshot_exclusion_not_verified")
                superseded_proofs.append({"fcode": code, "status": "superseded_refresh_rejected",
                    "returns_and_nav": "reprojected_current_rank", **date_proof,
                    "accepted_source_sha256": row["period_return_source_sha256"]})
            else:
                if (not row or row.get("period_return_last_status") != "missing"
                        or any(row.get(field) is not None for field in
                               (*RETURN_FIELDS, *RETURN_PROVENANCE_FIELDS, *RETURN_REJECTION_FIELDS))):
                    raise ReportExportError("missing_returns_exclusion_not_verified")
                missing_proofs.append({"fcode": code, "status": "missing",
                                       "returns_and_provenance": "all_null"})
    return "published_with_warnings" if changed else "unchanged_with_warnings", {
        "contract": "github-safe-publication-completion-v2" if superseded_proofs else "github-safe-publication-completion-v1", "status": "verified",
        "blocking_count": 0, "excluded_fcodes": codes, "rejection_proofs": proofs,
        "missing_returns_proofs": missing_proofs,
        **({"superseded_refresh_proofs": superseded_proofs} if superseded_proofs else {}),
        "formal_overlap_count": 0, "formal_csv_sha256": next(x["sha256"] for x in bundle["files"] if x["path"] == "formal.csv"),
        "scheduler_status_preserved": "degraded", "publication_run_id": after["run_id"],
    }
