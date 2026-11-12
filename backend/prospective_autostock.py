"""Capture actual report-time observations for future strategy experiments.

Publication is already selected when this runs. Counterfactual candidates are
request targets, never a new published list. Missing responses remain missing.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd

from backend.report_autostock_cache import comparison_input
from backend.report_autostock_evidence import COMPARE_COLUMNS, STATUS_COLUMNS, content_sha256

CONTRACT = "prospective-autostock-observations-2026-09-15.1"
MAX_BATCHES = 40
MAX_BATCH_CODES = 50
COMPARED = {"strict_match", "soft_pass_with_warnings", "hard_block"}
RISK_VARIANTS = (
    "raw_sharpe_sortino_score", "consistent_capped_sharpe_sortino",
    "standard_calmar_recomputed", "style_floor_calmar", "layer6_adjusted_calmar",
)


def _codes(frame: pd.DataFrame) -> set[str]:
    from backend import report_builder as production

    return set(frame["基金代码"].map(production._normalize_code)) - {""}


def _status_map(filters: dict[str, Any]) -> dict[str, dict[str, str]]:
    from backend import report_builder as production

    result: dict[str, dict[str, str]] = {}
    priority = {"hard_block": 4, "soft_pass_with_warnings": 3, "strict_match": 2,
                "not_checked": 1, "unknown": 0, "": 0}
    for key in ("dynamic_internal_watch_df", "internal_watch_df", "dynamic_official_df", "official_df"):
        for row in filters[key].to_dict(orient="records"):
            code = production._normalize_code(row.get("基金代码"))
            if not code:
                raise ValueError("prospective_status_code_missing")
            current = {column: ("" if pd.isna(row.get(column)) else str(row[column]))
                       for column in STATUS_COLUMNS}
            raw = production._normalize_autostock_status(current[STATUS_COLUMNS[0]])
            current[STATUS_COLUMNS[0]] = raw
            previous = result.get(code)
            if previous is None or priority.get(raw, 0) >= priority.get(previous[STATUS_COLUMNS[0]], 0):
                result[code] = current
    return result


def build_observation_plan(
    settings: Any, *, universe: pd.DataFrame, filters: dict[str, Any], report_inputs: dict[str, Any],
) -> tuple[dict[str, Any], pd.DataFrame]:
    """Use the same preselection as replay and retain a conservative union.

    Local annual and low-volatility release scenarios are subsets of their
    respective all-release population. Publication filters and final selection
    only remove members from that population. No status is invented to run a
    final selection and no later database is consulted.
    """
    from backend import report_builder as production
    from backend.low_vol_overlap_audit import LOW_VOL_STYLE_SCOPES
    from backend.risk_metric_formula_shadow import apply_metric_variant
    from backend.screening_baseline_replay import prepare_selection_candidates, resolve_evaluation_times
    from bond_fund_screening.config import CALMAR_MIN_DD_BY_STYLE, RISK_FREE_RATE

    if not universe.columns.is_unique or universe["基金代码"].map(production._normalize_code).duplicated().any():
        raise ValueError("prospective_universe_not_unique")
    clocks, _ = resolve_evaluation_times({"inputs": {"evaluation_times": filters["evaluation_times"]}})
    parameters = dict(settings=settings, decision_map=report_inputs["decision_map"],
                      soft_warning_map=report_inputs["soft_warning_map"], evaluation_times=clocks)
    baseline, _, _, _ = prepare_selection_candidates(scored_universe=universe, **parameters)

    def admitted(frame: pd.DataFrame) -> set[str]:
        return _codes(frame.loc[frame["dynamic_threshold_verdict"].eq("正式推荐")])

    annual_codes = _codes(baseline.loc[baseline["dynamic_threshold_reason_codes"].eq("below_annual_floor")
                                     & baseline["基础门禁通过"]])
    annual, _, _, _ = prepare_selection_candidates(
        scored_universe=universe, dynamic_release_codes=annual_codes, **parameters)
    families = [{"family": "annual_relief_union", "required_codes": sorted(admitted(annual)),
                 "relief_codes": sorted(annual_codes)}]
    for variant in RISK_VARIANTS:
        changed = apply_metric_variant(universe, variant=variant, risk_free_rate=float(RISK_FREE_RATE),
                                       min_drawdown_by_style=CALMAR_MIN_DD_BY_STYLE)
        candidate, _, _, _ = prepare_selection_candidates(
            scored_universe=production._assign_project_scores(changed), **parameters)
        families.append({"family": variant, "required_codes": sorted(admitted(candidate))})
    styles = {style for _, _, members in LOW_VOL_STYLE_SCOPES for style in members}
    low_vol_codes = _codes(baseline.loc[baseline["基础门禁通过"]
        & baseline["债基风格标签"].isin(styles) & ~baseline["dynamic_threshold_verdict"].eq("正式推荐")])
    low_vol, _, _, _ = prepare_selection_candidates(
        scored_universe=universe, dynamic_release_codes=low_vol_codes, **parameters)
    families.append({"family": "low_vol_dynamic_release_union", "required_codes": sorted(admitted(low_vol)),
                     "relief_codes": sorted(low_vol_codes), "styles": sorted(styles)})
    required = set().union(*(set(family["required_codes"]) for family in families))
    statuses = _status_map(filters)
    production_codes = set(filters["autostock_status_context"]["autostock_track_union"]["target_codes"])
    inherited = {code for code in required if (statuses.get(code) or {}).get(STATUS_COLUMNS[0]) in COMPARED}
    # Preserve the actual production outcome, including a recorded unavailable
    # comparison. A later research query must not replace an original hard block
    # or make a degraded original selection look as if it had been validated.
    pending = required - inherited - production_codes
    ordered: list[str] = []
    for family in families:
        ordered.extend(code for code in family["required_codes"] if code in pending and code not in ordered)
    current = universe.copy()
    current["基金代码"] = current["基金代码"].map(production._normalize_code)
    targets = current.set_index("基金代码", drop=False).loc[ordered, list(COMPARE_COLUMNS)].reset_index(drop=True)
    for column in STATUS_COLUMNS:
        targets[column] = "not_checked" if column == STATUS_COLUMNS[0] else ""
    plan = {
        "families": families, "required_codes": sorted(required), "target_codes": ordered,
        "production_target_codes": sorted(production_codes), "inherited_compared_codes": sorted(inherited),
        "production_unvalidated_codes": sorted((required & production_codes) - inherited),
        "risk_free_rate_assumption": float(RISK_FREE_RATE), "drawdown_floor_assumption": dict(CALMAR_MIN_DD_BY_STYLE),
        "max_batches": MAX_BATCHES, "batch_size": MAX_BATCH_CODES,
        "scope": "annual local subsets; five risk metric variants; low-volatility dynamic release subsets",
        "outside_scope": ["zero_downside_variants_require_original_risk_operands",
                          "holding_gate_removal", "other_score_or_threshold_variants"],
        "final_selection_computed": False,
    }
    return plan, targets


def _signed(payload: dict[str, Any]) -> dict[str, Any]:
    payload["content_sha256"] = content_sha256({key: value for key, value in payload.items() if key != "content_sha256"})
    return payload


def capture_strategy_observations(
    settings: Any, *, run_id: str, revision: str, generated_at: str,
    universe: pd.DataFrame, filters: dict[str, Any], report_inputs: dict[str, Any],
) -> dict[str, Any]:
    """Use remaining shared quota after publication comparisons, one batch at a time."""
    from backend import report_builder as production
    from backend.report_autostock_evidence import comparison_code_hashes

    capture: dict[str, Any] = {
        "schema_version": 1, "contract": CONTRACT, "run_id": run_id, "revision": revision,
        "report_generated_at": generated_at, "started_at": datetime.now().astimezone().isoformat(),
        "phase": "after_production_selection_before_input_snapshot",
        "status": "planning", "plan": None, "records": [], "observed_statuses": {},
        "code_sha256": comparison_code_hashes(settings.project_root),
        "production_results_modified": False, "final_selection_computed": False,
    }
    if (getattr(settings, "autostock_enabled", True) is False
            or getattr(settings, "publication_validation_provider", "autostock") != "autostock"):
        capture.update(status="disabled", reason="autostock_disabled_or_replaced",
                       target_count=None, compared_count=0,
                       finished_at=datetime.now().astimezone().isoformat())
        return _signed(capture)
    try:
        plan, targets = build_observation_plan(settings, universe=universe, filters=filters, report_inputs=report_inputs)
        capture["plan"] = plan
        for start in range(0, min(len(targets), MAX_BATCHES * MAX_BATCH_CODES), MAX_BATCH_CODES):
            context: dict[str, Any] = {}
            batch = targets.iloc[start:start + MAX_BATCH_CODES].copy()
            observed = production._supplement_official_autostock_statuses(
                batch, settings=settings, status_context=context, purpose="strategy_observation")
            capture["records"].extend(context.get("autostock_supplement_records", []))
            for row in observed.to_dict(orient="records"):
                if row[STATUS_COLUMNS[0]] in COMPARED:
                    capture["observed_statuses"][row["基金代码"]] = {column: row[column] for column in STATUS_COLUMNS}
            record = capture["records"][-1]
            meta = record.get("upstream_metadata") or {}
            # A new client is used for each bounded batch; do not reset its
            # failure circuit by repeatedly sending more batches after an
            # entirely unavailable response. Partial rows retain their gap.
            stop_outcomes = {"budget_unavailable", "quota_exceeded", "batch_failed", "empty_response",
                             "no_comparison_details", "missing_compare_columns", "no_valid_codes"}
            if record["outcome"] in stop_outcomes or meta.get("quota_exhausted"):
                break
        missing = sorted(set(plan["target_codes"]) - set(capture["observed_statuses"]))
        capture.update(status="captured" if not missing else "incomplete", missing_target_codes=missing,
                       target_count=len(plan["target_codes"]), compared_count=len(capture["observed_statuses"]))
    except (OSError, ValueError, KeyError, TypeError, RuntimeError) as error:
        capture.update(status="capture_failed", error=f"{type(error).__name__}: {error}",
                       target_count=None if capture["plan"] is None else len(capture["plan"]["target_codes"]),
                       compared_count=len(capture["observed_statuses"]))
    capture["finished_at"] = datetime.now().astimezone().isoformat()
    return _signed(capture)


def capture_summary(capture: dict[str, Any]) -> dict[str, Any]:
    """Small truthful projection; raw observations stay in the frozen snapshot."""
    result = {key: capture.get(key) for key in (
        "contract", "status", "target_count", "compared_count", "started_at", "finished_at", "error",
        "production_results_modified", "final_selection_computed",
    )}
    submitted = [record for record in capture["records"] if record.get("logical_request_submitted_to_client")]
    counts = [(record.get("upstream_metadata") or {}).get("http_attempt_count") for record in submitted]
    result["submitted_batch_count"] = len(submitted)
    result["http_attempt_count"] = sum(counts) if all(type(value) is int and value >= 0 for value in counts) else None
    return result


def _timestamp(value: str) -> datetime:
    timestamp = datetime.fromisoformat(value)
    return timestamp.replace(tzinfo=ZoneInfo("Asia/Shanghai")) if timestamp.tzinfo is None else timestamp


def validate_strategy_observations(
    settings: Any, *, frozen: dict[str, Any], report: dict[str, Any], universe: pd.DataFrame,
    production_statuses: dict[str, dict[str, str]],
) -> tuple[dict[str, Any], dict[str, dict[str, str]]]:
    """Recompute each comparison from its original local row and accepted API row."""
    from backend import report_builder as production
    from backend.report_autostock_evidence import validate_supplement_record
    from backend.screening_replay_snapshot import restore_frame

    capture = frozen["inputs"].get("prospective_autostock_capture")
    if capture is None:
        return {"available": False, "integrity_verified": None, "status": "not_captured"}, {}
    unsigned = {key: value for key, value in capture.items() if key != "content_sha256"}
    if (capture.get("schema_version") != 1 or capture.get("contract") != CONTRACT
            or capture.get("content_sha256") != content_sha256(unsigned)
            or (capture.get("run_id"), capture.get("revision"), capture.get("report_generated_at"))
            != (report["run_id"], report["publication_control"]["revision"], frozen["generated_at"])
            or capture.get("production_results_modified") is not False or capture.get("final_selection_computed") is not False):
        raise ValueError("prospective_capture_binding_invalid")
    start, end = _timestamp(capture["started_at"]), _timestamp(capture["finished_at"])
    if start > end or start < _timestamp(capture["report_generated_at"]):
        raise ValueError("prospective_capture_time_invalid")
    if capture["status"] == "disabled":
        # This is signed evidence that no observations were requested, not
        # available strategy coverage. Replay the recorded decision regardless
        # of the settings in use when the historical report is verified.
        if (capture.get("reason") != "autostock_disabled_or_replaced"
                or "plan" not in capture or capture["plan"] is not None
                or capture.get("records") != [] or capture.get("observed_statuses") != {}
                or "target_count" not in capture or capture["target_count"] is not None
                or type(capture.get("compared_count")) is not int or capture["compared_count"] != 0):
            raise ValueError("prospective_disabled_capture_invalid")
        return {"available": False, "integrity_verified": True, "status": "disabled",
                "reason": capture["reason"], "target_count": None, "compared_count": 0}, {}
    plan = capture["plan"]
    if plan is None:
        if capture["status"] != "capture_failed" or capture["records"] or capture["observed_statuses"]:
            raise ValueError("prospective_capture_missing_plan")
        return {"available": True, "integrity_verified": True, "status": "capture_failed", "compared_count": 0}, {}
    targets = plan["target_codes"]
    required = set().union(*(set(item["required_codes"]) for item in plan["families"]))
    if (len(set(targets)) != len(targets) or not set(targets) <= _codes(universe)
            or sorted(required) != plan["required_codes"] or not set(targets) <= required
            or capture["target_count"] != len(targets)):
        raise ValueError("prospective_capture_targets_invalid")
    protected = set(plan["production_target_codes"]) | {
        code for code, status in production_statuses.items() if status.get(STATUS_COLUMNS[0]) in COMPARED}
    for key in ("formal_after_all_filters", "dynamic_reference"):
        protected.update(_codes(restore_frame(frozen["inputs"]["frames"][key])))
    if set(targets) & protected:
        raise ValueError("prospective_capture_overwrites_production")
    local_rows = universe.assign(_code=universe["基金代码"].map(production._normalize_code)).set_index("_code")
    _, build_statuses, compare = production._load_autostock_runtime_for_report(settings)
    observed: dict[str, dict[str, str]] = {}
    seen: set[str] = set()
    normalize_args = dict(normalize_code=production._normalize_code, normalize_date=production._normalize_nav_date_cache_key)
    if len(capture["records"]) > MAX_BATCHES:
        raise ValueError("prospective_capture_batch_limit_exceeded")
    for record in capture["records"]:
        validate_supplement_record(record)
        if (record.get("purpose") != "strategy_observation" or record["cache_reuses"]
                or not start <= _timestamp(record["started_at"]) <= _timestamp(record["finished_at"]) <= end):
            raise ValueError("prospective_capture_record_scope_invalid")
        requested = record["requested_codes"]
        if (len(requested) > MAX_BATCH_CODES or len(set(requested)) != len(requested)
                or not set(requested) <= set(targets) or seen & set(requested)):
            raise ValueError("prospective_capture_request_codes_invalid")
        seen.update(requested)
        original_frame = restore_frame(record["input_frame"])
        if original_frame["基金代码"].tolist() != requested:
            raise ValueError("prospective_capture_request_input_mismatch")
        for original in original_frame.to_dict(orient="records"):
            code = original["基金代码"]
            if (comparison_input(original, **normalize_args) != comparison_input(local_rows.loc[code], **normalize_args)
                    or original[STATUS_COLUMNS[0]] != "not_checked"):
                raise ValueError("prospective_capture_record_input_mismatch")
        if record["comparisons"] and record["logical_request_submitted_to_client"] is not True:
            raise ValueError("prospective_capture_request_not_submitted")
        raw = {content_sha256(row): row for row in record.get("upstream_records") or []}
        details, failed = [], set()
        comparison_codes: set[str] = set()
        for comparison in record["comparisons"]:
            code = comparison["code"]
            expected_local = comparison_input(local_rows.loc[code], **normalize_args)
            original_local = comparison_input(comparison["local_row"], **normalize_args)
            if expected_local is None or original_local != expected_local or code in comparison_codes:
                raise ValueError("prospective_capture_local_input_mismatch")
            comparison_codes.add(code)
            upstream = raw[comparison["upstream_record_sha256"]]
            upstream_code = production._normalize_code(upstream.get("FCODE") or upstream.get("fcode") or upstream.get("code") or upstream.get("基金代码"))
            if upstream_code != code:
                raise ValueError("prospective_capture_upstream_code_mismatch")
            recomputed = compare(pd.Series(comparison["local_row"]), upstream, record["configuration"])
            if content_sha256(recomputed) != content_sha256(comparison["result"]):
                raise ValueError("prospective_capture_comparison_mismatch")
            detail = recomputed["details"]
            details.append(detail)
            if detail.get("失败原因"):
                failed.add(code)
        statuses = build_statuses(pd.DataFrame(details), failed_codes=failed,
            normalize_fcode=production._normalize_code,
            warning_text_builder=lambda reasons: "；".join(str(item).strip() for item in reasons if str(item).strip())) if details else {}
        if content_sha256(statuses) != content_sha256(record["status_map"]):
            raise ValueError("prospective_capture_status_map_mismatch")
        outputs = {row["基金代码"]: row for row in record["output_statuses"]}
        for code, status in statuses.items():
            row = outputs[code]
            raw_status = status.get("autostock_validation_status") or ""
            warnings = str(status.get("autostock_warning_display") or "")
            note = "；".join([str(reason) for reason in status.get("autostock_hard_reasons", []) if str(reason).strip()] + ([warnings] if warnings else []))
            expected = dict(zip(STATUS_COLUMNS, (raw_status, warnings, production._format_autostock_display_status(raw_status), note)))
            if {column: row[column] for column in STATUS_COLUMNS} != expected:
                raise ValueError("prospective_capture_output_mismatch")
            if raw_status in COMPARED:
                observed[code] = expected
    missing = sorted(set(targets) - set(observed))
    if observed != capture["observed_statuses"] or capture["compared_count"] != len(observed):
        raise ValueError("prospective_capture_observation_count_mismatch")
    if capture["status"] != "capture_failed" and (
            capture["missing_target_codes"] != missing or capture["status"] != ("captured" if not missing else "incomplete")):
        raise ValueError("prospective_capture_completeness_mismatch")
    return {"available": True, "integrity_verified": True, "status": capture["status"],
            "target_count": len(targets), "compared_count": len(observed), "missing_count": len(missing),
            "families": [item["family"] for item in plan["families"]], "outside_scope": plan["outside_scope"]}, observed
