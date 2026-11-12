"""Bind report risk rows to the actual captured operands, without a database.

An input journal can belong to an earlier calculation reused by a report. Its
fund, period, timestamp and every projected risk value must match that report;
matching a NAV date or a fund code alone does not establish this provenance.
"""
from __future__ import annotations

from datetime import date, datetime
import hashlib
import json
import math
from pathlib import Path
from typing import Any

import pandas as pd

from backend.artifact_paths import resolve_project_artifact_path
from backend.calculation_input_audit import _ObjectTape, _differences, _field, independent_standard_metrics
from backend.calculation_input_capture import validate_calculation_input_index
from core.utils.calculation_capture import CalculationCaptureError, _json, decode, validate_journal


RISK_FIELDS = (
    "period", "start_date", "end_date", "calculated_at", "nav_snapshot_signature", "data_points",
    "volatility", "max_drawdown", "max_drawdown_duration", "annual_return",
    "sharpe_ratio", "sortino_ratio", "raw_sharpe_ratio", "raw_sortino_ratio",
    "sharpe_ratio_cap_hit", "sortino_ratio_cap_hit", "calmar_ratio", "var_95", "var_99", "positive_ratio",
)
CONTRACT = "frozen-report-sortino-operands-2026-10-28.2"


def _sha(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def _wire_field(value: dict, field: str) -> Any:
    matches = [item for key, item in value.get("items", []) if key == field]
    if len(matches) != 1:
        raise CalculationCaptureError(f"risk_capture_field_missing_or_ambiguous:{field}")
    return matches[0]


def _cell(field: str, value: Any) -> Any:
    if value is None or value is pd.NA or value is pd.NaT:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if field in {"start_date", "end_date", "calculated_at"}:
        return pd.Timestamp(value).isoformat()
    if hasattr(value, "item"):
        value = value.item()
    return value


def _record_values(record: dict) -> dict:
    return {field: _cell(field, record.get(field)) for field in RISK_FIELDS}


def _json_record(record: dict) -> dict:
    return {key: value.isoformat() if isinstance(value, (datetime, date)) else value
            for key, value in record.items()}


def _source_files(index_path: Path, index: dict, project_root: Path) -> dict[str, str]:
    """Bind outer child metadata and the real logs as well as the journal index."""
    files = {str(index_path.relative_to(project_root)): _sha(index_path)}
    for session in index["sessions"]:
        if session["kind"] != "database_risk":
            continue
        binding = session["subprocess"]
        path = resolve_project_artifact_path(binding["meta_path"], project_root=project_root)
        metadata = json.loads(path.read_text())
        if (_sha(path) != binding["meta_sha256"] or metadata != binding["metadata"]
                or metadata.get("success") is not True or metadata.get("return_code") != 0
                or metadata.get("timed_out") is not False):
            raise CalculationCaptureError("risk_source_child_not_verified_success")
        files[str(path.relative_to(project_root))] = _sha(path)
        for key in ("stdout_path", "stderr_path"):
            log = resolve_project_artifact_path(metadata[key], project_root=project_root)
            if not log.is_file():
                raise CalculationCaptureError("risk_source_child_log_missing")
            files[str(log.relative_to(project_root))] = _sha(log)
    if index.get("capture_scope") == "collection":
        from types import SimpleNamespace
        from backend.collection_calculation_capture import validate_collection_capture_reference

        terminal_path = index_path.parent.parent / "terminal.json"
        terminal = json.loads(terminal_path.read_text())
        reference = {**terminal, "terminal_path": str(terminal_path.relative_to(project_root)),
                     "terminal_sha256": _sha(terminal_path)}
        settings = SimpleNamespace(project_root=project_root, reports_dir=project_root / "reports")
        validate_collection_capture_reference(settings, reference)
        if terminal.get("pipeline_outcome") != "succeeded":
            raise CalculationCaptureError("risk_source_collection_not_successful")
        for key in ("execution_path", "terminal_path"):
            path = resolve_project_artifact_path(reference[key], project_root=project_root)
            files[str(path.relative_to(project_root))] = _sha(path)
    return files


def _extract_session(directory: Path, session: dict, *, run_id: str, scratch_dir: Path, log,
                     expected_code_sha256: dict | None = None) -> list[dict]:
    tape = _ObjectTape(scratch_dir)
    pending, periods, observations = {}, {}, []

    def observe(row):
        kind = row["record"]
        if kind == "object":
            tape.put(row)
        elif kind == "call":
            parent = pending.get(row.get("parent_id"), {})
            item = {"kind": row["kind"], "parent_id": row.get("parent_id"), "call_id": row["call_id"],
                    "fcode": _field(row["arguments"], "fcode") or _field(row["tags"], "fcode") or parent.get("fcode"),
                    "period": _field(row["arguments"], "period") or parent.get("period")}
            if row["kind"] in {"risk.standard_metrics", "database_risk.period", "database_risk.write_record"}:
                item.update(arguments=row["arguments"], context=row["context"])
            pending[row["call_id"]] = item
        elif kind in {"return", "exception"}:
            item = pending.pop(row["call_id"])
            if kind == "exception" or item.get("period") != "1y":
                return
            call_kind = item["kind"]
            if call_kind == "risk.standard_metrics":
                arguments = decode(item["arguments"], tape.get)
                expected = decode(row["value"], tape.get)
                actual = independent_standard_metrics(**arguments)
                fields = ("volatility", "raw_sortino_ratio", "sortino_ratio", "sortino_ratio_cap_hit")
                if _differences({key: expected[key] for key in fields}, {key: actual[key] for key in fields}):
                    raise CalculationCaptureError("captured_sortino_policy_not_reproduced")
                values = list(map(float, arguments["returns"]))
                frequency, mar = float(arguments["periods_per_year"]), float(arguments["mar"])
                annual_return = float(arguments["annual_return"])
                if (not values or frequency <= 0 or not all(math.isfinite(v) for v in
                        [*values, frequency, mar, annual_return])):
                    raise CalculationCaptureError("captured_sortino_operands_invalid")
                moment = math.fsum(min(v - mar / frequency, 0.0) ** 2 for v in values) / len(values)
                parent = pending.get(item["parent_id"])
                if parent is None or parent["kind"] != "database_risk.period" or "standard" in parent:
                    raise CalculationCaptureError("sortino_requires_one_parent_period_call")
                parent["standard"] = {"standard_call_id": item["call_id"],
                    "standard_input_sha256": hashlib.sha256(_json(item["arguments"])).hexdigest(),
                    "observation_count": len(values), "periods_per_year": frequency, "annual_mar": mar,
                    "annual_return": annual_return, "downside_second_moment": moment,
                    "zero_downside": moment == 0.0, "undefined_downside": moment < 1e-20,
                    "production_fallback_triggered": False,
                    "positive_excess_return": annual_return - mar > 0.0,
                    "recorded_metrics": {key: expected[key] for key in fields}}
            elif call_kind == "database_risk.period":
                metrics = decode(row["value"], tape.get)
                if metrics is None:
                    return
                standard = item.get("standard")
                if (not standard or not item.get("fcode")
                        or metrics.get("data_points") != standard["observation_count"]
                        or _differences(standard["recorded_metrics"],
                                        {key: metrics.get(key) for key in standard["recorded_metrics"]})
                        or not math.isclose(metrics["annual_return"] / 100, standard["annual_return"],
                                            rel_tol=1e-12, abs_tol=1e-14)):
                    raise CalculationCaptureError("risk_period_standard_operands_disagree")
                digest = hashlib.sha256(_json(row["value"])).hexdigest()
                key = (item["fcode"], item["period"], digest)
                if key in periods:
                    raise CalculationCaptureError("ambiguous_risk_period_write_source")
                periods[key] = {key: value for key, value in standard.items() if key != "recorded_metrics"}
                periods[key]["period_call_id"] = item["call_id"]
            elif call_kind == "database_risk.write_record":
                metric_wire = _wire_field(item["arguments"], "metrics")
                key = (item["fcode"], item["period"], hashlib.sha256(_json(metric_wire)).hexdigest())
                if key not in periods:
                    raise CalculationCaptureError("risk_write_has_no_exact_period_source")
                record = decode(row["value"], tape.get)
                if record.get("fcode") != item["fcode"] or record.get("period") != "1y":
                    raise CalculationCaptureError("risk_write_identity_disagrees")
                calculated_at = decode(_wire_field(item["arguments"], "calculated_at"), tape.get)
                if record.get("calculated_at") != calculated_at:
                    raise CalculationCaptureError("risk_write_timestamp_disagrees")
                observations.append({**periods[key], "fcode": item["fcode"], "write_call_id": item["call_id"],
                                     "session_id": session["session_id"], "risk_record": _json_record(record)})
                if len(observations) % 1000 == 0:
                    log(f"frozen risk inputs: {run_id}: {len(observations)} annual write records bound")
    try:
        verified = validate_journal(directory / session["manifest"], expected_run_id=run_id, on_record=observe)
        if verified["manifest_sha256"] != session["manifest_sha256"] or verified["manifest"] != {
                key: session[key] for key in verified["manifest"]}:
            raise CalculationCaptureError("risk_journal_index_changed")
        if expected_code_sha256 is not None and verified["header"]["code_sha256"] != expected_code_sha256:
            raise CalculationCaptureError("risk_journal_source_index_disagrees")
        return observations
    finally:
        tape.close()


def read_frozen_risk_observations(
    index_paths: list[Path], *, project_root: Path, scratch_dir: Path, log=print,
) -> dict:
    """Read only named immutable journals; never search a database for substitutes."""
    root = Path(project_root).resolve()
    sources, observations, seen = [], [], set()
    for raw in index_paths:
        path = resolve_project_artifact_path(raw, project_root=root)
        if not path.is_relative_to(root / "reports"):
            raise CalculationCaptureError("risk_input_index_outside_reports")
        if path in seen:
            continue
        seen.add(path)
        index = validate_calculation_input_index(path)
        files = _source_files(path, index, root)
        digest = _sha(path)
        start = len(observations)
        for session in index["sessions"]:
            if session["kind"] != "database_risk":
                continue
            for observation in _extract_session(path.parent, session, run_id=index["run_id"],
                                                scratch_dir=scratch_dir, log=log,
                                                expected_code_sha256=index["code_sha256"]):
                observations.append({**observation, "source_index_sha256": digest})
        sources.append({"path": str(path.relative_to(root)), "sha256": digest,
                        "run_id": index["run_id"], "capture_scope": index.get("capture_scope", "workflow"),
                        "annual_write_records": len(observations) - start, "files": files,
                        "source_code_sha256": index["code_sha256"],
                        "parent_workflow_success_implied": False})
    evidence = {"contract": CONTRACT, "sources": sources, "observations": observations}
    verify_risk_observation_sources(evidence, project_root=root)
    return evidence


def verify_risk_observation_sources(evidence: dict, *, project_root: Path) -> None:
    for source in evidence["sources"]:
        for relative, digest in source["files"].items():
            path = resolve_project_artifact_path(relative, project_root=project_root)
            if _sha(path) != digest:
                raise CalculationCaptureError("frozen_risk_source_changed")
        index = resolve_project_artifact_path(source["path"], project_root=project_root)
        validate_calculation_input_index(index)


def bind_report_risk_observations(universe: pd.DataFrame, evidence: dict, *, generated_at: str) -> dict:
    """An unmatched risk row blocks complete-universe formula comparisons."""
    required = {"fcode", *RISK_FIELDS}
    if not required.issubset(universe.columns):
        return {"status": "missing_frozen_nav_window", "complete": False, "rows": [],
                "missing_risk_columns": sorted(required - set(universe.columns)),
                "missing_risk_input_codes": [], "source_risk_row_mismatch_codes": []}
    if universe["fcode"].duplicated().any() or universe["fcode"].isna().any():
        raise CalculationCaptureError("report_risk_fund_identity_not_unique")
    by_code = {}
    for item in evidence["observations"]:
        by_code.setdefault(str(item["fcode"]), []).append(item)
    matched, missing, mismatched, not_applicable, rows = [], [], [], [], []
    for row in universe.to_dict("records"):
        code = str(row["fcode"])
        expected = _record_values(row)
        if all(expected[key] is None for key in RISK_FIELDS
               if key not in {"sharpe_ratio_cap_hit", "sortino_ratio_cap_hit"}):
            not_applicable.append(code)
            continue
        candidates = by_code.get(code, [])
        exact = [item for item in candidates if set(RISK_FIELDS).issubset(item["risk_record"])
                 and _record_values(item["risk_record"]) == expected]
        if not exact:
            (mismatched if candidates else missing).append(code)
            fields = sorted({key for item in candidates for key, value in _record_values(item["risk_record"]).items()
                             if key not in item["risk_record"] or value != expected[key]})
            rows.append({"fcode": code, "status": "risk_row_mismatch" if candidates else "missing_captured_risk_row",
                         "mismatched_fields": ",".join(fields)})
            continue
        if len({item["standard_input_sha256"] for item in exact}) != 1:
            raise CalculationCaptureError("report_risk_operands_ambiguous")
        original = exact[0]
        written, published = pd.Timestamp(expected["calculated_at"]), pd.Timestamp(generated_at)
        if pd.isna(written) or pd.isna(published) or written > published:
            raise CalculationCaptureError("risk_row_not_observed_before_report")
        matched.append(original)
        rows.append({key: value for key, value in original.items() if key != "risk_record"} | {
            "status": "matched", "calculated_at": expected["calculated_at"],
            "start_date": expected["start_date"], "end_date": expected["end_date"],
            "stored_raw_sortino_ratio": expected["raw_sortino_ratio"], "mismatched_fields": ""})
    complete = not missing and not mismatched
    return {"contract": CONTRACT, "status": "matched" if complete else "missing_frozen_nav_window",
            "complete": complete, "universe_count": len(universe), "matched_risk_rows": len(matched),
            "report_rows_without_risk": len(not_applicable), "missing_risk_input_codes": sorted(missing),
            "source_risk_row_mismatch_codes": sorted(mismatched), "compared_fields": list(RISK_FIELDS),
            "zero_downside_count": sum(item["zero_downside"] for item in matched),
            "production_fallback_count": sum(item["production_fallback_triggered"] for item in matched),
            "annual_mar_values": sorted({item["annual_mar"] for item in matched}),
            "zero_downside_codes": sorted(item["fcode"] for item in matched if item.get("undefined_downside", item["zero_downside"])),
            "positive_zero_downside_codes": sorted(item["fcode"] for item in matched
                if item.get("undefined_downside", item["zero_downside"]) and item["positive_excess_return"]), "rows": rows}
