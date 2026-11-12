"""Read offline low-volatility experiments without changing frozen reports."""
from __future__ import annotations

import ast
import csv
from decimal import Decimal, DecimalException
import gzip
import hashlib
import io
import json
from pathlib import Path
import re
from typing import Any


AUDIT_NAME = "low_vol_annual_positive_ratio_overlap"
AUDIT_CONTRACT = "low-vol-overlap-sidecar-2026-09-12.1"
AUDIT_CODE_FILES = (
    "scripts/audit_low_vol_annual_positive_ratio_overlap.py",
    "scripts/audit_core_dynamic_overlap.py",
    "backend/low_vol_overlap_audit.py",
    "backend/frozen_strategy_audit.py",
    "backend/holding_period_gate_shadow.py",
    "backend/report_inputs.py",
    "backend/publish_pipeline.py",
)
SOURCE_KEYS = (
    "report_json", "screening_input_snapshot_json_gz", "filtered_csv",
    "watch_csv", "dynamic_filtered_csv", "dynamic_watch_csv",
)
LOW_VOL_STYLE_SCOPES = [
    ("style_mid_short", "中短债", ["中短债"]),
    ("style_composite_pure", "综合纯债", ["综合纯债"]),
    ("style_fixed_income_index", "固收指数", ["固收指数"]),
    ("style_low_vol_focus", "中短债+综合纯债+固收指数", ["中短债", "综合纯债", "固收指数"]),
]

LOW_VOL_RELEASE_SCENARIOS = [
    {
        "scenario_key": "all_dynamic_blocked_in_scope",
        "label": "scope all dynamic blocked",
        "mode": "all",
    },
    {
        "scenario_key": "annual_return_only",
        "label": "annual_return only",
        "required_metrics": ("annual_return",),
        "exact": True,
    },
    {
        "scenario_key": "positive_ratio_only",
        "label": "positive_ratio only",
        "required_metrics": ("positive_ratio",),
        "exact": True,
    },
    {
        "scenario_key": "annual_return_plus_positive_ratio",
        "label": "annual_return + positive_ratio",
        "required_metrics": ("annual_return", "positive_ratio"),
        "exact": True,
    },
    {
        "scenario_key": "annual_return_plus_score",
        "label": "annual_return + score",
        "required_metrics": ("annual_return", "score"),
        "exact": True,
    },
    {
        "scenario_key": "annual_return_plus_sharpe",
        "label": "annual_return + sharpe",
        "required_metrics": ("annual_return", "sharpe"),
        "exact": True,
    },
    {
        "scenario_key": "annual_return_positive_ratio_score",
        "label": "annual_return + positive_ratio + score",
        "required_metrics": ("annual_return", "positive_ratio", "score"),
        "exact": True,
    },
    {
        "scenario_key": "annual_return_positive_ratio_sharpe",
        "label": "annual_return + positive_ratio + sharpe",
        "required_metrics": ("annual_return", "positive_ratio", "sharpe"),
        "exact": True,
    },
    {
        "scenario_key": "contains_annual_return_and_positive_ratio",
        "label": "contains annual_return + positive_ratio",
        "required_metrics": ("annual_return", "positive_ratio"),
        "exact": False,
    },
    {
        "scenario_key": "contains_annual_return_without_positive_ratio",
        "label": "contains annual_return without positive_ratio",
        "required_metrics": ("annual_return",),
        "exclude_metrics": ("positive_ratio",),
        "exact": False,
    },
    {
        "scenario_key": "contains_positive_ratio_without_annual_return",
        "label": "contains positive_ratio without annual_return",
        "required_metrics": ("positive_ratio",),
        "exclude_metrics": ("annual_return",),
        "exact": False,
    },
]

SCOPE_KEYS = tuple(item[0] for item in LOW_VOL_STYLE_SCOPES)
SCENARIO_KEYS = tuple(item["scenario_key"] for item in LOW_VOL_RELEASE_SCENARIOS)
SUMMARY_SCENARIOS = {
    "annual_positive": "contains_annual_return_and_positive_ratio",
    "annual_without_positive": "contains_annual_return_without_positive_ratio",
    "positive_without_annual": "contains_positive_ratio_without_annual_return",
    "annual_only": "annual_return_only",
    "positive_only": "positive_ratio_only",
}
OVERLAP_COUNT_FIELDS = (
    "focus_dynamic_blocked_count", "focus_annual_return_hit_count", "focus_positive_ratio_hit_count",
    *(f"{prefix}_{suffix}" for prefix in SUMMARY_SCENARIOS
      for suffix in ("targeted_count", "direct_final_impact_count")),
)
BASELINE_CHECKS = {
    "complete_scoring_universe", "style_thresholds", "global_thresholds", "style_meta",
    "candidate_decisions_and_reasons", "post_review_members_scores_reasons",
    "formal_members_and_scores", "dynamic_members_and_scores", "all_final_selection_rejection_reasons",
}
MISSING_INPUTS = {
    "missing_tracker_history": "missing_tracker_history_codes",
    "missing_validation_observations": "missing_validation_codes",
    "missing_autostock_observations": "missing_autostock_codes",
    "missing_frozen_incremental_review": "missing_incremental_review_codes",
}
OUTCOME_FIELDS = (
    "formal_count", "direct_final_impact_count", "added_official_count", "removed_official_count",
    "net_final_count_delta", "direct_final_codes", "added_codes", "removed_codes",
)


def _digest(value: Any) -> str:
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return hashlib.sha256(raw.encode()).hexdigest()


def _sha(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def _require(condition: bool, reason: str) -> None:
    if not condition:
        raise ValueError(reason)


def _sha_text(value: Any) -> bool:
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None


def _path(root: Path, value: Any) -> Path:
    _require(isinstance(value, str) and bool(value.strip()), "source_path_missing")
    # Artifacts can be observed inside Docker or through the host workspace.
    relative = value.removeprefix("/app/") if value.startswith("/app/") else value
    path = (root / relative).resolve()
    path.relative_to(root)
    return path


def _code_hashes(root: Path) -> dict[str, str]:
    return {name: _sha(root / name) for name in AUDIT_CODE_FILES}


def build_low_vol_audit_definition(
    settings: Any, *, baseline: dict[str, Any], configuration: dict[str, Any],
    style_scopes: list[Any], scenarios: list[dict[str, Any]],
) -> dict[str, Any]:
    """Record the actual producer, replay code and frozen configuration."""
    root = Path(settings.project_root).resolve()
    replay_code = {name: _sha(_path(root, name)) for name in baseline["replay_code_sha256"]}
    _require(replay_code == baseline["replay_code_sha256"], "replay_code_changed_during_audit")
    definition = {
        "contract": AUDIT_CONTRACT,
        "audit_code_sha256": _code_hashes(root),
        "replay_code_sha256": replay_code,
        "frozen_inputs_sha256": baseline["inputs_sha256"],
        "frozen_configuration": configuration,
        "style_scopes": style_scopes,
        "scenarios": scenarios,
    }
    # JSON normalizes tuples identically before writing and after loading.
    definition = json.loads(json.dumps(definition, ensure_ascii=False, allow_nan=False))
    return {"definition": definition, "sha256": _digest(definition)}


def _codes(value: Any) -> set[str]:
    _require(isinstance(value, list), "scenario_codes_missing")
    _require(all(isinstance(code, str) and re.fullmatch(r"[0-9]{6}", code) for code in value), "scenario_code_invalid")
    _require(len(value) == len(set(value)), "scenario_codes_duplicated")
    return set(value)


def _count(value: Any) -> int:
    _require(type(value) is int and value >= 0, "scenario_count_invalid")
    return value


def _verify_sources(
    root: Path, report: dict[str, Any], audit: dict[str, Any],
) -> tuple[dict[str, Any], set[str]]:
    binding = audit["source_binding"]
    _require(binding["run_id"] == report["run_id"], "audit_run_mismatch")
    revision = report["publication_control"]["revision"]
    _require(binding["revision"] == revision == audit["source_publication_control"]["revision"], "audit_revision_mismatch")
    sources = binding["files"]
    _require(set(sources) == set(SOURCE_KEYS), "frozen_source_set_mismatch")
    report_path = _path(root, report["artifacts"]["report_json"])
    stored = json.loads(report_path.read_bytes())
    _require(stored["run_id"] == report["run_id"] and stored["publication_control"]["revision"] == revision,
             "source_report_identity_mismatch")
    baseline = audit["baseline_verification"]
    for key in SOURCE_KEYS:
        record = sources[key]
        path = _path(root, record["path"])
        _require(path.parent == report_path.parent, f"source_revision_directory_mismatch:{key}")
        _require(path == _path(root, stored["artifacts"][key]) == _path(root, report["artifacts"][key]),
                 f"source_artifact_path_mismatch:{key}")
        _require(_sha_text(record["sha256"]) and _sha(path) == record["sha256"], f"source_sha256_mismatch:{key}")
        saved = ({"path": baseline.get("snapshot_path"), "sha256": baseline.get("snapshot_sha256")}
                 if key == "screening_input_snapshot_json_gz" else baseline["source_files"][key])
        _require(_path(root, saved["path"]) == path and saved["sha256"] == record["sha256"],
                 f"baseline_source_binding_mismatch:{key}")
    _require(baseline.get("run_id") == report["run_id"] and baseline.get("baseline_verified") is True
             and baseline.get("status") == "matched" and baseline.get("failed_check_count") == 0,
             "baseline_not_verified")
    checks = baseline["checks"]
    _require(len(checks) == len(BASELINE_CHECKS) and {item["name"] for item in checks} == BASELINE_CHECKS
             and all(item.get("aligned") is True for item in checks), "baseline_checks_incomplete")
    _require(all(baseline.get(key) is False for key in ("later_database_reads", "missing_history_imputation",
                                                     "wall_clock_used_for_replayed_gates")), "baseline_live_inputs")
    with _path(root, sources["filtered_csv"]["path"]).open(encoding="utf-8-sig", newline="") as stream:
        formal_codes = _codes([row["基金代码"] for row in csv.DictReader(stream)])
    _require(_count(audit["baseline_final_count"]) == len(formal_codes), "baseline_final_count_mismatch")
    return sources, formal_codes


def _verify_definition(root: Path, audit: dict[str, Any]) -> dict[str, Any]:
    metadata = audit["audit_definition"]
    definition = metadata["definition"]
    baseline = audit["baseline_verification"]
    _require(metadata["sha256"] == _digest(definition), "audit_definition_sha256_mismatch")
    _require(definition["contract"] == AUDIT_CONTRACT, "audit_definition_contract_mismatch")
    _require(definition["audit_code_sha256"] == _code_hashes(root), "audit_code_changed")
    replay_code = definition["replay_code_sha256"]
    _require(bool(replay_code) and replay_code == baseline["replay_code_sha256"], "replay_code_binding_mismatch")
    for name, digest in replay_code.items():
        path = _path(root, name)
        _require(path.suffix == ".py" and _sha_text(digest) and _sha(path) == digest, "replay_code_changed")
    _require(_sha_text(definition["frozen_inputs_sha256"])
             and definition["frozen_inputs_sha256"] == baseline["inputs_sha256"], "frozen_inputs_binding_mismatch")
    _require(isinstance(definition["frozen_configuration"], dict) and bool(definition["frozen_configuration"]),
             "frozen_configuration_missing")
    _require([item[0] for item in definition["style_scopes"]] == list(SCOPE_KEYS), "style_definition_mismatch")
    _require([item["scenario_key"] for item in definition["scenarios"]] == list(SCENARIO_KEYS), "scenario_definition_mismatch")
    _require(_digest(definition["style_scopes"]) == _digest(LOW_VOL_STYLE_SCOPES)
             and _digest(definition["scenarios"]) == _digest(LOW_VOL_RELEASE_SCENARIOS), "scenario_policy_mismatch")
    return metadata


def _verify_scenarios(audit: dict[str, Any], baseline_codes: set[str]) -> tuple[dict[str, Any], dict[str, Any]]:
    rows = audit["scenario_impacts"]
    expected = {(scope, scenario) for scope in SCOPE_KEYS for scenario in SCENARIO_KEYS}
    indexed = {(row["scope_key"], row["scenario_key"]): row for row in rows}
    _require(len(rows) == len(expected) and set(indexed) == expected, "scenario_matrix_incomplete")
    completed = nonempty_completed = 0
    missing: dict[str, set[str]] = {}
    for row in rows:
        targets = _codes(row["target_codes"])
        _require(_count(row["targeted_count"]) == len(targets) and not targets & baseline_codes, "scenario_targets_mismatch")
        expected_id = hashlib.sha256(json.dumps(sorted(targets), separators=(",", ":")).encode()).hexdigest()[:20]
        _require(row["release_id"] == expected_id, "scenario_release_id_mismatch")
        if row["status"] == "computed":
            completed += 1
            nonempty_completed += bool(targets)
            added, removed, direct = (_codes(row[key]) for key in ("added_codes", "removed_codes", "direct_final_codes"))
            _require(not added & baseline_codes and removed <= baseline_codes, "scenario_membership_inconsistent")
            final = (baseline_codes - removed) | added
            _require(direct == final & targets, "scenario_direct_impact_inconsistent")
            for key, count in (("formal_count", len(final)), ("direct_final_impact_count", len(direct)),
                               ("added_official_count", len(added)), ("removed_official_count", len(removed))):
                _require(_count(row[key]) == count, f"scenario_outcome_count_mismatch:{key}")
            _require(type(row["net_final_count_delta"]) is int
                     and row["net_final_count_delta"] == len(final) - len(baseline_codes), "scenario_delta_mismatch")
        else:
            field = MISSING_INPUTS.get(row["status"])
            _require(field is not None, "scenario_terminal_unknown")
            codes = _codes(row[field])
            _require(bool(codes) and all(row.get(key) is None for key in OUTCOME_FIELDS), "incomplete_scenario_has_outcome")
            missing.setdefault(row["status"], set()).update(codes)
    _require(_count(audit["requested_scenario_count"]) == len(expected)
             and _count(audit["completed_scenario_count"]) == completed, "scenario_completion_count_mismatch")
    complete = completed == len(expected)
    _require(audit["status"] == ("computed" if complete else "scenario_inputs_incomplete")
             and audit["passed"] is complete and audit["strategy_comparison_allowed"] is complete,
             "scenario_completion_status_mismatch")
    scopes = {row["scope_key"]: row for row in audit["focus_scope_summary"]}
    _require(len(audit["focus_scope_summary"]) == len(SCOPE_KEYS) and set(scopes) == set(SCOPE_KEYS), "scope_summary_missing")
    for scope in SCOPE_KEYS:
        current = {key: indexed[(scope, key)] for key in SCENARIO_KEYS}
        all_codes = _codes(current["all_dynamic_blocked_in_scope"]["target_codes"])
        both = _codes(current[SUMMARY_SCENARIOS["annual_positive"]]["target_codes"])
        annual = _codes(current[SUMMARY_SCENARIOS["annual_without_positive"]]["target_codes"])
        positive = _codes(current[SUMMARY_SCENARIOS["positive_without_annual"]]["target_codes"])
        _require(not (both & annual or both & positive or annual & positive), "overlap_partition_inconsistent")
        _require(all(_codes(row["target_codes"]) <= all_codes for row in current.values()), "scenario_outside_scope")
        _require(_codes(current["annual_return_only"]["target_codes"]) <= annual
                 and _codes(current["positive_ratio_only"]["target_codes"]) <= positive, "exact_scenario_not_subset")
        for key, count in (("dynamic_blocked_count", len(all_codes)), ("annual_return_hit_count", len(both | annual)),
                           ("positive_ratio_hit_count", len(both | positive))):
            _require(_count(scopes[scope][key]) == count, "scope_count_mismatch")
    for scenario in SCENARIO_KEYS:
        style_sets = [_codes(indexed[(scope, scenario)]["target_codes"]) for scope in SCOPE_KEYS[:-1]]
        combined = set().union(*style_sets)
        _require(len(combined) == sum(map(len, style_sets))
                 and combined == _codes(indexed[("style_low_vol_focus", scenario)]["target_codes"]),
                 "combined_scope_membership_mismatch")
    focus = scopes["style_low_vol_focus"]
    _require(_count(audit["focus_style_dynamic_blocked_count"]) == focus["dynamic_blocked_count"], "focus_count_mismatch")
    coverage = {"requested_count": len(expected), "completed_count": completed,
                "completed_nonempty_target_count": nonempty_completed, "incomplete_count": len(expected) - completed,
                "missing_input_counts": {key: len(codes) for key, codes in sorted(missing.items())}}
    summary = {
        "focus_dynamic_blocked_count": focus["dynamic_blocked_count"],
        "focus_annual_return_hit_count": focus["annual_return_hit_count"],
        "focus_positive_ratio_hit_count": focus["positive_ratio_hit_count"],
    }
    for prefix, scenario in SUMMARY_SCENARIOS.items():
        row = indexed[("style_low_vol_focus", scenario)]
        summary[f"{prefix}_targeted_count"] = row["targeted_count"] if complete else None
        summary[f"{prefix}_direct_final_impact_count"] = row["direct_final_impact_count"] if complete else None
    return summary, coverage


def _verify_exports(root: Path, path: Path, audit: dict[str, Any], baseline: set[str]) -> None:
    artifacts = audit["artifacts"]
    key = "baseline_members" if audit["status"] == "baseline_verified" else "scenario_impacts"
    _require(bool(artifacts) and key in artifacts, "audit_exports_missing")
    frames = {}
    for name, record in artifacts.items():
        export = _path(root, record["path"])
        _require(export.parent == path.parent and export.name.startswith(path.stem + "_"), "audit_export_path_mismatch")
        raw = export.read_bytes()
        _require(len(raw) == record["bytes"] and hashlib.sha256(raw).hexdigest() == record["sha256"],
                 "audit_export_sha256_mismatch")
        plain = gzip.decompress(raw)
        _require(len(plain) == record["uncompressed_bytes"]
                 and hashlib.sha256(plain).hexdigest() == record["uncompressed_sha256"], "audit_export_content_mismatch")
        reader = csv.DictReader(io.StringIO(plain.decode("utf-8-sig"), newline=""))
        rows = list(reader)
        _require(len(rows) == record["rows"] and len(reader.fieldnames or []) == record["columns"], "audit_export_shape_mismatch")
        frames[name] = rows
    if key == "baseline_members":
        _require(_codes([row["基金代码"] for row in frames[key]]) == baseline, "baseline_export_membership_mismatch")
        return
    exported = {(row["scope_key"], row["scenario_key"]): row for row in frames[key]}
    scenarios = audit["scenario_impacts"]
    _require(len(exported) == len(frames[key]) == len(scenarios), "scenario_export_rows_mismatch")
    for scenario in scenarios:
        row = exported[(scenario["scope_key"], scenario["scenario_key"])]
        for field in ("status", "release_id"):
            _require(row[field] == scenario[field], "scenario_export_terminal_mismatch")
        for field in ("targeted_count", "formal_count", "direct_final_impact_count",
                      "added_official_count", "removed_official_count", "net_final_count_delta"):
            expected = scenario[field]
            _require(row[field] == "" if expected is None else row[field] != "" and Decimal(row[field]) == expected,
                     "scenario_export_count_mismatch")
        for field in ("target_codes", "direct_final_codes", "added_codes", "removed_codes"):
            _require(row[field] == "" if scenario[field] is None
                     else _codes(ast.literal_eval(row[field])) == _codes(scenario[field]), "scenario_export_codes_mismatch")
        release_key = f"release_{scenario['release_id']}_"
        if scenario["status"] == "computed":
            actual = _codes([item["基金代码"] for item in frames[release_key + "members"]])
            expected = (baseline - _codes(scenario["removed_codes"])) | _codes(scenario["added_codes"])
            _require(actual == expected, "scenario_members_export_mismatch")
        else:
            actual = _codes([item["基金代码"] for item in frames[release_key + "missing_inputs"]])
            _require(actual == _codes(scenario[MISSING_INPUTS[scenario["status"]]]), "scenario_gap_export_mismatch")


def load_low_vol_overlap_for_report(settings: Any, report: dict[str, Any]) -> dict[str, Any]:
    """Only the latest observation for this exact revision may supply impact."""
    root = Path(settings.project_root).resolve()
    identity = {"run_id": report.get("run_id"), "revision": (report.get("publication_control") or {}).get("revision")}
    result: dict[str, Any] = {
        "available": False, "passed": False, "status": "no_same_revision_audit", "run_id": identity["run_id"],
        "source_binding": identity, "source_verified_after_replay": False,
        "requested_scenario_count": None, "completed_scenario_count": None, "scenario_coverage": {},
        "report_binding": {"status": "missing", "artifact_path": None, "artifact_sha256": None},
        "recommendation": [], "strategy_effectiveness_verified": False,
        **dict.fromkeys(OVERLAP_COUNT_FIELDS),
    }
    try:
        _require(all(isinstance(value, str) and re.fullmatch(r"[A-Za-z0-9_.-]+", value) for value in identity.values()),
                 "report_identity_missing_or_invalid")
        directory = (Path(settings.reports_dir) / "audit" / AUDIT_NAME).resolve()
        directory.relative_to(root)
        for path in sorted(directory.glob(f"{AUDIT_NAME}_{identity['run_id']}_*.json"), reverse=True):
            _require(not path.is_symlink(), "audit_symlink_not_allowed")
            raw = path.read_bytes()
            result["report_binding"].update(artifact_path=str(path.relative_to(root)), artifact_sha256=hashlib.sha256(raw).hexdigest())
            audit = json.loads(raw)
            _require(isinstance(audit, dict) and audit.get("run_id") == identity["run_id"], "audit_run_mismatch")
            revision = (audit.get("source_binding") or {}).get("revision") or (audit.get("source_publication_control") or {}).get("revision")
            if revision is not None and revision != identity["revision"]:
                continue
            _require(audit.get("schema_version") == 3, "audit_provenance_contract_missing")
            _require(audit.get("input_contract") == "frozen-report-strategy-2026-09-11.1", "frozen_input_contract_mismatch")
            _require(audit.get("source_verified_after_replay") is True, "audit_source_not_verified")
            _require(all(audit.get(key) is False for key in ("later_database_reads", "current_upstream_queries",
                                                            "wall_clock_used_for_replayed_gates", "production_formulas_changed")),
                     "audit_live_inputs_or_formula_change")
            sources, formal = _verify_sources(root, report, audit)
            definition = _verify_definition(root, audit)
            if audit["status"] == "baseline_verified":
                _require(audit["requested_scenario_count"] == audit["completed_scenario_count"] == 0
                         and audit["scenario_impacts"] == [], "baseline_only_has_scenarios")
                result.update(status="baseline_only", baseline_verification={"baseline_verified": True, "status": "matched"})
            else:
                summary, coverage = _verify_scenarios(audit, formal)
                result.update(summary, status=audit["status"], passed=audit["passed"], available=audit["passed"],
                              baseline_verification={"baseline_verified": True, "status": "matched", "check_count": len(BASELINE_CHECKS)},
                              requested_scenario_count=coverage["requested_count"], completed_scenario_count=coverage["completed_count"],
                              scenario_coverage=coverage, recommendation=list(audit.get("recommendation") or []))
            _verify_exports(root, path, audit, formal)
            result.update(source_binding={**identity, "files": sources}, source_verified_after_replay=True,
                          generated_at=audit["generated_at"], audit_definition_sha256=definition["sha256"],
                          audit_code_sha256=definition["definition"]["audit_code_sha256"],
                          frozen_inputs_sha256=definition["definition"]["frozen_inputs_sha256"])
            result["report_binding"].update(status="matched", report_sha256=sources["report_json"]["sha256"],
                                             snapshot_sha256=sources["screening_input_snapshot_json_gz"]["sha256"])
            _require(path.read_bytes() == raw, "audit_changed_during_read")
            _verify_sources(root, report, audit)
            if not result["available"]:
                result.update(dict.fromkeys(OVERLAP_COUNT_FIELDS))
            return result
        result["report_binding"].update(artifact_path=None, artifact_sha256=None)
    except (OSError, ValueError, KeyError, TypeError, AttributeError, IndexError, SyntaxError, EOFError, DecimalException) as exc:
        result.update(available=False, passed=False, status="invalid_audit_evidence", source_verified_after_replay=False,
                      requested_scenario_count=None, completed_scenario_count=None, scenario_coverage={}, recommendation=[])
        result.update(dict.fromkeys(OVERLAP_COUNT_FIELDS))
        result["report_binding"].update(status="invalid", reason=f"{type(exc).__name__}:{exc}")
    return result


def low_vol_overlap_cache_token(settings: Any, run_id: str) -> str:
    """Invalidate API caches on additions, edits, removals and changed sources."""
    root = Path(settings.project_root).resolve()
    directory = Path(settings.reports_dir) / "audit" / AUDIT_NAME
    if not isinstance(run_id, str) or not re.fullmatch(r"[A-Za-z0-9_.-]+", run_id):
        return _digest({"status": "report_identity_missing"})
    records = []
    content_hashes: dict[Path, str] = {}
    for path in sorted(directory.glob(f"{AUDIT_NAME}_{run_id}_*.json")):
        try:
            _require(not path.is_symlink(), "audit_symlink_not_allowed")
            raw = path.read_bytes()
            records.append((str(path), hashlib.sha256(raw).hexdigest()))
            audit = json.loads(raw)
            sources = list((audit.get("source_binding") or {}).get("files", {}).values())
            sources += list((audit.get("artifacts") or {}).values())
            definition = (audit.get("audit_definition") or {}).get("definition") or {}
            sources += [{"path": name} for key in ("audit_code_sha256", "replay_code_sha256")
                        for name in (definition.get(key) or {})]
            for source in sources:
                source_path = _path(root, source["path"])
                stat = source_path.stat()
                if source_path not in content_hashes:
                    content_hashes[source_path] = _sha(source_path)
                records.append((str(source_path), stat.st_dev, stat.st_ino, stat.st_size,
                                stat.st_mtime_ns, stat.st_ctime_ns, content_hashes[source_path]))
        except (OSError, ValueError, KeyError, TypeError, AttributeError) as exc:
            records.append((str(path), type(exc).__name__))
    return _digest(records)
