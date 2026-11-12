from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from backend.artifact_paths import project_relative_artifact_path, resolve_project_artifact_path
from backend.config import Settings


class FrozenAnnualScenarioIncomplete(ValueError):
    """A known historical evidence gap, not a computed empty scenario."""

    def __init__(self, status: str, *, missing_codes: list[str], stage_frames: dict[str, Any]):
        super().__init__(status)
        self.status = status
        self.missing_codes = sorted(set(missing_codes))
        self.stage_frames = stage_frames


def build_incomplete_annual_shadow_summary(
    report: dict[str, Any], baseline: dict[str, Any], *,
    baseline_count: int | None = None, issue: FrozenAnnualScenarioIncomplete | None = None,
    baseline_only: bool = False,
) -> dict[str, Any]:
    """Keep uncomputed counts null even when a valid baseline has members."""
    verified = baseline.get("baseline_verified") is True
    completed_baseline_only = verified and baseline_only
    status = "baseline_verified" if completed_baseline_only else (
        "scenario_inputs_incomplete" if verified else "baseline_not_verified")
    payload = {
        "schema_version": 2, "run_id": report.get("run_id"), "status": status,
        "passed": completed_baseline_only, "baseline_verification": baseline,
        "source_report": (report.get("artifacts") or {}).get("report_json"),
        "source_publication_control": report.get("publication_control"),
        "scope": "report_selection_from_frozen_complete_universe",
        "later_database_reads": False, "runtime_upstream_requests": 0,
        "wall_clock_used_for_replayed_gates": False,
        "production_thresholds_changed": False, "strategy_comparison_allowed": False,
        "baseline_final_count": baseline_count if verified else None,
        "shadow_final_count": None, "net_final_count_delta": None,
        "shadow_added_count": None, "shadow_removed_count": None,
        "direct_relieved_final_count": None, "portfolio_direct_relieved_count": None,
        "annual_only_candidate_count": None,
        "scenario_status": "not_requested" if baseline_only else (issue.status if issue else "not_evaluated"),
        "local_style_band_shadow_scenarios": [],
        "local_style_band_shadow_judgement": {
            "recommended_action": "collect_frozen_scenario_inputs_before_comparison" if verified else "verify_frozen_baseline_first",
            "direct_relief_supported_scenario_total": None,
            "selection_side_effect_only_scenario_total": None,
        },
        "local_style_band_forward_validation_judgement": {
            "available": False, "status": "not_evaluated", "ready_scenario_total": None,
            "recommended_action": "freeze_valid_scenario_members_before_forward_validation",
            "threshold_change_supported_now": False,
        },
    }
    if issue:
        payload["missing_inputs"] = {"stage": issue.status, "codes": issue.missing_codes}
        candidate = issue.stage_frames.get("candidate_before_annual_relief")
        post_review = issue.stage_frames.get("scenario_post_review")
        if candidate is not None:
            payload["annual_only_candidate_count"] = int(candidate["dynamic_threshold_reason_codes"].eq("below_annual_floor").sum())
        if post_review is not None:
            payload["scenario_post_review_count"] = len(post_review)
        payload["not_evaluated_stages"] = {
            "missing_tracker_history": ["final_selection", "autostock_gate", "incremental_safety", "local_scenarios", "forward_validation"],
            "missing_validation_observations": ["validation_gate", "incremental_safety", "local_scenarios", "forward_validation"],
            "missing_autostock_observations": ["autostock_gate", "incremental_safety", "local_scenarios", "forward_validation"],
            "missing_frozen_incremental_review": ["incremental_safety", "local_scenarios", "forward_validation"],
        }[issue.status]
    return payload


def load_incomplete_annual_shadow_terminal(
    settings: Settings, *, run_id: str, stdout_tail: list[str], return_code: int,
) -> dict[str, Any] | None:
    """Only a bound, run-local artifact can explain a nonzero child exit."""
    if return_code != 2 or not stdout_tail:
        return None
    try:
        terminal = json.loads(stdout_tail[-1])
        if not isinstance(terminal, dict):
            return None
        path = resolve_project_artifact_path(terminal["artifact"], project_root=settings.project_root)
        directory = resolve_project_artifact_path(
            settings.reports_dir / "audit/annual_return_gate_shadow", project_root=settings.project_root)
        if path.parent != directory or not path.name.startswith(f"annual_return_gate_shadow_{run_id}_"):
            return None
        raw = path.read_bytes()
        if hashlib.sha256(raw).hexdigest() != terminal.get("artifact_sha256"):
            return None
        payload = json.loads(raw)
        if (not isinstance(payload, dict)
                or payload.get("schema_version") != 2 or payload.get("run_id") != run_id
                or payload.get("status") not in {"baseline_not_verified", "scenario_inputs_incomplete", "computed_with_scenario_gaps"}
                or payload.get("passed") is not False or terminal.get("passed") is not False
                or payload.get("status") != terminal.get("status")
                or payload.get("scenario_status") != terminal.get("scenario_status")):
            return None
        return {"status": payload["status"], "scenario_status": payload["scenario_status"],
                "artifact": str(path), "artifact_sha256": terminal["artifact_sha256"]}
    except (OSError, ValueError, KeyError, TypeError):
        return None


def load_latest_annual_return_gate_shadow_payload(
    settings: Settings,
    *,
    run_id: str | None = None,
    allow_fallback_to_any_run: bool = False,
    report: dict[str, Any] | None = None,
) -> tuple[Path | None, dict[str, Any] | None]:
    if report is not None:
        return _load_shadow_for_report(settings, report)
    artifact_dir = settings.reports_dir / "audit" / "annual_return_gate_shadow"
    if not artifact_dir.exists():
        return None, None
    pattern = (
        f"annual_return_gate_shadow_{run_id}_*.json"
        if str(run_id or "").strip()
        else "annual_return_gate_shadow_*.json"
    )
    files = sorted(artifact_dir.glob(pattern), reverse=True)
    if not files and str(run_id or "").strip() and allow_fallback_to_any_run:
        files = sorted(artifact_dir.glob("annual_return_gate_shadow_*.json"), reverse=True)
    if not files:
        return None, None
    path = files[0]
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return path, None
    return path, payload if isinstance(payload, dict) else None


def _project_relative_path(settings: Settings, value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("source_report_path_missing")
    return project_relative_artifact_path(value, project_root=settings.project_root)


def annual_shadow_report_binding(settings: Settings, report: dict[str, Any]) -> dict[str, Any]:
    """Identify the report actually read; a run can have multiple revisions."""
    relative = _project_relative_path(settings, (report.get("artifacts") or {}).get("report_json"))
    raw = (settings.project_root / relative).read_bytes()
    stored = json.loads(raw)
    run_id = report.get("run_id")
    revision = (report.get("publication_control") or {}).get("revision")
    if (not run_id or stored.get("run_id") != run_id
            or (stored.get("publication_control") or {}).get("revision") != revision):
        raise ValueError("source_report_identity_mismatch")
    return {"run_id": run_id, "revision": revision, "report_path": relative,
            "report_sha256": hashlib.sha256(raw).hexdigest()}


def _load_shadow_for_report(
    settings: Settings, report: dict[str, Any],
) -> tuple[Path | None, dict[str, Any]]:
    expected: dict[str, Any] = {
        "run_id": report.get("run_id"), "revision": (report.get("publication_control") or {}).get("revision"),
        "report_path": (report.get("artifacts") or {}).get("report_json"), "report_sha256": None,
    }

    def unavailable(status: str, reason: str, path: Path | None = None, raw: bytes | None = None):
        summary = build_incomplete_annual_shadow_summary(
            report, {"baseline_verified": False, "status": "not_evaluated", "reason": reason})
        summary["report_binding"] = {**expected, "status": status, "reason": reason,
            "artifact_path": str(path.relative_to(settings.project_root.resolve())) if path else None,
            "artifact_sha256": hashlib.sha256(raw).hexdigest() if raw is not None else None}
        return path, summary

    try:
        expected = annual_shadow_report_binding(settings, report)
        directory = resolve_project_artifact_path(
            settings.reports_dir / "audit/annual_return_gate_shadow", project_root=settings.project_root)
    except (OSError, ValueError, TypeError, AttributeError) as exc:
        return unavailable("invalid", f"source_report_unavailable:{type(exc).__name__}:{exc}")
    # A newer audit for another revision must not displace a matching one.
    paths = sorted(directory.glob(f"annual_return_gate_shadow_{expected['run_id']}_*.json"), reverse=True)
    for path in paths:
        raw = None
        try:
            resolved = resolve_project_artifact_path(path, project_root=settings.project_root)
            if resolved.parent != directory:
                raise ValueError("shadow_artifact_outside_audit_directory")
            raw = resolved.read_bytes()
            payload = json.loads(raw)
            if not isinstance(payload, dict) or payload.get("run_id") != expected["run_id"]:
                raise ValueError("shadow_run_identity_mismatch")
            control = payload.get("source_publication_control") or {}
            if control.get("revision") != expected["revision"]:
                continue
            if _project_relative_path(settings, payload.get("source_report")) != expected["report_path"]:
                raise ValueError("shadow_source_report_path_mismatch")
            source = payload.get("source_report_binding")
            if source is not None:
                if not isinstance(source, dict):
                    raise ValueError("shadow_source_report_binding_invalid")
                source = {**source, "report_path": _project_relative_path(settings, source.get("report_path"))}
                if source != expected:
                    raise ValueError("shadow_source_report_binding_mismatch")
            baseline = payload.get("baseline_verification") or {}
            saved = (baseline.get("source_files") or {}).get("report_json") or {}
            # Older schema-2 audits already stored this immutable file SHA.
            # An unverified baseline can be shown only with an explicit binding.
            if baseline.get("baseline_verified") is True or source is None:
                if (saved.get("sha256") != expected["report_sha256"]
                        or _project_relative_path(settings, saved.get("path")) != expected["report_path"]):
                    raise ValueError("shadow_source_report_sha256_mismatch")
            result = dict(payload)
            result["report_binding"] = {**expected, "status": "matched",
                "artifact_path": str(path.relative_to(settings.project_root.resolve())),
                "artifact_sha256": hashlib.sha256(raw).hexdigest()}
            return path, result
        except (OSError, ValueError, TypeError, AttributeError) as exc:
            # Do not turn a corrupt newest observation into an older success.
            return unavailable("invalid", f"{type(exc).__name__}:{exc}", path, raw)
    return unavailable("missing", "no_audit_for_report_revision")
