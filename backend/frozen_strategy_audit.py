"""Shared source binding and complete-input gates for strategy experiments."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pandas as pd

from backend.artifact_paths import resolve_project_artifact_path
from backend.incremental_review_replay import apply_scenario_incremental_review
from backend.runtime_replay import resolve_runtime_artifact_path
from backend.screening_baseline_replay import file_sha256, run_report_selection


FROZEN_STRATEGY_CONTRACT = "frozen-report-strategy-2026-09-11.1"
FROZEN_SOURCE_KEYS = ("report_json", "screening_input_snapshot_json_gz", "filtered_csv", "watch_csv",
                      "dynamic_filtered_csv", "dynamic_watch_csv")


def _resolve_frozen_source_path(value: Any, root: Path) -> Path | None:
    if not value:
        return None
    path = resolve_project_artifact_path(value, project_root=root)
    # Retain the existing relocation to the same run/revision in the archive.
    # Both the requested path and the relocated path must stay in this project.
    existing = resolve_runtime_artifact_path(path.relative_to(root.resolve()).as_posix(), project_root=root)
    return resolve_project_artifact_path(existing, project_root=root) if existing is not None else None


def bind_frozen_report_sources(settings: Any, report: dict[str, Any]) -> dict[str, Any]:
    """Bind one report revision; a matching run id alone is insufficient."""
    artifacts = report.get("artifacts") or {}
    configured_root = Path(settings.project_root)
    root = configured_root.resolve()
    report_path = _resolve_frozen_source_path(artifacts.get("report_json"), configured_root)
    if report_path is None or not report_path.is_file():
        raise ValueError("source_report_missing")
    if json.loads(report_path.read_text(encoding="utf-8")) != report:
        raise ValueError("source_report_payload_mismatch")
    files = {}
    for key in FROZEN_SOURCE_KEYS:
        path = _resolve_frozen_source_path(artifacts.get(key), configured_root)
        if path is None or not path.is_file():
            raise ValueError(f"frozen_source_missing:{key}")
        if path.parent.resolve() != report_path.parent.resolve():
            raise ValueError(f"frozen_source_revision_mismatch:{key}")
        files[key] = {"path": str(path.relative_to(root)), "sha256": file_sha256(path)}
    return {"run_id": report["run_id"], "revision": (report.get("publication_control") or {}).get("revision"),
            "files": files}


def run_frozen_strategy_scenario(
    context: dict[str, Any], *, scored_universe: pd.DataFrame,
    dynamic_threshold_override: tuple[dict[str, Any], dict[str, Any], dict[str, Any]] | None = None,
    dynamic_release_codes: set[str] | None = None,
) -> dict[str, Any]:
    """Change only the requested scores or thresholds, never their input dates.

    Counterfactual entrants can require history that the original report did not
    save. Such a stage is incomplete; a partially selected list is not a result.
    """
    inputs = context["snapshot"]["inputs"]
    result = run_report_selection(
        settings=context["settings"], scored_universe=scored_universe,
        decision_map=inputs["decision_map"], soft_warning_map=inputs["soft_warning_map"],
        tracker_features=context["frames"]["final_selection_tracker_features"],
        autostock_status_map=context["autostock_status_map"], evaluation_times=context["evaluation_times"],
        dynamic_threshold_override=dynamic_threshold_override,
        dynamic_release_codes=dynamic_release_codes,
        selection_tracker_history=context.get("selection_tracker_history"),
        observed_unvalidated_validation_codes=context.get(
            "observed_unvalidated_validation_codes",
            context.get("observed_unvalidated_autostock_codes"),
        ),
    )
    return apply_scenario_incremental_review(context, result)


def missing_scenario_inputs(result: dict[str, Any]) -> dict[str, Any]:
    # New validation providers report a generic evidence gap. Keep the legacy
    # AutoStock field so older frozen artifacts remain readable.
    return {key: result[key] for key in (
        "missing_tracker_history_codes", "missing_validation_codes", "missing_autostock_codes",
        "missing_incremental_review_codes",
    ) if key in result and result[key]}


def scenario_gap_frame(result: dict[str, Any]) -> pd.DataFrame:
    rows = [{"基金代码": code, "缺失阶段": stage}
            for key, stage in (("missing_tracker_history_codes", "Tracker历史"),
                               ("missing_validation_codes", "当前校验证据"),
                               ("missing_autostock_codes", "AutoStock观测（兼容旧 provider）"),
                               ("missing_incremental_review_codes", "新增样本安全复核"))
            for code in result.get(key, [])]
    return pd.DataFrame(rows, columns=["基金代码", "缺失阶段"])


def load_incomplete_score_shadow_terminal(
    settings: Any, *, run_id: str, stdout_tail: list[str], return_code: int,
) -> dict[str, Any] | None:
    """Distinguish an evidenced history gap from a failed audit process."""
    if return_code != 2 or not stdout_tail:
        return None
    try:
        terminal = json.loads(stdout_tail[-1])
        if not isinstance(terminal, dict):
            return None
        path = resolve_project_artifact_path(terminal["artifact_path"], project_root=settings.project_root)
        directory = resolve_project_artifact_path(
            Path(settings.reports_dir) / "audit/score_dedup_shadow", project_root=settings.project_root)
        if path.parent != directory or not path.name.startswith(f"score_dedup_shadow_{run_id}_"):
            return None
        raw = path.read_bytes()
        payload = json.loads(raw)
        scenario = payload.get("shadow_replay") or {}
        if (hashlib.sha256(raw).hexdigest() != terminal.get("artifact_sha256")
                or payload.get("schema_version") != 2 or payload.get("input_contract") != FROZEN_STRATEGY_CONTRACT
                or payload.get("run_id") != run_id or terminal.get("run_id") != run_id
                or payload.get("status") != "scenario_inputs_incomplete" or terminal.get("status") != payload["status"]
                or payload.get("passed") is not False or terminal.get("passed") is not False
                or (payload.get("baseline_verification") or {}).get("baseline_verified") is not True
                or scenario.get("final_count") is not None or payload.get("membership_impact") is not None
                or scenario.get("status") != terminal.get("scenario_status")
                or scenario.get("status") not in {"missing_tracker_history", "missing_validation_observations",
                                                   "missing_autostock_observations", "missing_frozen_incremental_review"}):
            return None
        gaps = missing_scenario_inputs(scenario)
        if not gaps or not all(isinstance(codes, list) and codes for codes in gaps.values()):
            return None
        return {"status": payload["status"], "scenario_status": scenario["status"],
                "missing_input_count": sum(len(codes) for codes in gaps.values()), "artifact": str(path)}
    except (OSError, ValueError, KeyError, TypeError, AttributeError):
        return None
