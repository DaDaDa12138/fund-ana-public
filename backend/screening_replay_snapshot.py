"""Typed, compressed inputs for reproducible publication strategy research.

Input availability is deliberately not a claim that a baseline was reproduced.
"""
from __future__ import annotations

from datetime import date, datetime
import gzip
import hashlib
import json
import math
from pathlib import Path
from typing import Any, Iterator

import pandas as pd

from backend.manager_metrics import manager_metric_contract
from backend.incremental_review_replay import FRAME_NAMES as INCREMENTAL_FRAME_NAMES, relative_source_paths

SCHEMA_VERSION = 1
CONFIG_FIELDS = (
    "exclude_holding_days_gt", "final_selection_target_max", "final_selection_group_min_ratio",
    "final_selection_high_score_new_fund_threshold", "final_selection_low_score_new_fund_threshold",
)
CODE_FILES = (
    "backend/report_builder.py", "backend/project_quality_thresholds.py", "backend/project_publish_decisions.py",
    "backend/bond_style_classifier.py",
    "backend/project_formal_groups.py", "backend/fee_semantics.py", "backend/project_tracker.py",
    "backend/manager_metrics.py",
    "backend/selection_tracker_history.py",
    "core/utils/report_history_store.py",
    "backend/forward_validation.py", "backend/incremental_review_replay.py",
    "backend/screening_replay_snapshot.py", "backend/screening_baseline_replay.py",
    "backend/report_inputs.py", "backend/publish_pipeline.py",
    "backend/prospective_autostock.py", "backend/risk_metric_formula_shadow.py",
    "backend/scale_semantics.py",
    "core/utils/holding_period_policy.py", "core/utils/holding_period_parser.py",
)


def _encode(value: Any) -> Any:
    if isinstance(value, dict):
        if value.get('contract') == 'report-history-frame-v1' and 'payload' in value:
            # A ledger proof already has a typed digest contract. Converting
            # its NaN/NA/date values with the screening-frame codec changes
            # the signed payload. Keep the ledger's own lossless wire types.
            from core.utils import report_history_store as history_store

            history_store.verify_embedded(value)
            return history_store.encode(value)
        return {str(key): _encode(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_encode(item) for item in value]
    if value is None or value is pd.NA or value is pd.NaT:
        return None
    if isinstance(value, (date, datetime, pd.Timestamp)):
        return {"__datetime__": value.isoformat()}
    if hasattr(value, "item"):
        value = value.item()
    if isinstance(value, float) and not math.isfinite(value):
        return None if math.isnan(value) else {"__float__": "inf" if value > 0 else "-inf"}
    return value


def _decode(value: Any) -> Any:
    if isinstance(value, dict):
        if set(value) == {"__float__"}:
            return float(value["__float__"])
        if set(value) == {"__datetime__"}:
            return pd.Timestamp(value["__datetime__"])
        return {key: _decode(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_decode(item) for item in value]
    return value


def freeze_frame(frame: pd.DataFrame) -> dict[str, Any]:
    if not frame.columns.is_unique:
        raise ValueError("replay_input_duplicate_columns")
    return {"columns": list(frame.columns), "dtypes": [str(dtype) for dtype in frame.dtypes],
            "rows": _encode(frame.astype(object).values.tolist()), "index": _encode(frame.index.tolist()),
            "index_name": frame.index.name, "index_dtype": str(frame.index.dtype)}


def restore_frame(payload: dict[str, Any]) -> pd.DataFrame:
    frame = pd.DataFrame(_decode(payload["rows"]), columns=payload["columns"])
    if len(payload["columns"]) != len(payload["dtypes"]):
        raise ValueError("replay_input_dtype_contract_invalid")
    for column, dtype in zip(payload["columns"], payload["dtypes"]):
        frame[column] = frame[column].astype(dtype)
    frame.index = pd.Index(_decode(payload["index"]), name=payload.get("index_name"), dtype=payload.get("index_dtype"))
    return frame


def source_code_hashes(project_root: Path) -> dict[str, str]:
    return {name: hashlib.sha256((project_root / name).read_bytes()).hexdigest() for name in CODE_FILES}


def _json_chunks(value: Any, *, sort_keys: bool = False) -> Iterator[bytes]:
    """Match the existing JSON bytes without building a second whole snapshot."""
    encoder = json.JSONEncoder(ensure_ascii=False, sort_keys=sort_keys,
                               separators=(',', ':'), allow_nan=False)
    pending = []
    size = 0
    for part in encoder.iterencode(value):
        pending.append(part)
        size += len(part)
        if size >= 64 * 1024:
            yield ''.join(pending).encode('utf-8')
            pending.clear()
            size = 0
    if pending:
        yield ''.join(pending).encode('utf-8')


def _json_sha256(value: Any) -> str:
    result = hashlib.sha256()
    for chunk in _json_chunks(value, sort_keys=True):
        result.update(chunk)
    return result.hexdigest()


def write_screening_input_snapshot(
    settings: Any, summary: dict[str, Any], *, universe: pd.DataFrame,
    filters: dict[str, Any], materialized: dict[str, Any], report_inputs: dict[str, Any],
) -> Path:
    frames = {
        "scored_universe_before_holding_gate": universe,
        "candidate_after_thresholds": filters["candidate_df"],
        "post_review": filters["dynamic_publish_official_df"],
        "final_selection_tracker_features": filters["final_selection_tracker_features_df"],
        "final_selection_contraction": filters["final_selection_contraction_df"],
        "formal_after_all_filters": materialized["official_df"],
        "dynamic_reference": filters["dynamic_official_df"],
    }
    # Freeze exactly the in-memory sources used to calculate manager aggregates.
    # Old snapshots without these frames remain readable and cannot gain coverage
    # by consulting a later database.
    metadata = report_inputs.get("metadata_df")
    profiles = report_inputs.get("manager_snapshot_df")
    manager_sources_available = isinstance(metadata, pd.DataFrame) and isinstance(profiles, pd.DataFrame)
    if manager_sources_available:
        binding_columns = [key for key in ("fcode", "manager_ids", "manager_names") if key in metadata]
        frames["fund_manager_bindings"] = metadata[binding_columns].copy()
        frames["manager_profiles"] = profiles.copy()
    monthly = filters.get("low_vol_monthly_replay_inputs") or {}
    monthly_sources_available = all(isinstance(monthly.get(key), pd.DataFrame) for key in ("risk_windows", "nav_history"))
    if monthly_sources_available:
        frames["low_vol_risk_windows"] = monthly["risk_windows"]
        frames["low_vol_nav_history"] = monthly["nav_history"]
    incremental_capture = materialized.get("incremental_review_capture")
    if isinstance(incremental_capture, dict) and incremental_capture:
        for key, name in INCREMENTAL_FRAME_NAMES.items():
            if isinstance(incremental_capture.get(key), pd.DataFrame):
                frames[name] = incremental_capture[key]
    inputs = {"frames": {key: freeze_frame(frame) for key, frame in frames.items()},
              "decision_map": _encode(report_inputs["decision_map"]), "soft_warning_map": _encode(report_inputs["soft_warning_map"]),
              "style_thresholds": _encode(filters["style_thresholds"]), "global_thresholds": _encode(filters["global_thresholds"]),
              "style_meta": _encode(filters["style_meta"]),
              "configuration": {key: getattr(settings, key) for key in CONFIG_FIELDS}}
    if manager_sources_available:
        inputs["manager_metric_contract"] = manager_metric_contract()
    if monthly_sources_available:
        inputs["low_vol_monthly_input_contract"] = _encode(monthly["contract"])
    if isinstance(incremental_capture, dict) and incremental_capture:
        inputs["incremental_review_capture"] = relative_source_paths(
            {**_encode({key: value for key, value in incremental_capture.items() if key not in INCREMENTAL_FRAME_NAMES}),
             "report_generated_at": summary["generated_at"]}, settings.project_root,
        )
    tracker_history = filters.get("selection_tracker_history")
    if isinstance(tracker_history, dict) and tracker_history:
        # Membership sets were read by this report's actual selection pass.
        # They are not per-fund features invented for the unselected universe.
        inputs["selection_tracker_history"] = {
            **_encode(tracker_history), "report_generated_at": summary["generated_at"],
        }
    if isinstance(filters.get("evaluation_times"), dict):
        inputs["evaluation_times"] = dict(filters["evaluation_times"])
    if isinstance(filters.get("prospective_autostock_capture"), dict):
        inputs["prospective_autostock_capture"] = _encode(filters["prospective_autostock_capture"])
    calculation_index = ((summary.get("artifacts") or {}).get("core") or {}).get("calculation_input_index_file")
    if calculation_index:
        from backend.calculation_input_capture import capture_index_reference

        inputs["calculation_input_capture"] = capture_index_reference(settings, calculation_index)
    collection_captures = ((summary.get("artifacts") or {}).get("core") or {}).get("collection_calculation_input_captures")
    if collection_captures:
        from backend.collection_calculation_capture import validate_collection_capture_reference

        inputs["collection_calculation_input_captures"] = [
            validate_collection_capture_reference(settings, reference) for reference in collection_captures
        ]
    payload = {"schema_version": SCHEMA_VERSION, "run_id": summary["run_id"], "generated_at": summary["generated_at"],
               "input_availability": "frozen", "baseline_reproduction_status": "not_verified",
               "snapshot_date": filters["effective_nav_candidate_date"], "inputs": inputs,
               "inputs_sha256": _json_sha256(inputs), "code_sha256": source_code_hashes(settings.project_root),
               "truth_policy": "actual_in_memory_frames_used_by_this_report_not_reconstructed_from_a_later_database",
               "score_universe_policy": "score_full_universe_before_holding_gate_as_production_does"}
    report_path = settings.project_root / summary["artifacts"]["report_json"]
    path = report_path.parent / "screening_input_snapshot.json.gz"
    with path.open("xb") as stream, gzip.GzipFile(fileobj=stream, mode="wb", mtime=0) as compressed:
        for chunk in _json_chunks(payload):
            compressed.write(chunk)
    return path


def load_screening_input_snapshot(path: Path, *, expected_run_id: str | None = None) -> dict[str, Any]:
    with gzip.open(path, "rt", encoding="utf-8") as stream:
        payload = json.load(stream)
    if payload.get("schema_version") != SCHEMA_VERSION or (expected_run_id and payload.get("run_id") != expected_run_id):
        raise ValueError("screening_input_snapshot_identity_mismatch")
    if _json_sha256(payload['inputs']) != payload.get("inputs_sha256"):
        raise ValueError("screening_input_snapshot_signature_mismatch")
    return payload
