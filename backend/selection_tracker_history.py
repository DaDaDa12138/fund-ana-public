"""Replay selection features from observed canonical memberships, without I/O.

The current candidate set is a selection-stage input. A counterfactual current
set does not create historical publications or change the formal Tracker.
"""
from __future__ import annotations

from collections import Counter
import hashlib
import re
from pathlib import Path
from typing import Any

import pandas as pd

from backend import project_tracker


CONTRACT = "selection_tracker_history_v1"
FEATURE_COLUMNS = ["基金代码", "出现次数", "出现频率(%)", "连续缺席期数", "当前是否在榜", "跟踪总期数"]
CURRENT_MEMBERSHIP_SCOPE = "post_review_candidates_for_selection_not_formal_publication"


def source_sha256(path: Path) -> str:
    from core.utils.report_history_store import original_hash
    return original_hash(path)


def _require(condition: bool, reason: str) -> None:
    if not condition:
        raise ValueError(f"selection_tracker_history_{reason}")


def _members(value: Any) -> set[str]:
    _require(isinstance(value, list), "members_missing")
    _require(all(isinstance(code, str) and re.fullmatch(r"[0-9]{6}", code) for code in value), "member_invalid")
    _require(len(value) == len(set(value)), "member_duplicated")
    return set(value)


def validate_selection_history(
    history: dict[str, Any], *, run_id: str, generated_at: str, snapshot_date: str,
    original_current: pd.DataFrame,
) -> None:
    """Check capture completeness and identity before enabling new candidates."""
    _require(history.get("contract") == CONTRACT, "contract_invalid")
    _require(history.get("status") == "complete", "incomplete")
    _require((history.get("run_id"), history.get("generated_at"), history.get("registry_nav_snapshot_date"))
             == (run_id, generated_at, snapshot_date), "report_identity_mismatch")
    selection = history.get("selection") or {}
    _require(selection.get("contract") == project_tracker.TRACKER_HISTORY_INPUT_CONTRACT
             and selection.get("complete") is True and selection.get("issues") == [], "selection_incomplete")
    records = selection.get("records")
    _require(isinstance(records, list) and selection.get("canonical_record_count") == len(records), "registry_coverage_invalid")
    _require(all(isinstance(row, dict) and row.get("outcome") in {
        "not_publishable", "current_run", "after_current_run", "empty_publication", "selected", "superseded_snapshot",
    } for row in records), "registry_outcome_invalid")
    selected = {row["run_id"]: row for row in records if row["outcome"] == "selected"}
    _require(len({row["run_id"] for row in records}) == len(records), "registry_run_duplicated")
    observations = history.get("observations")
    _require(isinstance(observations, list), "observations_missing")
    observed_ids = [row["run_id"] for row in observations]
    _require(observed_ids == selection.get("selected_history_run_ids")
             and len(set(observed_ids)) == len(observed_ids) and set(observed_ids) == set(selected), "source_coverage_mismatch")
    order = [(row["generated_at"], row["run_id"]) for row in observations]
    _require(order == sorted(order) and all(key < (generated_at, run_id) for key in order), "source_order_invalid")
    for row in observations:
        if 'history_record' in row:
            from core.utils.report_history_store import verify_embedded
            verify_embedded(row['history_record'])
            proof=row['history_record']
            _require(proof['payload']=={'member_codes':row['member_codes'],'signature':row['signature'],'nav_date':row['nav_date']},'ledger_projection_mismatch')
            _require(proof['sha256']==row['source_sha256'],'ledger_origin_mismatch')
        record = selected[row["run_id"]]
        _require(row.get("source_verified_unchanged") is True, "source_changed_during_read")
        _require(bool(re.fullmatch(r"[0-9a-f]{64}", str(row.get("source_sha256") or "")))
                 and bool(re.fullmatch(r"[0-9a-f]{64}", str(record.get("report_sha256") or ""))), "source_sha256_invalid")
        _require(row.get("source_path") == record.get("excel_path")
                 and row.get("generated_at") == record.get("generated_at")
                 and row.get("registry_nav_snapshot_date") == record.get("registry_nav_snapshot_date"), "source_binding_mismatch")
        _require(bool(re.fullmatch(r"[0-9a-f]{40}", str(row.get("signature") or ""))), "source_identity_invalid")
        _require(row.get("snapshot_key") == project_tracker._tracker_snapshot_key(row.get("nav_date"), row.get("signature")),
                 "snapshot_key_mismatch")
        _require(bool(_members(row.get("member_codes"))), "members_empty")
    current = history.get("current_observation") or {}
    signature, nav_date = project_tracker._tracker_source_identity(original_current)
    _require(current.get("membership_scope") == CURRENT_MEMBERSHIP_SCOPE, "current_scope_invalid")
    _require((current.get("signature"), current.get("nav_date"), current.get("snapshot_key"))
             == (signature, nav_date, project_tracker._tracker_snapshot_key(nav_date, signature)), "current_identity_mismatch")
    _require(_members(current.get("member_codes")) == _current_members(original_current), "current_membership_mismatch")
    _require(history.get("missing_history_imputation") is False, "imputed_history_forbidden")


def _current_members(frame: pd.DataFrame) -> set[str]:
    return {project_tracker._normalize_code(code)
            for code in frame.get("基金代码", pd.Series(dtype=object)).dropna().astype(str).tolist()
            if project_tracker._normalize_code(code)}


def replay_selection_tracker_features(history: dict[str, Any], current_df: pd.DataFrame) -> pd.DataFrame:
    """Use an already baseline-verified capture with this scenario's current set.

    An absent member is a real historical non-occurrence only within this complete
    captured source set. Missing captures never enter this function as empty data.
    """
    _require(history.get("contract") == CONTRACT and history.get("status") == "complete", "incomplete")
    if current_df.empty or "基金代码" not in current_df.columns:
        return pd.DataFrame(columns=FEATURE_COLUMNS)
    current_codes = _current_members(current_df)
    if not current_codes:
        return pd.DataFrame(columns=FEATURE_COLUMNS)
    snapshots = {row["snapshot_key"]: set(row["member_codes"]) for row in history["observations"]}
    signature, nav_date = project_tracker._tracker_source_identity(current_df)
    snapshot_key = project_tracker._tracker_snapshot_key(nav_date, signature)
    _require(bool(snapshot_key), "current_identity_unavailable")
    # Replacing a NAV date is intentional. Without a date, the exact scenario
    # content signature controls replacement, as in the production builder.
    snapshots[snapshot_key] = current_codes
    total_periods = len(snapshots)
    occurrences = Counter(code for codes in snapshots.values() for code in codes)
    return pd.DataFrame([
        {"基金代码": code, "出现次数": int(occurrences[code]),
         "出现频率(%)": round(occurrences[code] / total_periods * 100.0, 2),
         "连续缺席期数": 0, "当前是否在榜": "是", "跟踪总期数": int(total_periods)}
        for code in sorted(current_codes)
    ], columns=FEATURE_COLUMNS)
