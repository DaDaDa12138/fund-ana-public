"""As-observed inputs for the existing incremental safety review.

Previous membership is frozen. Relative ranks are recalculated from each
scenario's own current cohort; original percentiles are never copied to it.
"""
from __future__ import annotations

from datetime import datetime
import hashlib
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from backend.project_formal_groups import FORMAL_GROUP_LABEL, FORMAL_SCORE_LABEL

CONTRACT = "incremental-review-as-observed-2026-09-12.1"
INPUT_COLUMNS = (
    "基金代码", "基金名称", FORMAL_GROUP_LABEL, FORMAL_SCORE_LABEL,
    "1年-年化收益(%)", "1年-最大回撤(%)", "1年-波动率", "持有期状态",
)
FRAME_NAMES = {
    "current_input": "incremental_review_current_input",
    "previous_members": "incremental_review_previous_members",
    "review_result": "incremental_review_observed_result",
}


def read_observed_source(
    path: Path, observations: list[dict[str, Any]], reader: Callable[[bytes], Any], *, role: str,
) -> Any:
    """Hash exactly the bytes handed to the reader, including failed attempts."""
    observation: dict[str, Any] = {"path": str(path), "role": role, "status": "unreadable"}
    try:
        raw = path.read_bytes()
        observation.update(bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest())
        value = reader(raw)
        observation["status"] = "read"
        return value
    except Exception as exc:
        observation["error_type"] = type(exc).__name__
        raise
    finally:
        observations.append(observation)


def project_review_input(frame: pd.DataFrame) -> pd.DataFrame:
    """Preserve present values and types; absent columns stay absent."""
    return frame[[name for name in INPUT_COLUMNS if name in frame]].copy()


def relative_source_paths(value: Any, root: Path) -> Any:
    if isinstance(value, list):
        return [relative_source_paths(item, root) for item in value]
    if not isinstance(value, dict):
        return value
    result = {}
    for key, item in value.items():
        if key == 'payload' and value.get('contract') == 'report-history-frame-v1':
            # The payload digest includes its observed source paths. Only
            # normalize the proof envelope; signed content is immutable.
            result[key] = item
            continue
        if key in {"path", "filtered_excel"} and isinstance(item, str):
            try:
                item = str(Path(item).relative_to(root))
            except ValueError:
                pass
        result[key] = relative_source_paths(item, root)
    return result


def review_export_input(official: pd.DataFrame) -> pd.DataFrame:
    from backend import report_builder as production

    current = production._ensure_export_formal_score_column(official.copy()).rename(
        columns=production.PROJECT_EXPORT_RENAME_MAPPING,
    )
    current = production._drop_duplicate_columns(current)
    return project_review_input(production._append_formal_group_column(current))


def incremental_review_candidates(frame: pd.DataFrame) -> list[str]:
    from backend import report_builder as production

    return sorted(production._normalize_code(row["基金代码"]) for row in frame.to_dict(orient="records")
                  if row.get("项目准入方式") == "增强债基补充准入"
                  and row.get("项目质量信心") == "中信心"
                  and production._is_secondary_share_row(row))


def replay_review(current: pd.DataFrame, history: dict[str, Any]) -> pd.DataFrame:
    from backend.forward_validation import _build_incremental_review_rows

    if history["status"] == "no_previous_snapshot":
        return pd.DataFrame()
    if history["status"] != "captured":
        raise ValueError("incremental_review_history_unavailable")
    return _build_incremental_review_rows(
        current, pd.DataFrame({"基金代码": history["previous_codes"]}),
    )


def review_demoted_codes(official: pd.DataFrame, review: pd.DataFrame) -> set[str]:
    from backend import report_builder as production

    seed = production._build_incremental_safety_watch_seed_df(official, review)
    return {production._normalize_code(row.get("基金代码")) for row in seed.to_dict(orient="records")
            if production._is_incremental_safety_watch_candidate(row)}


def verify_incremental_capture(
    frozen: dict[str, Any], frames: dict[str, pd.DataFrame], official_before_review: pd.DataFrame | None,
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    """Additional capture gate; never replace the original nine baseline checks."""
    from backend.screening_baseline_replay import compare_frame

    capture = frozen["inputs"].get("incremental_review_capture")
    if capture is None:
        return {"status": "not_recorded", "available": False, "blocks_baseline": False}, None
    try:
        if (not isinstance(capture, dict) or capture.get("contract") != CONTRACT
                or capture.get("schema_version") != 1
                or (capture.get("run_id"), capture.get("report_generated_at"), capture.get("snapshot_date"))
                != (frozen["run_id"], frozen["generated_at"], frozen["snapshot_date"])):
            raise ValueError("incremental_review_capture_identity_mismatch")
        selected_at = datetime.fromisoformat(capture["selection_generated_at"])
        report_at = datetime.fromisoformat(frozen["generated_at"])
        if selected_at.tzinfo is not None or report_at.tzinfo is not None:
            from zoneinfo import ZoneInfo
            def local(value):
                return value.astimezone(ZoneInfo("Asia/Shanghai")).replace(tzinfo=None) if value.tzinfo else value
            selected_at, report_at = local(selected_at), local(report_at)
        if selected_at > report_at:
            raise ValueError("incremental_review_selection_after_report")
        if capture.get("status") == "incomplete":
            return {"status": "incomplete", "available": False, "blocks_baseline": False,
                    "reason": capture.get("reason"), "source_issues": (capture.get("selection") or {}).get("issues", [])}, None
        if capture.get("status") not in {"captured", "no_previous_snapshot"}:
            raise ValueError("incremental_review_capture_status_invalid")
        if official_before_review is None:
            return {"status": "baseline_stage_unavailable", "available": False, "blocks_baseline": False}, None
        selection = capture["selection"]
        selected = selection["selected_snapshots"]
        if selection.get("issues") or not selected:
            raise ValueError("incremental_review_selection_incomplete")
        if (selected[0]["run_id"], selected[0]["generated_at"], selected[0]["snapshot_date"]) != (
                capture["run_id"], capture["selection_generated_at"], capture["snapshot_date"]):
            raise ValueError("incremental_review_current_selection_mismatch")
        current = frames[FRAME_NAMES["current_input"]]
        observed_review = frames[FRAME_NAMES["review_result"]]
        expected_current = review_export_input(official_before_review)
        if list(current.columns) != list(expected_current.columns):
            raise ValueError("incremental_review_current_columns_mismatch")
        current_check = compare_frame("incremental_review_current_input", current, expected_current, list(current.columns))
        history: dict[str, Any] = {"contract": CONTRACT, "status": capture["status"]}
        if capture["status"] == "no_previous_snapshot":
            if len(selected) != 1 or capture.get("previous_snapshot") is not None:
                raise ValueError("incremental_review_no_previous_selection_mismatch")
            if FRAME_NAMES["previous_members"] in frames:
                raise ValueError("incremental_review_absent_previous_has_members")
        else:
            previous = capture["previous_snapshot"]
            if len(selected) < 2 or previous != selected[1]:
                raise ValueError("incremental_review_previous_selection_mismatch")
            previous_at = datetime.fromisoformat(previous["generated_at"])
            if previous_at.tzinfo is not None:
                from zoneinfo import ZoneInfo
                previous_at = previous_at.astimezone(ZoneInfo("Asia/Shanghai")).replace(tzinfo=None)
            if previous["snapshot_date"] >= capture["snapshot_date"] or previous_at > selected_at:
                raise ValueError("incremental_review_previous_not_as_of")
            source = previous["report_source"]
            if (not isinstance(source.get("sha256"), str) or len(source["sha256"]) != 64
                    or source.get("status") != "read" or not isinstance(source.get("bytes"), int) or source["bytes"] < 1):
                raise ValueError("incremental_review_previous_report_source_invalid")
            read = capture["previous_frame_read"]
            if read.get("status") != "complete" or not read.get("effective_source"):
                raise ValueError("incremental_review_previous_frame_unreadable")
            effective = read["effective_source"]
            if (effective not in read.get("sources", []) or effective.get("status") != "read"
                    or not isinstance(effective.get("sha256"), str) or len(effective["sha256"]) != 64):
                raise ValueError("incremental_review_previous_frame_source_invalid")
            members = frames[FRAME_NAMES["previous_members"]]
            if list(members.columns) != ["基金代码"] or len(members) != capture["previous_member_count"]:
                raise ValueError("incremental_review_previous_members_invalid")
            codes = members["基金代码"].tolist()
            if 'history_record' in effective:
                from core.utils import report_history_store as history_store
                proof=history_store.decode(effective['history_record'])
                history_store.verify_embedded(proof)
                original=history_store.payload_frame(proof['payload'])
                if (proof.get('path')!=previous['filtered_excel']
                        or proof['digest']!=effective['sha256']
                        or original['基金代码'].tolist()!=codes):
                    raise ValueError('incremental_review_history_record_binding_mismatch')
            if len(set(codes)) != len(codes) or any(not isinstance(code, str) or not code for code in codes):
                raise ValueError("incremental_review_previous_codes_invalid")
            history["previous_codes"] = codes
        rebuilt = replay_review(current, history)
        if list(rebuilt.columns) != list(observed_review.columns):
            raise ValueError("incremental_review_result_columns_mismatch")
        review_check = compare_frame("incremental_review_complete_result", observed_review, rebuilt, list(observed_review.columns))
        actual_demoted = sorted(review_demoted_codes(official_before_review, rebuilt))
        demotions_match = capture.get("demoted_codes") == actual_demoted
        passed = current_check["aligned"] and review_check["aligned"] and demotions_match
        check = {"status": "matched" if passed else "mismatch", "available": passed, "blocks_baseline": not passed,
                 "capture_status": capture["status"], "input_comparison": current_check, "result_comparison": review_check,
                 "demoted_codes_match": demotions_match, "demoted_codes": actual_demoted,
                 "previous_member_count": capture.get("previous_member_count"),
                 "later_source_reads": False, "scenario_percentiles_reused": False}
        return check, history if passed else None
    except (ValueError, KeyError, TypeError, AttributeError) as exc:
        return {"status": "mismatch", "available": False, "blocks_baseline": True, "reason": str(exc)}, None


def apply_scenario_incremental_review(context: dict[str, Any], result: dict[str, Any]) -> dict[str, Any]:
    from backend import report_builder as production

    if result["status"] != "computed":
        return result
    output = dict(result)
    official = result["official"]
    history = context.get("incremental_review_history")
    if history is None:
        missing = incremental_review_candidates(official)
        if missing:
            output.update(status="missing_frozen_incremental_review", missing_incremental_review_codes=missing)
            output["pre_incremental_selection"] = output.pop("official")
        return output
    review = replay_review(review_export_input(official), history)
    demoted = review_demoted_codes(official, review)
    if demoted:
        output["official"] = official.loc[~official["基金代码"].map(production._normalize_code).isin(demoted)].copy()
        if "publish_track" in output:
            output["publish_track"] = production._demote_incremental_safety_watch_codes(output["publish_track"], target_codes=demoted)
    output.update(incremental_review=review, incremental_review_removed_codes=sorted(demoted),
                  incremental_review_status=history["status"])
    return output
