from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
from core.utils.core_csv_storage import artifact_is_file, read_csv_artifact

from backend import report_builder as _report_builder
from backend.audit_support import read_json_file
from backend.project_publish_decisions import DATA_VALIDATION_COLUMNS, validation_status_value
from backend.runtime_replay import (
    build_replay_precision_summary,
    load_publish_chain_snapshot,
    load_runtime_payload,
    merge_runtime_artifacts,
    resolve_runtime_artifact_path,
)


PROJECT_ROOT = Path(__file__).resolve().parent.parent


def normalize_code(value: Any) -> str:
    return _report_builder._normalize_code(value)
def _resolve_artifact_path(
    path_value: Any,
    *,
    project_root: Path | str = PROJECT_ROOT,
) -> Path | None:
    if not path_value:
        return None
    root = Path(project_root)
    candidate = root / str(path_value)
    if artifact_is_file(candidate):
        return candidate

    relative = Path(str(path_value))
    parts = relative.parts
    if len(parts) >= 3 and parts[0] == "reports" and parts[1] == "runs":
        archive_candidate = root / "reports" / "archive" / "superseded_runs" / Path(*parts[2:])
        if artifact_is_file(archive_candidate):
            return archive_candidate
    return candidate
def build_candidate_universe(settings, artifacts: dict[str, Any]) -> pd.DataFrame:
    resolved_artifacts = merge_runtime_artifacts(artifacts)
    project_root = getattr(settings, "project_root", PROJECT_ROOT)
    long_df = _report_builder._read_tabular_file(
        _resolve_artifact_path(resolved_artifacts.get("long_csv"), project_root=project_root)
    )
    short_df = _report_builder._read_tabular_file(
        _resolve_artifact_path(resolved_artifacts.get("short_csv"), project_root=project_root)
    )
    metadata_df = _report_builder._read_metadata(settings.database_path)
    manager_snapshot_df = _report_builder._read_manager_snapshot(settings.database_path)
    manager_metrics_df = _report_builder._build_manager_metrics(metadata_df, manager_snapshot_df)
    long_term_feature_df, _ = _report_builder._build_long_term_feature_frame(settings.database_path)
    returns_df = _report_builder._read_return_snapshot(settings.database_path)
    risk_df = _report_builder._read_risk_snapshot(settings.database_path)
    accum_nav_df = _report_builder._read_latest_accum_nav(settings.database_path)
    pick_metrics_df = _report_builder._read_pick_snapshot_metrics(settings.database_path)
    support_df = _report_builder._build_support_frame(long_df, short_df)
    candidate_records_df = _report_builder._load_candidate_record_df(
        _resolve_artifact_path(
            resolved_artifacts.get("final_screening_decision_ledger_file"),
            project_root=project_root,
        )
    )
    return _report_builder._build_project_candidate_universe(
        candidate_records_df=candidate_records_df,
        metadata_df=metadata_df,
        manager_metrics_df=manager_metrics_df,
        returns_df=returns_df,
        risk_df=risk_df,
        long_term_feature_df=long_term_feature_df,
        accum_nav_df=accum_nav_df,
        pick_metrics_df=pick_metrics_df,
        support_df=support_df,
    )


def display_status_to_raw_status(value: Any) -> str:
    text = str(value or "").strip()
    mapping = {
        "通过": "strict_match",
        "通过（含软告警）": "soft_pass_with_warnings",
        "拦截": "hard_block",
        "未校验": "not_checked",
    }
    return mapping.get(text, "")


def load_actual_autostock_status_map(
    artifacts: dict[str, Any],
    *,
    project_root: Path | str = PROJECT_ROOT,
) -> dict[str, dict[str, str]]:
    def _clean_text(value: Any) -> str:
        if value is None or pd.isna(value):
            return ""
        text = str(value).strip()
        return "" if text.lower() in {"nan", "none", "null", "nat"} else text

    def _status_priority(raw_status: str) -> int:
        # 同一基金可能在固定/动态轨道留下不同状态。回放时保留更保守且
        # 信息量更高的证据，尤其不能让后读到的 unknown 覆盖 hard block。
        return {
            "hard_block": 4,
            "soft_pass_with_warnings": 3,
            "strict_match": 2,
            "not_checked": 1,
            "unknown": 0,
            "": 0,
        }.get(str(raw_status or "").strip(), 1)

    def _is_neutral(status: dict[str, str]) -> bool:
        return any(column in status for column in DATA_VALIDATION_COLUMNS)

    def _raw_status(status: dict[str, str]) -> str:
        if _is_neutral(status) and "数据校验原始状态" in status:
            return _report_builder._normalize_autostock_status(status["数据校验原始状态"])
        return _report_builder._normalize_autostock_status(validation_status_value(status))

    status_map: dict[str, dict[str, str]] = {}
    resolved_artifacts = merge_runtime_artifacts(artifacts)
    root = Path(project_root)
    source_specs = [
        ("dynamic_watch_csv", True),
        ("watch_csv", True),
        ("dynamic_filtered_csv", False),
        ("filtered_csv", False),
    ]
    for artifact_key, has_raw_status in source_specs:
        path_value = resolved_artifacts.get(artifact_key)
        if not path_value:
            continue
        path = _resolve_artifact_path(path_value, project_root=root)
        if path is None:
            continue
        if not artifact_is_file(path):
            continue
        df = read_csv_artifact(path, dtype=str)
        if df.empty or "基金代码" not in df.columns:
            continue
        for row in df.to_dict(orient="records"):
            code = normalize_code(row.get("基金代码"))
            if not code:
                continue
            if any(column in df.columns for column in DATA_VALIDATION_COLUMNS):
                # These fields describe a different provider. Legacy display
                # columns in the same export cannot supply a missing result.
                candidate = {column: _clean_text(row.get(column)) for column in DATA_VALIDATION_COLUMNS}
                if "数据校验原始状态" not in df.columns:
                    candidate["数据校验原始状态"] = display_status_to_raw_status(candidate["数据校验状态"])
                raw_status = _raw_status(candidate)
            else:
                display_status = _clean_text(row.get("AutoStock校验状态"))
                raw_status = (_clean_text(row.get("AutoStock原始校验状态")) if has_raw_status
                              else display_status_to_raw_status(display_status))
                if not raw_status and display_status:
                    raw_status = display_status_to_raw_status(display_status)
                warning_text = (_clean_text(row.get("AutoStock告警原因"))
                                or _clean_text(row.get("AutoStock校验备注")))
                candidate = {
                    "AutoStock原始校验状态": raw_status,
                    "AutoStock告警原因": warning_text,
                    "AutoStock校验状态": display_status,
                    "AutoStock校验备注": warning_text,
                }
            existing = status_map.get(code)
            if existing is not None and _is_neutral(candidate) != _is_neutral(existing):
                # A current neutral observation supersedes an older provider's
                # observation, even when the current result is missing.
                if _is_neutral(candidate):
                    status_map[code] = candidate
                continue
            if existing is None or _status_priority(raw_status) > _status_priority(_raw_status(existing)):
                status_map[code] = candidate
            elif (
                _status_priority(raw_status)
                == _status_priority(_raw_status(existing))
            ):
                if _is_neutral(candidate):
                    # Keep the selected observation's source, contract and
                    # explicit missing values together. Only matching results
                    # from the same contract can supplement warning text.
                    if all(candidate[column] == existing[column] for column in
                           ("数据校验原始状态", "数据校验来源", "数据校验契约版本")):
                        status_map[code] = {**existing, **{
                            column: candidate[column] or existing[column]
                            for column in ("数据校验备注", "数据校验告警原因")
                        }}
                    continue
                # 同等级证据允许后续正式导出补齐展示文案，但不丢失已有告警。
                status_map[code] = {
                    "AutoStock原始校验状态": raw_status or existing.get("AutoStock原始校验状态", ""),
                    "AutoStock告警原因": warning_text or existing.get("AutoStock告警原因", ""),
                    "AutoStock校验状态": display_status or existing.get("AutoStock校验状态", ""),
                    "AutoStock校验备注": warning_text or existing.get("AutoStock校验备注", ""),
                }
    return status_map


def load_actual_official_df(
    artifacts: dict[str, Any],
    *,
    project_root: Path | str = PROJECT_ROOT,
) -> pd.DataFrame:
    resolved_artifacts = merge_runtime_artifacts(artifacts)
    filtered_csv = resolved_artifacts.get("filtered_csv")
    if not filtered_csv:
        return pd.DataFrame()
    path = _resolve_artifact_path(filtered_csv, project_root=project_root)
    if path is None:
        return pd.DataFrame()
    if not artifact_is_file(path):
        return pd.DataFrame()
    df = read_csv_artifact(path)
    if "基金代码" in df.columns:
        df["基金代码"] = df["基金代码"].map(normalize_code)
    return df
