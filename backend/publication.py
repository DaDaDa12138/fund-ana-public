from __future__ import annotations

import json
import shutil
from datetime import datetime
from pathlib import Path
import threading
from typing import Any
from uuid import uuid4

from backend.config import Settings
from backend.effective_nav import load_effective_nav_snapshot
from backend.risk_coverage_semantics import build_risk_coverage_threshold_semantics
from backend.runtime_replay import build_replay_precision_summary
from backend.state_normalizer import normalize_snapshot_payload
from backend.publication_transaction import (
    load_publication_head,
    publication_io_lock,
    recover_publication_aliases,
    validate_committed_entry,
)

PUBLICATION_FINAL_COUNT_DROP_ABS_GUARD = 5
PUBLICATION_FINAL_COUNT_DROP_RATIO_GUARD = 0.02
PUBLICATION_DYNAMIC_FINAL_COUNT_DROP_ABS_GUARD = 10
PUBLICATION_DYNAMIC_FINAL_COUNT_DROP_RATIO_GUARD = 0.05
PUBLICATION_SCREENABLE_UNIVERSE_DROP_ABS_GUARD = 100
PUBLICATION_SCREENABLE_UNIVERSE_DROP_RATIO_GUARD = 0.03
PUBLICATION_PUBLISHABLE_POOL_DROP_ABS_GUARD = 100
PUBLICATION_PUBLISHABLE_POOL_DROP_RATIO_GUARD = 0.03
PUBLICATION_UNIVERSE_COUNT_DROP_ABS_GUARD = 100
PUBLICATION_UNIVERSE_COUNT_DROP_RATIO_GUARD = 0.03
PUBLICATION_RISK_EFFECTIVE_COUNT_DROP_ABS_GUARD = 100
PUBLICATION_RISK_EFFECTIVE_COUNT_DROP_RATIO_GUARD = 0.03
NON_FORMAL_PUBLICATION_MODES = {"validation_only", "diagnostic_only"}

PUBLISH_BLOCKER_GOVERNANCE = {
    "publication_artifact_integrity_failed": {
        "severity": "blocking",
        "action_path": "report_refresh",
        "governance_stage": "publication_artifacts",
    },
    "publish_snapshot_missing": {
        "severity": "blocking",
        "action_path": "collection",
        "governance_stage": "nav_snapshot",
    },
    "nav_snapshot_not_ready": {
        "severity": "blocking",
        "action_path": "collection",
        "governance_stage": "nav_snapshot",
    },
    "no_final_candidates": {
        "severity": "blocking",
        "action_path": "screening",
        "governance_stage": "screening_output",
    },
    "risk_snapshot_not_aligned": {
        "severity": "blocking",
        "action_path": "risk",
        "governance_stage": "risk_snapshot",
    },
    "risk_coverage_insufficient": {
        "severity": "blocking",
        "action_path": "risk",
        "governance_stage": "risk_snapshot",
    },
    "risk_publish_gate_failed": {
        "severity": "blocking",
        "action_path": "risk",
        "governance_stage": "risk_snapshot",
    },
    "data_source_blank_in_final_list": {
        "severity": "blocking",
        "action_path": "data_governance",
        "governance_stage": "source_traceability",
    },
    "data_source_fallback_in_final_list": {
        "severity": "warning",
        "action_path": "data_governance",
        "governance_stage": "source_traceability",
    },
    "publication_contraction_watch": {
        "severity": "warning",
        "action_path": "report_refresh",
        "governance_stage": "publication_guard",
    },
    "publish_hold_by_contraction_guard": {
        "severity": "blocking",
        "action_path": "report_refresh",
        "governance_stage": "legacy_publication_guard",
    },
    "validation_refresh_not_publishable": {
        "severity": "info",
        "action_path": "report_refresh",
        "governance_stage": "publication_control",
    },
    "diagnostic_run_not_publishable": {
        "severity": "info",
        "action_path": "workflow",
        "governance_stage": "publication_control",
    },
    "formal_lineage_missing_risk_calculation": {
        "severity": "blocking",
        "action_path": "risk",
        "governance_stage": "workflow_lineage",
    },
}

_json_payload_cache_lock = threading.Lock()
_json_payload_cache: dict[str, dict[str, Any]] = {}
_publication_registry_build_lock = threading.Lock()
PUBLICATION_ARCHIVE_INDEX_SCHEMA_VERSION = 6


def _safe_json_load(path: Path) -> dict[str, Any]:
    try:
        stat = path.stat()
    except OSError:
        return {}
    cache_key = str(path.resolve())
    mtime_ns = int(stat.st_mtime_ns)
    size = int(stat.st_size)
    with _json_payload_cache_lock:
        cached = _json_payload_cache.get(cache_key)
        if cached and int(cached.get("mtime_ns") or -1) == mtime_ns and int(cached.get("size") or -1) == size:
            payload = cached.get("payload")
            return dict(payload) if isinstance(payload, dict) else {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        return {}
    if not isinstance(payload, dict):
        return {}
    payload, _ = normalize_snapshot_payload(payload)
    with _json_payload_cache_lock:
        _json_payload_cache[cache_key] = {
            "mtime_ns": mtime_ns,
            "size": size,
            "payload": dict(payload),
        }
    return payload


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    normalized_payload, _ = normalize_snapshot_payload(payload)
    temp_file = path.parent / f"{path.name}.{uuid4().hex}.tmp"
    temp_file.write_text(
        json.dumps(normalized_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    temp_file.replace(path)


def _registry_semantically_equal(left: dict[str, Any], right: dict[str, Any]) -> bool:
    current = _normalize_registry_artifact_paths_for_semantic_compare(dict(left or {}))
    cached = _normalize_registry_artifact_paths_for_semantic_compare(dict(right or {}))
    current.pop("generated_at", None)
    cached.pop("generated_at", None)
    return current == cached


def _looks_like_artifact_path(text: str, run_id: str) -> bool:
    normalized = str(text or "").strip().replace("\\", "/")
    suffix = Path(normalized).suffix.lower()
    return bool(run_id) and suffix in {".json", ".md", ".xlsx", ".csv"} and f"/{run_id}/" in normalized


def _rebase_artifact_path(
    settings: Settings,
    text: str,
    *,
    run_id: str,
    actual_run_dir: Path,
) -> str:
    raw = str(text or "").strip()
    if not _looks_like_artifact_path(raw, run_id):
        return raw
    from core.utils.core_csv_storage import artifact_is_file

    candidate = Path(raw)
    project_root = getattr(settings, "project_root", None)

    def _rebase_to_run_local(path_value: Path) -> Path:
        parts = list(path_value.parts)
        run_marker = ("reports", "runs", run_id)
        for index in range(len(parts) - len(run_marker) + 1):
            if tuple(parts[index:index + len(run_marker)]) == run_marker:
                suffix_parts = parts[index + len(run_marker):]
                return actual_run_dir / Path(*suffix_parts) if suffix_parts else actual_run_dir
        return actual_run_dir / path_value.name

    if candidate.is_absolute():
        if artifact_is_file(candidate):
            return raw
        rebased = _rebase_to_run_local(candidate)
        return str(rebased) if artifact_is_file(rebased) else raw

    if project_root:
        absolute_candidate = (Path(project_root) / candidate).resolve()
        if artifact_is_file(absolute_candidate):
            return raw
        rebased = _rebase_to_run_local(candidate)
        if artifact_is_file(rebased):
            return str(rebased.relative_to(project_root))
    return raw


def _rebase_payload_artifact_paths(
    settings: Settings,
    payload: dict[str, Any],
    *,
    report_path: Path,
) -> dict[str, Any]:
    run_id = str(payload.get("run_id") or "").strip()
    actual_run_dir = report_path.parent.resolve()
    if actual_run_dir.parent.name == "revisions":
        actual_run_dir = actual_run_dir.parent.parent
    if not run_id or not actual_run_dir.exists():
        return payload

    def _walk(value: Any) -> Any:
        if isinstance(value, dict):
            return {key: _walk(item) for key, item in value.items()}
        if isinstance(value, list):
            return [_walk(item) for item in value]
        if isinstance(value, str):
            return _rebase_artifact_path(
                settings,
                value,
                run_id=run_id,
                actual_run_dir=actual_run_dir,
            )
        return value

    normalized = _walk(payload)
    if not isinstance(normalized, dict):
        return normalized
    return normalized


def _normalize_artifact_path_for_integrity_compare(text: str, run_id: str) -> str:
    raw = str(text or "").strip().replace("\\", "/")
    suffix = Path(raw).suffix.lower()
    if not run_id or suffix not in {".json", ".md", ".xlsx", ".csv"}:
        return text

    parts = [part for part in raw.split("/") if part]
    markers = (
        ("reports", "runs", run_id),
        ("reports", "archive", "superseded_runs", run_id),
    )
    for marker in markers:
        marker_len = len(marker)
        for index in range(len(parts) - marker_len + 1):
            if tuple(parts[index:index + marker_len]) == marker:
                suffix_parts = parts[index + marker_len:]
                return "/".join([*marker, *suffix_parts]) if suffix_parts else "/".join(marker)
    return text


def _normalize_payload_artifact_paths_for_integrity_compare(payload: dict[str, Any]) -> dict[str, Any]:
    run_id = str((payload or {}).get("run_id") or "").strip()
    if not run_id:
        return dict(payload or {})

    def _walk(value: Any) -> Any:
        if isinstance(value, dict):
            return {key: _walk(item) for key, item in value.items()}
        if isinstance(value, list):
            return [_walk(item) for item in value]
        if isinstance(value, str):
            return _normalize_artifact_path_for_integrity_compare(value, run_id)
        return value

    normalized = _walk(payload)
    return normalized if isinstance(normalized, dict) else dict(payload or {})


def _publication_payloads_semantically_equal(left: dict[str, Any], right: dict[str, Any]) -> bool:
    return (
        _normalize_payload_artifact_paths_for_integrity_compare(left)
        == _normalize_payload_artifact_paths_for_integrity_compare(right)
    )


def _normalize_registry_artifact_paths_for_semantic_compare(payload: dict[str, Any]) -> dict[str, Any]:
    def _walk(value: Any, inherited_run_id: str = "") -> Any:
        if isinstance(value, dict):
            run_id = str(
                value.get("run_id")
                or value.get("latest_canonical_run_id")
                or value.get("current_published_run_id")
                or inherited_run_id
                or ""
            ).strip()
            return {key: _walk(item, run_id) for key, item in value.items()}
        if isinstance(value, list):
            return [_walk(item, inherited_run_id) for item in value]
        if isinstance(value, str):
            return _normalize_artifact_path_for_integrity_compare(value, inherited_run_id)
        return value

    normalized = _walk(payload)
    return normalized if isinstance(normalized, dict) else dict(payload or {})


def _apply_top_level_count_compat(payload: dict[str, Any] | None) -> tuple[dict[str, Any], bool]:
    current = dict(payload or {})
    counts = dict(current.get("counts") or {})
    changed = False
    compat_fields = (
        "final_count",
        "dynamic_final_count",
        "project_tracker_current_active_count",
        "stable_bond_final_count",
        "enhanced_bond_final_count",
    )
    for field in compat_fields:
        count_value = counts.get(field)
        if count_value is None:
            continue
        normalized_value = int(count_value or 0)
        if current.get(field) != normalized_value:
            current[field] = normalized_value
            changed = True
    return current, changed


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _safe_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _normalize_publish_blocker(blocker: dict[str, Any] | None) -> dict[str, Any]:
    payload = dict(blocker or {})
    code = str(payload.get("code") or "").strip()
    governance = PUBLISH_BLOCKER_GOVERNANCE.get(code, {})
    severity = str(
        payload.get("severity")
        or governance.get("severity")
        or "blocking"
    ).strip().lower() or "blocking"
    action_path = str(
        payload.get("action_path")
        or governance.get("action_path")
        or ""
    ).strip() or None
    governance_stage = str(
        payload.get("governance_stage")
        or governance.get("governance_stage")
        or ""
    ).strip() or None
    payload["severity"] = severity
    payload["action_path"] = action_path
    payload["governance_stage"] = governance_stage
    payload["blocking"] = bool(severity == "blocking")
    return payload


def _resolve_risk_freshness(payload: dict[str, Any]) -> dict[str, Any]:
    freshness = payload.get("database_freshness") or {}
    risk_freshness = freshness.get("risk_1y_freshness") or {}
    return risk_freshness if isinstance(risk_freshness, dict) else {}


def _resolve_publication_mode(payload: dict[str, Any]) -> str:
    mode = str(((payload.get("publication_control") or {}).get("mode") or "")).strip().lower()
    return mode


def _resolve_publication_control(payload: dict[str, Any]) -> dict[str, Any]:
    control = payload.get("publication_control")
    return control if isinstance(control, dict) else {}


def _resolve_publication_workflow_options(payload: dict[str, Any]) -> dict[str, Any]:
    control = _resolve_publication_control(payload)
    options = control.get("workflow_options")
    return options if isinstance(options, dict) else {}


def _workflow_options_make_diagnostic_only(options: dict[str, Any]) -> bool:
    if bool(options.get("reuse_existing_outputs")):
        return True
    if bool(options.get("skip_risk_calculation")):
        return True
    if options.get("run_screening") is False:
        return True
    return False


def _is_formal_publication_eligible(payload: dict[str, Any]) -> bool:
    control = _resolve_publication_control(payload)
    if control.get("eligible_for_formal_publication") is False:
        return False
    if _resolve_publication_mode(payload) in NON_FORMAL_PUBLICATION_MODES:
        return False
    if _workflow_options_make_diagnostic_only(_resolve_publication_workflow_options(payload)):
        return False
    return True


def _resolve_run_lineage_blockers(report_path: Path) -> list[dict[str, Any]]:
    run_dir = report_path.parent
    if run_dir.parent.name == "revisions":
        run_dir = run_dir.parent.parent
    index_path = run_dir / "logs" / "subprocess" / "subprocess_index.json"
    try:
        payload = json.loads(index_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        return []
    rows = payload.get("items") if isinstance(payload, dict) else None
    if not isinstance(rows, list) or not rows:
        return []

    def _row_text(row: Any) -> str:
        if not isinstance(row, dict):
            return ""
        command = row.get("command")
        command_text = " ".join(str(item) for item in command) if isinstance(command, list) else str(command or "")
        return " ".join(
            [
                str(row.get("task_name") or ""),
                command_text,
            ]
        ).lower()

    row_texts = [_row_text(row) for row in rows]
    has_screening = any(
        "daily_screening_master" in text
        or "daily_final_screening" in text
        or "最终筛选" in text
        for text in row_texts
    )
    has_risk_calculation = any("calculate-risk" in text for text in row_texts)
    if not has_screening or has_risk_calculation:
        return []
    return [
        {
            "code": "formal_lineage_missing_risk_calculation",
            "label": "风险重算血缘缺失",
            "detail": "当前 run 执行了筛选但 subprocess index 中缺少 calculate-risk 子任务，不允许作为正式发布真值。",
            "severity": "blocking",
            "action_path": "risk",
            "governance_stage": "workflow_lineage",
        }
    ]


def _resolve_risk_coverage_ratio(risk_freshness: dict[str, Any]) -> float:
    coverage_value = risk_freshness.get("coverage_ratio_vs_candidate")
    if coverage_value is not None:
        try:
            parsed = float(coverage_value)
            effective_fund_count = _safe_int(risk_freshness.get("risk_1y_effective_fund_count"))
            candidate_fund_count = _safe_int(
                risk_freshness.get("risk_candidate_fund_count")
                or risk_freshness.get("candidate_fund_count")
            )
            if parsed > 0 or effective_fund_count <= 0 or candidate_fund_count <= 0:
                return parsed
        except (TypeError, ValueError):
            pass

    candidate_fund_count = _safe_int(
        risk_freshness.get("risk_candidate_fund_count")
        or risk_freshness.get("candidate_fund_count")
    )
    effective_fund_count = _safe_int(risk_freshness.get("risk_1y_effective_fund_count"))
    if candidate_fund_count > 0 and effective_fund_count > 0:
        return effective_fund_count / candidate_fund_count

    latest_fund_count = _safe_int(risk_freshness.get("risk_1y_latest_fund_count"))
    if candidate_fund_count > 0 and latest_fund_count > 0:
        return latest_fund_count / candidate_fund_count
    return 0.0


def _enrich_snapshot_with_risk_freshness(
    payload: dict[str, Any],
    snapshot: dict[str, Any],
) -> dict[str, Any]:
    if not snapshot:
        return snapshot

    enriched = dict(snapshot)
    risk_freshness = _resolve_risk_freshness(payload)
    if not risk_freshness:
        return enriched

    snapshot_candidate_date = str(enriched.get("candidate_date") or "").strip()
    risk_candidate_date = str(risk_freshness.get("candidate_date") or "").strip()
    if snapshot_candidate_date and risk_candidate_date and snapshot_candidate_date != risk_candidate_date:
        return enriched

    if not str(enriched.get("risk_1y_effective_end_date") or "").strip():
        effective_end_date = str(risk_freshness.get("risk_1y_effective_end_date") or "").strip()
        if effective_end_date:
            enriched["risk_1y_effective_end_date"] = effective_end_date

    if _safe_int(enriched.get("risk_1y_effective_fund_count")) <= 0:
        effective_fund_count = _safe_int(risk_freshness.get("risk_1y_effective_fund_count"))
        if effective_fund_count > 0:
            enriched["risk_1y_effective_fund_count"] = effective_fund_count

    if enriched.get("risk_1y_effective_coverage_ratio_vs_candidate") is None:
        coverage_ratio = _resolve_risk_coverage_ratio(risk_freshness)
        if coverage_ratio > 0:
            enriched["risk_1y_effective_coverage_ratio_vs_candidate"] = round(coverage_ratio, 6)

    coverage_semantics = build_risk_coverage_threshold_semantics(
        risk_freshness,
        coverage_ratio=enriched.get("risk_1y_effective_coverage_ratio_vs_candidate"),
        coverage_sufficient=(
            bool(risk_freshness.get("coverage_sufficient"))
            if risk_freshness.get("coverage_sufficient") is not None
            else None
        ),
    )
    enriched.update(coverage_semantics)

    return enriched


def _resolve_snapshot_section(payload: dict[str, Any]) -> dict[str, Any]:
    freshness = (payload.get("database_freshness") or {}).get("effective_nav_snapshot")
    if isinstance(freshness, dict) and str(freshness.get("candidate_date") or "").strip():
        return _enrich_snapshot_with_risk_freshness(payload, freshness)

    tracker_summary = (payload.get("project_publish_summary") or {}).get("project_tracker_summary") or {}
    candidate_date = str(
        tracker_summary.get("latest_run_nav_snapshot_date")
        or tracker_summary.get("latest_nav_snapshot_date")
        or ""
    ).strip()
    if not candidate_date:
        return {}
    return {
        "candidate_date": candidate_date,
        "signature": str(
            tracker_summary.get("latest_run_snapshot_signature")
            or tracker_summary.get("latest_run_snapshot_sign")
            or ""
        ).strip(),
        "candidate_fund_count": None,
        "coverage_ratio_vs_total": None,
        "source": "project_tracker_summary",
    }


def _resolve_group_key(payload: dict[str, Any]) -> str:
    snapshot = _resolve_snapshot_section(payload)
    candidate_date = str(snapshot.get("candidate_date") or "").strip()
    if candidate_date:
        return f"nav:{candidate_date}"
    generated_at = str(payload.get("generated_at") or "").strip()
    if generated_at:
        return f"day:{generated_at[:10]}"
    run_id = str(payload.get("run_id") or "").strip()
    return f"run:{run_id}" if run_id else "unknown"


def _sort_key(record: dict[str, Any]) -> tuple[Any, ...]:
    snapshot = record.get("snapshot") or {}
    generated_at = str(record.get("generated_at") or "").strip()
    publishable = 1 if bool(record.get("publishable")) else 0
    risk_publish_ready = 1 if bool(record.get("risk_publish_ready", True)) else 0
    has_snapshot = 1 if str(snapshot.get("candidate_date") or "").strip() else 0
    final_count = int(record.get("final_count") or 0)
    candidate_fund_count = int(snapshot.get("candidate_fund_count") or 0)
    coverage_ratio = _safe_float(snapshot.get("coverage_ratio_vs_total"))
    return (
        publishable,
        risk_publish_ready,
        generated_at,
        has_snapshot,
        final_count,
        candidate_fund_count,
        coverage_ratio,
        str(record.get("run_id") or ""),
    )


def _latest_sort_key(record: dict[str, Any]) -> tuple[Any, ...]:
    publishable = 1 if bool(record.get("publishable")) else 0
    generated_at = str(record.get("generated_at") or "").strip()
    return (
        publishable,
        generated_at,
        str(record.get("run_id") or ""),
    )


def _latest_run_sort_key(record: dict[str, Any]) -> tuple[Any, ...]:
    return (
        str(record.get("generated_at") or "").strip(),
        str(record.get("run_id") or ""),
    )


def _is_material_count_regression(
    current_value: int,
    candidate_value: int,
    *,
    abs_guard: int,
    ratio_guard: float,
) -> bool:
    if current_value <= 0 or candidate_value >= current_value:
        return False
    drop_count = current_value - candidate_value
    drop_ratio = drop_count / current_value
    return drop_count > abs_guard and drop_ratio > ratio_guard


def _is_material_final_count_regression(
    current_record: dict[str, Any],
    candidate_record: dict[str, Any],
) -> bool:
    current_count = int(current_record.get("final_count") or 0)
    candidate_count = int(candidate_record.get("final_count") or 0)
    return _is_material_count_regression(
        current_count,
        candidate_count,
        abs_guard=PUBLICATION_FINAL_COUNT_DROP_ABS_GUARD,
        ratio_guard=PUBLICATION_FINAL_COUNT_DROP_RATIO_GUARD,
    )


def _build_contraction_signal(
    *,
    field: str,
    label: str,
    baseline_value: int,
    candidate_value: int,
) -> dict[str, Any]:
    drop_count = max(0, baseline_value - candidate_value)
    drop_ratio = (drop_count / baseline_value) if baseline_value > 0 else 0.0
    return {
        "field": field,
        "label": label,
        "baseline_value": int(baseline_value),
        "candidate_value": int(candidate_value),
        "drop_count": int(drop_count),
        "drop_ratio": round(drop_ratio, 6),
    }


def _resolve_record_count(record: dict[str, Any], field: str) -> int:
    counts = record.get("counts") or {}
    return _safe_int(counts.get(field))


def _is_same_candidate_date_stable_publishable_comparison(
    current_record: dict[str, Any] | None,
    candidate_record: dict[str, Any] | None,
) -> bool:
    if not current_record or not candidate_record:
        return False
    current_snapshot = current_record.get("snapshot") or {}
    candidate_snapshot = candidate_record.get("snapshot") or {}
    current_candidate_date = str(current_snapshot.get("candidate_date") or "").strip()
    candidate_candidate_date = str(candidate_snapshot.get("candidate_date") or "").strip()
    if not current_candidate_date or current_candidate_date != candidate_candidate_date:
        return False
    if not bool(current_record.get("risk_publish_ready", True)) or not bool(candidate_record.get("risk_publish_ready", True)):
        return False
    return int(candidate_record.get("final_count") or 0) >= int(current_record.get("final_count") or 0)


def _resolve_publication_contraction_watchers(
    current_record: dict[str, Any] | None,
    candidate_record: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    if not current_record or not candidate_record:
        return []
    current_run_id = str(current_record.get("run_id") or "").strip()
    candidate_run_id = str(candidate_record.get("run_id") or "").strip()
    if not current_run_id or not candidate_run_id or current_run_id == candidate_run_id:
        return []
    if not bool(current_record.get("publishable")) or not bool(candidate_record.get("publishable")):
        return []

    current_snapshot = current_record.get("snapshot") or {}
    candidate_snapshot = candidate_record.get("snapshot") or {}
    allow_intentional_final_count_contraction = _allows_intentional_final_selection_contraction(
        current_record,
        candidate_record,
    )
    allow_same_snapshot_coverage_variation = _is_same_candidate_date_stable_publishable_comparison(
        current_record,
        candidate_record,
    )
    signals: list[dict[str, Any]] = []
    signal_fields: set[str] = set()

    def add_signal(field: str, label: str, baseline_value: int, candidate_value: int) -> None:
        signals.append(
            _build_contraction_signal(
                field=field,
                label=label,
                baseline_value=baseline_value,
                candidate_value=candidate_value,
            )
        )
        signal_fields.add(field)

    baseline_final_count = int(current_record.get("final_count") or 0)
    candidate_final_count = int(candidate_record.get("final_count") or 0)
    if (
        not allow_intentional_final_count_contraction
        and _is_material_count_regression(
            baseline_final_count,
            candidate_final_count,
            abs_guard=PUBLICATION_FINAL_COUNT_DROP_ABS_GUARD,
            ratio_guard=PUBLICATION_FINAL_COUNT_DROP_RATIO_GUARD,
        )
    ):
        add_signal("final_count", "正式主榜", baseline_final_count, candidate_final_count)

    baseline_dynamic_count = int(current_record.get("dynamic_final_count") or 0)
    candidate_dynamic_count = int(candidate_record.get("dynamic_final_count") or 0)
    if _is_material_count_regression(
        baseline_dynamic_count,
        candidate_dynamic_count,
        abs_guard=PUBLICATION_DYNAMIC_FINAL_COUNT_DROP_ABS_GUARD,
        ratio_guard=PUBLICATION_DYNAMIC_FINAL_COUNT_DROP_RATIO_GUARD,
    ):
        add_signal("dynamic_final_count", "动态参考榜", baseline_dynamic_count, candidate_dynamic_count)

    baseline_screenable_universe_count = _resolve_record_count(current_record, "screenable_universe_count")
    candidate_screenable_universe_count = _resolve_record_count(candidate_record, "screenable_universe_count")
    preferred_scope_available = (
        baseline_screenable_universe_count > 0 and candidate_screenable_universe_count > 0
    )
    if (
        not allow_same_snapshot_coverage_variation
        and _is_material_count_regression(
            baseline_screenable_universe_count,
            candidate_screenable_universe_count,
            abs_guard=PUBLICATION_SCREENABLE_UNIVERSE_DROP_ABS_GUARD,
            ratio_guard=PUBLICATION_SCREENABLE_UNIVERSE_DROP_RATIO_GUARD,
        )
    ):
        add_signal(
            "screenable_universe_count",
            "正式可筛池",
            baseline_screenable_universe_count,
            candidate_screenable_universe_count,
        )

    baseline_publishable_pool_count = _resolve_record_count(current_record, "publishable_pool_count")
    candidate_publishable_pool_count = _resolve_record_count(candidate_record, "publishable_pool_count")
    publishable_pool_available = (
        baseline_publishable_pool_count > 0 and candidate_publishable_pool_count > 0
    )
    if (
        not allow_same_snapshot_coverage_variation
        and _is_material_count_regression(
            baseline_publishable_pool_count,
            candidate_publishable_pool_count,
            abs_guard=PUBLICATION_PUBLISHABLE_POOL_DROP_ABS_GUARD,
            ratio_guard=PUBLICATION_PUBLISHABLE_POOL_DROP_RATIO_GUARD,
        )
    ):
        add_signal(
            "publishable_pool_count",
            "发布候选池",
            baseline_publishable_pool_count,
            candidate_publishable_pool_count,
        )

    baseline_candidate_fund_count = _safe_int(current_snapshot.get("candidate_fund_count"))
    candidate_candidate_fund_count = _safe_int(candidate_snapshot.get("candidate_fund_count"))
    if not preferred_scope_available:
        if _is_material_count_regression(
            baseline_candidate_fund_count,
            candidate_candidate_fund_count,
            abs_guard=PUBLICATION_UNIVERSE_COUNT_DROP_ABS_GUARD,
            ratio_guard=PUBLICATION_UNIVERSE_COUNT_DROP_RATIO_GUARD,
        ):
            add_signal(
                "candidate_fund_count",
                "可筛宇宙",
                baseline_candidate_fund_count,
                candidate_candidate_fund_count,
            )

    baseline_risk_effective_fund_count = _safe_int(current_snapshot.get("risk_1y_effective_fund_count"))
    candidate_risk_effective_fund_count = _safe_int(candidate_snapshot.get("risk_1y_effective_fund_count"))
    if (
        not allow_same_snapshot_coverage_variation
        and _is_material_count_regression(
            baseline_risk_effective_fund_count,
            candidate_risk_effective_fund_count,
            abs_guard=PUBLICATION_RISK_EFFECTIVE_COUNT_DROP_ABS_GUARD,
            ratio_guard=PUBLICATION_RISK_EFFECTIVE_COUNT_DROP_RATIO_GUARD,
        )
    ):
        add_signal(
            "risk_1y_effective_fund_count",
            "风险覆盖",
            baseline_risk_effective_fund_count,
            candidate_risk_effective_fund_count,
        )

    should_watch = (
        "final_count" in signal_fields
        or "screenable_universe_count" in signal_fields
        or "publishable_pool_count" in signal_fields
        or (
            not preferred_scope_available
            and {
                "candidate_fund_count",
                "risk_1y_effective_fund_count",
            }.issubset(signal_fields)
        )
        or (
            not preferred_scope_available
            and not publishable_pool_available
            and len(signal_fields) >= 3
        )
    )
    if not should_watch:
        return []

    baseline_candidate_date = str(current_snapshot.get("candidate_date") or "").strip() or "-"
    candidate_candidate_date = str(candidate_snapshot.get("candidate_date") or "").strip() or "-"
    detail_parts = [
        f"baseline={current_run_id}@{baseline_candidate_date}",
        f"candidate={candidate_run_id}@{candidate_candidate_date}",
        *[
            f"{signal['field']}:{signal['baseline_value']}->{signal['candidate_value']}"
            for signal in signals
        ],
    ]
    return [
        {
            "code": "publication_contraction_watch",
            "label": "发布收缩观察",
            "detail": "; ".join(detail_parts),
            "baseline_run_id": current_run_id,
            "candidate_run_id": candidate_run_id,
            "signal_count": len(signals),
            "signals": signals,
        }
    ]


def _should_promote_canonical_record(
    current_record: dict[str, Any],
    candidate_record: dict[str, Any],
) -> bool:
    current_publishable = bool(current_record.get("publishable"))
    candidate_publishable = bool(candidate_record.get("publishable"))
    if candidate_publishable != current_publishable:
        return candidate_publishable and not current_publishable

    current_risk_ready = bool(current_record.get("risk_publish_ready", True))
    candidate_risk_ready = bool(candidate_record.get("risk_publish_ready", True))
    if candidate_risk_ready != current_risk_ready:
        return candidate_risk_ready and not current_risk_ready

    return _latest_run_sort_key(candidate_record) > _latest_run_sort_key(current_record)


def _resolve_publish_flags(payload: dict[str, Any]) -> tuple[bool, bool]:
    counts = payload.get("counts") or {}
    freshness = payload.get("database_freshness") or {}
    effective_nav_snapshot = freshness.get("effective_nav_snapshot") or {}
    risk_freshness = _resolve_risk_freshness(payload)
    final_count = int(counts.get("final_count") or 0)
    nav_ready = bool(effective_nav_snapshot.get("ready", True))
    if risk_freshness:
        candidate_date = str(risk_freshness.get("candidate_date") or "").strip()
        effective_end_date = str(risk_freshness.get("risk_1y_effective_end_date") or "").strip()
        minimum_coverage_ratio = _safe_float(risk_freshness.get("minimum_coverage_ratio")) or 0.8
        dates_aligned = bool(
            risk_freshness.get("dates_aligned")
            if risk_freshness.get("dates_aligned") is not None
            else (candidate_date and effective_end_date and candidate_date == effective_end_date)
        )
        coverage_ratio = _resolve_risk_coverage_ratio(risk_freshness)
        risk_publish_ready = bool(
            risk_freshness.get("coverage_sufficient")
            or (dates_aligned and coverage_ratio >= minimum_coverage_ratio)
        )
    else:
        risk_publish_ready = bool(freshness.get("risk_1y_aligned_with_effective_nav", True))
    publishable = bool(final_count > 0 and nav_ready and risk_publish_ready)
    if not _is_formal_publication_eligible(payload):
        publishable = False
    return publishable, risk_publish_ready


def _resolve_publish_blockers(payload: dict[str, Any]) -> list[dict[str, Any]]:
    blockers: list[dict[str, Any]] = []
    counts = payload.get("counts") or {}
    freshness = payload.get("database_freshness") or {}
    effective_nav_snapshot = freshness.get("effective_nav_snapshot") or {}
    risk_freshness = _resolve_risk_freshness(payload)
    data_source_audit_summary = (
        (payload.get("project_publish_summary") or {}).get("data_source_audit_summary") or {}
    )
    artifact_audit = (payload.get("internal_audit") or {}).get("artifact_consistency")
    csv_return_provenance_checks_required = bool(
        _resolve_publication_control(payload).get("csv_return_provenance_checks_required")
    )
    drawdown_source_checks_required = bool(
        _resolve_publication_control(payload).get('drawdown_source_checks_required')
    )
    artifact_checks_required = bool(
        _resolve_publication_control(payload).get("artifact_checks_required")
    ) or csv_return_provenance_checks_required or drawdown_source_checks_required
    if (artifact_checks_required or artifact_audit is not None) and (
        (drawdown_source_checks_required and not _resolve_publication_control(payload).get('full_value_checks_required'))
        or not isinstance(artifact_audit, dict)
        or artifact_audit.get("aligned") is not True
        or bool(artifact_audit.get("blocker_count"))
        or not isinstance(artifact_audit.get("checks", []), list)
        or any(
            not isinstance(item, dict) or item.get("aligned") is not True
            for item in artifact_audit.get("checks", [])
        )
        or (artifact_checks_required and (
            not artifact_audit.get("checks")
            or artifact_audit.get("check_count") != len(artifact_audit.get("checks", []))
            or not {
                "formal_csv_tracker_membership", "formal_tracker_metric_values",
                "dynamic_csv_count", "publication_member_hard_gates",
            }.issubset({item.get("name") for item in artifact_audit.get("checks", [])})
        ))
        or (_resolve_publication_control(payload).get("full_value_checks_required") and not {
            "publication_input_signature", "formal_all_values_from_frozen_inputs", "formal_fee_and_source_provenance", "formal_workbook_complete_membership",
        }.issubset({item.get("name") for item in artifact_audit.get("checks", []) if isinstance(item, dict)}))
        or (csv_return_provenance_checks_required and not any(
            isinstance(item, dict) and item.get("name") == "csv_return_provenance_matches_frozen_inputs"
            for item in artifact_audit.get("checks", [])
        ))
        or (drawdown_source_checks_required and not any(
            isinstance(item, dict) and item.get('name') == 'formal_drawdown_matches_current_risk_source'
            for item in artifact_audit.get('checks', [])
        ))
    ):
        blockers.append(
            {
                "code": "publication_artifact_integrity_failed",
                "label": "发布产物不一致",
                "detail": "正式榜、Tracker 或同轮报告校验缺失/未通过；保留上一有效发布。",
            }
        )

    if _resolve_publication_control(payload).get("truthfulness_checks_required"):
        expected = {"publication_source_date_verified", "publication_selected_inputs_verified"}
        observed = {item.get("name") for item in (artifact_audit or {}).get("checks", [])
                    if isinstance(item, dict) and item.get("aligned") is True}
        if not expected.issubset(observed):
            blockers.append({"code": "publication_truthfulness_unverified", "label": "数据真实性输入未通过",
                             "detail": "新发布缺少有效数据日期、完整总回报输入或关键字段；保留上一有效发布。"})

    validation = payload.get("data_validation_audit") or {}
    if (validation.get("required") is True
            or (payload.get("data_sources") or {}).get("publication_validation_provider") == "eastmoney_cross_endpoint"):
        bound = any(isinstance(item, dict) and item.get("name") == "data_validation_snapshot_bound"
                    and item.get("aligned") is True for item in (artifact_audit or {}).get("checks", []))
        if (validation.get("required") is not True or not bound
                or validation.get("unavailable") or not validation.get("snapshot")):
            blockers.append({"code": "publication_data_validation_unverified", "label": "跨接口校验未通过",
                             "detail": "新来源缺少绑定当前报告的有效校验快照；保留上一有效发布。"})

    candidate_date = str(effective_nav_snapshot.get("candidate_date") or "").strip()
    if not candidate_date:
        blockers.append(
            {
                "code": "publish_snapshot_missing",
                "label": "快照缺失",
                "detail": "报告中缺少有效净值快照日期，无法判定发布口径。",
            }
        )
    elif not bool(effective_nav_snapshot.get("ready", True)):
        readiness_reason = str(effective_nav_snapshot.get("readiness_reason") or "").strip() or "effective_nav_not_ready"
        blockers.append(
            {
                "code": "nav_snapshot_not_ready",
                "label": "净值快照未就绪",
                "detail": readiness_reason,
            }
        )

    final_count = int(counts.get("final_count") or 0)
    if final_count <= 0:
        blockers.append(
            {
                "code": "no_final_candidates",
                "label": "正式主榜为空",
                "detail": "当前运行未生成可发布的正式推荐结果。",
            }
        )

    if risk_freshness:
        risk_candidate_date = str(risk_freshness.get("candidate_date") or "").strip()
        risk_effective_end_date = str(risk_freshness.get("risk_1y_effective_end_date") or "").strip()
        dates_aligned = bool(
            risk_freshness.get("dates_aligned")
            if risk_freshness.get("dates_aligned") is not None
            else (risk_candidate_date and risk_effective_end_date and risk_candidate_date == risk_effective_end_date)
        )
        coverage_ratio = _resolve_risk_coverage_ratio(risk_freshness)
        minimum_coverage_ratio = _safe_float(risk_freshness.get("minimum_coverage_ratio")) or 0.8
        coverage_sufficient = bool(risk_freshness.get("coverage_sufficient"))
        if not dates_aligned:
            blockers.append(
                {
                    "code": "risk_snapshot_not_aligned",
                    "label": "风险快照未对齐",
                    "detail": (
                        f"candidate_date={risk_candidate_date or '-'}, "
                        f"risk_end_date={risk_effective_end_date or '-'}"
                    ),
                }
            )
        elif not coverage_sufficient and coverage_ratio < minimum_coverage_ratio:
            blockers.append(
                {
                    "code": "risk_coverage_insufficient",
                    "label": "风险覆盖不足",
                    "detail": f"{coverage_ratio:.6f} < {minimum_coverage_ratio:.6f}",
                }
            )
    elif not bool(freshness.get("risk_1y_aligned_with_effective_nav", True)):
        blockers.append(
            {
                "code": "risk_publish_gate_failed",
                "label": "风险发布门禁未通过",
                "detail": "报告未提供可判定的 risk_1y_freshness，且对齐标记为 false。",
            }
        )

    final_blank_count = int(data_source_audit_summary.get("final_blank_count") or 0)
    final_fallback_count = int(data_source_audit_summary.get("final_fallback_count") or 0)
    if final_blank_count > 0:
        blockers.append(
            {
                "code": "data_source_blank_in_final_list",
                "label": "正式榜存在空来源记录",
                "detail": f"final_blank_count={final_blank_count}",
            }
        )
    elif final_fallback_count > 0:
        blockers.append(
            {
                "code": "data_source_fallback_in_final_list",
                "label": "正式榜存在回退来源记录",
                "detail": f"final_fallback_count={final_fallback_count}",
            }
        )

    publication_mode = _resolve_publication_mode(payload)
    if publication_mode == "validation_only":
        blockers.append(
            {
                "code": "validation_refresh_not_publishable",
                "label": "验证刷新不参与正式发布",
                "detail": "当前批次复用既有核心产物，仅用于验证或轻量刷新，不应改写正式发布指针。",
                "severity": "info",
                "action_path": "/api/publication",
                "governance_stage": "publication_control",
            }
        )
    elif not _is_formal_publication_eligible(payload):
        control = _resolve_publication_control(payload)
        blockers.append(
            {
                "code": "diagnostic_run_not_publishable",
                "label": "诊断运行不参与正式发布",
                "detail": (
                    str(control.get("reason") or "").strip()
                    or "当前批次缺少完整正式发布血缘，仅用于诊断或验证。"
                ),
                "severity": "info",
                "action_path": "workflow",
                "governance_stage": "publication_control",
            }
        )

    return [_normalize_publish_blocker(item) for item in blockers]


def _resolve_final_selection_contraction(payload: dict[str, Any]) -> dict[str, Any]:
    project_publish_summary = payload.get("project_publish_summary") or {}
    threshold_comparison = project_publish_summary.get("threshold_comparison") or {}
    publish_stage_audit = project_publish_summary.get("publish_stage_audit") or {}
    stage_rows = publish_stage_audit.get("stage_rows") or []
    final_stage = next(
        (
            item
            for item in stage_rows
            if isinstance(item, dict) and str(item.get("stage_key") or "").strip() == "final_selection"
        ),
        {},
    )

    removed_count = _safe_int(threshold_comparison.get("final_selection_removed_count"))
    if removed_count <= 0:
        removed_count = _safe_int(final_stage.get("removed_count"))

    stability_cap_removed_count = 0
    for item in final_stage.get("top_reasons") or []:
        if not isinstance(item, dict):
            continue
        if str(item.get("code") or "").strip() != "final_selection_stability_cap":
            continue
        stability_cap_removed_count = max(
            stability_cap_removed_count,
            _safe_int(item.get("count")),
        )

    input_count = _safe_int(final_stage.get("input_count"))
    passed_count = _safe_int(final_stage.get("passed_count"))
    if passed_count <= 0:
        passed_count = _safe_int((payload.get("counts") or {}).get("final_count"))

    return {
        "removed_count": removed_count,
        "input_count": input_count,
        "passed_count": passed_count,
        "stability_cap_removed_count": stability_cap_removed_count,
        "stability_cap_present": bool(stability_cap_removed_count > 0),
    }


def _allows_intentional_final_selection_contraction(
    current_record: dict[str, Any] | None,
    candidate_record: dict[str, Any] | None,
) -> bool:
    if not current_record or not candidate_record:
        return False

    contraction = candidate_record.get("final_selection_contraction") or {}
    if not bool(contraction.get("stability_cap_present")):
        return False
    if _safe_int(contraction.get("stability_cap_removed_count")) <= 0:
        return False
    if _safe_int(contraction.get("removed_count")) <= 0:
        return False

    current_dynamic_count = int(current_record.get("dynamic_final_count") or 0)
    candidate_dynamic_count = int(candidate_record.get("dynamic_final_count") or 0)
    if _is_material_count_regression(
        current_dynamic_count,
        candidate_dynamic_count,
        abs_guard=PUBLICATION_DYNAMIC_FINAL_COUNT_DROP_ABS_GUARD,
        ratio_guard=PUBLICATION_DYNAMIC_FINAL_COUNT_DROP_RATIO_GUARD,
    ):
        return False

    baseline_screenable_universe_count = _resolve_record_count(current_record, "screenable_universe_count")
    candidate_screenable_universe_count = _resolve_record_count(candidate_record, "screenable_universe_count")
    if _is_material_count_regression(
        baseline_screenable_universe_count,
        candidate_screenable_universe_count,
        abs_guard=PUBLICATION_SCREENABLE_UNIVERSE_DROP_ABS_GUARD,
        ratio_guard=PUBLICATION_SCREENABLE_UNIVERSE_DROP_RATIO_GUARD,
    ):
        return False

    baseline_publishable_pool_count = _resolve_record_count(current_record, "publishable_pool_count")
    candidate_publishable_pool_count = _resolve_record_count(candidate_record, "publishable_pool_count")
    if _is_material_count_regression(
        baseline_publishable_pool_count,
        candidate_publishable_pool_count,
        abs_guard=PUBLICATION_PUBLISHABLE_POOL_DROP_ABS_GUARD,
        ratio_guard=PUBLICATION_PUBLISHABLE_POOL_DROP_RATIO_GUARD,
    ):
        return False

    return True


def _build_record_summary(record: dict[str, Any] | None) -> dict[str, Any] | None:
    if not record:
        return None
    blockers = [_normalize_publish_blocker(item) for item in list(record.get("publish_blockers") or [])]
    snapshot = record.get("snapshot") or {}
    replay_precision = dict(record.get("replay_precision") or {})
    primary_blocker = blockers[0] if blockers else {}
    risk_coverage_ratio = _safe_float(
        snapshot.get("risk_1y_effective_coverage_ratio_vs_candidate")
        or snapshot.get("risk_1y_coverage_ratio_vs_candidate")
        or snapshot.get("coverage_ratio_vs_candidate")
    )
    risk_coverage_semantics = build_risk_coverage_threshold_semantics(
        snapshot,
        coverage_ratio=risk_coverage_ratio,
        coverage_sufficient=(
            bool(snapshot.get("coverage_sufficient"))
            if snapshot.get("coverage_sufficient") is not None
            else None
        ),
    )
    severity_order = {"blocking": 3, "warning": 2, "info": 1}
    highest_severity = "none"
    for item in blockers:
        severity = str(item.get("severity") or "").strip().lower()
        if severity_order.get(severity, 0) > severity_order.get(highest_severity, 0):
            highest_severity = severity
    publish_governance_summary = {
        "publishable": bool(record.get("publishable")),
        "risk_publish_ready": bool(record.get("risk_publish_ready", True)),
        "publish_blocked": not bool(record.get("publishable")),
        "blocker_total": len(blockers),
        "blocking_total": sum(1 for item in blockers if str(item.get("severity") or "").strip().lower() == "blocking"),
        "warning_total": sum(1 for item in blockers if str(item.get("severity") or "").strip().lower() == "warning"),
        "info_total": sum(1 for item in blockers if str(item.get("severity") or "").strip().lower() == "info"),
        "highest_severity": highest_severity,
        "primary_blocker_code": str(primary_blocker.get("code") or "").strip() or None,
        "primary_blocker_label": str(primary_blocker.get("label") or "").strip() or None,
        "primary_blocker_detail": str(primary_blocker.get("detail") or "").strip() or None,
        "primary_action_path": primary_blocker.get("action_path"),
        "primary_governance_stage": primary_blocker.get("governance_stage"),
        "status": (
            "publishable"
            if bool(record.get("publishable"))
            else ("blocked" if highest_severity == "blocking" else "held")
        ),
    }
    return {
        "run_id": str(record.get("run_id") or "").strip(),
        "generated_at": str(record.get("generated_at") or "").strip(),
        "group_key": str(record.get("group_key") or "").strip(),
        "final_count": int(record.get("final_count") or 0),
        "dynamic_final_count": int(record.get("dynamic_final_count") or 0),
        "project_tracker_current_active_count": int(
            _resolve_record_count(record, "project_tracker_current_active_count")
        ),
        "stable_bond_final_count": int(_resolve_record_count(record, "stable_bond_final_count")),
        "enhanced_bond_final_count": int(_resolve_record_count(record, "enhanced_bond_final_count")),
        "final_selection_contraction": dict(record.get("final_selection_contraction") or {}),
        "counts": {
            "screenable_universe_count": _resolve_record_count(record, "screenable_universe_count"),
            "publishable_pool_count": _resolve_record_count(record, "publishable_pool_count"),
            "project_tracker_current_active_count": _resolve_record_count(record, "project_tracker_current_active_count"),
            "stable_bond_final_count": _resolve_record_count(record, "stable_bond_final_count"),
            "enhanced_bond_final_count": _resolve_record_count(record, "enhanced_bond_final_count"),
        },
        "publishable": bool(record.get("publishable")),
        "risk_publish_ready": bool(record.get("risk_publish_ready", True)),
        "published": bool(record.get("published", False)),
        "publish_hold": bool(record.get("publish_hold", False)),
        "publish_hold_reason_codes": list(record.get("publish_hold_reason_codes") or []),
        "snapshot": {
            "candidate_date": str(snapshot.get("candidate_date") or "").strip() or None,
            "candidate_fund_count": _safe_int(snapshot.get("candidate_fund_count")),
            "coverage_ratio_vs_total": _safe_float(snapshot.get("coverage_ratio_vs_total")),
            "risk_1y_effective_end_date": str(snapshot.get("risk_1y_effective_end_date") or "").strip() or None,
            "risk_1y_effective_fund_count": _safe_int(snapshot.get("risk_1y_effective_fund_count")),
            "risk_1y_effective_coverage_ratio_vs_candidate": risk_coverage_ratio,
            "risk_coverage_semantics": risk_coverage_semantics,
        },
        "publish_blockers": blockers,
        "publish_blocked": not bool(record.get("publishable")),
        "primary_blocker_code": str(primary_blocker.get("code") or "").strip() if blockers else "",
        "primary_blocker_label": str(primary_blocker.get("label") or "").strip() if blockers else "",
        "primary_blocker_detail": str(primary_blocker.get("detail") or "").strip() if blockers else "",
        "publish_governance_summary": publish_governance_summary,
        "replay_precision": replay_precision,
        "report_path": str(record.get("report_path") or "").strip(),
    }


def _build_database_snapshot_summary(settings: Settings) -> dict[str, Any]:
    snapshot = load_effective_nav_snapshot(
        settings.database_path,
        coverage_ratio=settings.effective_nav_coverage_ratio,
        weekday_only=settings.effective_nav_weekday_only,
    ).to_dict()
    coverage_semantics = build_risk_coverage_threshold_semantics(
        {"coverage_ratio_vs_candidate": snapshot.get("risk_1y_effective_coverage_ratio_vs_candidate")}
    )
    return {
        "candidate_date": str(snapshot.get("candidate_date") or "").strip() or None,
        "candidate_fund_count": _safe_int(snapshot.get("candidate_fund_count")),
        "coverage_ratio_vs_total": _safe_float(snapshot.get("coverage_ratio_vs_total")),
        "ready": bool(snapshot.get("ready")),
        "readiness_reason": str(snapshot.get("readiness_reason") or "").strip() or None,
        "risk_1y_effective_end_date": str(snapshot.get("risk_1y_effective_end_date") or "").strip() or None,
        "risk_1y_effective_fund_count": _safe_int(snapshot.get("risk_1y_effective_fund_count")),
        "risk_1y_coverage_ratio_vs_candidate": coverage_semantics["coverage_ratio_vs_candidate"],
        "risk_coverage_semantics": coverage_semantics,
        "signature": str(snapshot.get("signature") or "").strip() or None,
    }


def _record_has_blocking_publish_blocker(record: dict[str, Any] | None) -> bool:
    if not isinstance(record, dict):
        return False
    for blocker in record.get("publish_blockers") or []:
        if not isinstance(blocker, dict):
            continue
        severity = str(blocker.get("severity") or "").strip().lower()
        if bool(blocker.get("blocking")) or severity == "blocking":
            return True
    return False


def _is_formal_processed_record(record: dict[str, Any] | None) -> bool:
    if not isinstance(record, dict) or not str(record.get("run_id") or "").strip():
        return False
    if _record_has_blocking_publish_blocker(record):
        return False
    return bool(
        record.get("publishable")
        or record.get("published")
    )


def _build_publication_integrity_summary(settings: Settings, registry: dict[str, Any]) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []

    def add_check(name: str, aligned: bool, detail: str, **extra: Any) -> None:
        payload: dict[str, Any] = {
            "name": name,
            "aligned": bool(aligned),
            "detail": detail,
        }
        payload.update(extra)
        checks.append(payload)

    latest_canonical_run_id = str(registry.get("latest_canonical_run_id") or "").strip()
    latest_canonical_report_path = Path(str(registry.get("latest_canonical_report_path") or "")).resolve() if str(registry.get("latest_canonical_report_path") or "").strip() else None
    add_check(
        "latest_canonical_run_declared",
        bool(latest_canonical_run_id),
        f"latest_canonical_run_id={latest_canonical_run_id or '-'}",
        latest_canonical_run_id=latest_canonical_run_id,
    )

    latest_report_payload = _safe_json_load(settings.latest_report_file)
    latest_report_run_id = str(latest_report_payload.get("run_id") or "").strip()
    add_check(
        "latest_json_matches_registry_run_id",
        bool(latest_canonical_run_id and latest_report_run_id and latest_canonical_run_id == latest_report_run_id),
        f"registry_run_id={latest_canonical_run_id or '-'}, latest_json_run_id={latest_report_run_id or '-'}",
        latest_canonical_run_id=latest_canonical_run_id,
        latest_json_run_id=latest_report_run_id,
    )

    add_check(
        "latest_canonical_report_exists",
        bool(latest_canonical_report_path and latest_canonical_report_path.exists() and latest_canonical_report_path.is_file()),
        f"latest_canonical_report_path={str(latest_canonical_report_path) if latest_canonical_report_path else '-'}",
        latest_canonical_report_path=str(latest_canonical_report_path) if latest_canonical_report_path else "",
    )

    canonical_payload = _safe_json_load(latest_canonical_report_path) if latest_canonical_report_path and latest_canonical_report_path.exists() else {}
    canonical_report_run_id = str(canonical_payload.get("run_id") or "").strip()
    add_check(
        "canonical_report_matches_registry_run_id",
        bool(latest_canonical_run_id and canonical_report_run_id and latest_canonical_run_id == canonical_report_run_id),
        f"registry_run_id={latest_canonical_run_id or '-'}, canonical_report_run_id={canonical_report_run_id or '-'}",
        latest_canonical_run_id=latest_canonical_run_id,
        canonical_report_run_id=canonical_report_run_id,
    )

    latest_matches_canonical = bool(
        latest_report_payload
        and canonical_payload
        and _publication_payloads_semantically_equal(latest_report_payload, canonical_payload)
    )
    add_check(
        "latest_json_matches_canonical_report_content",
        latest_matches_canonical,
        "latest.json 与 canonical 报告内容语义一致" if latest_matches_canonical else "latest.json 与 canonical 报告内容不一致",
    )

    aligned = all(bool(item.get("aligned")) for item in checks)
    blockers = [item for item in checks if not bool(item.get("aligned"))]
    return {
        "aligned": aligned,
        "check_count": len(checks),
        "blocker_count": len(blockers),
        "checks": checks,
        "blockers": blockers,
    }


def _resolve_current_published_record(
    settings: Settings,
    records: list[dict[str, Any]],
    latest_publishable_record: dict[str, Any] | None,
    *,
    allow_fallback: bool = True,
) -> dict[str, Any] | None:
    latest_report_file = getattr(settings, "latest_report_file", None)
    if latest_report_file:
        payload = _safe_json_load(Path(latest_report_file))
        if not _is_formal_publication_eligible(payload):
            payload = {}
        run_id = str(payload.get("run_id") or "").strip()
        if run_id:
            for record in records:
                if str(record.get("run_id") or "").strip() == run_id:
                    published_record = dict(record)
                    published_record["published"] = True
                    return published_record

    if allow_fallback and latest_publishable_record:
        published_record = dict(latest_publishable_record)
        published_record["published"] = True
        return published_record
    return None


def _merge_publication_watchers(
    record: dict[str, Any],
    extra_watchers: list[dict[str, Any]],
) -> dict[str, Any]:
    if not extra_watchers:
        return record
    blockers = [_normalize_publish_blocker(item) for item in list(record.get("publish_blockers") or [])]
    existing_codes = {
        str((item or {}).get("code") or "").strip()
        for item in blockers
        if isinstance(item, dict)
    }
    for item in extra_watchers:
        normalized_item = _normalize_publish_blocker(item)
        code = str((normalized_item or {}).get("code") or "").strip()
        if code and code in existing_codes:
            continue
        blockers.append(normalized_item)
        if code:
            existing_codes.add(code)
    updated = dict(record)
    updated["publish_blockers"] = blockers
    return updated


def _build_automation_layers(settings: Settings, registry: dict[str, Any]) -> dict[str, Any]:
    records = [
        *list(registry.get("canonical_runs") or []),
        *list(registry.get("superseded_runs") or []),
    ]
    # Validation/diagnostic refreshes are successful executions, but they are
    # deliberately outside the formal publication pipeline.  Letting one of
    # those records become ``latest_successful_run`` makes scheduler/dashboard
    # automation look blocked even though the current formal publication is
    # still healthy.  Records produced before this field existed remain
    # eligible so registry upgrades do not hide historical formal runs.
    successful_records = [
        record
        for record in records
        if record.get("formal_publication_eligible") is not False
    ]
    latest_successful_record = (
        sorted(successful_records, key=_latest_run_sort_key, reverse=True)[0]
        if successful_records
        else None
    )
    publishable_records = [record for record in records if bool(record.get("publishable"))]
    latest_publishable_record = publishable_records[0] if publishable_records else None
    formal_processed_records = [
        record for record in records if _is_formal_processed_record(record)
    ]
    latest_formal_processed_record = (
        sorted(formal_processed_records, key=_latest_run_sort_key, reverse=True)[0]
        if formal_processed_records
        else None
    )
    current_published_run_id = str(
        registry.get("current_published_run_id")
        or registry.get("latest_canonical_run_id")
        or ""
    ).strip()
    current_published_record = next(
        (
            {
                **record,
                "published": True,
            }
            for record in records
            if str(record.get("run_id") or "").strip() == current_published_run_id
        ),
        None,
    )
    if current_published_record is None:
        current_published_record = _resolve_current_published_record(
            settings,
            records,
            latest_publishable_record,
        )
    latest_successful_summary = _build_record_summary(latest_successful_record)
    latest_publishable_summary = _build_record_summary(latest_publishable_record)
    latest_formal_processed_summary = _build_record_summary(latest_formal_processed_record)
    current_published_summary = _build_record_summary(current_published_record)
    if current_published_summary:
        current_published_summary["published"] = True

    latest_successful_run_id = str((latest_successful_summary or {}).get("run_id") or "").strip()
    current_published_run_id = str((current_published_summary or {}).get("run_id") or "").strip()

    return {
        "latest_database_snapshot": _build_database_snapshot_summary(settings),
        "latest_successful_run": latest_successful_summary,
        "latest_formal_processed_run": latest_formal_processed_summary,
        "latest_publishable_run": latest_publishable_summary,
        "current_published_run": current_published_summary,
        "latest_successful_is_publishable": bool((latest_successful_summary or {}).get("publishable")),
        "latest_successful_is_currently_published": bool(
            latest_successful_run_id and current_published_run_id and latest_successful_run_id == current_published_run_id
        ),
        "latest_successful_blockers": list((latest_successful_summary or {}).get("publish_blockers") or []),
    }


def _collect_run_records(
    settings: Settings,
    *,
    candidate_report: tuple[Path, dict[str, Any]] | None = None,
    publication_head: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    seen_run_ids: set[str] = set()
    archived_runs_dir = getattr(settings, "archived_runs_dir", None)

    def _append_record(payload: dict[str, Any], report_path: Path, *, committed: bool = False) -> None:
        run_id = str(payload.get("run_id") or "").strip()
        if not run_id or run_id in seen_run_ids:
            return
        if (not committed and (payload.get("publication_control") or {}).get("commit_protocol")
                and _is_formal_publication_eligible(payload)):
            return  # Candidate discovery is never a publication operation.
        seen_run_ids.add(run_id)
        generated_at = str(payload.get("generated_at") or "").strip()
        snapshot = _resolve_snapshot_section(payload)
        publishable, risk_publish_ready = _resolve_publish_flags(payload)
        publish_blockers = _resolve_publish_blockers(payload)
        publish_blockers.extend(_resolve_run_lineage_blockers(report_path))
        publish_blockers = [_normalize_publish_blocker(item) for item in publish_blockers]
        if any(bool((item or {}).get("blocking")) for item in publish_blockers):
            publishable = False
        records.append(
            {
                "run_id": run_id,
                "generated_at": generated_at,
                "business_date": generated_at[:10] if generated_at else "",
                "publication_mode": _resolve_publication_mode(payload) or "formal",
                "formal_publication_eligible": _is_formal_publication_eligible(payload),
                "final_count": int((payload.get("counts") or {}).get("final_count") or 0),
                "dynamic_final_count": int((payload.get("counts") or {}).get("dynamic_final_count") or 0),
                "counts": dict(payload.get("counts") or {}),
                "publishable": publishable,
                "risk_publish_ready": risk_publish_ready,
                "publish_hold": False,
                "publish_hold_reason_codes": [],
                "group_key": _resolve_group_key(payload),
                "report_path": str(report_path),
                "run_dir": str(report_path.parent.parent.parent if report_path.parent.parent.name == "revisions" else report_path.parent),
                "publication_committed": committed,
                "snapshot": snapshot,
                "publish_blockers": publish_blockers,
                "final_selection_contraction": _resolve_final_selection_contraction(payload),
                "replay_precision": build_replay_precision_summary(
                    payload.get("artifacts") or {},
                    project_root=getattr(settings, "project_root", Path.cwd()),
                ),
                "artifacts": payload.get("artifacts") or {},
            }
        )

    head = publication_head if publication_head is not None else load_publication_head(settings)
    if candidate_report is not None:
        _append_record(candidate_report[1], candidate_report[0], committed=True)
    for entry in (head.get("committed_runs") or {}).values():
        if str(entry.get("run_id") or "") in seen_run_ids:
            continue
        report_path = validate_committed_entry(settings, entry)
        payload = _safe_json_load(report_path)
        if not payload or payload.get("run_id") != entry.get("run_id"):
            raise RuntimeError("committed_publication_report_invalid")
        _append_record(_rebase_payload_artifact_paths(settings, payload, report_path=report_path), report_path, committed=True)

    # P0-02.3 / B4: 当前 runs_dir 通常只剩数个进行中 / 未归档 run，始终全量解析。
    runs_dir = getattr(settings, "runs_dir", None)
    if runs_dir and Path(runs_dir).exists():
        for report_path in sorted(Path(runs_dir).glob("*/bond_analysis_report_*.json")):
            payload = _safe_json_load(report_path)
            if not payload:
                continue
            payload = _rebase_payload_artifact_paths(settings, payload, report_path=report_path)
            _append_record(payload, report_path)

    # 归档区可能有 150+ 个目录且内容不可变：走增量索引，mtime/size 匹配就复用缓存。
    if archived_runs_dir:
        archive_root = Path(archived_runs_dir)
        if archive_root.exists():
            index = _load_archive_index(settings)
            index_changed = False
            seen_archive_keys: set[str] = set()
            for report_path in sorted(archive_root.glob("*/bond_analysis_report_*.json")):
                try:
                    stat = report_path.stat()
                except OSError:
                    continue
                key = str(report_path)
                seen_archive_keys.add(key)
                mtime_ns = int(stat.st_mtime_ns)
                size = int(stat.st_size)
                cached_entry = index.get(key)
                if cached_entry and int(cached_entry.get("mtime_ns") or 0) == mtime_ns and int(cached_entry.get("size") or -1) == size:
                    cached_record = dict(cached_entry.get("record") or {})
                    precision = build_replay_precision_summary(
                        cached_record.get("artifacts"), project_root=getattr(settings, "project_root", Path.cwd()),
                    )
                    if precision != cached_record.get("replay_precision"):
                        cached_record["replay_precision"] = precision
                        index[key] = {**cached_entry, "record": cached_record}
                        index_changed = True
                    run_id = str(cached_record.get("run_id") or "").strip()
                    if run_id and run_id not in seen_run_ids:
                        seen_run_ids.add(run_id)
                        records.append(dict(cached_record))
                    continue

                payload = _safe_json_load(report_path)
                if not payload:
                    continue
                payload = _rebase_payload_artifact_paths(settings, payload, report_path=report_path)

                before_len = len(records)
                _append_record(payload, report_path)
                if len(records) == before_len:
                    continue
                record = dict(records[-1])
                index[key] = {
                    "mtime_ns": mtime_ns,
                    "size": size,
                    "record": record,
                }
                index_changed = True

            # 清理索引里那些已经不存在于归档目录的条目（手工删除 / 移走）。
            stale_keys = set(index.keys()) - seen_archive_keys
            if stale_keys:
                for key in stale_keys:
                    index.pop(key, None)
                index_changed = True

            if index_changed:
                _save_archive_index(settings, index)

    return records


def _archive_index_path(settings: Settings) -> Path:
    archived_runs_dir = getattr(settings, "archived_runs_dir", None)
    if archived_runs_dir:
        return Path(archived_runs_dir).parent / "_archive_index.json"
    return Path(settings.reports_dir) / "archive" / "_archive_index.json"


def _load_archive_index(settings: Settings) -> dict[str, dict[str, Any]]:
    index_path = _archive_index_path(settings)
    if not index_path.exists():
        return {}
    try:
        payload = json.loads(index_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        return {}
    if not isinstance(payload, dict):
        return {}
    if int(payload.get("schema_version") or 0) != PUBLICATION_ARCHIVE_INDEX_SCHEMA_VERSION:
        return {}
    entries = payload.get("entries")
    if not isinstance(entries, dict):
        return {}
    cleaned: dict[str, dict[str, Any]] = {}
    for key, entry in entries.items():
        if isinstance(key, str) and isinstance(entry, dict):
            cleaned[key] = entry
    return cleaned


def _save_archive_index(settings: Settings, index: dict[str, dict[str, Any]]) -> None:
    index_path = _archive_index_path(settings)
    index_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "schema_version": PUBLICATION_ARCHIVE_INDEX_SCHEMA_VERSION,
        "entries": index,
    }
    temp_file = index_path.parent / f"{index_path.name}.{uuid4().hex}.tmp"
    temp_file.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    temp_file.replace(index_path)


def build_publication_registry(
    settings: Settings,
    *,
    candidate_report: tuple[Path, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    head = load_publication_head(settings)
    records = _collect_run_records(settings, candidate_report=candidate_report, publication_head=head)
    groups: dict[str, list[dict[str, Any]]] = {}
    for record in records:
        groups.setdefault(str(record.get("group_key") or "unknown"), []).append(record)

    canonical_runs: list[dict[str, Any]] = []
    superseded_runs: list[dict[str, Any]] = []
    for group_key, group_records in groups.items():
        ordered = sorted(group_records, key=_latest_run_sort_key)
        canonical = ordered[0]
        for item in ordered[1:]:
            if _should_promote_canonical_record(canonical, item):
                canonical = item
        if bool(canonical.get("publishable")):
            previous_publishable_records = [
                record
                for record in ordered
                if bool(record.get("publishable"))
                and str(record.get("run_id") or "").strip() != str(canonical.get("run_id") or "").strip()
                and _latest_run_sort_key(record) < _latest_run_sort_key(canonical)
            ]
            if previous_publishable_records:
                previous_record = sorted(previous_publishable_records, key=_latest_run_sort_key, reverse=True)[0]
                contraction_watchers = _resolve_publication_contraction_watchers(previous_record, canonical)
                if contraction_watchers:
                    canonical = _merge_publication_watchers(canonical, contraction_watchers)
        canonical_record = {
            **canonical,
            "publish_state": "canonical",
            "superseded_run_ids": [
                item["run_id"]
                for item in sorted(
                    [record for record in group_records if record["run_id"] != canonical["run_id"]],
                    key=_latest_run_sort_key,
                    reverse=True,
                )
            ],
        }
        canonical_runs.append(canonical_record)
        for item in sorted(
            [record for record in group_records if record["run_id"] != canonical["run_id"]],
            key=_latest_run_sort_key,
            reverse=True,
        ):
            item_for_superseded = item
            superseded_runs.append(
                {
                    **item_for_superseded,
                    "publish_state": "superseded",
                    "canonical_run_id": canonical["run_id"],
                    "canonical_group_key": group_key,
                }
            )

    canonical_candidates = sorted(canonical_runs, key=_latest_run_sort_key)
    current_published_baseline = next(
        (
            item
            for item in canonical_candidates
            if bool(item.get("publishable"))
        ),
        None,
    )

    for candidate in canonical_candidates:
        if not bool(candidate.get("publishable")):
            continue
        if current_published_baseline is None:
            current_published_baseline = candidate
            continue
        if str(candidate.get("run_id") or "").strip() == str(current_published_baseline.get("run_id") or "").strip():
            continue
        if _latest_run_sort_key(candidate) <= _latest_run_sort_key(current_published_baseline):
            continue
        contraction_watchers = _resolve_publication_contraction_watchers(
            current_published_baseline,
            candidate,
        )
        if contraction_watchers:
            watched_run_id = str(candidate.get("run_id") or "").strip()
            watched_record = _merge_publication_watchers(candidate, contraction_watchers)
            for index, item in enumerate(canonical_runs):
                if str(item.get("run_id") or "").strip() == watched_run_id:
                    canonical_runs[index] = watched_record
                    candidate = watched_record
                    break
        current_published_baseline = candidate

    current_published_run_id = str((current_published_baseline or {}).get("run_id") or "").strip()
    if head and candidate_report is None:
        # A read may rebuild indexes but must never change an explicit commit.
        current_published_run_id = str(head["current_published_run_id"])
        committed = next((item for item in records if item["run_id"] == current_published_run_id), None)
        if committed is None or not committed.get("publishable"):
            raise RuntimeError("committed_publication_is_not_readable_or_publishable")
        group = str(committed.get("group_key") or "unknown")
        if not any(item["run_id"] == current_published_run_id for item in canonical_runs):
            replaced = [item for item in canonical_runs if item.get("group_key") == group]
            canonical_runs = [item for item in canonical_runs if item.get("group_key") != group]
            canonical_runs.append({**committed, "publish_state": "canonical", "superseded_run_ids": [item["run_id"] for item in replaced]})
            superseded_runs = [item for item in superseded_runs if item["run_id"] != current_published_run_id]
            superseded_runs.extend({**item, "publish_state": "superseded", "canonical_run_id": current_published_run_id, "canonical_group_key": group} for item in replaced)
    if current_published_run_id:
        for index, item in enumerate(canonical_runs):
            run_id = str(item.get("run_id") or "").strip()
            updated = dict(item)
            updated["published"] = bool(run_id and run_id == current_published_run_id)
            updated["publish_state"] = "published_canonical" if updated["published"] else "canonical"
            canonical_runs[index] = updated
    else:
        for index, item in enumerate(canonical_runs):
            updated = dict(item)
            updated["published"] = False
            updated["publish_state"] = "canonical"
            canonical_runs[index] = updated

    canonical_runs.sort(key=_latest_sort_key, reverse=True)
    superseded_runs.sort(key=lambda item: (str(item.get("generated_at") or ""), str(item.get("run_id") or "")), reverse=True)
    latest_canonical = next(
        (item for item in canonical_runs if bool(item.get("published"))),
        None,
    )
    if latest_canonical is None:
        latest_canonical = next(
            (item for item in canonical_runs if bool(item.get("publishable"))),
            canonical_runs[0] if canonical_runs else None,
        )
    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "commit_generation": head.get("generation") if head else None,
        "commit_protocol": head.get("schema_version") if head else None,
        "canonical_runs": canonical_runs,
        "superseded_runs": superseded_runs,
        "latest_canonical_run_id": latest_canonical.get("run_id") if latest_canonical else "",
        "latest_canonical_report_path": latest_canonical.get("report_path") if latest_canonical else "",
        "current_published_run_id": latest_canonical.get("run_id") if latest_canonical else "",
        "current_published_report_path": latest_canonical.get("report_path") if latest_canonical else "",
    }


def write_publication_registry(settings: Settings, registry: dict[str, Any]) -> None:
    _atomic_write_json(settings.publication_registry_file, registry)


def load_publication_registry(settings: Settings) -> dict[str, Any]:
    with publication_io_lock(settings), _publication_registry_build_lock:
        recover_publication_aliases(settings)
        registry = build_publication_registry(settings)
        registry["automation_layers"] = _build_automation_layers(settings, registry)
        registry["publication_integrity"] = _build_publication_integrity_summary(settings, registry)
        cached = _safe_json_load(settings.publication_registry_file) if settings.publication_registry_file.exists() else {}
        if not _registry_semantically_equal(registry, cached):
            write_publication_registry(settings, registry)
        current_published_report_path = str(registry.get("current_published_report_path") or "").strip()
        latest_report_file = getattr(settings, "latest_report_file", None)
        if current_published_report_path and latest_report_file:
            current_payload = _safe_json_load(Path(current_published_report_path))
            latest_payload = _safe_json_load(Path(latest_report_file))
            if current_payload and not _publication_payloads_semantically_equal(latest_payload, current_payload):
                _write_latest_report_payload(settings, current_payload)
                registry["publication_integrity"] = _build_publication_integrity_summary(settings, registry)
                if not _registry_semantically_equal(registry, _safe_json_load(settings.publication_registry_file)):
                    write_publication_registry(settings, registry)
        return registry


def _write_latest_report_payload(settings: Settings, payload: dict[str, Any]) -> None:
    if load_publication_head(settings):
        recover_publication_aliases(settings)
        return
    _atomic_write_json(settings.latest_report_file, payload)


def archive_superseded_runs(
    settings: Settings,
    *,
    keep_run_id: str | None = None,
) -> dict[str, Any]:
    with publication_io_lock(settings):
        return _archive_superseded_runs_locked(settings, keep_run_id=keep_run_id)


def _archive_superseded_runs_locked(settings: Settings, *, keep_run_id: str | None = None) -> dict[str, Any]:
    registry = build_publication_registry(settings)
    archived_items: list[dict[str, Any]] = []
    for record in registry.get("superseded_runs") or []:
        run_id = str(record.get("run_id") or "").strip()
        if record.get("publication_committed"):
            continue  # Immutable revisions remain at stable paths referenced by commit history.
        if not run_id or (keep_run_id and run_id == keep_run_id):
            continue
        source_dir = Path(str(record.get("run_dir") or ""))
        if not source_dir.exists() or source_dir.parent != settings.runs_dir:
            continue
        target_dir = settings.archived_runs_dir / source_dir.name
        if target_dir.exists():
            shutil.rmtree(target_dir)
        target_dir.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(source_dir), str(target_dir))
        archived_items.append(
            {
                "run_id": run_id,
                "source_dir": str(source_dir),
                "archived_dir": str(target_dir),
                "canonical_run_id": record.get("canonical_run_id"),
                "canonical_group_key": record.get("canonical_group_key"),
            }
        )

    refreshed_registry = build_publication_registry(settings)
    refreshed_registry["archived_superseded_runs"] = archived_items
    write_publication_registry(settings, refreshed_registry)
    return refreshed_registry


def load_canonical_report_payloads(settings: Settings) -> list[dict[str, Any]]:
    registry = load_publication_registry(settings)
    payloads: list[dict[str, Any]] = []
    published_run_id = str(
        registry.get("current_published_run_id")
        or registry.get("latest_canonical_run_id")
        or ""
    ).strip()
    records = sorted(registry.get("canonical_runs") or [], key=_latest_sort_key, reverse=True)
    if published_run_id:
        records.sort(
            key=lambda item: (
                1 if str(item.get("run_id") or "").strip() == published_run_id else 0,
                *_latest_sort_key(item),
            ),
            reverse=True,
        )
    for record in records:
        report_path = Path(str(record.get("report_path") or ""))
        if not report_path.exists():
            continue
        payload = _safe_json_load(report_path)
        if payload:
            payload = _rebase_payload_artifact_paths(settings, payload, report_path=report_path)
            payloads.append(payload)
    return payloads


def load_latest_canonical_report_payload(settings: Settings) -> dict[str, Any] | None:
    registry = load_publication_registry(settings)
    published_run_id = str(
        registry.get("current_published_run_id")
        or registry.get("latest_canonical_run_id")
        or ""
    ).strip()
    records = sorted(registry.get("canonical_runs") or [], key=_latest_sort_key, reverse=True)
    if published_run_id:
        records.sort(
            key=lambda item: (
                1 if str(item.get("run_id") or "").strip() == published_run_id else 0,
                *_latest_sort_key(item),
            ),
            reverse=True,
        )
    payload = None
    for record in records[:3]:
        report_path = Path(str(record.get("report_path") or ""))
        if not report_path.exists():
            continue
        loaded = _safe_json_load(report_path)
        if not loaded:
            continue
        payload = _rebase_payload_artifact_paths(settings, loaded, report_path=report_path)
        payload, compat_changed = _apply_top_level_count_compat(payload)
        if compat_changed and not record.get("publication_committed"):
            _atomic_write_json(report_path, payload)
        break
    if payload:
        current_latest = _safe_json_load(settings.latest_report_file)
        if not _publication_payloads_semantically_equal(current_latest, payload):
            _write_latest_report_payload(settings, payload)
    return payload


def load_current_published_report_payload(settings: Settings) -> dict[str, Any] | None:
    """严格读取当前正式发布；正式指针缺失或产物不可读时不回退诊断运行。"""
    registry = load_publication_registry(settings)
    published_run_id = str(registry.get("current_published_run_id") or "").strip()
    if not published_run_id:
        return None
    record = next(
        (
            item
            for item in registry.get("canonical_runs") or []
            if str((item or {}).get("run_id") or "").strip() == published_run_id
            and bool((item or {}).get("published"))
        ),
        None,
    )
    if not record:
        return None
    report_path = Path(str(record.get("report_path") or ""))
    if not report_path.exists() or not report_path.is_file():
        return None
    payload = _safe_json_load(report_path)
    if not payload or str(payload.get("run_id") or "").strip() != published_run_id:
        return None
    payload = _rebase_payload_artifact_paths(settings, payload, report_path=report_path)
    payload, _compat_changed = _apply_top_level_count_compat(payload)
    return payload
