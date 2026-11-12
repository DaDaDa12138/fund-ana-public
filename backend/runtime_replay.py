from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from backend.task_audit_storage import is_maintenance_audit_path, read_task_audit
from core.utils.core_csv_storage import artifact_is_file


PROJECT_ROOT = Path(__file__).resolve().parent.parent


def read_json_file(path: Path | str) -> dict[str, Any]:
    current = Path(path)
    try:
        if current.name.endswith((".json.gz", ".json.legacy.gz")) or is_maintenance_audit_path(current):
            return read_task_audit(current)
        payload = json.loads(current.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        return {}
    return payload if isinstance(payload, dict) else {}


def merge_runtime_artifacts(artifacts: dict[str, Any] | None) -> dict[str, Any]:
    current = artifacts if isinstance(artifacts, dict) else {}
    core = current.get("core")
    if not isinstance(core, dict):
        return current.copy()
    merged = core.copy()
    merged.update(current)
    return merged


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


def resolve_runtime_artifact_path(
    path_value: Any,
    *,
    project_root: Path | str = PROJECT_ROOT,
) -> Path | None:
    return _resolve_artifact_path(path_value, project_root=project_root)


def load_publish_chain_snapshot(
    artifacts: dict[str, Any] | None,
    *,
    project_root: Path | str = PROJECT_ROOT,
) -> dict[str, Any]:
    resolved_artifacts = merge_runtime_artifacts(artifacts)
    snapshot_path = _resolve_artifact_path(
        resolved_artifacts.get("publish_chain_snapshot_json"),
        project_root=project_root,
    )
    if snapshot_path is None or not snapshot_path.exists():
        return {}
    return read_json_file(snapshot_path)


def load_runtime_payload(settings, run_id: str) -> dict[str, Any]:
    report_path = Path(settings.runs_dir) / run_id / f"bond_analysis_report_{run_id}.json"
    if report_path.exists():
        return read_json_file(report_path)

    archive_path = Path(settings.archived_runs_dir) / run_id / f"bond_analysis_report_{run_id}.json"
    if archive_path.exists():
        return read_json_file(archive_path)

    raise RuntimeError(f"未找到 run_id={run_id} 的报告 JSON。")


def build_replay_precision_summary(
    artifacts: dict[str, Any] | None,
    *,
    project_root: Path | str = PROJECT_ROOT,
) -> dict[str, Any]:
    resolved_artifacts = merge_runtime_artifacts(artifacts)
    replay_input_keys = [
        "long_csv",
        "short_csv",
        "final_screening_decision_ledger_file",
        "bond_mainlist_quality_audit_json_file",
    ]
    resolved_replay_inputs: dict[str, str] = {}
    missing_replay_input_keys: list[str] = []
    for key in replay_input_keys:
        resolved = _resolve_artifact_path(
            resolved_artifacts.get(key),
            project_root=project_root,
        )
        if resolved is not None:
            resolved_replay_inputs[key] = str(resolved)
        if resolved is None or not artifact_is_file(resolved):
            missing_replay_input_keys.append(key)

    publish_chain_snapshot_path = _resolve_artifact_path(
        resolved_artifacts.get("publish_chain_snapshot_json"),
        project_root=project_root,
    )
    publish_chain_snapshot_available = bool(
        publish_chain_snapshot_path is not None and publish_chain_snapshot_path.exists()
    )
    publish_chain_snapshot = (
        read_json_file(publish_chain_snapshot_path)
        if publish_chain_snapshot_available
        else {}
    )
    publish_gate_inputs_available = bool(
        publish_chain_snapshot_available
        and bool((publish_chain_snapshot.get("dynamic_post_review_records") or []))
    )

    final_official_path = _resolve_artifact_path(
        resolved_artifacts.get("filtered_csv"),
        project_root=project_root,
    )
    final_official_csv_available = bool(
        final_official_path is not None and artifact_is_file(final_official_path)
    )
    exact_replay_inputs_available = not missing_replay_input_keys
    screening_snapshot = _resolve_artifact_path(resolved_artifacts.get("screening_input_snapshot_json_gz"), project_root=project_root)
    typed_inputs_available = bool(screening_snapshot and screening_snapshot.is_file())
    verification: dict[str, Any] = {"status": "not_verified", "baseline_verified": False}
    if typed_inputs_available:
        from backend.replay_evidence import read_evidence

        verification = read_evidence(Path(project_root), screening_snapshot)
    baseline_verified = bool(verification.get("baseline_verified"))
    if baseline_verified:
        status = "report_baseline_verified"
        status_label = "报告筛选基线已复现"
        detail = "冻结输入的评分、阈值、拒绝原因和完整名单核对通过；此证据不包括上游采集与风险计算重跑。"
    elif typed_inputs_available:
        status = "inputs_frozen"
        status_label = "输入已冻结，基线未通过验证"
        detail = "已保存带类型的历史输入，须实际回放并核对全部结果后才能用于策略比较。"
    elif final_official_csv_available:
        status = "reference_only"
        status_label = "历史结果可核对，基线未验证"
        detail = "保留了历史结果文件，但没有已通过的冻结输入回放；不据此声称精确复盘。"
    else:
        status = "missing"
        status_label = "无法精确回放"
        detail = "缺少核心历史输入，当前只能依赖现有数据库与残留产物做近似复盘。"

    return {
        "status": status,
        "status_label": status_label,
        "detail": detail,
        "input_availability": "frozen" if typed_inputs_available else "partial" if final_official_csv_available else "missing",
        "baseline_reproduction_status": verification.get("status"),
        "baseline_reproduction_verified": baseline_verified,
        "baseline_verification": verification,
        "upstream_collection_and_core_risk_recalculation_verified": False,
        "publish_gate_inputs_available": publish_gate_inputs_available,
        "typed_screening_inputs_available": typed_inputs_available,
        "exact_replay_inputs_available": bool(exact_replay_inputs_available),
        "publish_gate_exact_replay_available": baseline_verified,
        "final_layer_exact_replay_available": baseline_verified,
        "missing_replay_input_keys": missing_replay_input_keys,
        "resolved_replay_inputs": resolved_replay_inputs,
        "publish_chain_snapshot_json": str(publish_chain_snapshot_path) if publish_chain_snapshot_path else "",
        "publish_chain_snapshot_available": bool(publish_chain_snapshot_available),
        "final_official_csv": str(final_official_path) if final_official_path else "",
        "final_official_csv_available": bool(final_official_csv_available),
    }


def refresh_registry_replay_precision(
    registry: dict[str, Any], *, project_root: Path | str = PROJECT_ROOT,
) -> dict[str, Any]:
    """Refresh independent replay evidence without modifying a publication or its cache.

    Evidence may be written or invalidated after publication. Its status therefore
    cannot be cached solely by the publication generation or report mtime.
    """
    refreshed = dict(registry)
    by_run: dict[str, dict[str, Any]] = {}
    for group in ("canonical_runs", "superseded_runs"):
        if group not in registry:
            continue
        records = []
        for record in registry.get(group) or []:
            current = dict(record)
            precision = build_replay_precision_summary(current.get("artifacts"), project_root=project_root)
            current["replay_precision"] = precision
            by_run[str(current.get("run_id") or "")] = precision
            records.append(current)
        refreshed[group] = records
    layers = registry.get("automation_layers")
    if isinstance(layers, dict):
        layers = dict(layers)
        for key in ("current_published_run", "latest_successful_run"):
            record = layers.get(key)
            if isinstance(record, dict):
                current = dict(record)
                current["replay_precision"] = by_run.get(str(current.get("run_id") or "")) or build_replay_precision_summary(
                    current.get("artifacts"), project_root=project_root,
                )
                layers[key] = current
        refreshed["automation_layers"] = layers
    return refreshed
