from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def _normalize_code(value: Any) -> str:
    text = str(value or "").strip()
    return text.zfill(6) if text.isdigit() else text


def _project_path(settings: Any, path: Path) -> str:
    try:
        project_root = Path(getattr(settings, "project_root"))
        return str(path.resolve().relative_to(project_root.resolve()))
    except Exception:
        return str(path)


def _read_json_payload(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _latest_report_run_id(settings: Any) -> str:
    latest_report_file = getattr(settings, "latest_report_file", None)
    if not latest_report_file:
        return ""
    path = Path(latest_report_file)
    payload = _read_json_payload(path)
    return str(payload.get("run_id") or payload.get("source_report_run_id") or "").strip()


def _target_report_run(settings: Any) -> tuple[str, str]:
    """Return the run whose advisory governance queue should be consumed.

    Publication truth remains the current published report. The governance queue
    should prioritize the latest successful workflow run so held candidates can
    feed automatic AutoStock and long-term-history remediation.
    """

    publication_registry_file = getattr(settings, "publication_registry_file", None)
    if publication_registry_file:
        registry = _read_json_payload(Path(publication_registry_file))
        automation_layers = registry.get("automation_layers")
        if isinstance(automation_layers, dict):
            latest_successful = automation_layers.get("latest_successful_run")
            if isinstance(latest_successful, dict):
                run_id = str(latest_successful.get("run_id") or "").strip()
                if run_id:
                    return run_id, "publication_registry.automation_layers.latest_successful_run"
        latest_successful = registry.get("latest_successful_run")
        if isinstance(latest_successful, dict):
            run_id = str(latest_successful.get("run_id") or "").strip()
            if run_id:
                return run_id, "publication_registry.latest_successful_run"

    latest_report_run_id = _latest_report_run_id(settings)
    if latest_report_run_id:
        return latest_report_run_id, "latest_report_file"
    return "", ""


def _candidate_artifact_paths(settings: Any) -> list[Path]:
    reports_dir = getattr(settings, "reports_dir", None)
    if not reports_dir:
        return []
    audit_dir = Path(reports_dir) / "audit" / "final_selection_contraction_shadow"
    if not audit_dir.exists():
        return []
    return sorted(
        audit_dir.glob("final_selection_contraction_shadow_*.json"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )


def _extract_automation_queue(payload: dict[str, Any]) -> dict[str, Any]:
    return (
        (((payload.get("final_selection_contraction_shadow") or {}).get("incremental_candidate_safety_review") or {})
        .get("governance_summary") or {})
        .get("automation_queue") or {}
    )


def load_final_selection_governance_queue(settings: Any) -> dict[str, Any]:
    """Load the latest final-selection governance queue for the current report run.

    The queue is advisory only. Callers must keep their own safety gates, cooldowns,
    rate limits and candidate eligibility checks.
    """

    artifact_paths = _candidate_artifact_paths(settings)
    if not artifact_paths:
        return {
            "available": False,
            "reason": "final_selection_governance_artifact_missing",
            "queue_rows": [],
            "codes_by_action": {},
        }

    target_run_id, target_run_source = _target_report_run(settings)
    latest_available_payload: tuple[Path, dict[str, Any], dict[str, Any]] | None = None
    for path in artifact_paths:
        payload = _read_json_payload(path)
        if not payload:
            continue
        queue = _extract_automation_queue(payload)
        if not isinstance(queue, dict) or not queue.get("available"):
            continue
        if latest_available_payload is None:
            latest_available_payload = (path, payload, queue)
        artifact_run_id = str(payload.get("run_id") or payload.get("source_report_run_id") or "").strip()
        if target_run_id and artifact_run_id != target_run_id:
            continue
        return _normalize_queue_payload(
            settings,
            path,
            payload,
            queue,
            target_run_id=target_run_id,
            target_run_source=target_run_source,
        )

    if latest_available_payload is None:
        return {
            "available": False,
            "reason": "final_selection_governance_queue_missing",
            "queue_rows": [],
            "codes_by_action": {},
        }
    if target_run_id:
        path, payload, _ = latest_available_payload
        return {
            "available": False,
            "reason": "target_run_final_selection_governance_queue_missing",
            "queue_rows": [],
            "codes_by_action": {},
            "target_report_run_id": target_run_id,
            "target_report_run_source": target_run_source or None,
            "current_report_run_id": target_run_id,
            "latest_available_artifact_path": str(path),
            "latest_available_artifact_project_path": _project_path(settings, path),
            "latest_available_run_id": str(payload.get("run_id") or "").strip() or None,
            "latest_available_source_report_run_id": str(payload.get("source_report_run_id") or "").strip() or None,
        }
    path, payload, queue = latest_available_payload
    return _normalize_queue_payload(
        settings,
        path,
        payload,
        queue,
        target_run_id=target_run_id,
        target_run_source=target_run_source,
        fallback_reason="latest_available_queue_without_target_run",
    )


def _normalize_queue_payload(
    settings: Any,
    artifact_path: Path,
    payload: dict[str, Any],
    queue: dict[str, Any],
    *,
    target_run_id: str = "",
    target_run_source: str = "",
    fallback_reason: str | None = None,
) -> dict[str, Any]:
    rows = [dict(row) for row in (queue.get("queue_rows") or []) if isinstance(row, dict)]
    codes_by_action: dict[str, list[str]] = {}
    for row in rows:
        action_key = str(row.get("action_key") or "").strip()
        code = _normalize_code(row.get("基金代码") or row.get("fund_code"))
        if not action_key or not code:
            continue
        codes = codes_by_action.setdefault(action_key, [])
        if code not in codes:
            codes.append(code)
    return {
        "available": True,
        "reason": fallback_reason or "ok",
        "artifact_path": str(artifact_path),
        "artifact_project_path": _project_path(settings, artifact_path),
        "run_id": str(payload.get("run_id") or "").strip() or None,
        "source_report_run_id": str(payload.get("source_report_run_id") or "").strip() or None,
        "target_report_run_id": target_run_id or None,
        "target_report_run_source": target_run_source or None,
        "current_report_run_id": target_run_id or None,
        "generated_at": payload.get("generated_at"),
        "queue_total": int(queue.get("queue_total") or len(rows)),
        "candidate_code_total": int(queue.get("candidate_code_total") or 0),
        "queues": queue.get("queues") or {},
        "queue_rows": rows,
        "codes_by_action": codes_by_action,
    }


def load_final_selection_governance_action_codes(settings: Any, action_key: str) -> set[str]:
    payload = load_final_selection_governance_queue(settings)
    if not payload.get("available"):
        return set()
    normalized_action_key = str(action_key or "").strip()
    return {
        _normalize_code(code)
        for code in ((payload.get("codes_by_action") or {}).get(normalized_action_key) or [])
        if _normalize_code(code)
    }
