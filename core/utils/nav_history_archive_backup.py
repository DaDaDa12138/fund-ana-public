#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
NAV 历史归档专用备份与 checklist 绑定工具。

该模块只负责生成/登记回滚点，并刷新只读 readiness 证据；不执行归档、
删除或 VACUUM。
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple

from utils.sqlite_backup_snapshot import create_verified_sqlite_backup

from utils.fund_code_alias_audit import load_latest_json_artifact
from utils.nav_history_archive_control_pack import (
    NAV_HISTORY_ARCHIVE_CONTROL_PACK_JSON_PATTERN,
)
from utils.nav_history_archive_execution_checklist import (
    NAV_HISTORY_ARCHIVE_EXECUTION_CHECKLIST_JSON_PATTERN,
    load_latest_nav_history_archive_execution_checklist_payload,
    write_nav_history_archive_execution_checklist_artifacts,
)
from utils.nav_history_archive_maintenance_readiness_audit import (
    build_nav_history_archive_maintenance_readiness_audit_payload,
    write_nav_history_archive_maintenance_readiness_audit_artifacts,
)
from utils.nav_history_archive_maintenance_window_pack import (
    NAV_HISTORY_ARCHIVE_MAINTENANCE_WINDOW_PACK_JSON_PATTERN,
)
from utils.nav_history_archive_preflight import NAV_HISTORY_ARCHIVE_PREFLIGHT_JSON_PATTERN


PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_OUTPUT_DIR = "reports/audit/nav_history_archive"
DEFAULT_BACKUP_PREFIX = "bond_funds_pre_nav_archive"
BACKUP_EVIDENCE_JSON_PATTERN = "nav_history_archive_backup_evidence_*.json"


def _normalize_text(value) -> str:
    return str(value or "").strip()


def _normalize_dict(value) -> dict:
    return value if isinstance(value, dict) else {}


def _resolve_project_path(value: str | Path, *, project_root: Path = PROJECT_ROOT) -> Path:
    path = Path(value).expanduser()
    if path.is_absolute():
        return path.resolve()
    return (project_root / path).resolve()


def _resolve_artifact_path(value: str | Path, *, project_root: Path = PROJECT_ROOT) -> Path:
    path = Path(value).expanduser()
    if path.is_absolute():
        resolved = path.resolve()
        if resolved.exists():
            return resolved
        parts = resolved.parts
        for marker in ("reports", "backend", "config", "core", "scripts", "frontend"):
            if marker in parts:
                index = parts.index(marker)
                candidate = (project_root / Path(*parts[index:])).resolve()
                if candidate.exists():
                    return candidate
        return resolved
    parts = path.parts
    for marker in ("reports", "backend", "config", "core"):
        if marker in parts:
            index = parts.index(marker)
            candidate = (project_root / Path(*parts[index:])).resolve()
            if candidate.exists():
                return candidate
    return _resolve_project_path(path, project_root=project_root)


def _project_relative(path: str | Path, *, project_root: Path = PROJECT_ROOT) -> str:
    resolved = _resolve_artifact_path(path, project_root=project_root)
    try:
        return resolved.relative_to(project_root).as_posix()
    except ValueError:
        return str(resolved)


def _now_ts() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _write_json_md(
    *,
    payload: dict,
    output_dir: str | Path,
    prefix: str,
    markdown_title: str,
    project_root: Path = PROJECT_ROOT,
    timestamp: Optional[str] = None,
) -> tuple[Path, Path]:
    ts = timestamp or _now_ts()
    out_dir = _resolve_project_path(output_dir, project_root=project_root)
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / f"{prefix}_{ts}.json"
    md_path = out_dir / f"{prefix}_{ts}.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    lines = [
        f"# {markdown_title}",
        "",
        f"- generated_at: {payload.get('generated_at', '')}",
        f"- status: {payload.get('status', '')}",
    ]
    for key, value in (_normalize_dict(payload.get("summary"))).items():
        lines.append(f"- {key}: {value}")
    lines.extend(["", "## findings"])
    for item in payload.get("findings") or []:
        lines.append(f"- {item}")
    md_path.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")
    return json_path, md_path


def create_nav_history_archive_backup(
    *,
    source_db_path: str | Path,
    backup_dir: str | Path,
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
    project_root: str | Path = PROJECT_ROOT,
    backup_prefix: str = DEFAULT_BACKUP_PREFIX,
    timestamp: Optional[str] = None,
    compress: bool = True,
    keep_uncompressed: bool = False,
    compute_sha256: bool = True,
    dry_run: bool = False,
) -> tuple[Path, Path, dict]:
    """生成 NAV 归档专用 SQLite 备份证据。"""
    root = Path(project_root).resolve()
    source_path = _resolve_project_path(source_db_path, project_root=root)
    resolved_backup_dir = _resolve_project_path(backup_dir, project_root=root)
    ts = timestamp or _now_ts()
    backup_label = f"{backup_prefix}_{ts}"
    db_backup_path = resolved_backup_dir / f"{backup_label}.db"
    planned_artifact_path = (
        db_backup_path.with_suffix(db_backup_path.suffix + ".zst")
        if compress
        else db_backup_path
    )

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "status": "dry-run" if dry_run else "pending",
        "mode": "dry-run" if dry_run else "create",
        "summary": {
            "source_db_path": str(source_path),
            "source_db_exists": source_path.exists() and source_path.is_file(),
            "source_db_size_bytes": source_path.stat().st_size if source_path.exists() else 0,
            "backup_dir": str(resolved_backup_dir),
            "backup_label": backup_label,
            "backup_db_path": _project_relative(db_backup_path, project_root=root),
            "backup_artifact_path": _project_relative(planned_artifact_path, project_root=root),
            "backup_artifact_format": "zstd_sqlite" if compress else "sqlite",
            "compress": bool(compress),
            "keep_uncompressed": bool(keep_uncompressed),
            "created": False,
            "sqlite_quick_check": "",
            "backup_artifact_size_bytes": 0,
            "backup_artifact_sha256": "",
            "zstd_test_succeeded": False,
            "elapsed_seconds": 0.0,
        },
        "findings": [],
    }
    if not source_path.exists() or not source_path.is_file():
        payload["status"] = "blocked"
        payload["findings"].append(f"source database does not exist: {source_path}")
        return _write_json_md(
            payload=payload,
            output_dir=output_dir,
            prefix="nav_history_archive_backup_evidence",
            markdown_title="nav history archive backup evidence",
            project_root=root,
            timestamp=ts,
        ) + (payload,)
    if dry_run:
        payload["findings"].append("dry-run only; no backup file was created.")
        return _write_json_md(
            payload=payload,
            output_dir=output_dir,
            prefix="nav_history_archive_backup_evidence",
            markdown_title="nav history archive backup evidence",
            project_root=root,
            timestamp=ts,
        ) + (payload,)

    backup_result = create_verified_sqlite_backup(
        source_path, db_backup_path, purpose="nav_history_archive", backup_label=backup_label,
        compress=compress, keep_uncompressed=keep_uncompressed,
        check_source=False,
    )
    artifact_path = backup_result["artifact_path"]
    compression_summary = backup_result
    quick_check = backup_result["sqlite_quick_check"]
    sha256 = backup_result["artifact_sha256"] if compute_sha256 else ""
    elapsed = backup_result["elapsed_seconds"]
    payload["status"] = "success"
    payload["summary"].update(
        {
            "created": True,
            "backup_db_path": _project_relative(db_backup_path, project_root=root),
            "backup_db_exists": db_backup_path.exists(),
            "backup_artifact_path": _project_relative(artifact_path, project_root=root),
            "backup_artifact_absolute_path": str(artifact_path),
            "backup_artifact_size_bytes": artifact_path.stat().st_size,
            "backup_artifact_size_mb": round(artifact_path.stat().st_size / (1024 * 1024), 2),
            "backup_artifact_sha256": sha256,
            "verification_manifest_path": _project_relative(
                backup_result["verification_manifest_path"], project_root=root),
            "verification_manifest_sha256": backup_result["verification_manifest_sha256"],
            "recovery_verification": backup_result["recovery_verification"],
            "snapshot_sha256": backup_result["snapshot_sha256"],
            "decoded_sha256": backup_result["decoded_sha256"],
            "snapshot_metadata": backup_result["snapshot_metadata"],
            "backup_space_budget": backup_result["backup_space_budget"],
            "sqlite_quick_check": quick_check,
            "zstd_test_succeeded": bool(compression_summary.get("zstd_test_succeeded")),
            "uncompressed_removed": bool(compression_summary.get("uncompressed_removed")),
            "compression_elapsed_seconds": compression_summary.get(
                "compression_elapsed_seconds",
                0.0,
            ),
            "elapsed_seconds": elapsed,
        }
    )
    payload["findings"].append(
        "NAV archive dedicated backup was created and verified; no archive/delete/VACUUM was executed."
    )
    return _write_json_md(
        payload=payload,
        output_dir=output_dir,
        prefix="nav_history_archive_backup_evidence",
        markdown_title="nav history archive backup evidence",
        project_root=root,
        timestamp=ts,
    ) + (payload,)


def _load_payload_from_ref(
    *,
    source_artifact: str,
    fallback_pattern: str,
    output_dir: str | Path,
    project_root: Path,
) -> tuple[Path, dict]:
    if source_artifact:
        path = _resolve_artifact_path(source_artifact, project_root=project_root)
        return path, json.loads(path.read_text(encoding="utf-8"))
    path, payload = load_latest_json_artifact(str(fallback_pattern), output_dir=str(output_dir))
    return _resolve_artifact_path(path, project_root=project_root), payload


def bind_nav_history_archive_backup_to_checklist(
    *,
    backup_artifact_path: str | Path,
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
    project_root: str | Path = PROJECT_ROOT,
    backup_evidence_artifact: Optional[str | Path] = None,
    source_execution_checklist_artifact: Optional[str | Path] = None,
    timestamp: Optional[str] = None,
) -> tuple[Path, Path, Path, Path, dict]:
    """把真实备份路径绑定到新的 execution checklist，并刷新 readiness。"""
    root = Path(project_root).resolve()
    out_dir = _resolve_project_path(output_dir, project_root=root)
    backup_path = _resolve_project_path(backup_artifact_path, project_root=root)
    if not backup_path.exists() or not backup_path.is_file():
        raise FileNotFoundError(f"backup artifact does not exist: {backup_path}")

    if source_execution_checklist_artifact:
        checklist_path = _resolve_artifact_path(
            source_execution_checklist_artifact,
            project_root=root,
        )
        checklist_payload = json.loads(checklist_path.read_text(encoding="utf-8"))
    else:
        checklist_path, checklist_payload = load_latest_nav_history_archive_execution_checklist_payload(
            output_dir=str(out_dir)
        )
        checklist_path = _resolve_artifact_path(checklist_path, project_root=root)

    checklist_payload = dict(checklist_payload or {})
    execution_record_template = dict(checklist_payload.get("execution_record_template") or {})
    execution_record_template["backup_artifact_path"] = _project_relative(
        backup_path,
        project_root=root,
    )
    if backup_evidence_artifact:
        execution_record_template["backup_evidence_artifact_path"] = _project_relative(
            backup_evidence_artifact,
            project_root=root,
        )
    execution_record_template["backup_bound_at"] = datetime.now().isoformat(timespec="seconds")
    if not _normalize_text(execution_record_template.get("vacuum_decision")):
        execution_record_template["vacuum_decision"] = "defer_to_followup_window"
    checklist_payload["execution_record_template"] = execution_record_template
    checklist_payload["generated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    checklist_payload["message"] = (
        "当前 NAV 归档执行清单已绑定当期专用主库备份；页面仍不提供 apply。"
    )
    checklist_payload["action_hint"] = (
        "真实归档、删除和 VACUUM 仍必须等待冷库查询兼容、回滚证据和维护窗口门禁闭合。"
    )

    summary = dict(checklist_payload.get("summary") or {})
    summary["backup_artifact_path"] = execution_record_template["backup_artifact_path"]
    summary["backup_artifact_exists"] = True
    summary["backup_artifact_size_mb"] = round(backup_path.stat().st_size / (1024 * 1024), 2)
    summary["page_apply_supported"] = False
    summary["execution_mode"] = "manual_window_only"
    checklist_payload["summary"] = summary
    findings = [
        str(item or "").strip()
        for item in (checklist_payload.get("findings") or [])
        if str(item or "").strip()
    ]
    findings.insert(0, f"已绑定当期 NAV 归档专用备份：{summary['backup_artifact_path']}")
    checklist_payload["findings"] = findings

    source_window_artifact = _normalize_text(
        checklist_payload.get("source_nav_history_archive_maintenance_window_pack_artifact")
    )
    source_control_artifact = _normalize_text(
        checklist_payload.get("source_nav_history_archive_control_pack_artifact")
    )
    source_preflight_artifact = _normalize_text(
        checklist_payload.get("source_nav_history_archive_preflight_artifact")
    )
    window_path, window_payload = _load_payload_from_ref(
        source_artifact=source_window_artifact,
        fallback_pattern=NAV_HISTORY_ARCHIVE_MAINTENANCE_WINDOW_PACK_JSON_PATTERN,
        output_dir=out_dir,
        project_root=root,
    )
    control_path, control_payload = _load_payload_from_ref(
        source_artifact=source_control_artifact,
        fallback_pattern=NAV_HISTORY_ARCHIVE_CONTROL_PACK_JSON_PATTERN,
        output_dir=out_dir,
        project_root=root,
    )
    preflight_path, preflight_payload = _load_payload_from_ref(
        source_artifact=source_preflight_artifact,
        fallback_pattern=NAV_HISTORY_ARCHIVE_PREFLIGHT_JSON_PATTERN,
        output_dir=out_dir,
        project_root=root,
    )
    checklist_payload["source_nav_history_archive_maintenance_window_pack_artifact"] = _project_relative(
        window_path,
        project_root=root,
    )
    checklist_payload["source_nav_history_archive_control_pack_artifact"] = _project_relative(
        control_path,
        project_root=root,
    )
    checklist_payload["source_nav_history_archive_preflight_artifact"] = _project_relative(
        preflight_path,
        project_root=root,
    )

    checklist_json_path, checklist_md_path = write_nav_history_archive_execution_checklist_artifacts(
        checklist_payload,
        output_dir=str(out_dir),
        timestamp=timestamp,
    )
    checklist_json_project_path = _project_relative(checklist_json_path, project_root=root)
    readiness_payload = build_nav_history_archive_maintenance_readiness_audit_payload(
        project_root=str(root),
        execution_checklist_payload=checklist_payload,
        maintenance_window_pack_payload=window_payload,
        control_pack_payload=control_payload,
        preflight_payload=preflight_payload,
        source_execution_checklist_artifact=checklist_json_project_path,
        source_maintenance_window_pack_artifact=checklist_payload[
            "source_nav_history_archive_maintenance_window_pack_artifact"
        ],
        source_control_pack_artifact=checklist_payload[
            "source_nav_history_archive_control_pack_artifact"
        ],
        source_preflight_artifact=checklist_payload[
            "source_nav_history_archive_preflight_artifact"
        ],
        output_dir=str(out_dir),
    )
    readiness_json_path, readiness_md_path = (
        write_nav_history_archive_maintenance_readiness_audit_artifacts(
            readiness_payload,
            output_dir=str(out_dir),
            timestamp=timestamp,
        )
    )
    return (
        checklist_json_path,
        checklist_md_path,
        readiness_json_path,
        readiness_md_path,
        {
            "status": "success",
            "source_execution_checklist_artifact": _project_relative(
                checklist_path,
                project_root=root,
            ),
            "execution_checklist_artifact": _project_relative(
                checklist_json_path,
                project_root=root,
            ),
            "readiness_artifact": _project_relative(readiness_json_path, project_root=root),
            "summary": readiness_payload.get("summary") or {},
            "readiness_payload": readiness_payload,
        },
    )
