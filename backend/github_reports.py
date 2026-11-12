"""Export only committed presentation reports; never export a runtime directory."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import re
import shutil
from typing import Any
from backend.shanghai_time import now_shanghai


SCHEMA = "fund-ana-github-report-bundle-v1"
REPORT_FILES = {
    "filtered_excel": "formal.xlsx",
    "filtered_csv": "formal.csv",
    "report_markdown": "report.md",
    "project_tracker_excel": "tracker.xlsx",
    "forward_validation_excel": "forward-validation.xlsx",
    "forward_validation_json": "forward-validation.json",
    "forward_validation_markdown": "forward-validation.md",
}
REQUIRED = {"filtered_excel", "filtered_csv", "report_markdown", "project_tracker_excel"}
MAX_FILE_BYTES = 45 * 1024**2
MAX_BUNDLE_BYTES = 96 * 1024**2
IDENTIFIER = re.compile(r"[A-Za-z0-9_-]{1,100}\Z")


class ReportExportError(RuntimeError):
    pass


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024**2), b""):
            digest.update(block)
    return digest.hexdigest()


def write_json(path: Path, value: dict) -> None:
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False) + "\n", encoding="utf-8")


def checked_identifier(value: Any) -> str:
    if not isinstance(value, str) or not IDENTIFIER.fullmatch(value):
        raise ReportExportError("invalid_report_identity")
    return value


def export_current(settings: Any, output: Path) -> dict:
    """Copy a verified single publication under its existing publication lock."""
    from backend.publication_transaction import (
        load_publication_head, publication_io_lock, resolve_committed_path,
    )

    if output.exists() or output.is_symlink():
        raise ReportExportError("export_destination_exists")
    if any(parent.is_symlink() for parent in output.parents):
        raise ReportExportError("export_destination_symlink")
    output.mkdir(parents=True)
    try:
        with publication_io_lock(settings):
            head = load_publication_head(settings)  # Includes every original manifest SHA.
            if not head:
                raise ReportExportError("no_committed_publication")
            run_id = checked_identifier(head["current_published_run_id"])
            entry = head["committed_runs"][run_id]
            revision = checked_identifier(entry.get("revision"))
            report_path = resolve_committed_path(settings, entry["report_path"])
            source_manifest = resolve_committed_path(settings, entry["manifest_path"])
            report = json.loads(report_path.read_text(encoding="utf-8"))
            manifest = json.loads(source_manifest.read_text(encoding="utf-8"))
            if (report.get("run_id") != run_id or manifest.get("run_id") != run_id
                    or manifest.get("revision") != revision):
                raise ReportExportError("publication_identity_mismatch")
            if (report.get("publication_control") or {}).get("mode") in {"diagnostic_only", "validation_only"}:
                raise ReportExportError("non_formal_report")
            if not isinstance(report.get("counts", {}).get("final_count"), int) or report["counts"]["final_count"] < 1:
                raise ReportExportError("formal_count_unavailable")
            files = []
            omitted = []
            total = 0
            for role, name in REPORT_FILES.items():
                raw = (report.get("artifacts") or {}).get(role)
                if not raw:
                    if role in REQUIRED:
                        raise ReportExportError("required_report_missing:" + role)
                    omitted.append(role)
                    continue
                path = resolve_committed_path(settings, raw)
                if not path.resolve().is_relative_to(source_manifest.parent.resolve()):
                    raise ReportExportError("report_outside_committed_revision")
                relative = path.resolve().relative_to(source_manifest.parent.resolve()).as_posix()
                expected = manifest["files"].get(relative)
                if not expected or sha256(path) != expected:
                    raise ReportExportError("report_not_bound_to_manifest:" + role)
                size = path.stat().st_size
                total += size
                if size > MAX_FILE_BYTES or total > MAX_BUNDLE_BYTES:
                    raise ReportExportError("report_export_size_limit")
                shutil.copyfile(path, output / name)
                if sha256(output / name) != expected:
                    raise ReportExportError("report_changed_during_copy")
                files.append({"role": role, "path": name, "sha256": expected, "bytes": size})
            shutil.copyfile(source_manifest, output / "source-manifest.json")
            if sha256(output / "source-manifest.json") != entry["manifest_sha256"]:
                raise ReportExportError("source_manifest_changed")
            files.append({"role": "source_manifest", "path": "source-manifest.json",
                          "sha256": entry["manifest_sha256"], "bytes": (output / "source-manifest.json").stat().st_size})
            snapshot = (report.get("database_freshness") or {}).get("effective_nav_snapshot") or {}
            from core.version import __version__
            bundle = {
                "schema": SCHEMA,
                "publication": {"run_id": run_id, "revision": revision, "generation": head["generation"],
                                "committed_at": head.get("committed_at"), "generated_at": report.get("generated_at"),
                                "nav_date": snapshot.get("candidate_date"), "counts": report["counts"],
                                "source_report_sha256": entry["report_sha256"],
                                "source_manifest_sha256": entry["manifest_sha256"]},
                "exported_at": now_shanghai().isoformat(timespec="seconds"), "timezone": "Asia/Shanghai",
                "exporter_version": __version__, "files": files, "omitted_optional_roles": omitted,
                "scope": "selected_committed_presentation_files_not_a_complete_runtime_backup",
            }
            write_json(output / "bundle.json", bundle)
        verify_bundle(output)
        return bundle
    except BaseException:
        shutil.rmtree(output)
        raise


def verify_bundle(directory: Path) -> dict:
    if directory.is_symlink() or not directory.is_dir():
        raise ReportExportError("bundle_directory_invalid")
    metadata = directory / "bundle.json"
    if metadata.is_symlink() or not metadata.is_file() or metadata.stat().st_size > 1024**2:
        raise ReportExportError("bundle_metadata_invalid")
    bundle = json.loads(metadata.read_text(encoding="utf-8"))
    if bundle.get("schema") != SCHEMA:
        raise ReportExportError("bundle_schema_invalid")
    publication = bundle["publication"]
    for key in ("run_id", "revision"):
        checked_identifier(publication.get(key))
    for key in ("source_report_sha256", "source_manifest_sha256"):
        if not re.fullmatch(r"[0-9a-f]{64}", str(publication.get(key) or "")):
            raise ReportExportError("bundle_source_hash_invalid")
    roles = set()
    names = {"bundle.json"}
    total = 0
    allowed = {**REPORT_FILES, "source_manifest": "source-manifest.json"}
    for item in bundle["files"]:
        role, name = item["role"], item["path"]
        if role in roles or name in names or allowed.get(role) != name:
            raise ReportExportError("bundle_file_not_allowlisted")
        roles.add(role)
        names.add(name)
        path = directory / name
        if path.is_symlink() or not path.is_file() or path.stat().st_size != item["bytes"]:
            raise ReportExportError("bundle_file_identity_invalid")
        total += path.stat().st_size
        if path.stat().st_size > MAX_FILE_BYTES or total > MAX_BUNDLE_BYTES:
            raise ReportExportError("bundle_size_limit")
        if sha256(path) != item["sha256"]:
            raise ReportExportError("bundle_file_hash_mismatch")
    if not (REQUIRED | {"source_manifest"}) <= roles:
        raise ReportExportError("bundle_required_roles_missing")
    if {p.name for p in directory.iterdir()} != names:
        raise ReportExportError("bundle_unexpected_files")
    if sha256(directory / "source-manifest.json") != publication["source_manifest_sha256"]:
        raise ReportExportError("bundle_manifest_binding_invalid")
    return bundle


def parse_scheduler_output(text: str) -> dict:
    """Accept only the final JSON object, not an earlier success log line."""
    decoder = json.JSONDecoder()
    for match in re.finditer(r"(?m)^\{", text):
        try:
            value, end = decoder.raw_decode(text, match.start())
        except ValueError:
            continue
        if isinstance(value, dict) and "status" in value and not text[end:].strip():
            return value
    raise ReportExportError("scheduler_terminal_result_unreadable")


def task_outcome(result: dict, return_code: int, before: dict, after: dict) -> str:
    """Unknown/degraded/blocked/pending states are never reported as success."""
    nested = result.get("result") if isinstance(result.get("result"), dict) else {}
    if return_code != 0 or result.get("error") or result.get("task_terminal") is False:
        raise ReportExportError("scheduler_failed_or_incomplete")
    if result.get("status") not in {"success", "skipped"}:
        raise ReportExportError("scheduler_not_successful")
    reason = result.get("skip_reason") or nested.get("skip_reason") or ""
    if reason and any(token in reason for token in ("busy", "cooldown", "blocked", "not_ready", "failed")):
        raise ReportExportError("scheduler_deferred_or_blocked")
    if result.get("status") == "skipped" and reason not in {
        "current_snapshot_already_processed", "effective_nav_snapshot_already_screened",
        "effective_nav_date_already_screened_pending_raw_nav_completion", "effective_nav_date_already_screened",
    }:
        raise ReportExportError("scheduler_skip_reason_unverified")
    bound_run = (nested.get("screening_run_id") or nested.get("workflow_run_id")
                 or nested.get("run_id") or result.get("workflow_run_id"))
    if nested.get("screening_triggered") or nested.get("report_refresh_triggered"):
        if not bound_run or bound_run != after.get("run_id"):
            raise ReportExportError("task_publication_not_committed")
    publication = result.get("publication_result") or {}
    if publication.get("status") in {"blocked", "unknown"}:
        raise ReportExportError("task_publication_blocked")
    identity = lambda value: (value.get("run_id"), value.get("revision"))
    return "published" if identity(before) != identity(after) else "unchanged"
