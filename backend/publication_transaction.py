"""Immutable report revisions with one POSIX publication commit point.

``.publication_current`` is the authoritative, atomically replaced directory
symlink. Its descriptor, report and Tracker belong to the same generation.
The registry and per-run aliases are recoverable indexes, never commit points.
"""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shutil
import threading
from typing import Any, Iterator
from uuid import uuid4

PROTOCOL_VERSION = 1
CURRENT_LINK = ".publication_current"
STATE_NAME = "publication_state.json"
MANIFEST_NAME = "validated.json"
TRACKER_NAME = "最终优质基金累积跟踪.xlsx"
TRACKER_META_NAME = "最终优质基金累积跟踪_report_meta.json"
_process_lock = threading.RLock()
_local = threading.local()
_verified_files: dict[str, tuple[tuple[int, int, int, int], str]] = {}


class PublicationIntegrityError(RuntimeError):
    pass


def reports_root(settings: Any) -> Path:
    configured = getattr(settings, "reports_dir", None)
    if configured is not None:
        return Path(configured)
    latest = getattr(settings, "latest_report_file", None)
    if latest is not None:
        return Path(latest).parent
    registry = getattr(settings, "publication_registry_file", None)
    return Path(registry).parent if registry is not None else Path(settings.runs_dir).parent


def visible_report_paths(settings: Any) -> list[Path]:
    """Committed revisions plus legacy reports; candidates are never history."""
    head = load_publication_head(settings)
    entries = head.get("committed_runs") or {}
    result = [validate_committed_entry(settings, entry) for entry in entries.values()]
    for root in (Path(settings.runs_dir), Path(settings.archived_runs_dir)):
        for path in sorted(root.glob("*/bond_analysis_report_*.json")):
            if path.parent.name in entries:
                continue
            try:
                payload = _read_object(path)
            except PublicationIntegrityError:
                continue
            control = payload.get("publication_control") or {}
            if control.get("commit_protocol"):
                continue
            result.append(path)
    return result


@contextmanager
def publication_io_lock(settings: Any) -> Iterator[None]:
    """A stable inode shared by web, scheduler and operator processes."""
    path = reports_root(settings) / ".publication.lock"
    key = str(path.absolute())
    with _process_lock:
        held = getattr(_local, "held", set())
        if key in held:
            yield
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a+b") as stream:
            fcntl.flock(stream, fcntl.LOCK_EX)
            _local.held = held | {key}
            try:
                yield
            finally:
                _local.held = held
                fcntl.flock(stream, fcntl.LOCK_UN)


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _write_new_json(path: Path, payload: dict[str, Any]) -> None:
    # Candidate control files are immutable once created.
    with path.open("x", encoding="utf-8") as stream:
        json.dump(payload, stream, ensure_ascii=False, indent=2, allow_nan=False)
        stream.flush()
        os.fsync(stream.fileno())


def _replace_link(path: Path, target: Path) -> None:
    relative = os.path.relpath(target, start=path.parent)
    if path.is_symlink() and os.readlink(path) == relative:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        temporary.symlink_to(relative)
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _relative(settings: Any, path: Path) -> str:
    return str(path.absolute().relative_to(Path(settings.project_root).absolute()))


def resolve_committed_path(settings: Any, value: Any) -> Path:
    raw = Path(str(value))
    path = raw if raw.is_absolute() else Path(settings.project_root) / raw
    if not path.resolve().is_relative_to(Path(settings.project_root).resolve()):
        raise PublicationIntegrityError(f"publication_path_outside_project: {value}")
    return path


def _sha256(path: Path, *, durable: bool = False) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
        if durable:
            os.fsync(stream.fileno())
    return digest.hexdigest()


def _check_hash(path: Path, expected: str) -> None:
    try:
        stat = path.stat()
        stamp = (stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns)
        cached = _verified_files.get(str(path))
        actual = cached[1] if cached and cached[0] == stamp else _sha256(path)
    except OSError as exc:
        raise PublicationIntegrityError(f"committed_artifact_unreadable: {path}") from exc
    if not expected or actual != expected:
        raise PublicationIntegrityError(f"committed_artifact_hash_mismatch: {path}")
    _verified_files[str(path)] = (stamp, actual)


def _read_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise PublicationIntegrityError(f"publication_control_unreadable: {path}") from exc
    if not isinstance(payload, dict):
        raise PublicationIntegrityError(f"publication_control_invalid: {path}")
    return payload


def load_publication_head(settings: Any) -> dict[str, Any]:
    current = reports_root(settings) / CURRENT_LINK
    if not current.is_symlink() and not current.exists():
        return {}
    if not current.is_symlink():
        raise PublicationIntegrityError("publication_current_must_be_a_symlink")
    # Resolve once: a reader must not mix two descriptor generations.
    target = current.resolve()
    if not target.is_relative_to(reports_root(settings).resolve()):
        raise PublicationIntegrityError("publication_current_outside_reports")
    head = _read_object(target / STATE_NAME)
    run_id = head.get("current_published_run_id")
    records = head.get("committed_runs")
    if head.get("schema_version") != PROTOCOL_VERSION or not isinstance(records, dict) or run_id not in records:
        raise PublicationIntegrityError("publication_descriptor_invalid")
    if not isinstance(head.get("generation"), int) or head["generation"] < 0:
        raise PublicationIntegrityError("publication_generation_invalid")
    head["resolved_generation_dir"] = str(target)
    validate_committed_entry(settings, records[run_id])
    return head


def validate_committed_entry(settings: Any, entry: dict[str, Any]) -> Path:
    report = resolve_committed_path(settings, entry.get("report_path"))
    _check_hash(report, str(entry.get("report_sha256") or ""))
    if entry.get("manifest_path"):
        manifest_path = resolve_committed_path(settings, entry["manifest_path"])
        _check_hash(manifest_path, str(entry.get("manifest_sha256") or ""))
        manifest = _read_object(manifest_path)
        files = manifest.get("files")
        if manifest.get("schema_version") != PROTOCOL_VERSION or not isinstance(files, dict) or not files:
            raise PublicationIntegrityError("publication_manifest_invalid")
        for name, expected in files.items():
            artifact = manifest_path.parent / name
            if not artifact.resolve().is_relative_to(manifest_path.parent.resolve()):
                raise PublicationIntegrityError("publication_manifest_path_escape")
            if not artifact.exists():
                from backend.report_history_retention import replacement
                if replacement(settings,artifact,expected,manifest_path):
                    continue
            _check_hash(artifact, expected)
    return report


def new_report_revision(settings: Any, run_id: str) -> Path:
    if not run_id or Path(run_id).name != run_id or run_id in {".", ".."}:
        raise ValueError("invalid publication run_id")
    directory = Path(settings.runs_dir) / run_id / "revisions" / uuid4().hex
    directory.mkdir(parents=True, exist_ok=False)
    return directory


def expose_report_alias(settings: Any, report: dict[str, Any]) -> None:
    run_id = str(report["run_id"])
    source = resolve_committed_path(settings, report["artifacts"]["report_json"])
    alias = Path(settings.runs_dir) / run_id / f"bond_analysis_report_{run_id}.json"
    if source.absolute() != alias.absolute():
        _replace_link(alias, source)


def _validate_latest_alias_destination(settings: Any) -> Path:
    """A derived alias must never be written inside immutable run history."""
    alias = Path(os.path.abspath(settings.latest_report_file))
    # Inspect the parent only: the alias itself legitimately resolves to a
    # committed file. This also catches a path through .publication_current.
    parent = alias.parent.resolve()
    for name in ("runs_dir", "archived_runs_dir"):
        directory = getattr(settings, name, None)
        if directory is not None and parent.is_relative_to(Path(directory).resolve()):
            raise PublicationIntegrityError("publication_latest_alias_inside_run_history")
    return alias


def recover_publication_aliases(settings: Any, head: dict[str, Any] | None = None) -> None:
    """Repair only derived links. Never infer a commit from candidate files."""
    head = head or load_publication_head(settings)
    if not head:
        return
    latest_alias = _validate_latest_alias_destination(settings)
    root = reports_root(settings)
    current = root / CURRENT_LINK
    _replace_link(latest_alias, current / "report.json")
    _replace_link(root / TRACKER_NAME, current / TRACKER_NAME)
    _replace_link(root / TRACKER_META_NAME, current / TRACKER_META_NAME)
    for entry in head["committed_runs"].values():
        report_path = resolve_committed_path(settings, entry["report_path"])
        # The legacy report is already its own index; do not link it to itself.
        run_id = str(entry["run_id"])
        alias = Path(settings.runs_dir) / run_id / f"bond_analysis_report_{run_id}.json"
        if alias.absolute() != report_path.absolute():
            _replace_link(alias, report_path)


def _bootstrap_legacy(settings: Any, report: dict[str, Any] | None) -> dict[str, Any]:
    if not report:
        return {}
    root = reports_root(settings)
    directory = new_report_revision(settings, str(report["run_id"]))
    report_path = resolve_committed_path(settings, report["artifacts"]["report_json"])
    source_dir = report_path.parent
    old_prefix = _relative(settings, source_dir)
    new_prefix = _relative(settings, directory)
    def rewrite(value: Any) -> Any:
        if isinstance(value, dict):
            return {key: rewrite(item) for key, item in value.items()}
        if isinstance(value, list):
            return [rewrite(item) for item in value]
        if isinstance(value, str):
            for prefix in (old_prefix, str(source_dir)):
                if value.startswith(prefix + "/"):
                    return new_prefix + value[len(prefix):]
        return value
    def copy_artifacts(value: Any) -> None:
        if isinstance(value, dict):
            for item in value.values():
                copy_artifacts(item)
        elif isinstance(value, str):
            candidate = Path(value)
            candidate = candidate if candidate.is_absolute() else Path(settings.project_root) / candidate
            if candidate.is_file() and candidate.is_relative_to(source_dir):
                target = directory / candidate.relative_to(source_dir)
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(candidate, target)
    # Copy the original bytes before any same-run alias can replace their path.
    copy_artifacts(report["artifacts"])
    frozen_report = rewrite(report)
    frozen_report["publication_control"] = {**(frozen_report.get("publication_control") or {}),
                                             "commit_protocol": PROTOCOL_VERSION, "revision": directory.name,
                                             "legacy_bootstrap": True}
    frozen_report_path = directory / report_path.name
    frozen_report_path.write_text(json.dumps(frozen_report, ensure_ascii=False, indent=2), encoding="utf-8")
    if frozen_report["artifacts"].get("recommendations_detail_json"):
        from backend.recommendations_sidecar import materialize_recommendations_detail_sidecar

        materialize_recommendations_detail_sidecar(settings, frozen_report)
    _replace_link(directory / "report.json", frozen_report_path)
    for name, key in ((TRACKER_NAME, "project_tracker_excel"), (TRACKER_META_NAME, "project_tracker_excel_meta")):
        source = resolve_committed_path(settings, frozen_report["artifacts"][key])
        if not source.is_file():
            raise PublicationIntegrityError(f"legacy_publication_artifact_missing: {key}")
        if (directory / name).absolute() != source.absolute():
            _replace_link(directory / name, source)
    manifest_path = directory / MANIFEST_NAME
    _write_new_json(manifest_path, {"schema_version": PROTOCOL_VERSION, "run_id": report["run_id"],
                                  "files": {str(p.relative_to(directory)): _sha256(p, durable=True) for p in sorted(directory.rglob("*")) if p.is_file()}})
    entry = {"run_id": report["run_id"], "report_path": _relative(settings, frozen_report_path),
             "report_sha256": _sha256(frozen_report_path), "legacy": True,
             "manifest_path": _relative(settings, manifest_path), "manifest_sha256": _sha256(manifest_path)}
    head = {"schema_version": PROTOCOL_VERSION, "generation": 0,
            "current_published_run_id": report["run_id"], "committed_runs": {report["run_id"]: entry},
            "committed_at": datetime.now().isoformat(timespec="seconds"), "legacy_bootstrap": True}
    _write_new_json(directory / STATE_NAME, head)
    _replace_link(root / CURRENT_LINK, directory)
    recover_publication_aliases(settings, head)
    return head


def commit_report_revision(settings: Any, report: dict[str, Any], *, legacy_report: dict[str, Any] | None = None) -> dict[str, Any]:
    """Validate, fsync, then make one atomic pointer switch under the shared lock.

    Caller must run the business gates and prospective canonical selection first.
    Crashes before the switch leave the old publication; crashes after it leave
    the complete new publication and only require rebuilding derived indexes.
    """
    with publication_io_lock(settings):
        _validate_latest_alias_destination(settings)
        report_path = resolve_committed_path(settings, report["artifacts"]["report_json"])
        directory = report_path.parent
        run_id = str(report["run_id"])
        expected_parent = Path(settings.runs_dir) / run_id / "revisions"
        if directory.parent.resolve() != expected_parent.resolve():
            raise PublicationIntegrityError("publication_revision_directory_required")
        if (report.get("publication_control") or {}).get("commit_protocol") != PROTOCOL_VERSION:
            raise PublicationIntegrityError("publication_commit_protocol_required")
        if _read_object(report_path) != report:
            raise PublicationIntegrityError("publication_report_changed_before_commit")
        from backend.publication import _resolve_publish_blockers, _resolve_publish_flags, _resolve_run_lineage_blockers

        blockers = [*_resolve_publish_blockers(report), *_resolve_run_lineage_blockers(report_path)]
        if not _resolve_publish_flags(report)[0] or any(item.get("blocking", item.get("severity") == "blocking") for item in blockers):
            raise PublicationIntegrityError("publication_business_gates_failed")
        head = load_publication_head(settings)
        existing = (head.get("committed_runs") or {}).get(run_id) or {}
        if existing.get("report_path") == _relative(settings, report_path):
            recover_publication_aliases(settings, head)
            return head
        if (directory / STATE_NAME).exists():
            raise PublicationIntegrityError("prepared_revision_requires_new_attempt")
        # All advertised report files must be immutable within this revision.
        for key, value in (report.get("artifacts") or {}).items():
            if not isinstance(value, str) or Path(value).suffix not in {".json", ".xlsx", ".csv", ".md", ".gz"}:
                continue
            artifact = resolve_committed_path(settings, value)
            if not artifact.is_file() or not artifact.resolve().is_relative_to(directory.resolve()):
                raise PublicationIntegrityError(f"publication_artifact_not_frozen: {key}")
        from backend.report_history import mode as history_mode, import_report
        if history_mode(settings) != 'legacy':
            binding = import_report(settings, report_path, 'committed', require_complete=True)
            _write_new_json(directory / 'history_binding.json', binding)
        files = {str(path.relative_to(directory)): _sha256(path, durable=True)
                 for path in sorted(directory.rglob("*")) if path.is_file() and path.name != MANIFEST_NAME}
        manifest_path = directory / MANIFEST_NAME
        _write_new_json(manifest_path, {"schema_version": PROTOCOL_VERSION, "run_id": run_id,
                                      "revision": directory.name, "files": files})
        _replace_link(directory / "report.json", report_path)
        if not head:
            head = _bootstrap_legacy(settings, legacy_report)
        entries = dict(head.get("committed_runs") or {})
        entries[run_id] = {"run_id": run_id, "report_path": _relative(settings, report_path),
                           "report_sha256": _sha256(report_path),
                           "manifest_path": _relative(settings, manifest_path),
                           "manifest_sha256": _sha256(manifest_path), "revision": directory.name}
        next_head = {"schema_version": PROTOCOL_VERSION, "generation": int(head.get("generation", 0)) + 1,
                     "current_published_run_id": run_id, "committed_runs": entries,
                     "committed_at": datetime.now().isoformat(timespec="seconds"),
                     "previous_generation_dir": _relative(settings, (reports_root(settings) / CURRENT_LINK).resolve()) if head else None}
        _write_new_json(directory / STATE_NAME, next_head)
        for parent in [directory, *directory.parents]:
            _fsync_directory(parent)
            if parent == reports_root(settings):
                break
        # This is the only authoritative commit, for report, Tracker and history.
        _replace_link(reports_root(settings) / CURRENT_LINK, directory)
        recover_publication_aliases(settings, next_head)
        return next_head
