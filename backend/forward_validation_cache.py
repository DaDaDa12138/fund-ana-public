"""Small cache indexes over complete forward-validation artifacts.

The workbook belongs to its report. Cache misses never authorize rebuilding a
complete workbook from its JSON preview, nor deleting the source report.
"""
from __future__ import annotations

from contextlib import contextmanager
import fcntl
import hashlib
import json
import logging
import os
from pathlib import Path
from typing import Any, Iterator
from uuid import uuid4

from backend.publication_transaction import reports_root

REFERENCE_VERSION = 1
REFERENCE_CACHE_KEEP_COUNT = 128
REFERENCE_CACHE_PRUNE_BATCH = 16
LOGGER = logging.getLogger(__name__)


class WorkbookSourceChanged(OSError):
    """A reusable workbook disappeared or changed before it could be copied."""


def file_fingerprint(path: Path) -> dict[str, Any]:
    with path.open("rb") as stream:
        before = os.fstat(stream.fileno())
        digest = hashlib.file_digest(stream, "sha256").hexdigest()
        after = os.fstat(stream.fileno())
    if (before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (
        after.st_size, after.st_mtime_ns, after.st_ctime_ns
    ):
        raise WorkbookSourceChanged("forward_validation_source_changed_during_hash")
    return {"sha256": digest, "size_bytes": after.st_size}


def workbook_reference(settings: Any, path: Path) -> dict[str, Any]:
    root = reports_root(settings).resolve()
    source = path.resolve(strict=True)
    if not source.is_relative_to(root) or source.is_relative_to(
        (Path(settings.runs_dir) / ".forward_validation_cache").resolve()
    ):
        raise ValueError("forward_validation_reference_must_be_report_artifact")
    return {
        "schema_version": REFERENCE_VERSION,
        "reports_relative_path": source.relative_to(root).as_posix(),
        **file_fingerprint(source),
    }


def resolve_workbook_reference(settings: Any, reference: Any) -> Path | None:
    if not isinstance(reference, dict) or reference.get("schema_version") != REFERENCE_VERSION:
        return None
    raw = reference.get("reports_relative_path")
    digest = reference.get("sha256")
    size = reference.get("size_bytes")
    if not isinstance(raw, str) or not raw or Path(raw).is_absolute() or ".." in Path(raw).parts:
        return None
    if not isinstance(digest, str) or len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
        return None
    if type(size) is not int or size < 1:
        return None
    root = reports_root(settings).resolve()
    original = root / raw
    candidates = [original]
    # Legacy reports move as a directory. Do not search by basename alone or
    # accept a different revision that happens to have the same run id.
    for old, new in ((settings.runs_dir, settings.archived_runs_dir),
                     (settings.archived_runs_dir, settings.runs_dir)):
        old_path = Path(old).resolve()
        if original.is_relative_to(old_path):
            candidates.append(Path(new) / original.relative_to(old_path))
    for candidate in candidates:
        try:
            resolved = candidate.resolve(strict=True)
            if not resolved.is_relative_to(root) or resolved.is_relative_to(
                (Path(settings.runs_dir) / ".forward_validation_cache").resolve()
            ):
                continue
            if resolved.stat().st_size != size:
                continue
            if file_fingerprint(resolved) == {"sha256": digest, "size_bytes": size}:
                return resolved
        except (OSError, RuntimeError, ValueError):
            continue
    return None


@contextmanager
def cache_io_lock(cache_dir: Path) -> Iterator[None]:
    cache_dir.mkdir(parents=True, exist_ok=True)
    with (cache_dir / ".cache.lock").open("a+b") as stream:
        fcntl.flock(stream, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(stream, fcntl.LOCK_UN)


def replace_cache_json(path: Path, payload: dict[str, Any]) -> None:
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        with temporary.open("x", encoding="utf-8") as stream:
            json.dump(payload, stream, ensure_ascii=False, indent=2)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    finally:
        temporary.unlink(missing_ok=True)


def prune_reference_indexes(settings: Any, *, protected_key: str) -> dict[str, int]:
    """Bound verified redundant indexes; leave legacy/unknown evidence intact.

    Caller holds cache_io_lock. No workbook or source artifact is ever removed.
    Missing or changed sources retain their cache JSON for investigation.
    """
    cache_dir = Path(settings.runs_dir) / ".forward_validation_cache"
    entries = []
    for path in cache_dir.glob("*.json"):
        try:
            if path.is_symlink() or path.with_suffix(".xlsx").exists():
                continue
            payload = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(payload, dict) and "excel_reference" in payload:
                entries.append((path.stat().st_mtime_ns, path.name, path, payload))
        except (OSError, ValueError):
            continue
    excess = max(0, len(entries) - REFERENCE_CACHE_KEEP_COUNT)
    removed = 0
    checked = 0
    for _, _, path, payload in sorted(entries):
        if removed >= min(excess, REFERENCE_CACHE_PRUNE_BATCH):
            break
        if path.stem == protected_key:
            continue
        checked += 1
        source = resolve_workbook_reference(settings, payload.get("excel_reference"))
        if source is None:
            continue
        try:
            source_payload = json.loads(source.with_suffix(".json").read_text(encoding="utf-8"))
            if not isinstance(source_payload, dict) or not isinstance(payload.get("summary"), dict):
                continue
            if any(payload.get(key) != source_payload.get(key)
                   for key in ("summary", "detail_row_count", "detail_sheet_name")):
                continue
            path.unlink()
            removed += 1
        except (OSError, ValueError):
            continue
    return {"reference_count_before": len(entries), "checked": checked, "removed": removed,
            "remaining_above_limit": max(0, excess - removed)}


def copy_verified_workbook(source: Path, target: Path, fingerprint: dict[str, Any] | None) -> None:
    """Hash the bytes actually copied, then expose a complete independent file."""
    temporary = target.with_name(f".{target.name}.{uuid4().hex}.tmp")
    try:
        try:
            expected = fingerprint or file_fingerprint(source)
            reader = source.open("rb")
        except OSError as exc:
            raise WorkbookSourceChanged(str(exc)) from exc
        digest = hashlib.sha256()
        size = 0
        with reader, temporary.open("xb") as writer:
            while True:
                try:
                    chunk = reader.read(1024 * 1024)
                except OSError as exc:
                    raise WorkbookSourceChanged(str(exc)) from exc
                if not chunk:
                    break
                digest.update(chunk)
                size += len(chunk)
                writer.write(chunk)
            writer.flush()
            os.fsync(writer.fileno())
        if {"sha256": digest.hexdigest(), "size_bytes": size} != expected:
            raise WorkbookSourceChanged("forward_validation_source_changed_before_copy")
        os.replace(temporary, target)
    finally:
        temporary.unlink(missing_ok=True)


def preserve_cache_sources_before_removal(settings: Any, directory: Path) -> dict[str, Any]:
    """Detach live references before existing archive retention removes a run.

    This preserves the complete data that the old physical cache also retained,
    without pinning an entire report directory. Failure must stop its deletion.
    """
    directory = directory.resolve()
    archive_root = Path(settings.archived_runs_dir).resolve()
    if directory.parent != archive_root:
        raise ValueError("forward_cache_preservation_requires_archived_run")
    cache_dir = Path(settings.runs_dir) / ".forward_validation_cache"
    result: dict[str, Any] = {"archive_directory": str(directory), "preserved": []}
    if not cache_dir.exists():
        return result
    with cache_io_lock(cache_dir):
        for json_path in sorted(cache_dir.glob("*.json")):
            payload = json.loads(json_path.read_text(encoding="utf-8"))
            if not isinstance(payload, dict) or "excel_reference" not in payload:
                continue
            reference = payload["excel_reference"]
            raw = reference.get("reports_relative_path") if isinstance(reference, dict) else None
            if not isinstance(raw, str) or directory.name not in Path(raw).parts:
                continue
            source = resolve_workbook_reference(settings, reference)
            if source is None:
                raise WorkbookSourceChanged(f"forward_cache_archive_source_unverified: {json_path.name}")
            if not source.is_relative_to(directory):
                continue
            expected = {key: reference[key] for key in ("sha256", "size_bytes")}
            target = json_path.with_suffix(".xlsx")
            created_bytes = 0
            if target.exists():
                if target.is_symlink() or file_fingerprint(target) != expected:
                    raise WorkbookSourceChanged(f"forward_cache_existing_workbook_must_be_preserved: {target.name}")
            else:
                copy_verified_workbook(source, target, expected)
                created_bytes = target.stat().st_size
            detached = {key: value for key, value in payload.items() if key != "excel_reference"}
            detached["detached_excel_source"] = reference
            previous_json_size = json_path.stat().st_size
            replace_cache_json(json_path, detached)
            result["preserved"].append({
                "cache_key": json_path.stem, "workbook": str(target), **expected,
                "new_cache_bytes": created_bytes + json_path.stat().st_size - previous_json_size,
            })
    return result
