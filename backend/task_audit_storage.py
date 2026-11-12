"""Complete audit JSON and task logs, with bounded verified gzip reads.

Compression changes the file representation only. Task-result hashes still
refer to the original JSON values, while recovery manifests hash stored bytes.
"""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
import fcntl
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import re
import stat
from typing import Any, Callable
from uuid import uuid4
import zlib

MAX_TASK_AUDIT_BYTES = 64 * 1024 * 1024
MAX_RECEIPT_BYTES = 16 * 1024
MAINTENANCE_AUDIT_PREFIXES = {
    "nav_history_maintenance": "nav_history_maintenance",
    "long_term_history_maintenance": "long_term_history_maintenance",
    "long_term_history_gap": "long_term_history_gap",
    "risk_coverage_gap_audit": "risk_coverage_gap_audit",
    "data_truthfulness": "data_truthfulness_audit",
}
MAINTENANCE_AUDIT_FAMILIES = frozenset(MAINTENANCE_AUDIT_PREFIXES)
LEGACY_AUDIT_CSV_FAMILIES = frozenset({
    "score_dedup_shadow", "dynamic_threshold_robustness",
    "low_vol_risk_metric_distortion", "annual_return_gate_shadow",
})
TASK_LOG_NAME = re.compile(r"task_(?P<task_id>[1-9]\d*)_(?P<task_name>[A-Za-z0-9_.-]+)\.log")
TASK_LOG_TERMINAL_STATUSES = frozenset({"success", "degraded", "failed", "skipped", "warning"})
TASK_LOG_START = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} \[task-start\] "
                            r"task_id=(\d+) task_name=([A-Za-z0-9_.-]+) started_at=(\S+)$")
TASK_LOG_FINISH = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} \[task-finish\] "
                             r"status=([a-z_]+) finished_at=(\S+) task_run_audit_path=(\S+)$")


def is_legacy_task_audit_path(path: str | Path) -> bool:
    """Limit fallback to dated scheduler audits or named full-result sidecars."""
    current = Path(path)
    return (current.suffix == ".json" and re.fullmatch(r"\d{8}", current.parent.name) is not None
            and (current.parent.parent.name == "scheduler_task_runs"
                 or (current.parent.parent.name == "task_history_extra_data"
                     and current.name.startswith("task_history_extra_data_")))
            and current.parent.parent.parent.name == ".audit"
            and current.parent.parent.parent.parent.name == "reports")


def is_task_log_path(path: str | Path) -> bool:
    current = Path(path)
    return (current.parent.name == "tasks" and current.parent.parent.name == "logs"
            and TASK_LOG_NAME.fullmatch(current.name) is not None)


def is_maintenance_audit_path(path: str | Path) -> bool:
    """Only immutable, timestamped JSON in the named global audit namespaces."""
    current = Path(path)
    family = current.parent.name
    return (family in MAINTENANCE_AUDIT_FAMILIES
            and current.parent.parent.name == "audit"
            and current.parent.parent.parent.name == "reports"
            and re.fullmatch(re.escape(MAINTENANCE_AUDIT_PREFIXES[family])
                             + r"_\d{8}_\d{6}(?:_\d{6})?(?:_\d+)?\.json",
                             current.name) is not None)


def maintenance_audit_identity(path: Path, original: bytes) -> dict[str, str]:
    """Identify a completed artifact, without inventing a parent task or status."""
    if not is_maintenance_audit_path(path):
        raise ValueError("maintenance_audit_path_outside_scope")
    payload = json.loads(original)
    if not isinstance(payload, dict) or not isinstance(payload.get("generated_at"), str):
        raise ValueError("maintenance_audit_identity_missing")
    datetime.fromisoformat(payload["generated_at"])
    family = path.parent.name
    if family in {"nav_history_maintenance", "long_term_history_maintenance"}:
        required = ("audit_before", "audit_after", "maintenance_result")
    elif family == "long_term_history_gap":
        required = ("samples_by_action", "samples_by_blocker", "samples_by_source")
    elif family == "data_truthfulness":
        required = ("sections",)
        if (payload.get("artifact_type") != "data_truthfulness_audit"
                or type(payload.get("schema_version")) is not int or payload["schema_version"] != 1
                or payload.get("status") not in {"ok", "watch", "needs_review"}):
            raise ValueError("maintenance_audit_truthfulness_identity_invalid")
        for items, count in (("blockers", "blocker_count"), ("warnings", "warning_count")):
            if (not isinstance(payload.get(items), list) or type(payload.get(count)) is not int
                    or payload[count] != len(payload[items])):
                raise ValueError("maintenance_audit_completion_fields_missing")
    else:
        required = ("samples_by_reason", "primary_reason_counts")
        if "audit_error" not in payload:
            raise ValueError("maintenance_audit_completion_fields_missing")
    if any(not isinstance(payload.get(key), dict) for key in required):
        raise ValueError("maintenance_audit_completion_fields_missing")
    return {"family": family, "generated_at": payload["generated_at"]}


def is_legacy_audit_csv_path(path: str | Path) -> bool:
    """Only dated, run-bound CSV attachments in four global audit families."""
    current = Path(path)
    family = current.parent.name
    return (family in LEGACY_AUDIT_CSV_FAMILIES
            and current.parent.parent.name == "audit"
            and current.parent.parent.parent.name == "reports"
            and re.fullmatch(re.escape(family)
                             + r"_\d{8}_\d{6}_[A-Za-z0-9_.-]+_\d{8}_\d{6}(?:_\d{6})?_[A-Za-z0-9_]+\.csv",
                             current.name) is not None)


def legacy_audit_csv_identity(path: Path) -> dict[str, str]:
    """Bind unchanged context bytes, without asserting the old audit passed.

    Older receipts retain their exact identity. Independent producer timestamps
    require an explicit attachment binding, not a tolerance or a renamed date.
    Missing terminal fields remain missing; storage never infers audit success.
    """
    if not is_legacy_audit_csv_path(path):
        raise ValueError("legacy_audit_csv_path_outside_scope")
    family = path.parent.name
    matches = [candidate for candidate in path.parent.glob(f"{family}_*.json")
               if path.name.startswith(candidate.stem + "_")]
    if len(matches) != 1:
        raise ValueError("legacy_audit_csv_unique_context_required")
    context = matches[0]
    _no_symlinks(context)
    raw = _bounded_bytes(context, MAX_TASK_AUDIT_BYTES)
    payload = json.loads(raw)
    if not isinstance(payload, dict):
        raise ValueError("legacy_audit_csv_context_invalid")
    run_id, generated_at = payload.get("run_id"), payload.get("generated_at")
    if not isinstance(run_id, str) or not isinstance(generated_at, str):
        raise ValueError("legacy_audit_csv_context_identity_missing")
    timestamp = datetime.fromisoformat(generated_at)
    prefix = f"{family}_{run_id}_{timestamp.strftime('%Y%m%d_%H%M%S')}"
    identity = {"family": family, "run_id": run_id, "generated_at": generated_at,
                "context_name": context.name, "context_sha256": hashlib.sha256(raw).hexdigest()}
    if re.fullmatch(re.escape(prefix) + r"(?:_\d{6})?", context.stem) is not None:
        return identity
    named = re.fullmatch(re.escape(f"{family}_{run_id}_")
                         + r"(?P<timestamp>\d{8}_\d{6}(?:_\d{6})?)", context.stem)
    if named is None:
        raise ValueError("legacy_audit_csv_context_identity_mismatch")
    named_timestamp = named["timestamp"]
    datetime.strptime(named_timestamp, "%Y%m%d_%H%M%S_%f" if len(named_timestamp) > 15
                      else "%Y%m%d_%H%M%S")
    attachment = _legacy_csv_explicit_attachment(path, payload)
    return {**identity, "identity_contract": "explicit_attachment_independent_timestamps_v1",
            "filename_timestamp": named_timestamp, **attachment}


def _legacy_csv_explicit_attachment(path: Path, payload: dict) -> dict[str, str]:
    """Accept only a unique full attachment path in the original context."""
    relative = Path("reports/audit") / path.parent.name / path.name
    root = path.absolute().parents[3]
    permitted_paths = {str(relative), str(root / relative), str(Path("/app") / relative)}
    artifacts = payload.get("artifacts")
    if not isinstance(artifacts, dict):
        raise ValueError("legacy_audit_csv_explicit_attachment_required")
    matches = []
    for key, item in artifacts.items():
        value = item.get("path") if isinstance(item, dict) else item
        if not isinstance(value, str) or value not in permitted_paths:
            continue
        if not isinstance(key, str) or not key:
            raise ValueError("legacy_audit_csv_attachment_key_invalid")
        binding = {"artifact_key": key, "artifact_path": str(relative)}
        if isinstance(item, dict) and "sha256" in item:
            digest = item["sha256"]
            if not isinstance(digest, str) or re.fullmatch(r"[a-f0-9]{64}", digest) is None:
                raise ValueError("legacy_audit_csv_attachment_sha_invalid")
            binding["artifact_sha256"] = digest
        matches.append(binding)
    if len(matches) != 1:
        raise ValueError("legacy_audit_csv_unique_explicit_attachment_required")
    return matches[0]


def glob_json_artifacts(directory: Path, filename_glob: str) -> list[Path]:
    """Discover plain/native gzip JSON once, retaining migrated logical paths.

    Migrated files use .json.legacy.gz, distinct from new native .json.gz. Even
    a missing receipt remains discoverable at the original logical path and
    fails validation. Other file patterns retain their old behavior.
    """
    paths = set(directory.glob(filename_glob))
    if filename_glob.endswith(".json"):
        for stored in directory.glob(filename_glob + ".gz"):
            original = stored.with_suffix("")
            if original in paths:
                paths.add(original)
            else:
                paths.add(stored)
        for stored in directory.glob(filename_glob + ".legacy.gz"):
            original = stored.with_name(stored.name.removesuffix(".legacy.gz"))
            if is_maintenance_audit_path(original):
                paths.add(original)
    return sorted(paths)


def maintenance_audit_cache_token(path: Path) -> str:
    """Cheap storage invalidation only; a token is never a validity proof."""
    if not is_maintenance_audit_path(path):
        raise ValueError("maintenance_audit_path_outside_scope")
    return migrated_audit_cache_token(path)


def migrated_audit_cache_token(path: Path) -> str:
    """Storage/context identity for invalidation, never a validity proof."""
    if not (is_maintenance_audit_path(path) or is_legacy_audit_csv_path(path)):
        raise ValueError("migrated_audit_path_outside_scope")
    stored, receipt = migration_paths(path)
    parts = []
    for current in (stored, receipt):
        _no_symlinks(current)
        info = current.stat()
        parts.append(f"{info.st_mtime_ns}:{info.st_size}:{info.st_ctime_ns}")
    if is_legacy_audit_csv_path(path):
        # Changes to a bound context must invalidate a prior availability result.
        for context in sorted(path.parent.glob(f"{path.parent.name}_*.json")):
            if path.name.startswith(context.stem + "_"):
                _no_symlinks(context)
                info = context.stat()
                parts.append(f"{context.name}:{info.st_mtime_ns}:{info.st_size}:{info.st_ctime_ns}")
    return "gzip:" + ":".join(parts)


def task_log_timestamp(value: Any) -> datetime:
    if not isinstance(value, str) or not value:
        raise ValueError("task_log_timestamp_missing")
    return datetime.fromisoformat(value).astimezone()


def task_log_terminal_fields(path: Path, original: bytes) -> dict[str, Any]:
    """Bind the original log's first/last records to its named task and audit."""
    if not is_task_log_path(path):
        raise ValueError("task_log_path_outside_scope")
    lines = original.decode("utf-8").splitlines()
    start = TASK_LOG_START.fullmatch(lines[0]) if lines else None
    finish = TASK_LOG_FINISH.fullmatch(lines[-1]) if lines else None
    match = TASK_LOG_NAME.fullmatch(path.name)
    if (start is None or finish is None or int(start[1]) != int(match["task_id"])
            or start[2] != match["task_name"] or finish[1] not in TASK_LOG_TERMINAL_STATUSES):
        raise ValueError("task_log_complete_terminal_markers_required")
    began, ended = task_log_timestamp(start[3]), task_log_timestamp(finish[2])
    if ended < began:
        raise ValueError("task_log_terminal_time_invalid")
    root = path.absolute().parents[2]
    audit_path = (root / finish[3].removeprefix("/app/")).absolute()
    expected = root / "reports/.audit/scheduler_task_runs" / began.strftime("%Y%m%d") / (
        began.strftime("%Y%m%d_%H%M%S") + "_" + start[2] + ".json")
    if audit_path not in (expected, Path(f"{expected}.gz")):
        raise ValueError("task_log_terminal_audit_path_mismatch")
    return {"task_history_id": int(start[1]), "task_name": start[2], "status": finish[1],
            "started_at": start[3], "finished_at": finish[2],
            "source_audit_path": audit_path.relative_to(root).as_posix()}


@contextmanager
def task_log_io_lock(directory: Path, *, blocking: bool = True):
    """Coordinate append and archival across processes before opening a log.

    A directory lock also prevents a waiting writer from recreating an empty
    original after an archival transaction unlinks its plain representation.
    """
    if directory.name != "tasks" or directory.parent.name != "logs":
        raise ValueError("task_log_lock_directory_invalid")
    _no_symlinks(directory)
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / ".storage.lock"
    _no_symlinks(path)
    descriptor = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX | (0 if blocking else fcntl.LOCK_NB))
        try:
            yield
        finally:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
    finally:
        os.close(descriptor)


def migration_paths(path: str | Path) -> tuple[Path, Path]:
    """Only an adjacent named gzip and receipt can represent an old artifact."""
    current = Path(path)
    if not (is_legacy_task_audit_path(current) or is_task_log_path(current)
            or is_maintenance_audit_path(current) or is_legacy_audit_csv_path(current)):
        raise ValueError("task_audit_migration_path_invalid")
    suffix = ".legacy.gz" if is_maintenance_audit_path(current) or is_legacy_audit_csv_path(current) else ".gz"
    return Path(f"{current}{suffix}"), Path(f"{current}{suffix}.receipt")


def _bounded_bytes(path: Path, max_bytes: int) -> bytes:
    if max_bytes < 1:
        raise ValueError("task_audit_exceeds_read_budget")
    with path.open("rb") as stream:
        data = stream.read(max_bytes + 1)
    if len(data) > max_bytes:
        raise ValueError("task_audit_exceeds_read_budget")
    return data


def _no_symlinks(path: Path) -> None:
    if any(part.is_symlink() for part in (path, *path.parents)):
        raise ValueError("task_audit_migration_symlink_not_supported")


def _read_migrated_artifact(
    path: str | Path, *, max_bytes: int = MAX_TASK_AUDIT_BYTES,
) -> tuple[bytes, bytes, dict[str, Any]]:
    """Verify both representations; the first result is the exact original bytes.

    A receipt is a separate storage record. SQL, task values and old source SHA
    bindings retain their original identity and are never rewritten by migration.
    """
    original_path = Path(path)
    compressed_path, receipt_path = migration_paths(original_path)
    for current in (original_path, compressed_path, receipt_path):
        _no_symlinks(current)

    def unique_pairs(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("task_audit_migration_duplicate_receipt_key")
            result[key] = value
        return result

    receipt = json.loads(_bounded_bytes(receipt_path, MAX_RECEIPT_BYTES), object_pairs_hook=unique_pairs)
    if (not isinstance(receipt, dict) or receipt.get("schema_version") != 1
            or receipt.get("codec") != "gzip" or receipt.get("original_name") != original_path.name
            or receipt.get("stored_name") != compressed_path.name):
        raise ValueError("task_audit_migration_receipt_identity_mismatch")
    for key in ("original_bytes", "stored_bytes"):
        value = receipt.get(key)
        if type(value) is not int or not 0 < value <= max_bytes:
            raise ValueError("task_audit_exceeds_read_budget")
    for key in ("original_sha256", "stored_sha256"):
        if not isinstance(receipt.get(key), str) or not re.fullmatch(r"[a-f0-9]{64}", receipt[key]):
            raise ValueError("task_audit_migration_receipt_sha_invalid")
    stored = _bounded_bytes(compressed_path, max_bytes)
    if len(stored) != receipt["stored_bytes"] or hashlib.sha256(stored).hexdigest() != receipt["stored_sha256"]:
        raise ValueError("task_audit_migration_stored_sha_mismatch")
    original = decode_task_audit_bytes(stored, compressed=True, max_bytes=max_bytes)
    if (len(original) != receipt["original_bytes"]
            or hashlib.sha256(original).hexdigest() != receipt["original_sha256"]):
        raise ValueError("task_audit_migration_original_sha_mismatch")
    return original, stored, receipt


def read_migrated_task_audit(
    path: str | Path, *, max_bytes: int = MAX_TASK_AUDIT_BYTES,
) -> tuple[bytes, bytes, dict[str, Any]]:
    if not is_legacy_task_audit_path(path):
        raise ValueError("task_audit_migration_path_invalid")
    return _read_migrated_artifact(path, max_bytes=max_bytes)


def read_migrated_maintenance_audit(
    path: str | Path, *, max_bytes: int = MAX_TASK_AUDIT_BYTES,
) -> tuple[bytes, bytes, dict[str, Any]]:
    if not is_maintenance_audit_path(path):
        raise ValueError("maintenance_audit_path_outside_scope")
    original, stored, receipt = _read_migrated_artifact(path, max_bytes=max_bytes)
    identity = receipt.get("original_identity")
    if (not isinstance(identity, dict)
            or any(type(identity.get(key)) is not int or identity[key] < 0
                   for key in ("size", "mtime_ns", "mode", "uid", "gid"))
            or identity["size"] != len(original) or not stat.S_ISREG(identity["mode"])
            or receipt.get("audit_identity") != maintenance_audit_identity(Path(path), original)):
        raise ValueError("maintenance_audit_receipt_identity_mismatch")
    return original, stored, receipt


def read_migrated_audit_csv(
    path: str | Path, *, max_bytes: int = MAX_TASK_AUDIT_BYTES,
) -> tuple[bytes, bytes, dict[str, Any]]:
    if not is_legacy_audit_csv_path(path):
        raise ValueError("legacy_audit_csv_path_outside_scope")
    original, stored, receipt = _read_migrated_artifact(path, max_bytes=max_bytes)
    identity = receipt.get("original_identity")
    audit_identity = legacy_audit_csv_identity(Path(path))
    if (not isinstance(identity, dict)
            or any(type(identity.get(key)) is not int or identity[key] < 0
                   for key in ("size", "mtime_ns", "mode", "uid", "gid"))
            or identity["size"] != len(original) or not stat.S_ISREG(identity["mode"])
            or receipt.get("audit_identity") != audit_identity
            or ("artifact_sha256" in audit_identity
                and audit_identity["artifact_sha256"] != receipt["original_sha256"])):
        raise ValueError("legacy_audit_csv_receipt_identity_mismatch")
    return original, stored, receipt


def read_migrated_task_log(
    path: str | Path, *, max_bytes: int = MAX_TASK_AUDIT_BYTES,
) -> tuple[bytes, bytes, dict[str, Any]]:
    if not is_task_log_path(path):
        raise ValueError("task_log_migration_path_invalid")
    original, stored, receipt = _read_migrated_artifact(path, max_bytes=max_bytes)
    identity = receipt.get("original_identity")
    task = receipt.get("task_identity")
    match = TASK_LOG_NAME.fullmatch(Path(path).name)
    if (not isinstance(identity, dict) or not isinstance(task, dict)
            or any(type(identity.get(key)) is not int or identity[key] < 0
                   for key in ("size", "mtime_ns", "mode", "uid", "gid"))
            or identity["size"] != len(original) or not stat.S_ISREG(identity["mode"])
            or type(task.get("task_history_id")) is not int
            or task["task_history_id"] != int(match["task_id"])
            or task.get("task_name") != match["task_name"]
            or task.get("status") not in TASK_LOG_TERMINAL_STATUSES
            or not isinstance(task.get("finished_at"), str) or not task["finished_at"]
            or not re.fullmatch(r"[a-f0-9]{64}", str(task.get("source_audit_sha256") or ""))):
        raise ValueError("task_log_migration_identity_invalid")
    terminal = task_log_terminal_fields(Path(path), original)
    if (any(task.get(key) != terminal[key] for key in (
            "task_history_id", "task_name", "status", "source_audit_path"))
            or any(task_log_timestamp(task.get(key)) != task_log_timestamp(terminal[key])
                   for key in ("started_at", "finished_at"))):
        raise ValueError("task_log_migration_terminal_identity_mismatch")
    return original, stored, receipt


def read_original_artifact_bytes(
    path: str | Path, *, max_bytes: int = MAX_TASK_AUDIT_BYTES,
) -> bytes:
    """Read bytes bound by the requested historical path, before JSON decoding.

    Explicit .json.gz/.log.gz paths mean stored gzip bytes. A missing supported
    original path is reconstructed only through a verified adjacent receipt.
    """
    current = Path(path)
    try:
        data = _bounded_bytes(current, max_bytes)
    except FileNotFoundError:
        if current.is_symlink():
            raise
        from core.utils.core_csv_storage import has_stored_csv, read_stored_csv

        if has_stored_csv(current):
            original = read_stored_csv(current)[0]
            if len(original) > max_bytes:
                raise ValueError("core_csv_original_exceeds_reader_budget")
            return original
        if is_task_log_path(current):
            return read_migrated_task_log(current, max_bytes=max_bytes)[0]
        if is_legacy_task_audit_path(current):
            return read_migrated_task_audit(current, max_bytes=max_bytes)[0]
        if is_maintenance_audit_path(current):
            return read_migrated_maintenance_audit(current, max_bytes=max_bytes)[0]
        if is_legacy_audit_csv_path(current):
            return read_migrated_audit_csv(current, max_bytes=max_bytes)[0]
        raise
    if current.name.endswith((".json.gz", ".log.gz", ".json.legacy.gz", ".csv.legacy.gz")):
        legacy_maintenance = current.name.endswith((".json.legacy.gz", ".csv.legacy.gz"))
        original_path = (current.with_name(current.name.removesuffix(".legacy.gz"))
                         if legacy_maintenance else current.with_suffix(""))
        receipt_path = Path(f"{current}.receipt")
        reader = (read_migrated_task_log if is_task_log_path(original_path)
                  else read_migrated_task_audit if is_legacy_task_audit_path(original_path)
                  else read_migrated_maintenance_audit if legacy_maintenance
                  and is_maintenance_audit_path(original_path)
                  else read_migrated_audit_csv if legacy_maintenance
                  and is_legacy_audit_csv_path(original_path) else None)
        if reader and (legacy_maintenance or receipt_path.exists() or receipt_path.is_symlink()):
            _, verified, _ = reader(original_path, max_bytes=max_bytes)
            if data != verified:
                raise ValueError("task_audit_migration_changed_during_read")
    return data


def artifact_exists(path: str | Path) -> bool:
    current = Path(path)
    if current.is_file():
        return True
    from core.utils.core_csv_storage import artifact_is_file, has_stored_csv

    if has_stored_csv(current):
        return artifact_is_file(current)
    if not (is_legacy_task_audit_path(current) or is_task_log_path(current)
            or is_maintenance_audit_path(current) or is_legacy_audit_csv_path(current)):
        return False
    try:
        read_original_artifact_bytes(current)
        return True
    except (OSError, ValueError):
        return False


def original_artifact_size(path: str | Path) -> int:
    current = Path(path)
    try:
        return current.stat().st_size
    except FileNotFoundError:
        return len(read_original_artifact_bytes(current))


def task_log_metadata(path: str | Path) -> dict[str, int]:
    current = Path(path)
    try:
        info = current.stat()
        return {"size": info.st_size, "mtime_ns": info.st_mtime_ns}
    except FileNotFoundError:
        original, _, receipt = read_migrated_task_log(current)
        return {"size": len(original), "mtime_ns": receipt["original_identity"]["mtime_ns"]}


def read_artifact_text_tail(path: Path, *, max_bytes: int, include_head: bool = False) -> str:
    """Keep bounded raw-file tail reads; migrated logs verify their full bytes."""
    if max_bytes <= 0:
        return ""
    try:
        with path.open("rb") as stream:
            size = os.fstat(stream.fileno()).st_size
            head = stream.read(max_bytes) if include_head else b""
            if size > max_bytes:
                stream.seek(-max_bytes, 2)
            tail = stream.read(max_bytes) if not include_head or size > max_bytes else b""
    except FileNotFoundError:
        original = read_original_artifact_bytes(path)
        head = original[:max_bytes] if include_head else b""
        tail = original[-max_bytes:] if not include_head or len(original) > max_bytes else b""
    return ((head + b"\n" + tail) if include_head else tail).decode("utf-8", errors="replace")


def decode_task_audit_bytes(
    data: bytes, *, compressed: bool, max_bytes: int = MAX_TASK_AUDIT_BYTES,
) -> bytes:
    if max_bytes < 1 or len(data) > max_bytes:
        raise ValueError("task_audit_exceeds_read_budget")
    if not compressed:
        return data
    try:
        with gzip.GzipFile(fileobj=io.BytesIO(data), mode="rb") as stream:
            original = stream.read(max_bytes + 1)
    except (OSError, EOFError, zlib.error) as exc:
        raise ValueError("task_audit_gzip_invalid") from exc
    if len(original) > max_bytes:
        raise ValueError("task_audit_exceeds_read_budget")
    return original


def read_task_audit_bytes(
    path: str | Path, *, max_bytes: int = MAX_TASK_AUDIT_BYTES,
) -> bytes:
    current = Path(path)
    data = read_original_artifact_bytes(current, max_bytes=max_bytes)
    return decode_task_audit_bytes(data, compressed=current.name.endswith((".json.gz", ".json.legacy.gz")),
                                  max_bytes=max_bytes)


def read_task_audit(path: str | Path, *, max_bytes: int = MAX_TASK_AUDIT_BYTES) -> dict[str, Any]:
    payload = json.loads(read_task_audit_bytes(path, max_bytes=max_bytes).decode("utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("task_audit_payload_not_object")
    return payload


def write_task_audit(
    path: str | Path, payload: dict[str, Any], *, replace_existing: bool = False,
    default: Callable[[Any], Any] | None = None,
) -> Path:
    """Verify the actual written bytes before exposing a new or sealed audit."""
    current = Path(path)
    if not current.name.endswith((".json", ".json.gz")):
        raise ValueError("task_audit_json_suffix_required")
    if not isinstance(payload, dict):
        raise ValueError("task_audit_payload_not_object")
    legacy = current.with_suffix("") if current.name.endswith(".json.gz") else current
    if is_legacy_task_audit_path(legacy) or is_maintenance_audit_path(legacy):
        receipt = migration_paths(legacy)[1]
        if receipt.exists() or receipt.is_symlink():
            raise ValueError("task_audit_migrated_record_is_read_only")
    original = (json.dumps(payload, ensure_ascii=False, indent=2, default=default) + "\n").encode("utf-8")
    if len(original) > MAX_TASK_AUDIT_BYTES:
        raise ValueError("task_audit_exceeds_read_budget")
    compressed = current.name.endswith(".json.gz")
    stored = gzip.compress(original, compresslevel=6, mtime=0) if compressed else original
    current.parent.mkdir(parents=True, exist_ok=True)
    temporary = current.with_name(f".{current.name}.{uuid4().hex}.tmp")
    try:
        with temporary.open("xb") as stream:
            stream.write(stored)
            stream.flush()
            os.fsync(stream.fileno())
        if decode_task_audit_bytes(temporary.read_bytes(), compressed=compressed) != original:
            raise ValueError("task_audit_written_bytes_mismatch")
        if replace_existing:
            os.replace(temporary, current)
        else:
            # A repeated task identity must not overwrite prior evidence.
            os.link(temporary, current)
        descriptor = os.open(current.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    finally:
        temporary.unlink(missing_ok=True)
    return current
