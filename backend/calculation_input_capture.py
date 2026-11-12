"""Bind frozen core calculation journals to actual completed workflow children."""
from __future__ import annotations

from collections import Counter
from contextlib import nullcontext
from datetime import datetime, timedelta
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
from typing import Any

from core.utils.calculation_capture import (
    CalculationCaptureError, MAX_DIRECTORY_BYTES, WRITE_LOCK_NAME,
    capture_storage_write, decode, validate_journal,
)

PERIOD_TASKS = {
    **{f"long-{year}year": year * 365 for year in range(1, 6)},
    "short-1week": 7, "short-1month": 30, "short-3month": 90, "short-6month": 180,
}
PERIOD_MODULES = {
    name: "bond_fund_screening.analyzers.bond_analyzer_" + name.split("-", 1)[1]
    for name in PERIOD_TASKS
}
MAX_CAPTURE_FILES = 32
MAX_RUN_BYTES = MAX_DIRECTORY_BYTES


def _pack_members(manifest: dict) -> dict[str, dict]:
    if manifest.get("schema_version") in (2, 3):
        from core.utils.calculation_pack_storage import pack_members

        return pack_members(manifest.get("storage"))
    if manifest.get("schema_version") != 1 or "storage" in manifest:
        raise CalculationCaptureError("invalid_capture_manifest_storage")
    return {}


def _sha(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def _safe(path: Path) -> Path:
    path = Path(os.path.abspath(path))
    if any(parent.is_symlink() for parent in (path, *path.parents)):
        raise CalculationCaptureError("capture_binding_symlink")
    return path


def _runs_dir(settings: Any) -> Path:
    reports = Path(getattr(settings, "reports_dir", Path(settings.project_root) / "reports"))
    return _safe(Path(getattr(settings, "runs_dir", reports / "runs")))


def _write_immutable(path: Path, raw: bytes, *, bounded_capture: bool = False) -> None:
    temporary = path.with_name(path.name + ".partial")
    with capture_storage_write(path.parent, len(raw)) if bounded_capture else nullcontext():
        with temporary.open("xb") as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        os.link(temporary, path)
        temporary.unlink()
    fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def prepare_capture_directory(settings: Any, run_id: str) -> Path:
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,120}", run_id):
        raise CalculationCaptureError("invalid_capture_workflow_run_id")
    directory = _safe(_runs_dir(settings) / run_id / "calculation_inputs")
    # A previous execution must never be silently appended to or overwritten.
    directory.mkdir(parents=True, exist_ok=False)
    return directory


def _capture_members(directory: Path) -> list[Path]:
    files = sorted(directory.iterdir())
    lock = directory / WRITE_LOCK_NAME
    if lock in files:
        if lock.is_symlink() or not lock.is_file() or lock.stat().st_size != 0:
            raise CalculationCaptureError("invalid_capture_write_lock")
        files.remove(lock)
    return files


def _argv_matches(header: dict, meta: dict) -> bool:
    command, argv = meta.get("command") or [], header.get("argv") or []
    if (len(command) < 2 or not argv or header.get("cwd") != meta.get("cwd")
            or not isinstance(header.get("project_root"), str)):
        return False
    source = header.get("entrypoint_source")
    if source not in (header.get("code_sha256") or {}):
        return False
    root = PurePosixPath(header["project_root"])
    actual = PurePosixPath(argv[0])
    if not actual.is_absolute():
        actual = PurePosixPath(header["cwd"]) / actual
    if actual != root / source:
        return False
    if command[1] == "-m":
        if len(command) < 3:
            return False
        module = command[2]
        if module not in {*PERIOD_MODULES.values(), "bond_fund_screening.tasks.daily_final_screening"}:
            return False
        expected = "core/" + module.replace(".", "/") + ".py"
        return source == expected and argv[1:] == command[3:]
    expected_script = PurePosixPath(command[1])
    if not expected_script.is_absolute():
        expected_script = PurePosixPath(meta["cwd"]) / expected_script
    return (source == "core/main.py" and expected_script == actual
            and argv[1:] == command[2:] and command[2:3] == ["calculate-risk"])


def _bind_subprocess(settings: Any, capture: dict, subprocess_index: dict) -> dict:
    header = capture["header"]
    matches = []
    log_root = _safe(Path(settings.project_root) / subprocess_index["log_dir_relative"])
    for item in subprocess_index.get("items", []):
        relative = item.get("meta_path_relative")
        if not relative:
            continue
        meta_path = _safe(Path(settings.project_root) / relative)
        if meta_path.parent != log_root or meta_path.stat().st_size > 4 * 1024**2:
            raise CalculationCaptureError("capture_child_metadata_outside_run")
        meta = json.loads(meta_path.read_text())
        if not _argv_matches(header, meta):
            continue
        if any(meta.get(key) != item.get(key) for key in ("task_name", "command", "return_code", "success", "timed_out", "started_at", "finished_at")):
            raise CalculationCaptureError("capture_child_index_metadata_disagree")
        if meta.get("return_code") != 0 or meta.get("success") is not True or meta.get("timed_out") is not False:
            raise CalculationCaptureError("capture_child_not_successfully_completed")
        start = datetime.fromisoformat(header["started_at"])
        finish = datetime.fromisoformat(capture["terminal"]["finished_at"])
        child_start = datetime.fromisoformat(meta["started_at"])
        child_finish = datetime.fromisoformat(meta["finished_at"])
        if child_start.tzinfo is None:
            child_start = child_start.replace(tzinfo=start.tzinfo)
        if child_finish.tzinfo is None:
            child_finish = child_finish.replace(tzinfo=finish.tzinfo)
        if not child_start <= start <= finish <= child_finish + timedelta(seconds=1):
            continue
        matches.append({"task_name": item["task_name"], "meta_path": relative,
                        "meta_sha256": _sha(meta_path), "metadata": meta})
    if len(matches) != 1:
        raise CalculationCaptureError("capture_requires_one_matching_completed_child")
    return matches[0]


def build_calculation_input_index(
    settings: Any, directory: Path, *, run_id: str, planned_tasks: list[str], subprocess_index: dict,
    collection_execution: dict | None = None,
) -> Path:
    directory = _safe(directory)
    scope = "collection" if collection_execution is not None else "workflow"
    execution_bytes = None
    if collection_execution is not None:
        from backend.collection_calculation_capture import collection_execution_root, validate_execution

        validate_execution(collection_execution)
        if collection_execution["capture_run_id"] != run_id:
            raise CalculationCaptureError("collection_capture_run_mismatch")
        root = collection_execution_root(settings, collection_execution)
        execution_path = _safe(root / "execution.json")
        if execution_path.stat().st_size > 64 * 1024:
            raise CalculationCaptureError("collection_execution_too_large")
        execution_bytes = execution_path.read_bytes()
        if json.loads(execution_bytes) != collection_execution:
            raise CalculationCaptureError("collection_execution_changed")
    else:
        root = _runs_dir(settings) / run_id
    if directory != _safe(root / "calculation_inputs") or subprocess_index.get("run_id") != run_id:
        raise CalculationCaptureError("capture_directory_or_subprocess_run_mismatch")
    log_root = _safe(root / "logs/subprocess")
    index_path = _safe(Path(settings.project_root) / str(subprocess_index.get("index_path_relative") or ""))
    if (index_path != log_root / "subprocess_index.json"
            or _safe(Path(settings.project_root) / str(subprocess_index.get("log_dir_relative") or "")) != log_root
            or index_path.stat().st_size > 8 * 1024**2):
        raise CalculationCaptureError("capture_subprocess_index_outside_run")
    index_bytes = index_path.read_bytes()
    bound_index = json.loads(index_bytes)
    for field in ("run_id", "items", "log_dir_relative", "index_path_relative"):
        if subprocess_index.get(field) != bound_index.get(field):
            raise CalculationCaptureError("capture_subprocess_index_changed")
    files = _capture_members(directory)
    manifests = [path for path in files if path.name.endswith(".manifest.json")]
    if len(manifests) > MAX_CAPTURE_FILES or any(not path.is_file() or path.is_symlink() for path in files):
        raise CalculationCaptureError("invalid_capture_directory_members")
    from core.utils.calculation_pack_storage import MAX_PACK_FILES, is_pack_name

    if any(not path.name.endswith((".manifest.json", ".jsonl.gz")) and not is_pack_name(path.name) for path in files):
        raise CalculationCaptureError("unfinished_or_unexpected_capture_files")
    if sum(is_pack_name(path.name) for path in files) > MAX_PACK_FILES:
        raise CalculationCaptureError("workflow_capture_pack_count_budget_exhausted")
    if sum(path.stat().st_size for path in files) > MAX_RUN_BYTES:
        raise CalculationCaptureError("workflow_capture_disk_budget_exhausted")
    expected = {}
    if "calculate-risk" in planned_tasks:
        expected["database_risk"] = 1
    if "daily-screening-master" in planned_tasks:
        expected.update(screening_period=9, final_screening=1)
    checks, kinds, periods, used_files, code_basis = [], Counter(), set(), set(), None
    for path in manifests:
        capture = validate_journal(path, expected_run_id=run_id)
        manifest, header = capture["manifest"], capture["header"]
        if header.get("capture_scope", "workflow") != scope:
            raise CalculationCaptureError("calculation_capture_scope_mismatch")
        if manifest["operation_outcome"] != "returned":
            raise CalculationCaptureError("capture_operation_did_not_return")
        binding = _bind_subprocess(settings, capture, bound_index)
        context = decode(header["context"])
        kind = manifest["kind"]
        if kind == "screening_period":
            days = context.get("period_days")
            if days != PERIOD_TASKS.get(binding["task_name"]) or days in periods:
                raise CalculationCaptureError("capture_screening_period_mismatch")
            periods.add(days)
        elif kind == "database_risk" and binding["task_name"] != "calculate-risk":
            raise CalculationCaptureError("capture_risk_task_mismatch")
        elif kind == "final_screening" and binding["task_name"] != "任务3: 最终筛选":
            raise CalculationCaptureError("capture_final_screening_task_mismatch")
        kinds[kind] += 1
        if code_basis is None:
            code_basis = header["code_sha256"]
        elif code_basis != header["code_sha256"]:
            raise CalculationCaptureError("calculation_sources_changed_within_workflow")
        used_files.update((path.name, manifest["file"]))
        used_files.update(_pack_members(manifest))
        checks.append({"manifest": path.name, "manifest_sha256": capture["manifest_sha256"],
                       **manifest, "context": header["context"], "subprocess": binding,
                       "typed_objects_roundtripped": capture["typed_objects_roundtripped"]})
    if dict(kinds) != expected or {path.name for path in files} != used_files:
        raise CalculationCaptureError("missing_or_extra_workflow_calculation_captures")
    payload = {"schema_version": 1, "run_id": run_id, "capture_scope": scope,
               "created_at": datetime.now().astimezone().isoformat(),
               "capture_complete": True, "scientific_reproduction_status": "not_verified",
               "expected_scopes": expected, "period_days": sorted(periods), "sessions": checks,
               "code_sha256": code_basis or {}, "total_compressed_bytes": sum(path.stat().st_size for path in files),
               "scope": "Actual calls made by these completed children, including NULL, skipped and raised outcomes. Reused earlier risk rows do not acquire historical inputs from this capture.",
               "subprocess_index": "bound_subprocess_index.json",
               "subprocess_index_sha256": hashlib.sha256(index_bytes).hexdigest(),
               "original_subprocess_index_path": subprocess_index["index_path_relative"]}
    # The ordinary index is appended to by report audits after this stage.
    # Bind the exact completed-child evidence now, not that mutable pathname.
    _write_immutable(directory / payload["subprocess_index"], index_bytes, bounded_capture=True)
    if execution_bytes is not None:
        payload.update(collection_execution="bound_collection_execution.json",
                       collection_execution_sha256=hashlib.sha256(execution_bytes).hexdigest())
        _write_immutable(directory / payload["collection_execution"], execution_bytes, bounded_capture=True)
    target = directory / f"calculation_input_index_{run_id}.json"
    _write_immutable(target, json.dumps(payload, ensure_ascii=False, sort_keys=True,
                                       separators=(",", ":"), allow_nan=False).encode("utf-8") + b"\n",
                     bounded_capture=True)
    return target


def validate_calculation_input_index(path: Path, *, full_journals: bool = False) -> dict:
    path = _safe(path)
    if path.stat().st_size > 8 * 1024**2:
        raise CalculationCaptureError("calculation_input_index_too_large")
    payload = json.loads(path.read_text())
    if payload.get("capture_scope") == "standalone":
        from backend.standalone_calculation_capture import validate_standalone_index

        return validate_standalone_index(path, payload, full_journals=full_journals)
    run_id = payload.get("run_id", "")
    if (payload.get("schema_version") != 1 or payload.get("capture_complete") is not True
            or not isinstance(run_id, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,120}", run_id)
            or path.name != f"calculation_input_index_{run_id}.json"
            or payload.get("subprocess_index") != "bound_subprocess_index.json"):
        raise CalculationCaptureError("incomplete_calculation_input_index")
    bound_path = _safe(path.parent / payload["subprocess_index"])
    if bound_path.stat().st_size > 8 * 1024**2 or _sha(bound_path) != payload["subprocess_index_sha256"]:
        raise CalculationCaptureError("bound_calculation_subprocess_index_changed")
    bound = json.loads(bound_path.read_text())
    if bound.get("run_id") != run_id:
        raise CalculationCaptureError("bound_calculation_subprocess_run_mismatch")
    sessions = payload.get("sessions", [])
    if not isinstance(sessions, list) or len(sessions) > MAX_CAPTURE_FILES:
        raise CalculationCaptureError("invalid_calculation_capture_sessions")
    kinds, periods, members = Counter(), set(), {path.name, bound_path.name}
    scope = payload.get("capture_scope", "workflow")
    if scope == "collection":
        from backend.collection_calculation_capture import validate_execution

        if payload.get("collection_execution") != "bound_collection_execution.json":
            raise CalculationCaptureError("missing_bound_collection_execution")
        execution_path = _safe(path.parent / payload["collection_execution"])
        if (execution_path.stat().st_size > 64 * 1024
                or _sha(execution_path) != payload.get("collection_execution_sha256")):
            raise CalculationCaptureError("bound_collection_execution_changed")
        execution = json.loads(execution_path.read_text())
        validate_execution(execution)
        if execution["capture_run_id"] != run_id:
            raise CalculationCaptureError("bound_collection_execution_run_mismatch")
        members.add(execution_path.name)
    elif scope != "workflow" or "collection_execution" in payload:
        raise CalculationCaptureError("invalid_calculation_capture_scope")
    total = 0
    shared_packs = {}
    for session in sessions:
        identifier = session.get("session_id", "")
        if (not isinstance(identifier, str) or not re.fullmatch(r"[0-9a-f]{32}", identifier)
                or session.get("manifest") != identifier + ".manifest.json"
                or session.get("file") != identifier + ".jsonl.gz" or session.get("run_id") != run_id
                or session.get("capture_scope", "workflow") != scope
                or session.get("operation_outcome") != "returned"):
            raise CalculationCaptureError("invalid_calculation_session_identity")
        for name, digest in ((session["manifest"], session["manifest_sha256"]), (session["file"], session["sha256"])):
            member = _safe(path.parent / name)
            if name in members or not member.is_file() or _sha(member) != digest:
                raise CalculationCaptureError("calculation_capture_member_changed")
            members.add(name)
            total += member.stat().st_size
        manifest = json.loads((path.parent / session["manifest"]).read_text())
        if any(session.get(key) != value for key, value in manifest.items()):
            raise CalculationCaptureError("calculation_manifest_index_disagree")
        for name, descriptor in _pack_members(manifest).items():
            if name in shared_packs:
                if shared_packs[name] != descriptor:
                    raise CalculationCaptureError("calculation_shared_pack_manifest_conflict")
                continue
            member = _safe(path.parent / name)
            if (name in members or not member.is_file() or member.stat().st_size != descriptor["bytes"]
                    or _sha(member) != descriptor["sha256"]):
                raise CalculationCaptureError("calculation_capture_pack_changed")
            from core.utils.calculation_pack_storage import MAX_PACK_FILES

            if len(shared_packs) >= MAX_PACK_FILES:
                raise CalculationCaptureError("workflow_capture_pack_count_budget_exhausted")
            shared_packs[name] = descriptor
            members.add(name)
            total += member.stat().st_size
        binding = session["subprocess"]
        matching = [item for item in bound.get("items", []) if item.get("meta_path_relative") == binding["meta_path"]]
        if len(matching) != 1 or any(binding["metadata"].get(key) != matching[0].get(key)
                                    for key in ("task_name", "command", "return_code", "success", "timed_out", "started_at", "finished_at")):
            raise CalculationCaptureError("calculation_child_binding_changed")
        kinds[session["kind"]] += 1
        if session["kind"] == "screening_period":
            days = decode(session["context"]).get("period_days")
            if days != PERIOD_TASKS.get(binding["task_name"]) or days in periods:
                raise CalculationCaptureError("calculation_bound_period_mismatch")
            periods.add(days)
        if full_journals:
            checked = validate_journal(path.parent / session["manifest"], expected_run_id=run_id)
            if checked["header"]["code_sha256"] != payload["code_sha256"]:
                raise CalculationCaptureError("calculation_bound_source_mismatch")
    if (dict(kinds) != payload.get("expected_scopes") or sorted(periods) != payload.get("period_days")
            or total != payload.get("total_compressed_bytes") or total > MAX_RUN_BYTES
            or {member.name for member in _capture_members(path.parent)} != members):
        raise CalculationCaptureError("incomplete_calculation_capture_members")
    return payload


def capture_index_reference(settings: Any, raw_path: str) -> dict:
    path = Path(raw_path)
    path = _safe(path if path.is_absolute() else Path(settings.project_root) / path)
    reports = _safe(Path(settings.reports_dir))
    if not path.exists() and path.is_relative_to(Path("/app")):
        path = _safe(Path(settings.project_root) / path.relative_to("/app"))
    if not path.exists() and path.is_relative_to(reports / "runs"):
        relative = path.relative_to(reports / "runs")
        if len(relative.parts) == 3 and relative.parts[1] == "calculation_inputs":
            path = _safe(reports / "archive/superseded_runs" / relative)
    if not path.is_relative_to(reports) or path.stat().st_size > 8 * 1024**2:
        raise CalculationCaptureError("capture_index_outside_reports_or_oversized")
    payload = validate_calculation_input_index(path)
    scope = payload.get("capture_scope", "workflow")
    return {"path": str(path.relative_to(settings.project_root)), "sha256": _sha(path),
            "capture_scope": scope, "capture_run_id": payload["run_id"],
            "source_workflow_run_id": payload["run_id"] if scope == "workflow" else None, "capture_complete": True,
            "scientific_reproduction_status": "not_verified", "scopes": payload["expected_scopes"],
            "period_days": payload["period_days"]}
