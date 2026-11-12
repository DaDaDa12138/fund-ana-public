"""Bounded archive comparisons/integrity observations; never switch or delete data."""
from __future__ import annotations

from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import sys
import time
from typing import Any, Callable
from uuid import uuid4


def _atomic_json(path: Path, value: dict) -> None:
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        with temporary.open("x", encoding="utf-8") as stream:
            json.dump(value, stream, ensure_ascii=False, indent=2, allow_nan=False)
            stream.flush()
            os.fsync(stream.fileno())
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def _relative(root: Path, path: str | Path) -> str:
    resolved = Path(path)
    if not resolved.is_absolute():
        resolved = root / resolved
    return str(resolved.resolve().relative_to(root.resolve()))


def _window_summary(entries: list[dict], signature: str, required: int) -> dict:
    observations = [item for item in entries if item.get("binding_signature") == signature]
    by_window: dict[int, dict] = {}
    for item in observations:
        by_window[item["window_id"]] = item
    ordered = [by_window[window] for window in sorted(by_window)]
    consecutive = 0
    for item in reversed(ordered):
        if item.get("status") != "matched":
            break
        consecutive += 1
    return {"observed_window_count": len(ordered), "consecutive_matched_windows": consecutive,
            "required_windows": required, "observation_requirement_met": consecutive >= required,
            "production_reader_changed": False, "source_retirement_authorized": False}


def archive_observation_plan(root: Path, bindings: dict) -> dict:
    """Bind observations to every production Parquet shard, including new rotations."""
    copies = list(bindings.get("shadow_copies") or [])
    declared = bindings.get("production_reader")
    if declared == "sqlite":
        return {"mode": "read_only_shadow", "copies": copies,
                "copy_modes": ["read_only_shadow"] * len(copies)}
    from core.utils.screening_history_archive_manifest import load_screening_archive_manifest

    target = root / bindings.get("archive_target_path", "backend/data/archives/screening_history_details_archive.db")
    state = load_screening_archive_manifest(target, project_root=root)
    if state["status"] != "ready":
        raise ValueError("observation_production_archive_manifest_not_ready")
    by_id = {shard["id"]: shard for shard in state["shards"] if shard["role"] == "immutable"}
    if len({item["shard_id"] for item in copies}) != len(copies):
        raise ValueError("observation_duplicate_binding")
    for shard in by_id.values():
        if shard["format"] == "parquet":
            derived = {"shard_id": shard["id"],
                       "directory": str(Path(shard["absolute_path"]).parent.relative_to(root.resolve())),
                       "manifest_sha256": shard["manifest_sha256"], "source_sha256": shard["source_sha256"]}
            matches = [item for item in copies if item["shard_id"] == shard["id"]]
            if matches and matches[0] != derived:
                raise ValueError("observation_explicit_production_binding_mismatch")
            if not matches:
                copies.append(derived)
    modes = []
    for binding in copies:
        shard = by_id.get(binding["shard_id"])
        if shard is None or (declared == "parquet" and shard["format"] != "parquet"):
            raise ValueError("observation_shard_format_not_ready")
        modes.append("read_only_integrity" if shard["format"] == "parquet" else "read_only_shadow")
    if not modes:
        raise ValueError("observation_no_archive_copies")
    return {"mode": modes[0] if len(set(modes)) == 1 else "read_only_mixed", "copies": copies, "copy_modes": modes}


def observe_due_archive_shadow(
    settings: Any, *, task_id: int | None, task_name: str, trigger_source: str,
    log: Callable[[str], None], clock: Callable[[], float] = time.time,
) -> dict:
    """Called inside the scheduler owner lock, after the business runner returns."""
    if trigger_source not in {"cron", "interval"}:
        return {"status": "skipped", "reason": "not_an_automatic_trigger"}
    root = Path(settings.project_root).resolve()
    bindings_path = root / "config/screening_parquet_bindings.json"
    if not bindings_path.exists():
        return {"status": "skipped", "reason": "no_explicit_shadow_binding"}
    raw_bindings = bindings_path.read_bytes()
    bindings = json.loads(raw_bindings)
    config = bindings.get("automatic_shadow") or {}
    if config.get("enabled") is not True:
        return {"status": "skipped", "reason": "automatic_shadow_disabled"}
    if bindings.get("schema_version") != 1 or bindings.get("production_reader") not in {"sqlite", "hybrid", "parquet"} or not bindings.get("shadow_copies"):
        raise ValueError("automatic_archive_observer_requires_explicit_bindings")
    plan = archive_observation_plan(root, bindings)
    expected_mode = plan["mode"]
    interval = int(config.get("interval_hours", 4)) * 3600
    repeats = int(config.get("repeats", 3))
    timeout = int(config.get("timeout_seconds", 180))
    required = int(config.get("required_windows", 7))
    if not (3600 <= interval <= 86400 and 1 <= repeats <= 10 and 30 <= timeout <= 600 and required >= 7):
        raise ValueError("automatic_shadow_resource_or_observation_bounds_invalid")
    binding_hash = hashlib.sha256(raw_bindings).hexdigest()
    code_hashes = {
        name: hashlib.sha256((root / name).read_bytes()).hexdigest()
        for name in ("backend/screening_archive_reader.py", "scripts/audit_screening_archive_reader.py",
                     "core/utils/screening_archive_access.py", "core/utils/screening_history_archive_manifest.py",
                     "core/database/screening_decision_payloads.py", "core/database/screening_pick_payloads.py",
                     "backend/screening_archive_shadow.py")
    }
    signature = hashlib.sha256(json.dumps([binding_hash, code_hashes, plan], sort_keys=True).encode()).hexdigest()
    directory = Path(settings.reports_dir) / "audit/screening_archive_reader"
    directory.mkdir(parents=True, exist_ok=True)
    index_path = directory / "automatic_windows.json"
    window_id = int(clock() // interval)
    with (directory / ".automatic_windows.lock").open("a+b") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return {"status": "skipped", "reason": "shadow_observer_busy"}
        try:
            state = json.loads(index_path.read_text()) if index_path.exists() else {"schema_version": 1, "entries": []}
            if state.get("schema_version") != 1 or not isinstance(state.get("entries"), list):
                raise ValueError("automatic_shadow_index_invalid")
            entries = state["entries"]
            invalidated = False
            for previous in entries:
                if previous.get("binding_signature") != signature or previous.get("status") != "matched":
                    continue
                try:
                    proof = root / _relative(root, previous["artifact_path"])
                    if hashlib.sha256(proof.read_bytes()).hexdigest() != previous["artifact_sha256"]:
                        raise ValueError("automatic_shadow_artifact_hash_mismatch")
                except (OSError, ValueError, KeyError) as exc:
                    previous.update({"status": "invalid_evidence", "error": str(exc)})
                    invalidated = True
            if invalidated:
                _atomic_json(index_path, state)
            existing = [item for item in entries if item.get("binding_signature") == signature and item.get("window_id") == window_id]
            if existing and existing[-1].get("status") != "running":
                return {"status": "skipped", "reason": "window_already_observed", "window_id": window_id,
                        **_window_summary(entries, signature, required)}
            if existing:
                existing[-1].update({"status": "interrupted", "error": "observer_lock_released_before_completion"})
            entry = {"attempt_id": uuid4().hex, "binding_signature": signature, "bindings_sha256": binding_hash,
                     "mode": expected_mode,
                     "reader_code_sha256": code_hashes, "window_id": window_id,
                     "window_start_utc": datetime.fromtimestamp(window_id * interval, timezone.utc).isoformat(),
                     "task_id": task_id, "task_name": task_name, "trigger_source": trigger_source,
                     "started_at_utc": datetime.fromtimestamp(clock(), timezone.utc).isoformat(), "status": "running"}
            entries.append(entry)
            _atomic_json(index_path, state)
            from backend.core_runner import run_structured_subprocess

            try:
                completed = run_structured_subprocess(
                    task_name="screening-archive-shadow",
                    cmd=[sys.executable, "scripts/audit_screening_archive_reader.py", "--bindings", str(bindings_path),
                         "--expected-bindings-sha256", binding_hash, "--repeats", str(repeats), "--sample-offset", str(window_id)],
                    cwd=root, log=log, timeout=timeout,
                    log_dir=directory / "logs" / entry["attempt_id"], tail_limit=8,
                )
                entry.update({"return_code": completed.return_code, "duration_seconds": completed.duration_seconds,
                              "timed_out": completed.timed_out, "subprocess_meta": _relative(root, completed.meta_path)})
                if not completed.success:
                    raise ValueError("archive_shadow_subprocess_not_successful")
                output = next(json.loads(line) for line in reversed(completed.stdout_tail) if line.strip().startswith("{"))
                artifact = root / _relative(root, output["artifact_path"])
                artifact_bytes = artifact.read_bytes()
                evidence = json.loads(artifact_bytes)
                if (evidence.get("passed") is not True or evidence.get("mode") != expected_mode
                        or evidence.get("bindings_sha256") != binding_hash
                        or evidence.get("observation_plan") != plan
                        or len(evidence.get("copies") or []) != len(plan["copies"])
                        or [item.get("binding") for item in evidence["copies"]] != plan["copies"]
                        or any(item.get("status") != "matched" or item.get("aggregation_aligned") is not True
                               or len(item.get("comparisons") or []) < 7
                               or any(row.get("aligned") is not True for row in item["comparisons"])
                               for item in evidence["copies"])):
                    raise ValueError("archive_shadow_evidence_not_verified")
                entry.update({"status": "matched", "artifact_path": _relative(root, artifact),
                              "artifact_sha256": hashlib.sha256(artifact_bytes).hexdigest(),
                              "peak_process_rss_kib": max(item["peak_process_rss_kib"] for item in evidence["copies"]),
                              "comparison_count": sum(len(item["comparisons"]) for item in evidence["copies"])})
            except Exception as exc:
                entry.update({"status": "failed", "error": f"{type(exc).__name__}: {exc}"})
                log(f"[screening-archive-shadow] 对照未通过: {entry['error']}")
            entry["finished_at_utc"] = datetime.fromtimestamp(clock(), timezone.utc).isoformat()
            summary = _window_summary(entries, signature, required)
            state["latest_summary"] = summary
            _atomic_json(index_path, state)
            log(f"[screening-archive-shadow] status={entry['status']} consecutive={summary['consecutive_matched_windows']}/{required}; mode={expected_mode}; 生产历史读取={bindings['production_reader']}")
            return {**entry, **summary, "index_path": _relative(root, index_path)}
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)
