"""Reuse replay summaries only while every inspected input retains its identity.

This cache is independent of publication validation. A request still validates
the committed head and manifest; replay evidence written after publication is
observed on the next request, without a time-based stale window.
"""
from __future__ import annotations

from collections import OrderedDict
from copy import deepcopy
import json
from pathlib import Path
import threading
from typing import Any

from backend import replay_evidence, runtime_replay
from backend.task_audit_storage import (
    is_legacy_audit_csv_path, is_legacy_task_audit_path, is_maintenance_audit_path,
    is_task_log_path, migration_paths,
)
from core.utils.core_csv_storage import csv_storage_paths

_INPUT_KEYS = (
    "long_csv", "short_csv", "final_screening_decision_ledger_file",
    "bond_mainlist_quality_audit_json_file", "publish_chain_snapshot_json",
    "filtered_csv", "screening_input_snapshot_json_gz",
)
_CACHE_LIMIT = 512
_cache: OrderedDict[str, tuple[tuple[Path, ...], tuple, dict[str, Any]]] = OrderedDict()
_cache_lock = threading.Lock()


def _identity(path: Path) -> tuple:
    try:
        info = path.lstat()
        identity = (info.st_dev, info.st_ino, info.st_mode, info.st_size,
                    info.st_mtime_ns, info.st_ctime_ns)
        if path.is_symlink():
            target = path.stat()
            identity += (target.st_dev, target.st_ino, target.st_mode, target.st_size,
                         target.st_mtime_ns, target.st_ctime_ns)
        return identity
    except OSError as error:
        return ("unavailable", error.errno)


def _storage_dependencies(path: Path) -> list[Path]:
    dependencies = [path]
    if path.is_file():
        if path.name.endswith((".json.gz", ".json.legacy.gz")):
            dependencies.append(Path(f"{path}.receipt"))
        return dependencies
    if path.suffix == ".csv":
        dependencies.extend(csv_storage_paths(path))
    if (is_legacy_task_audit_path(path) or is_task_log_path(path)
            or is_maintenance_audit_path(path) or is_legacy_audit_csv_path(path)):
        dependencies.extend(migration_paths(path))
        if is_legacy_audit_csv_path(path):
            # Context additions/removals must also invalidate receipt verification.
            dependencies.append(path.parent)
            dependencies.extend(path.parent.glob(f"{path.parent.name}_*.json"))
    return dependencies


def _dependencies(inputs: dict[str, Any], root: Path) -> tuple[Path, ...]:
    paths: list[Path] = []
    for value in inputs.values():
        if not value:
            continue
        relative = Path(str(value))
        candidate = root / relative
        paths.extend(_storage_dependencies(candidate))
        if len(relative.parts) >= 3 and relative.parts[:2] == ("reports", "runs"):
            paths.extend(_storage_dependencies(
                root / "reports/archive/superseded_runs" / Path(*relative.parts[2:])
            ))
    snapshot = runtime_replay.resolve_runtime_artifact_path(
        inputs.get("screening_input_snapshot_json_gz"), project_root=root,
    )
    if snapshot is not None and snapshot.is_file():
        proof = replay_evidence.evidence_path(root, replay_evidence.content_hash(snapshot))
        paths.append(proof)
        try:
            payload = json.loads(proof.read_text(encoding="utf-8"))
            sources = payload.get("source_files") if isinstance(payload, dict) else None
            for source in sources.values() if isinstance(sources, dict) else ():
                if isinstance(source, dict) and isinstance(source.get("path"), str):
                    paths.extend(_storage_dependencies(root / source["path"]))
        except (OSError, ValueError):
            pass  # The authoritative builder preserves absent/invalid evidence.
    return tuple(dict.fromkeys(paths))


def _summary(artifacts: dict[str, Any] | None, root: Path) -> dict[str, Any]:
    merged = runtime_replay.merge_runtime_artifacts(artifacts)
    inputs = {key: merged.get(key) for key in _INPUT_KEYS}
    key = json.dumps([str(root), inputs], sort_keys=True, default=str)
    with _cache_lock:
        cached = _cache.get(key)
        if cached is not None:
            paths, identities, payload = cached
            if tuple(_identity(path) for path in paths) == identities:
                _cache.move_to_end(key)
                return deepcopy(payload)
            _cache.pop(key, None)
        paths = _dependencies(inputs, root)
        before = tuple(_identity(path) for path in paths)
        payload = runtime_replay.build_replay_precision_summary(artifacts, project_root=root)
        if (_dependencies(inputs, root) == paths
                and tuple(_identity(path) for path in paths) == before):
            _cache[key] = (paths, before, deepcopy(payload))
            while len(_cache) > _CACHE_LIMIT:
                _cache.popitem(last=False)
        return payload


def refresh_registry_replay_precision(
    registry: dict[str, Any], *, project_root: Path | str = runtime_replay.PROJECT_ROOT,
) -> dict[str, Any]:
    """Refresh all registry records while preserving independently mutable evidence."""
    root = Path(project_root).resolve()
    refreshed = dict(registry)
    by_run: dict[str, dict[str, Any]] = {}
    for group in ("canonical_runs", "superseded_runs"):
        if group not in registry:
            continue
        records = []
        for record in registry.get(group) or []:
            current = dict(record)
            precision = _summary(current.get("artifacts"), root)
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
                current["replay_precision"] = by_run.get(str(current.get("run_id") or "")) or _summary(
                    current.get("artifacts"), root,
                )
                layers[key] = current
        refreshed["automation_layers"] = layers
    return refreshed
