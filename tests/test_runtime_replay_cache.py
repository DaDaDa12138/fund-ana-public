"""Replay cache invalidation uses isolated synthetic files, never fund data."""
from __future__ import annotations

import json
import os
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from backend import replay_evidence, runtime_replay, runtime_replay_cache


@pytest.fixture(autouse=True)
def clear_summary_cache():
    runtime_replay_cache._cache.clear()
    yield
    runtime_replay_cache._cache.clear()


def _registry(snapshot="snapshot.gz"):
    record = {"run_id": "synthetic-fixture", "artifacts": {
        "screening_input_snapshot_json_gz": snapshot,
    }}
    return {"commit_generation": 1, "canonical_runs": [record],
            "automation_layers": {"current_published_run": record}}


def _proof(root, snapshot):
    report = root / "report.json"
    report.write_text('{"synthetic":true}')
    proof = {"snapshot_sha256": replay_evidence.content_hash(snapshot), "baseline_verified": True,
             "status": "matched", "scope": replay_evidence.REPORT_BASELINE_SCOPE,
             "later_database_reads": False, "missing_history_imputation": False,
             "source_files": {key: {"path": report.name, "sha256": replay_evidence.content_hash(report)}
                              for key in replay_evidence.REQUIRED_BASELINE_SOURCES},
             "checks": [{"name": name, "aligned": True}
                        for name in replay_evidence.REQUIRED_BASELINE_CHECKS]}
    replay_evidence.write_evidence(root, proof)
    return report, proof


def _current(registry):
    return registry["automation_layers"]["current_published_run"]["replay_precision"]


def test_unchanged_inputs_are_built_once_without_mutating_cached_payload(tmp_path, monkeypatch):
    (tmp_path / "snapshot.gz").write_bytes(b"synthetic snapshot")
    registry = _registry()
    original = json.dumps(registry, sort_keys=True)
    calls = []
    builder = runtime_replay.build_replay_precision_summary

    def observed(*args, **kwargs):
        calls.append(1)
        return builder(*args, **kwargs)

    monkeypatch.setattr(runtime_replay, "build_replay_precision_summary", observed)
    first = runtime_replay_cache.refresh_registry_replay_precision(registry, project_root=tmp_path)
    _current(first)["status"] = "client mutation"
    second = runtime_replay_cache.refresh_registry_replay_precision(registry, project_root=tmp_path)
    assert calls == [1]
    assert _current(second)["status"] == "inputs_frozen"
    assert second == runtime_replay.refresh_registry_replay_precision(registry, project_root=tmp_path)
    assert json.dumps(registry, sort_keys=True) == original


def test_new_evidence_and_source_invalidation_are_visible_without_publication_change(tmp_path):
    snapshot = tmp_path / "snapshot.gz"
    snapshot.write_bytes(b"synthetic snapshot")
    registry = _registry()
    first = runtime_replay_cache.refresh_registry_replay_precision(registry, project_root=tmp_path)
    assert _current(first)["status"] == "inputs_frozen"
    report, proof = _proof(tmp_path, snapshot)
    second = runtime_replay_cache.refresh_registry_replay_precision(registry, project_root=tmp_path)
    assert _current(second)["baseline_reproduction_verified"] is True
    timestamp = report.stat().st_mtime_ns
    report.write_text('{"synthetic":null}')  # Same byte length, restored mtime.
    os.utime(report, ns=(timestamp, timestamp))
    third = runtime_replay_cache.refresh_registry_replay_precision(registry, project_root=tmp_path)
    assert _current(third)["baseline_reproduction_verified"] is False
    assert _current(third)["baseline_verification"]["status"] == "invalid_evidence"
    assert first["commit_generation"] == second["commit_generation"] == third["commit_generation"]
    assert third == runtime_replay.refresh_registry_replay_precision(registry, project_root=tmp_path)


def test_modified_or_deleted_proof_and_replaced_snapshot_invalidate(tmp_path):
    snapshot = tmp_path / "snapshot.gz"
    snapshot.write_bytes(b"synthetic snapshot")
    _, proof = _proof(tmp_path, snapshot)
    registry = _registry()
    first = runtime_replay_cache.refresh_registry_replay_precision(registry, project_root=tmp_path)
    assert _current(first)["baseline_reproduction_verified"] is True
    proof["checks"][0]["aligned"] = False
    replay_evidence.write_evidence(tmp_path, proof)
    second = runtime_replay_cache.refresh_registry_replay_precision(registry, project_root=tmp_path)
    assert _current(second)["baseline_reproduction_verified"] is False
    replay_evidence.evidence_path(tmp_path, proof["snapshot_sha256"]).unlink()
    assert _current(runtime_replay_cache.refresh_registry_replay_precision(
        registry, project_root=tmp_path))["status"] == "inputs_frozen"
    replacement = tmp_path / "replacement.gz"
    replacement.write_bytes(b"synthetic changed snapshot")
    replacement.replace(snapshot)
    assert runtime_replay_cache.refresh_registry_replay_precision(
        registry, project_root=tmp_path) == runtime_replay.refresh_registry_replay_precision(
            registry, project_root=tmp_path)


def test_publish_chain_and_archive_fallback_availability_invalidate(tmp_path):
    original = tmp_path / "reports/runs/fixture/publish.json"
    archived = tmp_path / "reports/archive/superseded_runs/fixture/publish.json"
    registry = {"canonical_runs": [{"run_id": "fixture", "artifacts": {
        "publish_chain_snapshot_json": str(original.relative_to(tmp_path)),
    }}]}
    refresh = lambda: runtime_replay_cache.refresh_registry_replay_precision(registry, project_root=tmp_path)
    assert refresh()["canonical_runs"][0]["replay_precision"]["publish_gate_inputs_available"] is False
    archived.parent.mkdir(parents=True)
    archived.write_text('{"dynamic_post_review_records":[{"synthetic":true}]}')
    assert refresh()["canonical_runs"][0]["replay_precision"]["publish_gate_inputs_available"] is True
    original.parent.mkdir(parents=True)
    original.write_text('{"dynamic_post_review_records":[]}')
    assert refresh()["canonical_runs"][0]["replay_precision"]["publish_gate_inputs_available"] is False
    original.unlink()
    assert refresh()["canonical_runs"][0]["replay_precision"]["publish_gate_inputs_available"] is True


def test_storage_receipts_and_targets_participate_in_dependency_identity(tmp_path):
    csv = tmp_path / "synthetic.csv"
    stored, receipt = runtime_replay_cache.csv_storage_paths(csv)
    dependencies = runtime_replay_cache._storage_dependencies(csv)
    assert csv in dependencies and stored in dependencies and receipt in dependencies
    before = tuple(runtime_replay_cache._identity(path) for path in dependencies)
    stored.write_bytes(b"synthetic compressed representation")
    receipt.write_text('{"synthetic":"storage receipt"}')
    assert tuple(runtime_replay_cache._identity(path) for path in dependencies) != before
    target = tmp_path / "synthetic-target.json"
    target.write_text('{"synthetic":null}')
    link = tmp_path / "synthetic-link.json"
    link.symlink_to(target.name)
    before = runtime_replay_cache._identity(link)
    replacement = tmp_path / "synthetic-replacement.json"
    replacement.write_text('{"synthetic":true}')
    original_timestamp = target.stat().st_mtime_ns
    os.utime(replacement, ns=(original_timestamp, original_timestamp))
    replacement.replace(target)  # Atomic writers change inode even at the same timestamp.
    assert runtime_replay_cache._identity(link) != before


def test_summary_changed_during_build_is_not_cached(tmp_path, monkeypatch):
    snapshot = tmp_path / "snapshot.gz"
    snapshot.write_bytes(b"synthetic before")
    registry = _registry()
    original = runtime_replay.build_replay_precision_summary

    def changed_during_build(*args, **kwargs):
        payload = original(*args, **kwargs)
        snapshot.write_bytes(b"synthetic after")
        return payload

    monkeypatch.setattr(runtime_replay, "build_replay_precision_summary", changed_during_build)
    runtime_replay_cache.refresh_registry_replay_precision(registry, project_root=tmp_path)
    assert not runtime_replay_cache._cache


def test_proof_source_switch_during_dependency_capture_is_not_cached(tmp_path, monkeypatch):
    snapshot = tmp_path / "snapshot.gz"
    snapshot.write_bytes(b"synthetic snapshot")
    _, proof = _proof(tmp_path, snapshot)
    replacement = tmp_path / "new-source.json"
    replacement.write_text('{"synthetic":true}')
    original = runtime_replay_cache._dependencies
    first = True

    def switching(inputs, root):
        nonlocal first
        paths = original(inputs, root)
        if first:
            first = False
            proof["source_files"] = {
                key: {"path": replacement.name, "sha256": replay_evidence.content_hash(replacement)}
                for key in replay_evidence.REQUIRED_BASELINE_SOURCES
            }
            replay_evidence.write_evidence(tmp_path, proof)
        return paths

    monkeypatch.setattr(runtime_replay_cache, "_dependencies", switching)
    first_result = runtime_replay_cache.refresh_registry_replay_precision(_registry(), project_root=tmp_path)
    assert _current(first_result)["baseline_reproduction_verified"] is True
    assert not runtime_replay_cache._cache
    runtime_replay_cache.refresh_registry_replay_precision(_registry(), project_root=tmp_path)
    replacement.write_text('{"synthetic":null}')
    changed = runtime_replay_cache.refresh_registry_replay_precision(_registry(), project_root=tmp_path)
    assert _current(changed)["baseline_reproduction_verified"] is False
