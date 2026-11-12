"""Complete operational evidence survives compression, sealing and recovery."""
from copy import deepcopy
from datetime import datetime
import gzip
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from backend import task_audit_storage as storage
from backend.maintenance_backup_recovery import RecoveryAudit, _verify_task_sidecar
from backend.scheduler_diagnostics import (
    _read_json_object, build_recent_scheduler_failure_recovery_summary,
)
from backend.scheduler_task_timing import seal_task_run_audit
from scripts.audit_data_truthfulness import _read_json


@pytest.mark.parametrize("suffix", [".json", ".json.gz"])
def test_complete_operational_values_and_sealed_completion_survive(tmp_path, suffix):
    target = tmp_path / ("fixture_task" + suffix)
    payload = {"audit_version": 2, "persistence_stage": "audit_collected_before_completion",
               "task": {"task_history_id": 3, "finished_at": None},
               "task_result": {"status": "skipped", "unknown_count": None, "observed_zero": 0},
               "workflow_summary": {"logs_tail": ["完整的操作记录"] * 100}}
    storage.write_task_audit(target, payload)
    before = target.read_bytes()
    assert storage.read_task_audit(target) == payload
    if suffix == ".json.gz":
        assert before[:2] == b"\x1f\x8b"
        assert len(before) < len(gzip.decompress(before))
    result = {"status": "skipped", "started_at": "2026-09-12T06:00:00",
              "finished_at": "2026-09-12T06:00:12.345678", "records_processed": None,
              "observed_zero": 0, "skip_reason": "effective_nav_snapshot_already_screened"}
    seal_task_run_audit(target, result)
    sealed = storage.read_task_audit(target)
    assert sealed["persistence_stage"] == "completion_recorded"
    assert sealed["task_result"] == result
    assert sealed["task"]["finished_at"] == result["finished_at"]
    assert sealed["workflow_summary"] == payload["workflow_summary"]
    assert _read_json_object(target) == _read_json(target) == sealed
    with pytest.raises(ValueError, match="already_sealed"):
        seal_task_run_audit(target, result)
    assert storage.read_task_audit(target) == sealed


def test_new_delivery_refuses_duplicate_identity_and_preserves_existing_bytes(tmp_path):
    target = tmp_path / "fixture.json.gz"
    storage.write_task_audit(target, {"records_processed": None})
    before = target.read_bytes()
    with pytest.raises(FileExistsError):
        storage.write_task_audit(target, {"records_processed": 0})
    assert target.read_bytes() == before
    assert list(tmp_path.glob(".*.tmp")) == []


def test_failed_atomic_seal_keeps_complete_prepared_evidence(tmp_path, monkeypatch):
    target = tmp_path / "fixture.json.gz"
    prepared = {"persistence_stage": "audit_collected_before_completion", "task_result": {"unknown": None}}
    storage.write_task_audit(target, prepared)
    before = target.read_bytes()

    def fail(*args):
        raise OSError("isolated atomic replace failure")

    monkeypatch.setattr(storage.os, "replace", fail)
    with pytest.raises(OSError, match="isolated atomic replace"):
        seal_task_run_audit(target, {"status": "skipped", "finished_at": "2026-09-12T06:01:00"})
    assert target.read_bytes() == before
    assert storage.read_task_audit(target) == prepared
    assert list(tmp_path.glob(".*.tmp")) == []


@pytest.mark.parametrize("damage", ["truncated", "crc", "not_gzip"])
def test_corruption_cannot_be_read_as_valid_operational_evidence(tmp_path, damage):
    target = tmp_path / "fixture.json.gz"
    stored = gzip.compress(b'{"status":"success","unknown":null}')
    if damage == "truncated":
        stored = stored[:-3]
    elif damage == "crc":
        stored = stored[:-8] + bytes([stored[-8] ^ 1]) + stored[-7:]
    else:
        stored = b'{"status":"success"}'
    target.write_bytes(stored)
    with pytest.raises(ValueError, match="gzip_invalid"):
        storage.read_task_audit(target)
    assert _read_json_object(target) == _read_json(target) == {}
    assert target.read_bytes() == stored


def test_budget_applies_after_decompression(tmp_path):
    target = tmp_path / "fixture.json.gz"
    target.write_bytes(gzip.compress(b"x" * 1000))
    assert target.stat().st_size < 100
    with pytest.raises(ValueError, match="read_budget"):
        storage.read_task_audit_bytes(target, max_bytes=100)


def test_writer_verifies_bytes_before_delivery(tmp_path, monkeypatch):
    target = tmp_path / "fixture.json.gz"
    valid_but_wrong = gzip.compress(b'{"unknown":0}')
    monkeypatch.setattr(storage.gzip, "compress", lambda *args, **kwargs: valid_but_wrong)
    with pytest.raises(ValueError, match="written_bytes_mismatch"):
        storage.write_task_audit(target, {"unknown": None})
    assert not target.exists()
    assert list(tmp_path.iterdir()) == []


def test_gzip_task_backup_proof_hashes_stored_bytes_and_full_result_separately(tmp_path):
    path = tmp_path / "task.json.gz"
    original = {"status": "degraded", "records_processed": None, "records_inserted": 0,
                "warning_reason": "fixture_operational_failure"}
    payload = {"persistence_stage": "completion_recorded", "task": {"task_history_id": 7},
               "task_result": original}
    storage.write_task_audit(path, payload)
    raw = json.dumps(original, ensure_ascii=False, separators=(",", ":")).encode()
    compact = {"task_history_extra_data_compacted": True, "task_history_extra_data_schema_version": 1,
               "task_history_full_payload": {"available": True, "source": "scheduler_task_run_audit",
                   "sidecar_project_path": path.name, "full_payload_bytes": len(raw),
                   "full_payload_sha256": hashlib.sha256(raw).hexdigest()}}
    audit = RecoveryAudit(tmp_path)
    proof = _verify_task_sidecar(json.dumps(compact), 7, audit)
    assert proof["sha256"] == hashlib.sha256(raw).hexdigest()
    assert audit.files[str(path)]["sha256"] == hashlib.sha256(path.read_bytes()).hexdigest()
    assert proof["restored_post_audit_fields"] == []
    mutated = deepcopy(payload)
    mutated["task_result"]["records_processed"] = 0
    storage.write_task_audit(path, mutated, replace_existing=True)
    with pytest.raises(ValueError, match="value_mismatch"):
        _verify_task_sidecar(json.dumps(compact), 7, RecoveryAudit(tmp_path))


def test_failure_recovery_fallback_discovers_old_json_and_new_gzip(tmp_path):
    root = tmp_path / "reports/.audit/scheduler_task_runs/20260912"
    root.mkdir(parents=True)
    task_name = "scheduled_snapshot_watchdog_refresh"
    for index, (status, suffix) in enumerate((("failed", ".json"), ("success", ".json.gz")), 1):
        timestamp = f"2026-09-12T06:0{index}:00"
        payload = {"generated_at": timestamp,
                   "task": {"task_name": task_name, "task_key": "snapshot_watchdog",
                            "started_at": timestamp, "finished_at": timestamp, "status": status},
                   "task_result": {"task_name": task_name, "status": status,
                                   "started_at": timestamp, "finished_at": timestamp}}
        storage.write_task_audit(root / (f"fixture_{index}" + suffix), payload)
    result = build_recent_scheduler_failure_recovery_summary(
        SimpleNamespace(project_root=tmp_path, reports_dir=tmp_path / "reports"),
        now=datetime(2026, 9, 12, 7),
    )
    assert result["source"] == "scheduler_task_audit_artifacts"
    assert result["scanned_run_total"] == 2
    assert result["summary"]["recovered_sequence_total"] == 1
    assert result["summary"]["open_failure_sequence_total"] == 0
