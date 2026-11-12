"""Preserve complete historical task values while reducing physical storage."""
from copy import deepcopy
import gzip
import hashlib
import inspect
import json
import os
from pathlib import Path
from types import SimpleNamespace
import time

from flask import Flask
import pytest

from backend import legacy_task_audit_compression as migration
from backend import task_audit_storage as storage
from backend.api_routes.reports import register_reports_routes
from backend.maintenance_backup_recovery import RecoveryAudit, _verify_task_sidecar
from backend.task_history_extra_data_sidecar import write_task_history_extra_data_sidecar
from scripts.audit_data_truthfulness import _file_evidence

SCOPE = "legacy_task_history_extra_data_json"
TASK = "scheduled_daily_bond_refresh"


def encoded(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode()


def legacy_sidecar(root):
    path = root / f"reports/.audit/task_history_extra_data/20260801/task_history_extra_data_7_{TASK}_20260801_080000.json"
    path.parent.mkdir(parents=True)
    result = {"task_name": TASK, "status": "degraded", "records_processed": None, "records_failed": 0,
              "warning_reason": "historical fixture only", "logs": ["完整历史日志"] * 3000}
    full = encoded(result)
    payload = {"schema_version": 1, "task_history_id": 7, "task_name": TASK,
               "source": "guarded_task_history_extra_data_sidecarization", "full_payload": result,
               "full_payload_bytes": len(full), "full_payload_sha256": hashlib.sha256(full).hexdigest()}
    original = (json.dumps(payload, ensure_ascii=False, indent=3) + "  \n\n").encode()
    path.write_bytes(original)
    os.utime(path, (time.time() - 7200,) * 2)
    return path, original, payload


def binding(path, payload):
    return json.dumps({"task_history_extra_data_compacted": True, "task_history_extra_data_schema_version": 1,
        "task_history_full_payload": {"available": True, "source": payload["source"],
            "sidecar_project_path": str(path), "full_payload_bytes": payload["full_payload_bytes"],
            "full_payload_sha256": payload["full_payload_sha256"]}})


def test_native_sidecar_gzip_keeps_complete_values_original_binding_and_collision_evidence(tmp_path):
    original = {"task_name": TASK, "status": "failed", "missing": None, "zero": 0,
                "logs": ["failure history retained"] * 3000}
    raw = json.dumps(original, ensure_ascii=False, indent=3)
    kwargs = dict(reports_dir=tmp_path / "reports", task_info={"id": 7}, task_name=TASK,
                  timestamp="20260801_080000", original_extra_data_text=raw)
    metadata = write_task_history_extra_data_sidecar(original, **kwargs)
    path = Path(metadata["sidecar_path"])
    before = path.read_bytes()
    assert path.name.endswith(".json.gz") and before.startswith(b"\x1f\x8b")
    assert metadata["sidecar_size_bytes"] == len(before) < len(raw.encode())
    payload = storage.read_task_audit(path)
    assert payload["full_payload"] == original
    assert payload["original_extra_data_sha256"] == hashlib.sha256(raw.encode()).hexdigest()
    assert payload["original_extra_data_bytes"] == len(raw.encode())
    proof = _verify_task_sidecar(binding(path, payload), 7, RecoveryAudit(tmp_path))
    assert proof["sha256"] == metadata["full_payload_sha256"] == hashlib.sha256(encoded(original)).hexdigest()
    with pytest.raises(FileExistsError):
        write_task_history_extra_data_sidecar({**original, "missing": 0}, **kwargs)
    assert path.read_bytes() == before and not list(path.parent.glob(".*.tmp"))


def test_legacy_sidecar_migration_preserves_original_url_sha_consumers_and_independent_restore(tmp_path):
    path, original, payload = legacy_sidecar(tmp_path)
    settings = SimpleNamespace(project_root=tmp_path)
    evidence = _file_evidence(settings, path)
    original_proof = _verify_task_sidecar(binding(path, payload), 7, RecoveryAudit(tmp_path))
    assert migration.plan_legacy_task_audit_compression(tmp_path)["candidate_count"] == 0
    plan = migration.plan_legacy_task_audit_compression(tmp_path, scope=SCOPE)
    assert plan["scope"] == SCOPE and plan["candidate_count"] == 1 and plan["removed_count"] == 0
    outcome = migration.compress_legacy_task_audit(tmp_path, plan["candidates"][0])
    assert outcome["status"] == "compressed" and outcome["independent_restore_verified"]
    assert outcome["estimated_net_file_allocation_reduction_bytes"] > 0
    stored, receipt = storage.migration_paths(path)
    assert storage.read_original_artifact_bytes(path) == original == gzip.decompress(stored.read_bytes())
    assert storage.read_task_audit(path) == storage.read_task_audit(stored) == payload
    assert _file_evidence(settings, path) == evidence
    audit = RecoveryAudit(tmp_path)
    assert _verify_task_sidecar(binding(path, payload), 7, audit) == original_proof
    assert set(audit.files) == {str(stored), str(receipt)}
    restored = tmp_path / "restored.json"
    migration.restore_legacy_task_audit(path, restored)
    assert restored.read_bytes() == original and restored.stat().st_ino != stored.stat().st_ino
    app = Flask(__name__)
    callbacks = {name: lambda *a, **kw: {} for name in inspect.signature(register_reports_routes).parameters if name != "app"}
    callbacks["resolve_project_file"] = lambda value: tmp_path / value
    register_reports_routes(app, **callbacks)
    url = "/api/files/" + str(path.relative_to(tmp_path))
    with app.test_client() as client:
        response = client.get(url + "?download=1")
        assert response.status_code == 200 and response.data == original
        assert path.name in response.headers["Content-Disposition"]
        assert client.get(url, headers={"If-None-Match": response.headers["ETag"]}).status_code == 304
        stored.write_bytes(stored.read_bytes()[:-4])
        assert client.get(url).status_code == 409
    assert not storage.artifact_exists(path)
    with pytest.raises(ValueError, match="changed"):
        audit.assert_stable()


@pytest.mark.parametrize("damage", ["full_sha", "bytes", "task_id", "task_name", "running", "source", "schema"])
def test_unbound_or_unfinished_legacy_sidecars_are_retained(tmp_path, damage):
    path, _, payload = legacy_sidecar(tmp_path)
    if damage == "full_sha":
        payload["full_payload"]["records_processed"] = 0
    elif damage == "bytes":
        payload["full_payload_bytes"] -= 1
    elif damage == "task_id":
        payload["task_history_id"] = 8
    elif damage == "task_name":
        payload["task_name"] = "different fixture"
    elif damage == "running":
        payload["full_payload"]["status"] = "running"
    elif damage == "source":
        payload["source"] = "unrecognized producer"
    else:
        payload["schema_version"] = 2
    raw = json.dumps(payload).encode()
    path.write_bytes(raw)
    os.utime(path, (time.time() - 7200,) * 2)
    plan = migration.plan_legacy_task_audit_compression(tmp_path, scope=SCOPE)
    assert plan["candidate_count"] == 0 and plan["retained_count"] == 1
    assert path.read_bytes() == raw


@pytest.mark.parametrize("producer", ["null", "publication", "both"])
@pytest.mark.parametrize("fault", [None, "original_value", "compact_value", "sealed", "wrong_sha"])
def test_legacy_producer_branches_require_exact_original_full_payload_sha(tmp_path, producer, fault):
    original = {"status": "success", "warning_reason": None, "degraded_stage": "", "missing": None,
                "zero": 0, "degraded": False}
    full = dict(original)
    if producer in {"null", "both"}:
        full["task_run_audit_status"] = "written"
        full["task_run_audit_error"] = None
    full["task_run_audit"] = {"artifact_path": "audit.json", "status": "success"}
    full["task_run_audit_path"] = "audit.json"
    if producer in {"publication", "both"}:
        full["workflow_run_id"] = "fixture"
        full["status"] = "degraded"
        full["warning_reason"] = "formal_publication_blocked_unpublished"
        full["degraded_stage"] = "formal_publication"
        full["publication_result"] = {"status": "blocked", "run_id": "fixture"}
        full["status_detail"] = "publication_blocked_unpublished"
    data = encoded(full)
    compact = {key: deepcopy(value) for key, value in full.items() if value is not None and value != ""}
    compact.update(task_history_extra_data_compacted=True, task_history_extra_data_schema_version=1,
                   task_history_full_payload={"available": True, "source": "scheduler_task_run_audit",
                       "sidecar_project_path": "audit.json", "full_payload_bytes": len(data),
                       "full_payload_sha256": hashlib.sha256(data).hexdigest()})
    audit = {"task": {"task_history_id": 7}, "task_result": dict(original)}
    if fault == "original_value":
        audit["task_result"]["missing"] = 0
    elif fault == "compact_value":
        compact["task_run_audit_path"] = "changed.json"
    elif fault == "sealed":
        audit["persistence_stage"] = "completion_recorded"
    elif fault == "wrong_sha":
        compact["task_history_full_payload"]["full_payload_sha256"] = "0" * 64
    path = tmp_path / "audit.json"
    path.write_text(json.dumps(audit))
    raw, stored_before = json.dumps(compact), path.read_bytes()
    if fault:
        with pytest.raises(ValueError, match="task_recovery_sidecar_value_mismatch"):
            _verify_task_sidecar(raw, 7, RecoveryAudit(tmp_path))
    else:
        proof = _verify_task_sidecar(raw, 7, RecoveryAudit(tmp_path))
        assert proof["bytes"] == len(data) and proof["sha256"] == hashlib.sha256(data).hexdigest()
        assert proof.get("restored_producer_null_fields", []) == (["task_run_audit_error"] if producer != "publication" else [])
        assert proof.get("restored_publication_fields_from_compact", []) == (
            ["status", "warning_reason", "degraded_stage"] if producer != "null" else [])
    assert path.read_bytes() == stored_before and raw == json.dumps(compact)


def test_no_null_is_added_when_original_binding_did_not_include_it(tmp_path):
    # A "written" summary alone never proves that the producer emitted a NULL.
    original = {"status": "success", "missing": None}
    full = {**original, "task_run_audit_status": "written"}
    raw = encoded(full)
    path = tmp_path / "audit.json"
    path.write_text(json.dumps({"task": {"task_history_id": 7}, "task_result": original}))
    compact = {"task_run_audit_status": "written", "task_history_extra_data_compacted": True,
        "task_history_extra_data_schema_version": 1,
        "task_history_full_payload": {"available": True, "source": "scheduler_task_run_audit",
            "sidecar_project_path": "audit.json", "full_payload_bytes": len(raw),
            "full_payload_sha256": hashlib.sha256(raw).hexdigest()}}
    proof = _verify_task_sidecar(json.dumps(compact), 7, RecoveryAudit(tmp_path))
    assert "restored_producer_null_fields" not in proof


@pytest.mark.parametrize("wrong_scope", [False, True])
def test_cli_scope_is_bound_to_the_preview_and_candidate_namespace(tmp_path, monkeypatch, wrong_scope):
    from scripts import compress_legacy_task_audits as cli

    path, original, _ = legacy_sidecar(tmp_path)
    plan = migration.plan_legacy_task_audit_compression(tmp_path, scope=SCOPE)
    if wrong_scope:
        plan["scope"] = "legacy_scheduler_task_audit_json"
    plan_path = tmp_path / "plan.json"
    plan_path.write_text(json.dumps(plan))
    output = tmp_path / "execution.json"
    monkeypatch.setattr(cli, "load_settings", lambda _: SimpleNamespace(
        project_root=tmp_path, reports_dir=tmp_path / "reports", state_file=tmp_path / "state/workflow.json"))
    monkeypatch.setattr(cli.sys, "argv", ["compress", "--apply", "--plan", str(plan_path), "--output", str(output)])
    if wrong_scope:
        with pytest.raises(SystemExit) as exc:
            cli.main()
        assert exc.value.code == 2 and path.read_bytes() == original and not output.exists()
    else:
        assert cli.main() == 0
        result = json.loads(output.read_text())
        assert result["scope"] == SCOPE and result["removed_count"] == result["compressed_count"] == 1
        assert result["failed_count"] == result["unprocessed_count"] == 0
        assert storage.read_original_artifact_bytes(path) == original
