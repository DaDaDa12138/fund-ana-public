"""Historical explanations use genuine task12623 inputs and isolated faults."""
from copy import deepcopy
import json
import sqlite3
from types import SimpleNamespace

import pytest

from backend import refresh_collection_correction as correction
from test_refresh_collection_reconciliation import recorded_task, forwarded_task


def create(task):
    record = correction.build_correction(task.settings, task.row["id"])
    path = correction.write_correction(task.settings, record)
    return record, path


def payload(task):
    return {"daily_run": {"task_name": task.row["task_name"], "status": task.row["status"],
                          "started_at": task.row["started_at"], "screening_run_id": task.run_id,
                          "task_run_audit_json_project_path": str(task.task_audit_path.relative_to(task.settings.project_root))}}


def test_genuine_phases_explain_status_and_final_pending_without_repair(recorded_task):
    task = recorded_task
    protected = {path: path.read_bytes() for path in
                 [task.task_audit_path, task.task_log, task.settings.scheduler_state_file]}
    record, path = create(task)
    result = correction.load_verified_correction(task.settings, task.row["id"])
    assert result["verified"] is True
    assert result["recorded"] == {"status": "success", "degraded": False, "degraded_stage": "",
                                   "warning_reason": None, "pending_count": 425}
    assert result["derived"]["status"] == "degraded"
    assert result["derived"]["pending_count"] == 226 and result["derived"]["blocking_count"] == 0
    assert result["derived"]["warning_reason"] == "period_return_date_lag_quarantined"
    assert [phase["saved_fund_count"] for phase in result["phases"]] == [200, 199]
    assert result["phases"][-1]["quarantined_date_lag_fcodes"] == ["016314"]
    assert result["original_task_status_preserved"] is True
    assert result["underlying_collection_issue_resolved"] is False
    assert result["parent_log_bound_phase_count"] == 1
    assert not any(view["source"].startswith("state.") for view in record["proof"]["reconciliation"]["parent_views"])
    assert all(not key.startswith("/") for key in result["source_files"])
    assert json.loads(json.dumps(result)) == result
    assert {source: source.read_bytes() for source in protected} == protected
    with sqlite3.connect(task.settings.database_path) as conn:
        assert conn.execute("SELECT status,extra_data FROM task_history WHERE id=?", (task.row["id"],)).fetchone() == (
            "success", task.row["extra_data"])
    with pytest.raises(FileExistsError):
        correction.write_correction(task.settings, record)
    assert json.loads(path.read_bytes()) == record


@pytest.mark.parametrize("damage", ["parent_log", "phase_stdout", "response", "supporting_nav_log", "sql", "audit", "index"])
def test_source_or_sql_change_invalidates_even_semantically_harmless_edits(recorded_task, damage):
    task = recorded_task
    record, _ = create(task)
    if damage == "sql":
        with sqlite3.connect(task.settings.database_path) as conn:
            # The original value is NULL; NULL + 1 would not inject a change.
            conn.execute("UPDATE task_history SET records_processed=1 WHERE id=?", (task.row["id"],))
            assert conn.execute("SELECT records_processed FROM task_history WHERE id=?", (task.row["id"],)).fetchone()[0] != task.row["records_processed"]
    elif damage == "audit":
        task.task_audit_path.write_bytes(task.task_audit_path.read_bytes() + b"\n")
    else:
        if damage == "parent_log":
            source = task.task_log
        elif damage == "phase_stdout":
            source = task.resolve(task.period_item["stdout_path_relative"])
        elif damage == "response":
            source = task.resolve(task.summary["response_artifact"])
        elif damage == "index":
            source = task.index_path
        else:
            sources = record["proof"]["reconciliation"]["source_files"]
            name = next(name for name in sources if "collect-nav" in name and name.endswith("stdout.log"))
            source = task.settings.project_root / name
        source.write_bytes(source.read_bytes() + b"\n")
    result = correction.load_verified_correction(task.settings, task.row["id"])
    assert result["status"] == "invalid" and result["verified"] is False
    assert "derived" not in result and "recorded" not in result


@pytest.mark.parametrize("claim", ["derived_status", "pending", "original_status", "task_id", "source_list", "phase_count", "resolved", "rule"])
def test_changed_claim_cannot_pass_by_rehashing_the_record(recorded_task, claim):
    task = recorded_task
    record, path = create(task)
    proof = record["proof"]
    if claim == "derived_status":
        proof["reconciliation"]["derived"]["status"] = "success"
    elif claim == "pending":
        proof["reconciliation"]["derived"]["pending_count"] = 0
    elif claim == "original_status":
        proof["recorded"]["status"] = "degraded"
    elif claim == "task_id":
        proof["task_identity"]["id"] += 1
    elif claim == "source_list":
        proof["reconciliation"]["source_files"].pop(next(iter(proof["reconciliation"]["source_files"])))
    elif claim == "phase_count":
        proof["reconciliation"]["phases"].pop()
    elif claim == "resolved":
        proof["underlying_collection_issue_resolved"] = True
    else:
        record["rule_id"] = "unrecognized_rule"
    record["record_sha256"] = correction.object_hash({key: value for key, value in record.items() if key != "record_sha256"})
    path.write_text(json.dumps(record))
    result = correction.load_verified_correction(task.settings, task.row["id"])
    assert result["verified"] is False and result["status"] == "invalid"


@pytest.mark.parametrize("change", ["later_task", "missing_state"])
def test_later_mutable_state_does_not_replace_sealed_historical_views(recorded_task, change):
    task = recorded_task
    create(task)
    before = correction.load_verified_correction(task.settings, task.row["id"])
    if change == "missing_state":
        task.settings.scheduler_state_file.unlink()
    else:
        state = deepcopy(task.state)
        state["last_runs"][task.row["task_name"]].update(started_at="2026-09-13T07:30:00", status="success", screening_run_id="later")
        state["task_closures"]["daily_refresh"].update(started_at="2026-09-13T07:30:00", status="success", screening_run_id="later")
        task.settings.scheduler_state_file.write_text(json.dumps(state))
    assert correction.load_verified_correction(task.settings, task.row["id"]) == before


def test_truthfully_recorded_collection_does_not_get_a_false_correction(forwarded_task):
    with pytest.raises(ValueError, match="no_historical_discrepancy"):
        correction.build_correction(forwarded_task.settings, forwarded_task.row["id"])


def test_incomplete_phase_evidence_never_generates_an_explanation(recorded_task):
    task = recorded_task
    task.resolve(task.period_item["meta_path_relative"]).unlink()
    with pytest.raises(ValueError, match="reconciliation_incomplete"):
        correction.build_correction(task.settings, task.row["id"])
    assert not correction.correction_path(task.settings, task.row["id"]).exists()


def test_absence_never_creates_or_reads_a_business_database(tmp_path):
    settings = SimpleNamespace(project_root=tmp_path, database_path=tmp_path / "absent.db")
    assert correction.load_verified_correction(settings, 1) == {"status": "absent", "verified": False, "task_id": 1}
    assert correction.annotate_scheduler_payload(settings, {"daily_run": {"status": "success"}}) == {"daily_run": {"status": "success"}}
    assert not settings.database_path.exists()


@pytest.mark.parametrize("invalid_id", [False, True, 0, -1, 1.5, "12623"])
def test_task_identity_requires_a_positive_integer(tmp_path, invalid_id):
    settings = SimpleNamespace(project_root=tmp_path, database_path=tmp_path / "absent.db")
    result = correction.load_verified_correction(settings, invalid_id)
    assert result["verified"] is False and result["status"] == "invalid"
    assert not settings.database_path.exists()


@pytest.mark.parametrize("fault", ["duplicate_key", "nonfinite", "oversized"])
def test_malformed_explanation_is_invalid_without_publishing_claims(recorded_task, fault, monkeypatch):
    task = recorded_task
    _, path = create(task)
    if fault == "duplicate_key":
        path.write_text('{"schema_version":1,"schema_version":1}')
    elif fault == "nonfinite":
        path.write_text('{"schema_version":NaN}')
    else:
        monkeypatch.setattr(correction, "MAX_RECORD_BYTES", 8)
    result = correction.load_verified_correction(task.settings, task.row["id"])
    assert result["status"] == "invalid" and "derived" not in result


def test_annotations_preserve_cache_and_keep_original_status(recorded_task):
    task = recorded_task
    create(task)
    cached = payload(task)
    original = deepcopy(cached)
    response = correction.annotate_scheduler_payload(task.settings, cached)
    assert response["daily_run"]["status"] == "success"
    assert response["daily_run"][correction.FIELD]["derived"]["status"] == "degraded"
    assert response["daily_run"][correction.FIELD]["recorded"]["pending_count"] == 425
    assert response["historical_collection_outcomes"]["records"][0]["task_id"] == task.row["id"]
    assert cached == original
    task.task_log.write_bytes(task.task_log.read_bytes() + b"\n")
    invalid = correction.annotate_scheduler_payload(task.settings, response)
    assert invalid["daily_run"]["status"] == "success"
    assert invalid["daily_run"][correction.FIELD]["verified"] is False
    assert "derived" not in invalid["daily_run"][correction.FIELD]
    assert invalid["historical_collection_outcomes"]["records"][0]["verified"] is False
    assert response["daily_run"][correction.FIELD]["verified"] is True


@pytest.mark.parametrize("mismatch", ["time", "status", "run"])
def test_payload_cannot_borrow_another_task_explanation(recorded_task, mismatch):
    task = recorded_task
    create(task)
    value = payload(task)
    run = value["daily_run"]
    if mismatch == "time":
        run["started_at"] = "2026-09-13T07:30:00"
    elif mismatch == "status":
        run["status"] = "failed"
    else:
        run["screening_run_id"] = "another_run"
    result = correction.annotate_scheduler_payload(task.settings, value)
    assert result["daily_run"][correction.FIELD]["status"] == "invalid"
    assert "derived" not in result["daily_run"][correction.FIELD]
    assert result["historical_collection_outcomes"]["records"][0]["verified"] is True


def test_task_endpoint_returns_verified_absent_and_invalid(recorded_task, monkeypatch):
    from backend import app as app_module
    task = recorded_task
    monkeypatch.setattr(app_module, "settings", task.settings)
    client = app_module.app.test_client()
    url = f"/api/scheduler/tasks/{task.row['id']}/collection-outcome"
    response = client.get(url)
    assert response.status_code == 404 and response.headers["Cache-Control"] == "no-store"
    create(task)
    response = client.get(url)
    assert response.status_code == 200 and response.json["verified"] is True
    assert response.json["recorded"]["status"] == "success"
    assert response.json["derived"]["pending_count"] == 226
    task.task_log.write_text(task.task_log.read_text() + "\n")
    assert client.get(url).status_code == 409


@pytest.mark.parametrize("endpoint,builder", [
    ("/api/scheduler/status", "_build_scheduler_status_summary_cached"),
    ("/api/scheduler/status/detail", "_build_scheduler_status_detail_variant_cached"),
    ("/api/scheduler/status/detail?detail=1", "_build_scheduler_status_detail_index_cached"),
    ("/api/scheduler/status/detail?section=task_closures", "_build_scheduler_status_detail_section_cached"),
])
def test_status_api_rechecks_after_each_cached_response(recorded_task, monkeypatch, endpoint, builder):
    from backend import app as app_module
    task = recorded_task
    create(task)
    cached = payload(task)
    original = deepcopy(cached)
    monkeypatch.setattr(app_module, "settings", task.settings)
    monkeypatch.setattr(app_module, "ensure_embedded_scheduler", lambda: None)
    # Publication profiling is outside this task-only fixture. The route's
    # response cache and fresh collection verification remain exercised.
    monkeypatch.setattr(app_module, "_with_scheduler_detail_build_profile", lambda value, *args, **kwargs: value)
    monkeypatch.setattr(app_module, builder, lambda *args, **kwargs: cached)
    client = app_module.app.test_client()
    first = client.get(endpoint)
    assert first.status_code == 200
    assert first.json["daily_run"][correction.FIELD]["verified"] is True
    task.task_log.write_text(task.task_log.read_text() + "\n")
    second = client.get(endpoint)
    assert second.status_code == 200 and second.json["daily_run"][correction.FIELD]["verified"] is False
    assert second.json["daily_run"]["status"] == "success"
    assert cached == original


@pytest.mark.parametrize("endpoint,builder,container", [
    ("/api/health", "_build_health_payload", None),
    ("/api/health/summary", "_build_health_summary_payload", None),
    ("/api/bootstrap/summary", "_build_bootstrap_summary_payload", "health"),
    ("/api/dashboard/overview", "_build_dashboard_overview_response", "scheduler_overview"),
])
def test_homepage_and_health_views_use_fresh_task_bound_explanations(recorded_task, monkeypatch, endpoint, builder, container):
    from backend import app as app_module
    from backend.api_payloads.dashboard import _reduce_scheduler_run_for_overview
    task = recorded_task
    create(task)
    run = payload(task)["daily_run"]
    if container == "scheduler_overview":
        run = _reduce_scheduler_run_for_overview(run)
    cached = {"daily_run": run}
    if container:
        cached = {container: cached}
    original = deepcopy(cached)
    monkeypatch.setattr(app_module, "settings", task.settings)
    monkeypatch.setattr(app_module, "ensure_embedded_scheduler", lambda: None)
    monkeypatch.setattr(app_module, builder, lambda *args, **kwargs: cached)
    monkeypatch.setattr(app_module, "_load_dashboard_overview_payload_for_pytest", lambda: {})
    monkeypatch.setattr(app_module, "_load_dashboard_overview_publication_for_pytest", lambda: {})
    monkeypatch.setattr(app_module, "_load_scheduler_state_snapshot_cached", lambda: {})
    monkeypatch.setattr(app_module, "_attach_dashboard_overview_source_contract", lambda value, *args, **kwargs: value)
    client = app_module.app.test_client()
    first = client.get(endpoint)
    assert first.status_code == 200
    result = first.json[container] if container else first.json
    assert result["daily_run"][correction.FIELD]["verified"] is True
    task.task_log.write_text(task.task_log.read_text() + "\n")
    second = client.get(endpoint)
    assert second.status_code == 200
    result = second.json[container] if container else second.json
    assert result["daily_run"][correction.FIELD]["verified"] is False
    assert cached == original


def test_legacy_homepage_sidecar_without_task_identity_is_not_reused(tmp_path, monkeypatch):
    from backend import app as app_module
    monkeypatch.setattr(app_module, "settings", SimpleNamespace(reports_dir=tmp_path))
    path = tmp_path / ".cache/dashboard_overview_response.json"
    path.parent.mkdir()
    basis = {"schema_version": 1}
    path.write_text(json.dumps({"schema_version": 1, "cache_basis": basis, "cache_key": app_module._dashboard_overview_cache_key(basis),
                               "response": {"scheduler_overview": {"daily_run": {"status": "success"}}}}))
    assert app_module._load_dashboard_overview_sidecar(basis) is None
