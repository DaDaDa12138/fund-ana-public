"""Task metadata fixtures: never substitute successful writes for material changes."""
from copy import deepcopy
from dataclasses import replace
import gzip
import hashlib
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from backend.scheduler_maintenance_metrics import maintenance_metrics, restore_weekly_maintenance_metrics
from backend.scheduler_status_support import build_task_closure_summary, summarize_task_closures_for_state
from backend.scheduler_state import SchedulerStateStore
from backend.scheduler_diagnostics import (
    build_scheduler_child_task_digest, build_scheduler_overview_summary, summarize_scheduler_maintenance_summary_for_api,
    summarize_maintenance_summary_for_status_summary, summarize_scheduler_run_for_api, build_scheduler_status_payload,
)
from backend.api_payloads.dashboard import (
    _reduce_maintenance_summary_for_overview, _reduce_scheduler_child_task_digest_for_overview,
    _reduce_weekly_run_for_overview,
)
from backend.task_history_extra_data_sidecar import compact_task_result_for_history_if_needed

TASK = "scheduled_weekly_bond_maintenance"


def result_fixture():
    return {"task_name": TASK, "status": "degraded", "started_at": "2026-09-13T03:00:00",
            "finished_at": "2026-09-13T05:34:26.692081",
            "nav_history_maintenance": {"maintenance_result": {
                "processed_total": 400, "attempted_total": 396, "recovered_total": 339,
                "failed_total": 4, "nav_records_inserted": 1323270}},
            "long_term_history_maintenance": {"status": "success", "candidate_total": 406,
                "processed_total": 406, "changed_total": None, "maintenance_result": {
                    "processed_total": 406, "annual_success_total": 0, "rank_success_total": 406}}}


def closure(result):
    return build_task_closure_summary(task_key="weekly_maintenance", task_name=TASK, result=result)


def assert_counts(payload):
    nav = payload["child_tasks"]["nav_history_maintenance"]
    long_term = payload["child_tasks"]["long_term_history_maintenance"]
    assert {k: nav[k] for k in ("processed_total", "attempted_total", "recovered_total", "failed_total", "nav_records_inserted")} == {
        "processed_total": 400, "attempted_total": 396, "recovered_total": 339, "failed_total": 4, "nav_records_inserted": 1323270}
    assert nav["changed_total"] is None
    assert long_term["processed_total"] == long_term["rank_success_total"] == 406
    assert long_term["annual_success_total"] == 0 and long_term["changed_total"] is None
    assert long_term["activity_observed"] is True
    assert payload["status"] == "degraded"


def test_counters_survive_history_compaction_state_reload_and_repeated_api_projection(tmp_path):
    raw = result_fixture()
    audit_path = "reports/.audit/scheduler_task_runs/20260913/20260913_030000_scheduled_weekly_bond_maintenance.json.gz"
    log_path = f"logs/tasks/task_12696_{TASK}.log"
    raw.update(task_history_id=12696, task_run_audit={
        "artifact_path": str(tmp_path / audit_path),
        "artifact_path_relative": audit_path, "artifact_project_path": audit_path,
        "task_log_path": str(tmp_path / log_path),
        "task_log_path_relative": log_path, "task_log_project_path": log_path,
    })
    original = deepcopy(raw)
    artifact_fields = {
        "task_run_audit_json": str(tmp_path / audit_path),
        "task_run_audit_json_relative": audit_path, "task_run_audit_json_project_path": audit_path,
        "task_log_file": str(tmp_path / log_path),
        "task_log_file_relative": log_path, "task_log_file_project_path": log_path,
        "task_history_id": 12696,
    }
    compact = compact_task_result_for_history_if_needed(raw, force=True, write_sidecar=False,
                                                       task_info={"id": 12696}, task_name=TASK)
    assert_counts(compact)
    assert {key: compact[key] for key in artifact_fields} == artifact_fields
    state = SchedulerStateStore(tmp_path / "scheduler_state.json")
    state.finish_task(TASK, compact)
    reloaded = SchedulerStateStore(state.state_file).snapshot()["last_runs"][TASK]
    for _ in range(3):
        reloaded = closure(reloaded)
        reloaded = summarize_task_closures_for_state({"weekly_maintenance": reloaded})["weekly_maintenance"]
        assert_counts(reloaded)
        assert {key: reloaded[key] for key in artifact_fields} == artifact_fields
    api = summarize_scheduler_run_for_api(reloaded, project_root=tmp_path)
    assert_counts(api)
    assert {key: api[key] for key in artifact_fields} == artifact_fields
    digest = build_scheduler_child_task_digest(api["child_tasks"])
    assert digest["counts"]["changed_total"] == 0
    assert digest["counts"]["change_unknown_total"] == 2
    assert digest["counts"]["activity_total"] == 2
    assert all(item["changed_total"] is None for item in digest["items"])
    assert raw == original


def test_legacy_policy_skip_survives_unknown_counters():
    raw = {"status": "success", "child_tasks": {"long_term_history_maintenance": {
        "status": "skipped", "skip_reason": "disabled_by_config",
        "no_action_category": "disabled_by_policy", "candidate_total": 0,
        "processed_total": 0, "changed_total": 0,
    }}}
    child = closure(raw)["child_tasks"]["long_term_history_maintenance"]
    assert child["no_action_category"] == "disabled_by_policy"
    assert child["status"] == "skipped"
    assert child["processed_total"] is None and child["changed_total"] is None


@pytest.mark.parametrize("value", [None, "", True, False, -1, 1.5, "bad", "0.5"])
def test_invalid_or_missing_counters_stay_unknown(value):
    raw = result_fixture()
    raw["nav_history_maintenance"]["maintenance_result"]["processed_total"] = value
    observed = closure(raw)["child_tasks"]["nav_history_maintenance"]
    assert observed["processed_total"] is None
    assert observed["no_action_category"] is None


def test_zero_and_explicit_null_never_fall_through_to_success_counters():
    sample = {"processed_total": 0, "changed_total": None,
              "maintenance_result": {"processed_total": 7, "rank_success_total": 406, "annual_success_total": 0}}
    projected = maintenance_metrics(sample)
    assert projected["processed_total"] == 0 and projected["changed_total"] is None
    assert projected["rank_success_total"] == 406 and projected["annual_success_total"] == 0
    assert maintenance_metrics({})["activity_observed"] is None


@pytest.mark.parametrize("project", [summarize_scheduler_maintenance_summary_for_api,
                                    summarize_maintenance_summary_for_status_summary,
                                    _reduce_maintenance_summary_for_overview])
def test_compact_and_summary_endpoints_preserve_null_zero_attempts_and_source(project):
    data = {"nav_processed_total": None, "nav_attempted_total": 396, "nav_records_inserted": 1323270,
            "long_term_changed_total": None, "long_term_annual_success_total": 0,
            "metrics_source": {"status": "bound_task_audit", "task_history_id": 12696}}
    observed = project(data)
    for key, value in data.items():
        assert observed[key] == value
    assert observed["nav_failed_total"] is None


@pytest.fixture
def legacy_task(tmp_path):
    path = tmp_path / "reports/.audit/scheduler_task_runs/20260913/20260913_030000_scheduled_weekly_bond_maintenance.json.gz"
    path.parent.mkdir(parents=True)
    full = result_fixture()
    task = {k: full[k] for k in ("task_name", "status", "started_at", "finished_at")}
    task.update(task_history_id=12696, task_key="weekly_maintenance")
    audit = {"task": task, "task_result": full, "persistence_stage": "completion_recorded"}
    path.write_bytes(gzip.compress(json.dumps(audit).encode()))
    encoded = json.dumps(full, ensure_ascii=False, separators=(",", ":")).encode()
    relative = path.relative_to(tmp_path).as_posix()
    run = {k: full[k] for k in ("task_name", "status", "started_at", "finished_at")}
    run.update(task_history_id=12696, task_run_audit_path="/app/" + relative,
               task_log_file=f"/app/logs/tasks/task_12696_{TASK}.log",
               child_tasks={"nav_history_maintenance": {"processed_total": 400, "changed_total": 339},
                            "long_term_history_maintenance": {"processed_total": 406, "changed_total": 406}})
    compact = {**run, "task_history_full_payload": {
        "available": True, "source": "scheduler_task_run_audit",
        "full_payload_sha256": hashlib.sha256(encoded).hexdigest(), "full_payload_bytes": len(encoded)}}
    settings = SimpleNamespace(project_root=tmp_path, database_path=tmp_path / "tasks.db",
                               reports_dir=tmp_path / "reports")
    with sqlite3.connect(settings.database_path) as conn:
        conn.execute("CREATE TABLE task_history (id INTEGER PRIMARY KEY, task_name TEXT, status TEXT, started_at TEXT, completed_at TEXT, extra_data TEXT)")
        conn.execute("INSERT INTO task_history VALUES(?,?,?,?,?,?)",
                     (12696, TASK, "degraded", full["started_at"].replace("T", " "),
                      full["finished_at"].replace("T", " "), json.dumps(compact)))
    return settings, run, path, audit


def test_legacy_counts_recover_only_from_the_exact_sealed_sql_bound_task(legacy_task):
    settings, run, path, audit = legacy_task
    original = path.read_bytes()
    restored = restore_weekly_maintenance_metrics(run, settings)
    assert_counts(closure(restored))
    source = restored["maintenance_metrics_source"]
    assert source["status"] == "bound_task_audit" and source["task_history_id"] == 12696
    assert source["sql_identity_verified"] is True and len(source["audit_payload_sha256"]) == 64
    assert path.read_bytes() == original and run["child_tasks"]["long_term_history_maintenance"]["changed_total"] == 406


def test_lightweight_status_resolves_the_same_legacy_task_before_projecting_it(legacy_task):
    settings, run, _, _ = legacy_task
    run["action_category"] = "scan_only_no_material_change"
    state = {"last_runs": {TASK: run}}
    original = deepcopy(state)

    def no_heavy_builder(*args):
        raise AssertionError("the lightweight status must not invoke a full overview")

    payload = build_scheduler_status_payload(
        state, include_detail=False,
        build_scheduler_overview_summary=no_heavy_builder,
        build_scheduler_overview_api_summary_fn=no_heavy_builder,
        build_scheduler_chain_audit=no_heavy_builder,
        build_automation_autonomy_summary=lambda *args: {},
        restore_weekly_maintenance_fn=lambda value: restore_weekly_maintenance_metrics(value, settings),
    )
    assert_counts(payload["weekly_run"])
    assert payload["weekly_run"]["maintenance_metrics_source"]["task_history_id"] == 12696
    assert state == original
    assert_counts(summarize_scheduler_run_for_api(payload["weekly_run"]))
    compact = _reduce_weekly_run_for_overview(payload["weekly_run"])
    assert compact["task_history_id"] == 12696
    assert compact["maintenance_metrics_source"] == payload["weekly_run"]["maintenance_metrics_source"]


def test_actual_web_task_summary_entry_recovers_legacy_counts(legacy_task, monkeypatch):
    import backend.app as web

    settings, run, _, _ = legacy_task
    run["action_category"] = "scan_only_no_material_change"
    original = deepcopy(run)
    monkeypatch.setattr(web, "settings", settings)
    projected = web._summarize_scheduler_run_for_api(run)
    assert_counts(projected)
    assert projected["maintenance_metrics_source"]["task_history_id"] == 12696
    assert_counts(web._summarize_scheduler_run_for_api(projected))
    assert run == original


@pytest.mark.parametrize("builder_name", ["_build_health_scheduler_payload", "_build_scheduler_status_summary_payload"])
def test_actual_health_and_status_builders_keep_bound_maintenance_counts(legacy_task, monkeypatch, builder_name):
    import backend.app as web

    local, run, _, _ = legacy_task
    run["action_category"] = "scan_only_no_material_change"
    runtime = replace(web.settings, project_root=local.project_root, database_path=local.database_path,
                      reports_dir=local.reports_dir)
    monkeypatch.setattr(web, "settings", runtime)
    # Ancillary publication/risk diagnostics are outside this task-metadata test.
    for name in ("_load_latest_report_cached", "_load_latest_report_snapshot_cached", "_load_risk_lag_summary",
                 "_load_risk_sync_summary", "_load_risk_coverage_gap_summary", "_load_database_overview",
                 "_load_current_final_selection_governance_queue_summary_cached", "_build_scheduler_overview_summary",
                 "_load_publication_registry_cached", "_load_publication_registry_snapshot_cached",
                 "_load_current_published_report_cached"):
        monkeypatch.setattr(web, name, lambda *args, **kwargs: {})
    monkeypatch.setattr(web, "_resolve_lightweight_scheduler_runtime_digest_sources", lambda *args, **kwargs: ({}, {}))
    monkeypatch.setattr(web, "_load_scheduler_summary_fragment_cached", lambda name, basis, builder: (builder(), {}))
    state = {"enabled": True, "last_runs": {TASK: run}}
    original = deepcopy(state)
    payload = getattr(web, builder_name)(state)
    assert_counts(payload["weekly_run"])
    assert payload["weekly_run"]["maintenance_metrics_source"]["task_history_id"] == 12696
    health_projection = web._build_health_scheduler_status_summary(payload)
    assert_counts(health_projection["weekly_run"])
    assert state == original


def test_restored_full_counts_take_precedence_over_old_children_with_existing_action_category():
    raw = result_fixture()
    raw.update(action_category="scan_only_no_material_change", child_tasks={
        "nav_history_maintenance": {"processed_total": 0, "changed_total": 339},
        "long_term_history_maintenance": {"processed_total": 0, "changed_total": 406},
    })
    assert_counts(summarize_scheduler_run_for_api(raw))


def test_homepage_digest_keeps_unknown_changes_and_explicit_zero_separate():
    digest = {"items": [{"task_key": "long_term_history_maintenance",
                        "changed_total": None, "processed_total": 406,
                        "candidate_total": 0, "activity_observed": True}]}
    child = _reduce_scheduler_child_task_digest_for_overview(digest)["items"][0]
    assert child["changed_total"] is None
    assert child["processed_total"] == 406 and child["candidate_total"] == 0
    assert child["activity_observed"] is True


def test_overview_uses_one_selected_task_and_retains_real_counts_when_stored_closure_is_empty(legacy_task):
    settings, run, _, _ = legacy_task
    class SchedulerStub:
        def __init__(self, settings): pass
        def _build_empty_detail_payload_audit(self): return {}
    state = {"last_runs": {TASK: run}, "task_closures": {"weekly_maintenance": {
        "task_name": TASK, "status": "success", "started_at": "2026-09-06T03:00:00", "child_tasks": {}}}}
    overview = build_scheduler_overview_summary(state, settings=settings, project_scheduler_cls=SchedulerStub,
        select_task_closure_source_fn=lambda **kwargs: kwargs["last_runs"].get(kwargs["task_name"], {}),
        build_task_schedule_health_fn=lambda *args: {}, resolve_run_latest_screened_snapshot_fn=lambda *args: {},
        build_schedule_stability_summary_fn=lambda *args: {}, build_scheduler_task_closure_digest_fn=lambda *args: {})
    assert_counts(overview["weekly_run"])
    expected = {"nav_processed_total": 400, "nav_attempted_total": 396, "nav_recovered_total": 339,
                "nav_failed_total": 4, "nav_records_inserted": 1323270, "long_term_processed_total": 406,
                "long_term_changed_total": None, "long_term_annual_success_total": 0, "long_term_rank_success_total": 406}
    for projection in (lambda x: x, summarize_scheduler_maintenance_summary_for_api,
                       summarize_maintenance_summary_for_status_summary):
        observed = projection(overview["maintenance_summary"])
        assert {k: observed[k] for k in expected} == expected
        assert observed["metrics_source"]["task_history_id"] == 12696


@pytest.mark.parametrize("fault", ["run_id", "run_time", "run_status", "run_name", "audit_id", "audit_time",
                                 "unsealed", "payload_sha", "missing", "sql_status", "sql_path", "symlink"])
def test_mismatched_or_unavailable_audit_never_supplies_other_task_counters(legacy_task, fault):
    settings, original, path, audit = legacy_task
    run = deepcopy(original)
    if fault == "run_id": run["task_history_id"] = 12697
    elif fault == "run_time": run["started_at"] = "2026-09-14T03:00:00"
    elif fault == "run_status": run["status"] = "success"
    elif fault == "run_name": run["task_name"] = "scheduled_daily_bond_refresh"
    elif fault == "audit_id": audit["task"]["task_history_id"] = 12697
    elif fault == "audit_time": audit["task"]["finished_at"] = "2026-09-14T03:00:00"
    elif fault == "unsealed": audit["persistence_stage"] = "starting"
    elif fault == "payload_sha": audit["task_result"]["nav_history_maintenance"]["maintenance_result"]["nav_records_inserted"] = 8
    elif fault.startswith("sql_"):
        with sqlite3.connect(settings.database_path) as conn:
            if fault == "sql_status": conn.execute("UPDATE task_history SET status='success'")
            else:
                compact = json.loads(conn.execute("SELECT extra_data FROM task_history").fetchone()[0])
                compact["task_run_audit_path"] = compact["task_run_audit_path"].replace("030000", "040000")
                conn.execute("UPDATE task_history SET extra_data=?", (json.dumps(compact),))
    if fault == "missing": path.unlink()
    elif fault == "symlink":
        other = path.with_name("other.json.gz"); path.rename(other); path.symlink_to(other)
    else: path.write_bytes(gzip.compress(json.dumps(audit).encode()))
    restored = restore_weekly_maintenance_metrics(run, settings)
    assert restored["maintenance_metrics_source"]["status"] == "unavailable"
    children = closure(restored)["child_tasks"]
    for child in children.values():
        assert child["processed_total"] is None and child["changed_total"] is None
        assert child["maintenance_metrics_status"] == "legacy_unverified"
    assert restored["status"] == run["status"]
