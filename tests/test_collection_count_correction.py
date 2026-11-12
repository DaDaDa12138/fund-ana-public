"""Synthetic sources exercise the immutable historical stage-count contract."""
from __future__ import annotations

from dataclasses import replace
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from backend import collection_count_correction as counts
from backend.config import load_settings


def _execution(total, success, failed, skipped, unit):
    return (f"任务完成统计\n总任务数: {total:,}\n已处理: {total:,}\n成功: {success:,}\n"
            f"跳过: {skipped:,}\n失败: {failed:,}\n平均速度: 1.00 {unit}/秒\n")


def _evidence(source, total, success, failed, skipped=0, *, legacy=False):
    task, stage, *_ = counts.SOURCES[source]
    value = {"schema_version": 1, "task": task, "source": source, "stage": stage,
             "target_count": total, "request_count": total, "response_count": total,
             "accepted_response_count": success, "failed_response_count": total - success,
             "has_data_response_count": success, "empty_response_count": total - success,
             "transport_exception_count": 0, "returned_record_count": success,
             "saved_fund_count": success, "written_record_count": success, "save_failed_count": 0}
    if not legacy:
        value.update(success_count=success, failed_count=failed, skipped_count=skipped)
    return value


def _write(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False), encoding="utf-8")


@pytest.fixture
def history(tmp_path):
    reports = tmp_path / "reports"
    reports.mkdir()
    settings = replace(load_settings(), project_root=tmp_path, reports_dir=reports,
                       database_path=tmp_path / "tasks.db", scheduler_state_file=tmp_path / "state.json")
    # The latest state intentionally belongs to a different task; history must
    # resolve through the requested SQL row and its own audit, not current state.
    _write(settings.scheduler_state_file, {"last_runs": {counts.TASK_NAME: {"started_at": "2026-09-07T03:00:00"}}})
    task = {"task_history_id": 1, "task_name": counts.TASK_NAME, "status": "degraded",
            "started_at": "2026-08-23 03:00:00", "finished_at": "2026-08-23T03:10:00"}
    _write(reports / "task.json", {"task": task})
    (tmp_path / "task.log").write_text(
        f"[task-start] task_id=1 task_name={counts.TASK_NAME} started_at=2026-08-23T03:00:00\n"
        "subprocess_index=/app/reports/index.json count=2\n")
    extra = {"task_run_audit_path": "reports/task.json", "task_log_path": "task.log", "records_processed": 2,
             "records_succeeded": 2, "records_failed": 0}
    with sqlite3.connect(settings.database_path) as conn:
        conn.execute("CREATE TABLE task_history(id INTEGER PRIMARY KEY, task_name TEXT, status TEXT, started_at TEXT, completed_at TEXT, records_processed INTEGER, records_inserted INTEGER, records_updated INTEGER, records_failed INTEGER, extra_data TEXT)")
        conn.execute("INSERT INTO task_history VALUES (1, ?, 'degraded', '2026-08-23 03:00:00', '2026-08-23 03:10:07', 0, 0, 0, 0, ?)", (counts.TASK_NAME, json.dumps(extra)))
    by_task = {
        "collect-details": [(_evidence("fundMNDetailInformation", 4, 3, 0, 1), _execution(4, 3, 1, 0, "只基金"))],
        "collect-managers": [(_evidence("fundMNMangerList", 3, 2, 1), _execution(3, 2, 1, 0, "只基金")),
                             (_evidence("fundMSNMangerInfo", 2, 2, 0, legacy=True), _execution(2, 2, 0, 0, "位经理"))],
    }
    # The old manager-list schema lacks skipped_count; only the complete block proves zero.
    by_task["collect-managers"][0][0].pop("skipped_count")
    items = []
    for name, sources in by_task.items():
        stdout = reports / f"{name}.stdout.log"
        stdout.write_text("".join(block for _, block in sources)
                          + "成功: 2\n失败: 0\n跳过: 0\n"
                          + "\n".join("api_source_evidence=" + json.dumps(value) for value, _ in sources))
        (reports / f"{name}.stderr.log").write_text("")
        meta = {"task_name": name, "command": ["python", "main.py", name], "started_at": "2026-08-23T03:01:00",
                "finished_at": "2026-08-23T03:05:00", "return_code": 0, "timed_out": False, "success": True,
                "stdout_path": f"/app/reports/{name}.stdout.log", "stderr_path": f"/app/reports/{name}.stderr.log"}
        _write(reports / f"{name}.meta.json", meta)
        items.append({**meta, "stdout_path_relative": f"reports/{name}.stdout.log", "stderr_path_relative": f"reports/{name}.stderr.log",
                      "meta_path_relative": f"reports/{name}.meta.json"})
    _write(reports / "index.json", {"items": items})
    return settings


def _stages(proof):
    return {stage["stage"]: stage for stage in proof["stages"]}


def test_gzip_audit_keeps_stage_counts_and_physical_source_hash(history):
    import gzip
    import hashlib

    baseline = counts.build_correction(history, 1)
    path = history.reports_dir / "task.json.gz"
    path.write_bytes(gzip.compress((history.reports_dir / "task.json").read_bytes(), mtime=0))
    with sqlite3.connect(history.database_path) as connection:
        extra = json.loads(connection.execute("SELECT extra_data FROM task_history WHERE id=1").fetchone()[0])
        extra["task_run_audit_path"] = "reports/task.json.gz"
        connection.execute("UPDATE task_history SET extra_data=? WHERE id=1", (json.dumps(extra),))
    record = counts.build_correction(history, 1)
    assert record["proof"]["stages"] == baseline["proof"]["stages"]
    assert record["proof"]["whole_task_totals"] is None
    assert record["proof"]["source_files"]["reports/task.json.gz"] == hashlib.sha256(path.read_bytes()).hexdigest()
    counts.write_correction(history, record)
    assert counts.load_verified_correction(history, 1)["verified"] is True
    projected = counts.apply_to_weekly_payload(history, {
        "task_name": counts.TASK_NAME, "started_at": "2026-08-23T03:00:00",
        "task_run_audit_json_project_path": "reports/task.json.gz",
    })
    assert projected["historical_collection_counts"]["verified"] is True


def _change_evidence(settings, source, mutate):
    task = counts.SOURCES[source][0]
    path = settings.reports_dir / f"{task}.stdout.log"
    lines = path.read_text().splitlines()
    for i, line in enumerate(lines):
        if line.startswith("api_source_evidence="):
            value = json.loads(line.split("=", 1)[1])
            if value["source"] == source:
                mutate(value)
                lines[i] = "api_source_evidence=" + json.dumps(value)
    path.write_text("\n".join(lines))


def test_separate_entities_write_evidence_and_originals_preserved(history):
    originals = {path: path.read_bytes() for path in history.project_root.rglob("*") if path.is_file()}
    record = counts.build_correction(history, 1)
    stage = _stages(record["proof"])
    assert stage["manager_list"]["collector_outcomes"] == {
        "target_count": 3, "processed_count": 3, "success_count": 2, "failed_count": 1, "skipped_count": 0}
    assert stage["manager_list"]["entity_type"] == "fund"
    assert stage["manager_detail"]["entity_type"] == "manager"
    assert stage["manager_detail"]["basis"] == "legacy_complete_detail_writes_and_execution_summary"
    assert stage["manager_detail"]["collector_outcomes"]["success_count"] == 2
    assert record["proof"]["original_sql_counts"]["records_processed"] == 0
    assert record["proof"]["original_summary_counts"]["records_processed"] == 2
    assert record["proof"]["whole_task_totals"] is None
    assert record["proof"]["unique_fund_total"] is None
    path = counts.write_correction(history, record)
    result = counts.load_verified_correction(history, 1)
    assert result["verified"] is True
    assert result["underlying_collection_issue_resolved"] is False
    assert "source_evidence" not in result["stages"][0]
    assert {p: p.read_bytes() for p in originals} == originals
    with pytest.raises(FileExistsError):
        counts.write_correction(history, record)
    assert json.loads(path.read_text()) == record


def test_detail_failure_and_skipped_labels_are_both_preserved(history):
    stage = _stages(counts.build_correction(history, 1)["proof"])["fund_detail"]
    assert stage["collector_outcomes"]["skipped_count"] == 1
    assert stage["collector_outcomes"]["failed_count"] == 0
    assert stage["execution_outcomes"]["failed_count"] == 1
    assert stage["execution_outcomes"]["skipped_count"] == 0
    assert stage["logged_written_record_count"] == 3
    assert stage["outcome_labels_match"] is False


def test_zero_target_requires_explicit_zero_activity_and_no_fake_executor(history):
    _change_evidence(history, "fundMSNMangerInfo", lambda value: value.update(_evidence("fundMSNMangerInfo", 0, 0, 0, legacy=True)))
    path = history.reports_dir / "collect-managers.stdout.log"
    path.write_text(path.read_text().replace(_execution(2, 2, 0, 0, "位经理"), ""))
    stage = _stages(counts.build_correction(history, 1)["proof"])["manager_detail"]
    assert stage["collector_outcomes"]["target_count"] == 0
    assert stage["collector_outcomes"]["failed_count"] == 0
    assert stage["execution_outcomes"] is None
    assert stage["outcome_labels_match"] is None


@pytest.mark.parametrize("field,value", [
    ("written_record_count", 0), ("save_failed_count", 1), ("target_count", None), ("target_count", True),
    ("target_count", -2), ("target_count", "2"), ("accepted_response_count", 1), ("request_count", 3),
    ("success_count", 2), ("empty_response_count", 1), ("failed_response_count", 1),
])
def test_legacy_details_never_infer_success_from_http_or_partial_counts(history, field, value):
    _change_evidence(history, "fundMSNMangerInfo", lambda data: data.update({field: value}))
    with pytest.raises(ValueError):
        counts.build_correction(history, 1)


@pytest.mark.parametrize("change", ["sql", "audit", "log", "stdout", "meta", "counts", "hash", "source_list", "totals", "resolved", "rule"])
def test_changed_original_or_unsupported_claim_is_invalid(history, change):
    record = counts.build_correction(history, 1)
    path = counts.write_correction(history, record)
    if change == "sql":
        with sqlite3.connect(history.database_path) as conn:
            conn.execute("UPDATE task_history SET records_processed = 9 WHERE id = 1")
    elif change in {"audit", "log", "stdout", "meta"}:
        names = {"audit": "reports/task.json", "log": "task.log", "stdout": "reports/collect-managers.stdout.log", "meta": "reports/collect-managers.meta.json"}
        source = history.project_root / names[change]
        source.write_text(source.read_text() + "\n")
    else:
        if change == "counts":
            _stages(record["proof"])["manager_list"]["collector_outcomes"]["failed_count"] = 0
        elif change == "source_list":
            record["proof"]["source_files"] = {}
        elif change == "totals":
            record["proof"]["unique_fund_total"] = 5
        elif change == "resolved":
            record["proof"]["underlying_collection_issue_resolved"] = True
        elif change == "rule":
            record["rule_id"] = "different_rule"
        if change != "hash":
            record["record_sha256"] = counts._hash({key: value for key, value in record.items() if key != "record_sha256"})
        else:
            record["record_sha256"] = "0" * 64
        _write(path, record)
    result = counts.load_verified_correction(history, 1)
    assert result["status"] == "invalid" and result["verified"] is False


@pytest.mark.parametrize("change", ["audit_identity", "log_identity", "duplicate_attempt", "outside_task", "missing_source", "duplicate_source", "incomplete_block", "manager_disagreement", "running_task"])
def test_unbound_or_incomplete_history_is_not_repaired(history, change):
    path = history.reports_dir / "collect-managers.stdout.log"
    if change == "audit_identity":
        audit = json.loads((history.reports_dir / "task.json").read_text())
        audit["task"]["task_history_id"] = 2
        _write(history.reports_dir / "task.json", audit)
    elif change == "log_identity":
        log = history.project_root / "task.log"
        log.write_text(log.read_text().replace("task_id=1", "task_id=2"))
    elif change == "duplicate_attempt":
        index = json.loads((history.reports_dir / "index.json").read_text())
        index["items"].append(index["items"][1])
        _write(history.reports_dir / "index.json", index)
    elif change == "outside_task":
        index = json.loads((history.reports_dir / "index.json").read_text())
        index["items"][1]["started_at"] = "2026-08-22T03:01:00"
        meta = json.loads((history.reports_dir / "collect-managers.meta.json").read_text())
        meta["started_at"] = "2026-08-22T03:01:00"
        _write(history.reports_dir / "index.json", index)
        _write(history.reports_dir / "collect-managers.meta.json", meta)
    elif change == "missing_source":
        (history.reports_dir / "collect-managers.stderr.log").unlink()
    elif change == "duplicate_source":
        line = next(line for line in path.read_text().splitlines() if line.startswith("api_source_evidence="))
        path.write_text(path.read_text() + "\n" + line)
    elif change == "incomplete_block":
        path.write_text(path.read_text().replace("已处理: 2\n", ""))
    elif change == "manager_disagreement":
        _change_evidence(history, "fundMNMangerList", lambda value: value.update(failed_count=0, skipped_count=1))
    else:
        with sqlite3.connect(history.database_path) as conn:
            conn.execute("UPDATE task_history SET status = 'running' WHERE id = 1")
    with pytest.raises((ValueError, OSError)):
        counts.build_correction(history, 1)


def test_payload_keeps_raw_counts_and_cannot_reuse_another_task(history):
    counts.write_correction(history, counts.build_correction(history, 1))
    payload = {"task_name": counts.TASK_NAME, "status": "degraded", "started_at": "2026-08-23T03:00:00",
               "records_processed": 0, "task_run_audit_json_project_path": "reports/task.json"}
    result = counts.apply_to_weekly_payload(history, payload)
    assert result["records_processed"] == 0 and result["status"] == "degraded"
    assert result["historical_collection_counts"]["verified"] is True
    stale = counts.apply_to_weekly_payload(history, {**result, "started_at": "2026-09-06T03:00:00"})
    assert stale["historical_collection_counts"]["verified"] is False
    assert "stages" not in stale["historical_collection_counts"]
    assert "historical_collection_counts" not in counts.apply_to_weekly_payload(history, {})
    assert counts.load_verified_correction(history, 2)["status"] == "absent"


def test_sql_null_is_kept_separate_from_explicit_zero(history):
    with sqlite3.connect(history.database_path) as conn:
        conn.execute("UPDATE task_history SET records_updated = NULL WHERE id = 1")
    proof = counts.build_correction(history, 1)["proof"]
    assert proof["original_sql_counts"]["records_updated"] is None
    assert proof["original_sql_counts"]["records_failed"] == 0


def test_missing_explanation_is_absent_with_minimal_audit_settings(tmp_path):
    settings = SimpleNamespace(project_root=tmp_path, database_path=tmp_path / "does_not_exist.db")
    assert counts.load_verified_correction(settings, 1) == {"status": "absent", "verified": False, "task_id": 1}
    assert not settings.database_path.exists()


def test_route_uses_verified_counts_and_reports_missing_or_invalid(history, monkeypatch):
    from backend import app as app_module
    monkeypatch.setattr(app_module, "settings", history)
    client = app_module.app.test_client()
    assert client.get("/api/scheduler/tasks/1/collection-counts").status_code == 404
    counts.write_correction(history, counts.build_correction(history, 1))
    response = client.get("/api/scheduler/tasks/1/collection-counts")
    assert response.status_code == 200 and response.json["verified"] is True
    (history.project_root / "task.log").write_text("changed")
    assert client.get("/api/scheduler/tasks/1/collection-counts").status_code == 409


def test_api_summary_keeps_verified_count_provenance(history, monkeypatch):
    from backend import app as app_module
    monkeypatch.setattr(app_module, "settings", history)
    counts.write_correction(history, counts.build_correction(history, 1))
    raw = {"task_name": counts.TASK_NAME, "status": "degraded", "started_at": "2026-08-23T03:00:00",
           "records_processed": 0, "task_run_audit_json_project_path": "reports/task.json"}
    summary = app_module._summarize_scheduler_run_for_api(raw)
    assert summary["historical_collection_counts"]["verified"] is True
    from backend.scheduler_diagnostics import summarize_scheduler_run_for_api
    repeated = summarize_scheduler_run_for_api(summary)
    assert repeated["historical_collection_counts"] == summary["historical_collection_counts"]
