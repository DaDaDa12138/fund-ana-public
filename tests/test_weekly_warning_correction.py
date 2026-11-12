"""Synthetic persisted-task fixtures test explanation without rewriting history."""
from __future__ import annotations
from dataclasses import replace
import json
from pathlib import Path
import sqlite3
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from backend.config import load_settings
from backend import weekly_warning_correction as correction


@pytest.fixture
def persisted_weekly(tmp_path):
    reports = tmp_path / "reports"
    reports.mkdir()
    settings = replace(load_settings(), project_root=tmp_path, reports_dir=reports,
                       database_path=tmp_path / "tasks.db", scheduler_state_file=tmp_path / "scheduler.json")
    log = tmp_path / "task.log"
    log.write_text("[maintenance] subprocess_index=reports/index.json count=1\n")
    stdout = reports / "stdout.log"
    stdout.write_text('api_source_evidence=' + json.dumps({"source": "fundMNMangerList", "target_count": 2,
                     "request_count": 2, "success_count": 1, "failed_count": 1, "skipped_count": 0}) + '\n')
    (reports / "stderr.log").write_text('')
    (reports / "index.json").write_text(json.dumps({"items": [{"task_name": "collect-managers", "command": ["fixture"],
                   "stdout_path_relative": "reports/stdout.log", "stderr_path_relative": "reports/stderr.log"}]}))
    values = {"status": "degraded", "started_at": "2026-09-06T03:00:00", "warning_reason": None,
              "degraded_stage": "collect-managers", "task_log_path": "task.log", "task_run_audit_path": "reports/task.json",
              "task_run_audit_json_project_path": "reports/task.json"}
    (reports / "task.json").write_text(json.dumps({"task": {"task_history_id": 1, "task_name": correction.TASK_NAME},
                                                  "task_result": values}))
    settings.scheduler_state_file.write_text(json.dumps({"last_runs": {correction.TASK_NAME: values},
                                                      "task_closures": {"weekly_maintenance": values}}))
    with sqlite3.connect(settings.database_path) as conn:
        conn.execute("CREATE TABLE task_history(id INTEGER PRIMARY KEY, task_name TEXT, status TEXT, started_at TEXT, extra_data TEXT)")
        conn.execute("INSERT INTO task_history VALUES (?, ?, ?, ?, ?)", (1, correction.TASK_NAME, 'degraded', '2026-09-06 03:00:00', json.dumps(values)))
    return settings


def test_correction_explains_cause_and_preserves_all_original_files(persisted_weekly):
    settings = persisted_weekly
    original = {p: p.read_bytes() for p in settings.project_root.rglob('*') if p.is_file()}
    record = correction.build_correction(settings, 1)
    path = correction.write_correction(settings, record)
    result = correction.load_verified_correction(settings, 1)
    assert result["verified"] is True
    assert result["correction"]["warning_reason"] == "manager_collection_failed"
    assert result["correction"]["task_status"] == "degraded"
    assert result["underlying_collection_issue_resolved"] is False
    assert result["recomputed_classification"]["summary_failed"] == 1
    assert {p: p.read_bytes() for p in original} == original
    with pytest.raises(FileExistsError):
        correction.write_correction(settings, record)
    assert json.loads(path.read_text()) == record


def test_gzip_audit_keeps_degradation_and_physical_evidence_hash(persisted_weekly):
    import gzip
    import hashlib

    settings = persisted_weekly
    path = settings.reports_dir / "task.json.gz"
    old = (settings.reports_dir / "task.json").read_text()
    path.write_bytes(gzip.compress(old.replace("reports/task.json", "reports/task.json.gz").encode(), mtime=0))
    with sqlite3.connect(settings.database_path) as connection:
        raw = connection.execute("SELECT extra_data FROM task_history WHERE id=1").fetchone()[0]
        connection.execute("UPDATE task_history SET extra_data=? WHERE id=1",
                           (raw.replace("reports/task.json", "reports/task.json.gz"),))
    state = settings.scheduler_state_file.read_text()
    settings.scheduler_state_file.write_text(state.replace("reports/task.json", "reports/task.json.gz"))
    record = correction.build_correction(settings, 1)
    assert record["source_files"]["reports/task.json.gz"] == hashlib.sha256(path.read_bytes()).hexdigest()
    correction.write_correction(settings, record)
    result = correction.load_verified_correction(settings, 1)
    assert result["verified"] is True
    assert result["correction"]["task_status"] == "degraded"
    assert result["correction"]["warning_reason"] == "manager_collection_failed"
    assert result["underlying_collection_issue_resolved"] is False


def test_pre_company_classification_remains_exactly_reproducible(persisted_weekly):
    """An immutable v1 record must survive the addition of another collector."""
    settings = persisted_weekly
    record = correction.build_correction(settings, 1)
    # Freeze the pre-company classifier contract, rather than deriving the
    # expected output with the current classifier as build_correction does.
    expected = {
        "degraded": True,
        "degraded_stage": "collect-managers",
        "summary_total": 2,
        "summary_success": 1,
        "summary_failed": 1,
        "summary_skipped": 0,
        "manager_source_counts": [{
            "source": "fundMNMangerList", "summary_total": 2,
            "summary_success": 1, "summary_failed": 1, "summary_skipped": 0,
        }],
        "manager_summary_source": "api_source_evidence",
        "detail_summary_total": 0,
        "detail_summary_success": 0,
        "detail_summary_failed": 0,
        "datas_type_error_count": 0,
        "manager_failure_count": 0,
        "manager_retry_count": 0,
        "manager_terminal_failure_count": 0,
        "summary_by_stage": {"collect-managers": {
            "summary_total": 2, "summary_success": 1,
            "summary_failed": 1, "summary_skipped": 0,
        }},
        "reason": "manager_collection_failed",
    }
    record["recomputed_classification"] = expected
    record["record_sha256"] = correction.object_hash({
        key: value for key, value in record.items() if key != "record_sha256"
    })
    path = correction.write_correction(settings, record)
    original = path.read_bytes()
    result = correction.load_verified_correction(settings, 1)
    assert result["verified"] is True
    assert result["recomputed_classification"] == expected
    assert result["underlying_collection_issue_resolved"] is False
    assert path.read_bytes() == original


@pytest.mark.parametrize("change", ["log", "task", "audit", "claim", "source_list", "state_snapshot"])
def test_changed_original_or_unsupported_claim_invalidates_correction(persisted_weekly, change):
    settings = persisted_weekly
    record = correction.build_correction(settings, 1)
    path = correction.write_correction(settings, record)
    if change in {"log", "audit"}:
        source = settings.reports_dir / ("stdout.log" if change == "log" else "task.json")
        source.write_text(source.read_text() + '\n')
    elif change == "task":
        with sqlite3.connect(settings.database_path) as conn:
            conn.execute("UPDATE task_history SET status = 'success' WHERE id = 1")
    else:
        if change == "claim":
            record["correction"]["underlying_collection_issue_resolved"] = True
        elif change == "source_list":
            record["source_files"] = {}
        else:
            record["originals"]["scheduler_state"]["task_closures.weekly_maintenance"]["snapshot"]["status"] = "success"
        record["record_sha256"] = correction.object_hash({key: value for key, value in record.items() if key != "record_sha256"})
        path.write_text(json.dumps(record))
    result = correction.load_verified_correction(settings, 1)
    assert result["status"] == "invalid" and result["verified"] is False


def test_correction_does_not_follow_a_newest_file_or_another_task(persisted_weekly):
    settings = persisted_weekly
    correction.write_correction(settings, correction.build_correction(settings, 1))
    assert correction.load_verified_correction(settings, 2)["verified"] is False
    assert correction.correction_for_weekly_payload(settings, {})["verified"] is False
    assert correction.correction_for_weekly_payload(settings, {"task_run_audit_json_project_path": "reports/task.json"})["verified"] is True
