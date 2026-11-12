"""Isolated recovery checks; no production database or upstream data inputs."""
from contextlib import closing
import hashlib
import json
from pathlib import Path
import sqlite3

import pytest

from utils import task_history_cleanup_execution as cleanup


@pytest.fixture
def history(tmp_path):
    path = tmp_path / "hot.db"
    with closing(sqlite3.connect(path)) as conn, conn:
        conn.execute("CREATE TABLE task_history(id INTEGER PRIMARY KEY,task_name TEXT NOT NULL,status TEXT NOT NULL,"
                     "started_at TEXT NOT NULL,completed_at TEXT,duration INTEGER,records_processed INTEGER,"
                     "records_inserted INTEGER,records_updated INTEGER,records_failed INTEGER,error_message TEXT,"
                     "error_traceback TEXT,extra_data TEXT)")
        conn.executemany("INSERT INTO task_history VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)", [
            (1, "collector", "success", "2026-05-01 01:00:00", "2026-05-01 01:01:00", 60, 10, 0, 10, None, None, "", '{"missing":null,"zero":0}'),
            (2, "collector", "success", "2026-09-09 01:00:00", "2026-09-09 01:01:00", 60, 11, 0, 11, 0, "", None, None),
            (3, "collector", "failed", "2026-05-01 02:00:00", "2026-05-01 02:01:00", 60, 1, 0, 0, 1, "failed", "trace", ""),
        ])
    return path


def plan(path):
    result = cleanup.build_task_history_cleanup_plan_payload(
        db_path=path, generated_at="2026-09-10T12:00:00", retention_days=14, recent_keep_per_task_status=1)
    assert [row["id"] for row in result["candidate_rows"]] == [1]
    return result


def apply(path, preview):
    return cleanup.execute_task_history_cleanup(
        db_path=path, plan_payload=preview, output_dir=path.parent / "audit", archive_dir=path.parent / "archive",
        mode="apply", confirm_apply=cleanup.REQUIRED_APPLY_CONFIRMATION, backup_dir=path.parent / "backups",
        compress_backup=False)[2]


def all_rows(path):
    with closing(sqlite3.connect(path)) as conn:
        conn.row_factory = sqlite3.Row
        return [dict(row) for row in conn.execute("SELECT * FROM task_history ORDER BY id")]


@pytest.mark.parametrize("column,value", [
    ("extra_data", None), ("extra_data", ""), ("extra_data", "0"), ("extra_data", "null"),
    ("completed_at", ""), ("started_at", " 2026-05-01 01:00:00 "),
    ("task_name", " collector "),
])
def test_task_archive_keeps_all_original_values_including_null_and_empty(history, column, value):
    with closing(sqlite3.connect(history)) as conn, conn:
        conn.execute(f'UPDATE task_history SET "{column}"=? WHERE id=1', (value,))
    original = all_rows(history)
    result = apply(history, plan(history))
    assert result["status"] == "success"
    archive = Path(result["archive"]["rows_archive_absolute_path"])
    entries = [json.loads(line) for line in archive.read_text().splitlines()]
    assert [entry["row"] for entry in entries] == original[:1]
    assert all_rows(history) == original[1:]


@pytest.mark.parametrize("column,value", [("records_failed", 0), ("duration", 61), ("error_message", "changed")])
def test_all_task_columns_are_bound_to_the_cleanup_plan(history, column, value):
    preview = plan(history)
    with closing(sqlite3.connect(history)) as conn, conn:
        conn.execute(f'UPDATE task_history SET "{column}"=? WHERE id=1', (value,))
    before = all_rows(history)
    with pytest.raises(RuntimeError, match="fingerprint mismatch"):
        apply(history, preview)
    assert all_rows(history) == before


def test_task_writer_lock_is_taken_before_archiving(history, monkeypatch):
    archive_rows = cleanup._archive_rows
    outcomes = []

    def archive_then_competing_writer(*args, **kwargs):
        result = archive_rows(*args, **kwargs)
        with closing(sqlite3.connect(history, timeout=0)) as other:
            try:
                other.execute("UPDATE task_history SET records_failed=9 WHERE id=1")
                other.commit()
            except sqlite3.OperationalError as error:
                assert "locked" in str(error)
                outcomes.append("blocked_by_writer_lock")
            else:
                outcomes.append("concurrent_change_was_committed")
        return result

    monkeypatch.setattr(cleanup, "_archive_rows", archive_then_competing_writer)
    result = apply(history, plan(history))
    (history.parent / "late_write_result.json").write_text(json.dumps({"outcomes": outcomes, "execution": result}, indent=2))
    assert result["status"] == "success"
    assert outcomes == ["blocked_by_writer_lock"]


def test_changed_task_archive_is_blocked_before_deletion(history, monkeypatch):
    archive_rows = cleanup._archive_rows

    def archive_then_damage(*args, **kwargs):
        result = archive_rows(*args, **kwargs)
        Path(result["rows_archive_absolute_path"]).write_text('{}\n')
        return result

    monkeypatch.setattr(cleanup, "_archive_rows", archive_then_damage)
    before = all_rows(history)
    with pytest.raises(ValueError, match="task_archive"):
        apply(history, plan(history))
    assert all_rows(history) == before


def test_valid_archive_checksum_does_not_hide_changed_task_values(history, monkeypatch):
    from utils.screening_archive_access import recovery_file_identities

    archive_rows = cleanup._archive_rows

    def archive_wrong_values_with_valid_checksums(*args, **kwargs):
        result = archive_rows(*args, **kwargs)
        path = Path(result["rows_archive_absolute_path"])
        entry = json.loads(path.read_text())
        entry["row"]["records_failed"] = 0
        raw = [entry["row"][column] for column in cleanup.TASK_HISTORY_COLUMNS]
        entry["row_sha256"] = hashlib.sha256(json.dumps(raw, ensure_ascii=False, separators=(",", ":")).encode()).hexdigest()
        path.write_text(json.dumps(entry, ensure_ascii=False) + "\n")
        result["rows_archive_sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
        result["recovery_file_identities"] = recovery_file_identities(
            [path, Path(result["snapshot_watchdog_skipped_summary_absolute_path"])])
        return result

    monkeypatch.setattr(cleanup, "_archive_rows", archive_wrong_values_with_valid_checksums)
    before = all_rows(history)
    with pytest.raises(ValueError, match="task_archive_full_values_mismatch"):
        apply(history, plan(history))
    assert all_rows(history) == before


def test_task_archive_change_after_delete_rolls_back_and_records_zero_committed_deletions(history, monkeypatch):
    delete_ids = cleanup._delete_ids

    def delete_then_damage(conn, *args, **kwargs):
        count = delete_ids(conn, *args, **kwargs)
        assert count == 1
        next((history.parent / "archive").rglob("*.jsonl")).write_text('{}\n')
        return count

    monkeypatch.setattr(cleanup, "_delete_ids", delete_then_damage)
    before = all_rows(history)
    with pytest.raises(ValueError, match="task_archive"):
        apply(history, plan(history))
    assert all_rows(history) == before
    checkpoint = json.loads(next((history.parent / "audit").glob("task_history_cleanup_execution_*.json")).read_text())
    assert checkpoint["status"] == "blocked" and checkpoint["transaction_rolled_back"] is True
    assert checkpoint["summary"]["deleted_count"] == 0


def test_unrecognized_task_column_is_not_silently_dropped(history):
    with closing(sqlite3.connect(history)) as conn, conn:
        conn.execute("ALTER TABLE task_history ADD COLUMN future_detail TEXT")
        conn.execute("UPDATE task_history SET future_detail='must survive cleanup' WHERE id=1")
    before = all_rows(history)
    with pytest.raises(ValueError, match="task_history_schema"):
        apply(history, plan(history))
    assert all_rows(history) == before


def test_task_backup_identifies_its_actual_recovery_purpose(history):
    result = apply(history, plan(history))
    assert result["backup"]["snapshot_metadata"]["purpose"] == "task_history_cleanup"
