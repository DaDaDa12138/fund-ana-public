"""Rank archive continuity and failure recovery with isolated structural fixtures."""
from __future__ import annotations

from datetime import datetime, timedelta
import json
from pathlib import Path
import sqlite3
import sys
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "core"))
from utils import rank_history_archive_rolling_runner as runner
from utils import rank_archive_checkpoint as checkpoints
from database.rank_history_queries import (
    attach_rank_history_archive_if_available, begin_rank_history_read_snapshot,
    build_rank_history_union_source_sql, load_latest_rank_history_archive_binding,
    read_latest_rank_dates,
)

COLUMNS = "fcode,rank_date,period,rank_position,category_total,percentile,created_at"


def databases(root: Path, *, stale: str = "missing") -> tuple[Path, Path, dict]:
    hot, cold = root / "hot.db", root / "cold.db"
    with sqlite3.connect(hot) as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("""CREATE TABLE fund_rank_history (id INTEGER PRIMARY KEY,
            fcode TEXT NOT NULL, rank_date DATE NOT NULL, period TEXT,
            rank_position INTEGER, category_total INTEGER, percentile REAL, created_at TEXT,
            UNIQUE(fcode,rank_date,period))""")
        values = [("window-a", "2024-06-02", "overall", 0, 10, 0.0, "2024-06-04"),
                  ("window-b", "2024-06-03", None, None, 10, None, "2024-06-04")]
        old = [("old-a", "2024-05-31", "overall", 1, 10, 0.1, "2024-06-04"),
               ("old-b", "2024-06-01", "overall", 2, 10, 0.2, "2024-06-04")]
        conn.executemany(f"INSERT INTO fund_rank_history ({COLUMNS}) VALUES (?,?,?,?,?,?,?)",
                         (old if stale else []) + values)
    with sqlite3.connect(cold) as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        runner._ensure_archive_table(conn, "fund_rank_history_archive")
        conn.execute("INSERT INTO fund_rank_history_archive VALUES ('cold-only', '2024-05-30', 'overall', 3, 10, 0.3, '2024-06-04')")
        if stale in {"exact", "newer"}:
            conn.executemany(f"INSERT INTO fund_rank_history_archive ({COLUMNS}) VALUES (?,?,?,?,?,?,?)", old)
            if stale == "newer":
                conn.execute("UPDATE fund_rank_history_archive SET percentile=0.5,created_at='2024-06-03' WHERE fcode LIKE 'old-%'")
        count = conn.execute("SELECT COUNT(*) FROM fund_rank_history_archive").fetchone()[0]
    binding = {"summary": {"runner_status": "archive_binding_active", "archive_binding_active": True,
        "archive_cutoff_date": "2024-06-02", "archive_target_absolute_path": str(cold),
        "active_db_path": str(hot), "archive_table_name": "fund_rank_history_archive",
        "existing_archive_row_count": count, "existing_archive_fund_count": count}}
    runner.write_rank_history_archive_apply_runner_artifacts(binding, output_dir=root / "audit", timestamp="20240101_000000")
    return hot, cold, binding


def run_page(root: Path, binding: dict | None = None, *, target: str = "2024-06-04", mode: str = "rolling_archive") -> dict:
    return runner.build_rank_history_archive_rolling_runner_payload(
        db_path=root / "hot.db", project_root=root, output_dir=str(root / "audit"),
        active_binding_payload=binding, governance_payload={"summary": {"archive_cutoff_date": target}},
        mode=mode, page_limit=1)


def visible(root: Path) -> list[tuple]:
    with sqlite3.connect(root / "hot.db") as conn:
        begin_rank_history_read_snapshot(conn)
        binding = load_latest_rank_history_archive_binding(project_root=root, audit_dir=root / "audit")
        attached = attach_rank_history_archive_if_available(
            conn, archive_path=binding["archive_path"], archive_cutoff_date=binding["archive_cutoff_date"])
        return conn.execute(build_rank_history_union_source_sql(attached) + " ORDER BY fcode,rank_date").fetchall()


@pytest.mark.parametrize("stale", ["missing", "exact", "newer"])
def test_same_cutoff_repair_continues_across_pages_without_losing_values(tmp_path: Path, stale: str) -> None:
    hot, cold, _ = databases(tmp_path, stale=stale)
    before = visible(tmp_path)
    preview = run_page(tmp_path, target="2024-06-02", mode="dry_run")
    assert preview["summary"]["runner_status"] == "rolling_archive_ready"
    for index in range(2):
        page = run_page(tmp_path, target="2024-06-02")
        assert not page["blockers"]
        assert page["summary"]["hot_before_active_cutoff_row_count"] == 1 - index
        assert visible(tmp_path) == before
    assert page["summary"]["runner_status"] == "rolling_archive_complete_binding_ready"
    assert page["summary"]["archive_cutoff_date"] == "2024-06-02"
    with sqlite3.connect(cold) as conn:
        assert conn.execute("SELECT COUNT(*) FROM fund_rank_history_archive").fetchone()[0] == 3
        if stale == "newer":
            assert conn.execute("SELECT COUNT(*) FROM fund_rank_history_archive_revisions").fetchone()[0] == 2
    assert run_page(tmp_path, target="2024-06-02")["summary"]["runner_status"] == "rolling_archive_not_required"


def test_partial_new_window_stays_readable_and_deletes_only_after_durable_binding(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    databases(tmp_path, stale="")
    before = visible(tmp_path)
    first = run_page(tmp_path)
    assert first["summary"]["rolling_archive_inserted_row_count"] == 1
    assert first["summary"]["rolling_hot_delete_deleted_row_count"] == 0
    assert first["summary"]["hot_before_current_cutoff_row_count"] == 2
    assert visible(tmp_path) == before
    delete = runner._execute_rolling_hot_delete_page

    def verify_binding_before_delete(**kwargs):
        binding = load_latest_rank_history_archive_binding(project_root=tmp_path, audit_dir=tmp_path / "audit")
        assert binding["archive_cutoff_date"] == "2024-06-04"
        assert visible(tmp_path) == before
        return delete(**kwargs)

    monkeypatch.setattr(runner, "_execute_rolling_hot_delete_page", verify_binding_before_delete)
    second = run_page(tmp_path)
    assert not second["blockers"]
    assert second["summary"]["active_binding_committed_before_window_delete"]
    assert visible(tmp_path) == before
    third = run_page(tmp_path)
    assert not third["blockers"]
    assert third["summary"]["hot_before_current_cutoff_row_count"] == 0
    assert visible(tmp_path) == before


def test_old_reader_keeps_hot_snapshot_when_binding_advances(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    hot, cold, _ = databases(tmp_path, stale="")
    before = visible(tmp_path)
    run_page(tmp_path)
    reader = sqlite3.connect(hot)
    try:
        begin_rank_history_read_snapshot(reader)
        old = load_latest_rank_history_archive_binding(project_root=tmp_path, audit_dir=tmp_path / "audit")
        assert old["archive_cutoff_date"] == "2024-06-02"
        # Writer finishes before this old reader attaches/queries the cold DB.
        assert not run_page(tmp_path)["blockers"]
        attached = attach_rank_history_archive_if_available(reader, archive_path=cold, archive_cutoff_date=old["archive_cutoff_date"])
        assert reader.execute(build_rank_history_union_source_sql(attached) + " ORDER BY fcode,rank_date").fetchall() == before
        assert set(read_latest_rank_dates(reader, attached)) == {row[0] for row in before}
    finally:
        reader.close()


@pytest.mark.parametrize("failure", ["delete", "binding"])
def test_copy_checkpoint_recovers_after_interrupted_stale_page(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: str) -> None:
    _, cold, original = databases(tmp_path)
    before = visible(tmp_path)
    with monkeypatch.context() as patcher:
        if failure == "delete":
            patcher.setattr(runner, "_execute_stale_hot_before_active_delete_page", lambda **kw: {
                "status": "failed", "error": "injected_delete_failure", "delete_sql_executed": False})
        else:
            def broken_write(*args, **kwargs):
                raise OSError("injected_binding_failure")
            patcher.setattr(runner, "write_rank_history_archive_apply_runner_artifacts", broken_write)
        failed = run_page(tmp_path, original)
    assert failed["summary"]["runner_status"] == "rolling_archive_blocked"
    assert visible(tmp_path) == before
    resumed = run_page(tmp_path)
    assert resumed["summary"]["recovered_reconcile_checkpoint"]
    assert not resumed["blockers"]
    assert visible(tmp_path) == before
    with sqlite3.connect(cold) as conn:
        assert conn.execute(f"SELECT COUNT(*) FROM {checkpoints.TABLE}").fetchone()[0] >= 1
        committed_at = conn.execute(f"SELECT committed_at FROM {checkpoints.TABLE} ORDER BY id DESC LIMIT 1").fetchone()[0]
        assert datetime.fromisoformat(committed_at).utcoffset() == timedelta(hours=8)


def test_binding_failure_keeps_new_window_hot_and_retry_is_idempotent(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    hot, _, _ = databases(tmp_path, stale="")
    before = visible(tmp_path)
    run_page(tmp_path)
    def broken_write(*args, **kwargs):
        raise OSError("injected_binding_failure")
    with monkeypatch.context() as patcher:
        patcher.setattr(runner, "write_rank_history_archive_apply_runner_artifacts", broken_write)
        failed = run_page(tmp_path)
    assert failed["blockers"]
    with sqlite3.connect(hot) as conn:
        assert conn.execute("SELECT COUNT(*) FROM fund_rank_history").fetchone()[0] == 2
    assert visible(tmp_path) == before
    resumed = run_page(tmp_path)
    assert not resumed["blockers"]
    assert resumed["summary"]["rolling_archive_inserted_row_count"] == 0
    assert visible(tmp_path) == before


def test_unexplained_cold_count_change_and_regressed_cutoff_block(tmp_path: Path) -> None:
    hot, cold, _ = databases(tmp_path)
    with sqlite3.connect(cold) as conn:
        conn.execute("INSERT INTO fund_rank_history_archive VALUES ('unexplained', '2024-05-31', 'overall', NULL, NULL, NULL, NULL)")
    failed = run_page(tmp_path, target="2024-06-02")
    assert failed["summary"]["runner_status"] == "rolling_archive_blocked"
    assert not failed["summary"]["delete_sql_executed"]
    assert any(gate["key"] == "active_archive_reconciled" and gate["status"] == "blocked" for gate in failed["gate_checks"])
    regressed = run_page(tmp_path, target="2024-06-01")
    assert any(gate["key"] == "cutoff_progress" and gate["status"] == "blocked" for gate in regressed["gate_checks"])


def test_checkpoint_failure_rolls_back_copy_and_keeps_business_data(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _, cold, _ = databases(tmp_path)
    before = visible(tmp_path)
    def fail_checkpoint(*args, **kwargs):
        raise sqlite3.OperationalError("injected_checkpoint_failure")
    monkeypatch.setattr(runner, "record_checkpoint", fail_checkpoint)
    failed = run_page(tmp_path)
    assert failed["blockers"]
    assert visible(tmp_path) == before
    with sqlite3.connect(cold) as conn:
        assert conn.execute("SELECT COUNT(*) FROM fund_rank_history_archive").fetchone()[0] == 1


def weekly_scheduler(root: Path, monkeypatch: pytest.MonkeyPatch, max_pages: int):
    from backend import scheduler as scheduler_module
    scheduler = scheduler_module.ProjectScheduler.__new__(scheduler_module.ProjectScheduler)
    scheduler.settings = SimpleNamespace(
        scheduler_weekly_nav_history_enabled=False, scheduler_weekly_nav_history_limit=0,
        scheduler_weekly_plain_open_tail_follow_up_enabled=False,
        scheduler_weekly_plain_open_tail_follow_up_category="plain_open_tail_follow_up",
        scheduler_weekly_plain_open_tail_follow_up_skip_risk_refresh=False,
        scheduler_weekly_future_outlier_monitor_enabled=False,
        scheduler_weekly_future_outlier_monitor_category="future_outlier_monitor",
        scheduler_weekly_holding_period_edge_audit_enabled=False,
        scheduler_weekly_nav_history_consistency_audit_enabled=False,
        scheduler_weekly_screening_history_cleanup_preview_enabled=False,
        scheduler_weekly_nav_history_archive_preview_enabled=False,
        scheduler_weekly_rank_history_governance_audit_enabled=True,
        scheduler_weekly_rank_history_archive_enabled=True,
        scheduler_weekly_rank_history_archive_mode="auto",
        scheduler_weekly_rank_history_archive_page_limit=1,
        scheduler_weekly_rank_history_archive_max_pages_per_run=max_pages,
        scheduler_weekly_long_term_history_enabled=False,
        scheduler_weekly_long_term_history_limit=0,
        scheduler_weekly_long_term_history_refresh_stale=True,
        scheduler_weekly_regression_smoke_enabled=False, scheduler_weekly_regression_smoke_groups=(),
        database_path=root / "hot.db", reports_dir=root / "reports", project_root=root,
    )
    scheduler.workflow_manager = SimpleNamespace(is_running=lambda: False)
    scheduler.task_recorder = scheduler.state_store = None
    scheduler._log = lambda message: None
    monkeypatch.setattr(scheduler_module, "run_project_pipeline", lambda *a, **kw: {})
    monkeypatch.setattr(scheduler_module, "run_bond_universe_sync", lambda *a, **kw: {"diffs_after_sync": {
        "missing_in_local_vs_tiantian_total": 0, "missing_in_local_vs_autostock_total": 0,
        "missing_in_local_active_vs_tiantian_total": 0, "missing_in_local_active_vs_autostock_total": 0,
        "newly_synced_codes_total": 0,
    }})
    return scheduler_module, scheduler


@pytest.mark.parametrize("max_pages,expected_pages,exhausted", [(1, 1, True), (6, 4, False)])
def test_scheduler_runs_bounded_real_pages_and_saves_remaining_work(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, max_pages: int, expected_pages: int, exhausted: bool,
) -> None:
    databases(tmp_path)
    before = visible(tmp_path)
    module, scheduler = weekly_scheduler(tmp_path, monkeypatch, max_pages)
    calls = []
    def actual_page(**kwargs):
        calls.append(kwargs)
        assert kwargs["page_limit"] == 1
        return run_page(tmp_path)
    monkeypatch.setattr(module, "build_rank_history_archive_rolling_runner_payload", actual_page)
    result = scheduler._execute_weekly_maintenance()
    archive = result["rank_history_archive_rolling"]
    assert result["rank_history_archive_rolling_error"] is None
    assert len(calls) == archive["page_count"] == expected_pages
    assert archive["budget_exhausted"] is exhausted
    assert len({p["artifact_path"] for p in archive["pages"]}) == expected_pages
    for key, value in archive["totals"].items():
        assert value == sum(p[key] for p in archive["pages"])
    assert visible(tmp_path) == before
    if not exhausted:
        assert archive["summary"]["hot_before_current_cutoff_row_count"] == 0


@pytest.mark.parametrize("failure", ["exception", "no_progress", "blocked"])
def test_scheduler_stops_and_reports_failure_without_independent_apply_fallback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: str,
) -> None:
    databases(tmp_path)
    module, scheduler = weekly_scheduler(tmp_path, monkeypatch, 6)
    def failing_page(**kwargs):
        if failure == "exception":
            raise OSError("injected_archive_failure")
        return {"summary": {"active_binding_available": True,
            "runner_status": "rolling_archive_blocked" if failure == "blocked" else "rolling_archive_in_progress",
            "blocker_count": 1 if failure == "blocked" else 0}}
    page = Mock(side_effect=failing_page)
    apply = Mock(side_effect=AssertionError("second_archive_runner_must_not_run"))
    monkeypatch.setattr(module, "build_rank_history_archive_rolling_runner_payload", page)
    monkeypatch.setattr(module, "build_rank_history_archive_apply_runner_payload", apply)
    result = scheduler._execute_weekly_maintenance()
    assert page.call_count == 1 and apply.call_count == 0
    assert result["rank_history_archive_rolling_error"]
    assert result["rank_history_archive_rolling"]["status"] == "failed"
    assert result["status"] == "degraded"


def test_failed_atomic_binding_publish_leaves_previous_binding_readable(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from utils import rank_history_archive_apply_runner as writer
    databases(tmp_path)
    old = load_latest_rank_history_archive_binding(project_root=tmp_path, audit_dir=tmp_path / "audit")
    def fail_replace(*args, **kwargs):
        raise OSError("injected_replace_failure")
    monkeypatch.setattr(writer.os, "replace", fail_replace)
    with pytest.raises(OSError, match="injected_replace_failure"):
        writer.write_rank_history_archive_apply_runner_artifacts(
            {"summary": {"archive_binding_active": True, "archive_cutoff_date": "2024-06-04"}},
            output_dir=tmp_path / "audit", timestamp="20260101_000000")
    assert load_latest_rank_history_archive_binding(project_root=tmp_path, audit_dir=tmp_path / "audit") == old
    assert not list((tmp_path / "audit").glob("*.tmp"))


def test_stale_repair_can_finish_across_an_empty_new_date_window(tmp_path: Path) -> None:
    hot, _, _ = databases(tmp_path)
    with sqlite3.connect(hot) as conn:
        conn.execute("DELETE FROM fund_rank_history WHERE rank_date >= '2024-06-02'")
    before = visible(tmp_path)
    run_page(tmp_path)
    finished = run_page(tmp_path)
    assert finished["summary"]["runner_status"] == "rolling_archive_complete_binding_ready"
    assert finished["summary"]["archive_cutoff_date"] == "2024-06-04"
    assert visible(tmp_path) == before


def test_unavailable_database_keeps_unknown_counts_and_original_error(tmp_path: Path) -> None:
    bad = tmp_path / "hot.db"
    bad.write_bytes(b"not a sqlite database")
    payload = runner.build_rank_history_archive_rolling_runner_payload(
        db_path=bad, project_root=tmp_path, output_dir=str(tmp_path / "audit"), mode="dry_run")
    summary = payload["summary"]
    assert summary["runner_status"] == "rolling_archive_blocked"
    assert summary["hot_before_current_cutoff_row_count"] is None
    assert summary["archive_before_current_cutoff_row_count"] is None
    assert summary["rolling_archive_required"] is None
    assert summary["current_cutoff_inference_error"] == "file is not a database"
    assert summary["rolling_hot_delete_deleted_row_count"] == 0


@pytest.mark.parametrize("blocked,expected_exit", [(False, 0), (True, 2)])
def test_cli_blocked_state_uses_nonzero_exit_code(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys, blocked: bool, expected_exit: int) -> None:
    from scripts import run_rank_history_archive_rolling_runner as cli
    payload = {"summary": {"blocker_count": int(blocked)}, "blockers": ["inspection_failed"] if blocked else []}
    monkeypatch.setattr(cli, "load_settings", lambda *a: SimpleNamespace(database_path=tmp_path/"hot.db", project_root=tmp_path))
    monkeypatch.setattr(cli, "build_rank_history_archive_rolling_runner_payload", lambda **kw: payload)
    monkeypatch.setattr(cli, "write_rank_history_archive_rolling_runner_artifacts", lambda *a, **kw: (tmp_path/"out.json", tmp_path/"out.md"))
    monkeypatch.setattr(sys, "argv", ["rank-archive", "--mode", "dry_run"])
    assert cli.main() == expected_exit
    assert json.loads(capsys.readouterr().out)["status"] == ("blocked" if blocked else "success")
