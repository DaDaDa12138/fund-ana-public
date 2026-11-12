"""Archive key/index regressions; all rows here are isolated test fixtures."""
from __future__ import annotations

from datetime import date
from pathlib import Path
import sqlite3
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "core"))
from utils import rank_history_archive_rolling_runner as runner
from utils.rank_history_archive_preflight import ARCHIVE_COLUMNS


def _schema(conn: sqlite3.Connection, table: str, date_type: str) -> None:
    conn.execute(f"""CREATE TABLE {table} (
        fcode TEXT, rank_date {date_type}, period TEXT, rank_position INTEGER,
        category_total INTEGER, percentile REAL, created_at DATETIME)""")


@pytest.mark.parametrize("affinity", ["DATE", "NUM", "TEXT"])
def test_indexed_match_preserves_null_zero_period_and_revision_semantics(affinity: str) -> None:
    with sqlite3.connect(":memory:") as conn:
        _schema(conn, "hot_source", affinity)
        _schema(conn, "cold", "TEXT")
        conn.execute("CREATE UNIQUE INDEX cold_key ON cold(fcode, rank_date, COALESCE(period, ''))")
        cases = [
            (0, 0, None, None, "old", "new", 1, 1),
            (None, None, "overall", "overall", None, "new", 1, 1),
            (None, 0, "overall", "overall", "old", "new", None, 1),
            (0, 1, "overall", "overall", "old", "new", 0, 1),
            (0, 0, None, "", "old", "new", 0, 1),
            (0, 0, "overall", "3M", "old", "new", 0, 0),
            (0, 0, "overall", "overall", "invalid", None, 1, 1),
        ]
        for i, (hp, cp, hperiod, cperiod, ht, ct, _, _) in enumerate(cases):
            conn.execute("INSERT INTO hot_source VALUES (?, ?, ?, ?, ?, ?, ?)",
                         (str(i), "2024-02-29", hperiod, 0, None, hp, ht))
            conn.execute("INSERT INTO cold VALUES (?, ?, ?, ?, ?, ?, ?)",
                         (str(i), "2024-02-29", cperiod, 0, None, cp, ct))
        conn.execute("CREATE TEMP TABLE hot AS SELECT * FROM hot_source")
        runner._require_iso_rank_dates(conn, "hot")
        exact = runner._rank_exact_match_sql("h", "a", list(ARCHIVE_COLUMNS))
        key = runner._rank_key_match_sql("h", "a")
        legacy_exact = exact.replace("CAST(h.rank_date AS TEXT)", "h.rank_date")
        legacy_key = key.replace("CAST(h.rank_date AS TEXT)", "h.rank_date")
        results = conn.execute(f"""SELECT ({exact}), ({key}), ({legacy_exact}), ({legacy_key})
            FROM hot h JOIN cold a ON a.fcode = h.fcode ORDER BY h.fcode""").fetchall()
        assert results == [(c[6], c[7], c[6], c[7]) for c in cases]
        plan = conn.execute(f"""EXPLAIN QUERY PLAN SELECT h.fcode FROM hot h WHERE EXISTS
            (SELECT 1 FROM cold a WHERE {key})""").fetchall()
        assert any("cold_key" in row[3] and "rank_date=?" in row[3] and "<expr>=?" in row[3]
                   for row in plan)


@pytest.mark.parametrize("value", [
    None, 0, 0.5, 20240531, "020240531", "20240531", "2024-5-31", "2024-02-30",
    "2024-05-31T00:00:00", "2024-05-31 00:00:00", "2024-05-31 ", "", b"2024-05-31",
])
def test_ambiguous_dates_are_rejected_without_normalizing(value: object) -> None:
    with sqlite3.connect(":memory:") as conn:
        _schema(conn, "hot", "DATE")
        conn.execute("INSERT INTO hot(rank_date) VALUES (?)", (value,))
        before = conn.execute("SELECT rank_date, typeof(rank_date) FROM hot").fetchall()
        with pytest.raises(sqlite3.IntegrityError, match="invalid_rank_date_for_text_archive_match"):
            runner._require_iso_rank_dates(conn, "hot")
        assert conn.execute("SELECT rank_date, typeof(rank_date) FROM hot").fetchall() == before


def test_numeric_date_cast_is_not_treated_as_legacy_equivalent() -> None:
    with sqlite3.connect(":memory:") as conn:
        _schema(conn, "hot", "DATE")
        _schema(conn, "cold", "TEXT")
        conn.execute("INSERT INTO hot(rank_date) VALUES (1)")
        conn.execute("INSERT INTO cold(rank_date) VALUES ('01')")
        assert conn.execute("SELECT a.rank_date=h.rank_date, a.rank_date=CAST(h.rank_date AS TEXT) "
                            "FROM hot h, cold a").fetchone() == (1, 0)
        with pytest.raises(sqlite3.IntegrityError):
            runner._require_iso_rank_dates(conn, "hot")


def _databases(tmp_path: Path, hot_date: str = "2024-05-31") -> tuple[Path, Path]:
    hot, cold = tmp_path / "hot.db", tmp_path / "cold.db"
    with sqlite3.connect(hot) as conn:
        _schema(conn, "fund_rank_history", "DATE")
        conn.execute("INSERT INTO fund_rank_history VALUES (?, ?, 'overall', 0, 100, 0.0, ?)",
                     ("test", hot_date, "2024-06-02"))
    with sqlite3.connect(cold) as conn:
        _schema(conn, "fund_rank_history_archive", "TEXT")
        runner._ensure_archive_table(conn, "fund_rank_history_archive")
        conn.execute("INSERT INTO fund_rank_history_archive VALUES (?, ?, 'overall', 5, 100, 0.05, ?)",
                     ("test", hot_date, "2024-06-01"))
    return hot, cold


def _dump(path: Path) -> list[str]:
    with sqlite3.connect(path) as conn:
        return list(conn.iterdump())


@pytest.mark.parametrize("operation,invalid_date", [
    ("_execute_stale_hot_archive_reconcile_page", "2024-02-30"),
    ("_execute_stale_hot_before_active_delete_page", "2024-02-30"),
    ("_execute_rolling_archive_export_page", "2024-06-02T12:00:00"),
    ("_execute_rolling_hot_delete_page", "2024-06-02T12:00:00"),
])
def test_invalid_date_blocks_each_mutation_atomically(
    tmp_path: Path, operation: str, invalid_date: str,
) -> None:
    hot, cold = _databases(tmp_path, invalid_date)
    before = (_dump(hot), _dump(cold))
    kwargs = dict(db_path=hot, archive_target_path=cold,
                  archive_table_name="fund_rank_history_archive", active_cutoff_date=date(2024, 6, 2),
                  columns=list(ARCHIVE_COLUMNS), page_limit=10)
    if "rolling" in operation:
        kwargs["current_cutoff_date"] = date(2024, 6, 3)
    result = getattr(runner, operation)(**kwargs)
    assert result["status"] == "failed"
    assert "invalid_rank_date_for_text_archive_match" in result["error"]
    assert (_dump(hot), _dump(cold)) == before


def test_date_affinity_revision_is_preserved_before_exact_hot_delete(tmp_path: Path) -> None:
    hot, cold = _databases(tmp_path)
    kwargs = dict(db_path=hot, archive_target_path=cold,
                  archive_table_name="fund_rank_history_archive", active_cutoff_date=date(2024, 6, 2),
                  columns=list(ARCHIVE_COLUMNS), page_limit=10)
    result = runner._execute_stale_hot_archive_reconcile_page(**kwargs)
    assert result["status"] == "reconciled"
    assert result["revision_audit_row_count"] == 1
    assert result["verified_row_count"] == 1
    with sqlite3.connect(cold) as conn:
        assert conn.execute("SELECT rank_position, percentile FROM fund_rank_history_archive").fetchone() == (0, 0.0)
        revision = conn.execute("""SELECT old_rank_position, old_percentile, old_created_at,
            new_rank_position, new_percentile, new_created_at, source
            FROM fund_rank_history_archive_revisions""").fetchone()
        assert revision == (5, 0.05, "2024-06-01", 0, 0.0, "2024-06-02", "hot_reingestion_newer_source")
        assert conn.execute("SELECT COUNT(*) FROM fund_rank_history_archive_revisions").fetchone()[0] == 1
    result = runner._execute_stale_hot_before_active_delete_page(**kwargs)
    assert result["deleted_row_count"] == 1
    with sqlite3.connect(hot) as conn:
        assert conn.execute("SELECT COUNT(*) FROM fund_rank_history").fetchone()[0] == 0


@pytest.mark.parametrize("source_time", ["2024-05-31", "2024-06-01", None, "invalid"])
def test_unproven_newer_revision_preserves_both_databases(tmp_path: Path, source_time: str | None) -> None:
    hot, cold = _databases(tmp_path)
    with sqlite3.connect(hot) as conn:
        conn.execute("UPDATE fund_rank_history SET created_at=?", (source_time,))
    before = (_dump(hot), _dump(cold))
    result = runner._execute_stale_hot_archive_reconcile_page(
        db_path=hot, archive_target_path=cold, archive_table_name="fund_rank_history_archive",
        active_cutoff_date=date(2024, 6, 2), columns=list(ARCHIVE_COLUMNS), page_limit=10,
    )
    assert result["status"] == "failed"
    assert "stale_hot_conflict_source_order_unresolved" in result["error"]
    assert (_dump(hot), _dump(cold)) == before


@pytest.mark.parametrize("hot_value,cold_value", [(None, 0), (0, None)])
def test_missing_business_value_cannot_be_deleted_as_real_zero(
    tmp_path: Path, hot_value: float | None, cold_value: float | None,
) -> None:
    hot, cold = _databases(tmp_path)
    with sqlite3.connect(hot) as conn:
        conn.execute("UPDATE fund_rank_history SET percentile=?", (hot_value,))
    with sqlite3.connect(cold) as conn:
        conn.execute("UPDATE fund_rank_history_archive SET rank_position=0, percentile=?", (cold_value,))
    before = (_dump(hot), _dump(cold))
    result = runner._execute_stale_hot_before_active_delete_page(
        db_path=hot, archive_target_path=cold, archive_table_name="fund_rank_history_archive",
        active_cutoff_date=date(2024, 6, 2), columns=list(ARCHIVE_COLUMNS), page_limit=10,
    )
    assert result["deleted_row_count"] == 0
    assert (_dump(hot), _dump(cold)) == before


def test_rolling_inspection_error_is_a_blocker_not_empty_success(tmp_path: Path) -> None:
    hot, cold = _databases(tmp_path, "2024-06-02T12:00:00")
    with sqlite3.connect(cold) as conn:
        conn.execute("UPDATE fund_rank_history_archive SET rank_date='2024-05-31'")
    before = (_dump(hot), _dump(cold))
    payload = runner.build_rank_history_archive_rolling_runner_payload(
        db_path=hot, project_root=tmp_path, mode="rolling_archive", page_limit=10,
        governance_payload={"summary": {"archive_cutoff_date": "2024-06-03"}},
        active_binding_payload={"summary": {
            "archive_binding_active": True, "runner_status": "archive_binding_active",
            "archive_cutoff_date": "2024-06-02", "archive_target_path": str(cold),
            "existing_archive_row_count": 1, "existing_archive_fund_count": 1,
        }},
    )
    assert any("invalid_rank_date_for_text_archive_match" in item for item in payload["blockers"])
    assert not payload["summary"]["archive_sql_executed"]
    assert not payload["summary"]["delete_sql_executed"]
    assert (_dump(hot), _dump(cold)) == before
