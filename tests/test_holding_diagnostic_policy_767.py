"""Risk diagnostics must use the same concrete holding terms as publication."""
from datetime import date, timedelta
import sqlite3
from types import SimpleNamespace

import pytest

from backend.effective_nav import (
    _count_risk_candidate_funds,
    _count_risk_funds_for_candidate,
    _holding_period_limit_predicate,
    _resolve_latest_candidate_risk_snapshot,
)
from backend.risk_lag_audit import _candidate_cte_sql, _uncovered_candidate_total
from backend import risk_sync_audit


def _schema(connection, *, full=True):
    extra = (", shortname TEXT, fullname TEXT, holding_period_source TEXT, holding_keywords TEXT"
             if full else "")
    connection.execute("CREATE TABLE funds (fcode TEXT PRIMARY KEY, has_holding_period INTEGER, "
                       "holding_days INTEGER" + extra + ")")
    connection.execute("CREATE TABLE fund_returns (fcode TEXT PRIMARY KEY, latest_nav_date TEXT)")
    connection.execute("CREATE TABLE fund_risk_metrics (fcode TEXT, period TEXT, end_date TEXT)")


@pytest.mark.parametrize("flag,days,source,keyword,shortname,fullname,blocked", [
    (1, 999, "name", "定开", "隔离定开债券", None, False),
    (1, 999, "detail_generic_closed_cycle", "封闭期投资", "隔离债券", None, False),
    (1, 999, "api", "999天持有", "隔离债券", None, True),
    (0, 180, "api", None, "隔离债券", None, True),
    (0, None, None, None, "隔离普通债券", None, False),
    (0, None, None, None, "隔离30天持有债券", "隔离180天持有期债券基金", True),
    (0, None, None, None, None, "隔离180天持有期债券基金", True),
    (0, None, None, None, None, None, False),
])
def test_sql_holding_policy_uses_names_and_preserves_null(
    flag, days, source, keyword, shortname, fullname, blocked,
):
    with sqlite3.connect(":memory:") as connection:
        _schema(connection)
        connection.execute("INSERT INTO funds VALUES ('000001',?,?,?,?,?,?)",
                           (flag, days, shortname, fullname, source, keyword))
        predicate = _holding_period_limit_predicate(connection)
        assert bool(connection.execute(f"SELECT {predicate} FROM funds f").fetchone()[0]) is blocked
        assert connection.execute("SELECT holding_days FROM funds").fetchone()[0] == days
    # Diagnostics retain unknown contracts in the denominator; they do not
    # convert that retention into permission for formal publication.


def test_risk_coverage_denominator_and_lag_samples_share_policy():
    with sqlite3.connect(":memory:") as connection:
        _schema(connection)
        connection.executemany("INSERT INTO funds VALUES (?,?,?,?,?,?,?)", [
            ("000001", 1, 999, "隔离定开债券", None, "name", "定开"),
            ("000002", 1, 999, "隔离长期债券", None, "api", "999天持有"),
            ("000003", 0, None, "隔离30天持有债券", "隔离180天持有期债券基金", None, None),
            ("000004", 0, None, "隔离普通债券", None, None, None),
        ])
        connection.executemany("INSERT INTO fund_returns VALUES (?, '2026-10-20')",
                               [(f"{i:06d}",) for i in range(1, 5)])
        connection.execute("INSERT INTO fund_risk_metrics VALUES ('000004','1y','2026-10-20')")
        assert _count_risk_candidate_funds(connection, candidate_date="2026-10-20") == 2
        assert _count_risk_funds_for_candidate(
            connection, candidate_date="2026-10-20", risk_end_date="2026-10-20",
        ) == 1
        assert _resolve_latest_candidate_risk_snapshot(
            connection, candidate_date="2026-10-20",
        ) == ("2026-10-20", 1)
        assert _uncovered_candidate_total(connection, "2026-10-20", "2026-10-20") == 1
        candidates = connection.execute(
            f"WITH {_candidate_cte_sql(connection)} SELECT fcode FROM candidate ORDER BY fcode",
            ("2026-10-20",),
        ).fetchall()
        assert [row[0] for row in candidates] == ["000001", "000004"]


def test_legacy_schema_without_name_source_keywords_remains_supported():
    with sqlite3.connect(":memory:") as connection:
        _schema(connection, full=False)
        connection.executemany("INSERT INTO funds VALUES (?,?,?)", [
            ("000001", 0, None), ("000002", 0, 180), ("000003", 1, 999),
        ])
        connection.executemany("INSERT INTO fund_returns VALUES (?, '2026-10-20')",
                               [(f"{i:06d}",) for i in range(1, 4)])
        assert _count_risk_candidate_funds(connection, candidate_date="2026-10-20") == 1
        candidates = connection.execute(
            f"WITH {_candidate_cte_sql(connection)} SELECT fcode FROM candidate", ("2026-10-20",),
        ).fetchall()
        assert candidates == [("000001",)]
        assert connection.execute("SELECT holding_days FROM funds WHERE fcode='000001'").fetchone()[0] is None


@pytest.mark.parametrize("full", [False, True])
def test_risk_sync_keeps_missing_days_and_applies_concrete_terms(tmp_path, monkeypatch, full):
    database = tmp_path / "risk_sync.db"
    with sqlite3.connect(database) as connection:
        _schema(connection, full=full)
        connection.execute("CREATE TABLE fund_nav_history (fcode TEXT, nav_date TEXT)")
        if full:
            connection.executemany("INSERT INTO funds VALUES (?,?,?,?,?,?,?)", [
                ("000001", 0, None, "隔离普通债券", None, None, None),
                ("000002", 1, 999, "隔离定开债券", None, "name", "定开"),
                ("000003", 1, 999, "隔离长期债券", None, "api", "999天持有"),
                ("000004", 0, None, "隔离30天持有债券", "隔离180天持有期债券基金", None, None),
            ])
        else:
            connection.executemany("INSERT INTO funds VALUES (?,?,?)", [
                ("000001", 0, None), ("000002", 0, 30), ("000003", 1, 999), ("000004", 0, 180),
            ])
        dates = [(date(2025, 10, 20) + timedelta(days=12*i)).isoformat() for i in range(31)]
        connection.executemany("INSERT INTO fund_returns VALUES (?,?)",
                               [(f"{i:06d}", dates[-1]) for i in range(1, 5)])
        connection.executemany("INSERT INTO fund_nav_history VALUES (?,?)",
                               [(f"{i:06d}", when) for i in range(1, 5) for when in dates])
    monkeypatch.setattr(risk_sync_audit, "_load_core_config", lambda _: {
        "risk_metrics": {"periods": ["1y"], "min_data_points": 30},
    })
    monkeypatch.setattr(risk_sync_audit, "_resolve_changed_nav_risk_candidates", lambda *args, **kwargs: [])
    payload = risk_sync_audit.build_risk_sync_audit(SimpleNamespace(
        project_root=tmp_path, database_path=database, reports_dir=None,
    ))
    assert payload["audit_error"] is None
    assert payload["period_eligible_gap_candidate_total"] == 4
    assert payload["actionable_candidate_total"] == (1 if full else 2)
    assert payload["low_priority_cadence_tail_total"] == (1 if full else 0)
    null_sample = next(row for row in payload["candidate_samples"] if row["fcode"] == "000001")
    assert null_sample["holding_days"] is None
    assert {row["fcode"] for row in payload["actionable_candidate_samples"]} == (
        {"000001"} if full else {"000001", "000002"}
    )
