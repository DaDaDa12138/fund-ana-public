"""Long-term maintenance uses the same holding-period scope as publication."""

import sqlite3
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from backend import long_term_history_maintenance as maintenance


class _FixedDatetime(datetime):
    @classmethod
    def now(cls, tz=None):
        return cls(2026, 10, 21, 15, 0, tzinfo=tz)


def _read_fund(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    shortname="测试普通债券A",
    fullname=None,
    holding_days=None,
    source=None,
    keywords=None,
    legacy_schema=False,
    runtime_excluded=False,
    latest_nav_date="2026-10-20",
    risk_ready=True,
    buyable=1,
):
    database = tmp_path / "maintenance.db"
    with sqlite3.connect(database) as conn:
        conn.execute(
            """CREATE TABLE funds (
                fcode TEXT, shortname TEXT, ftype TEXT, type_code TEXT,
                estab_date TEXT, has_holding_period INTEGER,
                holding_days INTEGER, is_buyable INTEGER
            )"""
        )
        conn.execute(
            "INSERT INTO funds VALUES ('000001', ?, '债券型-混合债', '32', '2010-01-01', ?, ?, ?)",
            (shortname, int(holding_days is not None), holding_days, buyable),
        )
        if not legacy_schema:
            for name in ("fullname", "holding_period_source", "holding_keywords"):
                conn.execute(f"ALTER TABLE funds ADD COLUMN {name} TEXT")
            conn.execute(
                "UPDATE funds SET fullname=?, holding_period_source=?, holding_keywords=?",
                (fullname, source, keywords),
            )
        conn.execute("CREATE TABLE fund_returns (fcode TEXT, latest_nav_date TEXT)")
        conn.execute("INSERT INTO fund_returns VALUES ('000001', ?)", (latest_nav_date,))
        conn.execute("CREATE TABLE fund_annual_returns (fcode TEXT)")
        conn.execute("CREATE TABLE fund_rank_history (fcode TEXT)")
        conn.execute(
            """CREATE TABLE fund_risk_metrics (
                fcode TEXT, period TEXT, end_date TEXT, annual_return REAL,
                sharpe_ratio REAL, max_drawdown REAL, volatility REAL
            )"""
        )
        if risk_ready:
            conn.execute(
                "INSERT INTO fund_risk_metrics VALUES ('000001', '1y', '2026-10-20', 3.0, 1.0, 0.01, 0.02)"
            )

    monkeypatch.setattr(maintenance, "datetime", _FixedDatetime)
    monkeypatch.setattr(
        maintenance,
        "_load_runtime_excluded_fund_codes_lightweight",
        lambda: {"000001"} if runtime_excluded else set(),
    )
    monkeypatch.setattr(maintenance, "load_latest_nav_history_archive_binding", lambda: {})
    monkeypatch.setattr(maintenance, "load_latest_rank_history_archive_binding", lambda: {})
    monkeypatch.setattr(
        maintenance,
        "load_effective_nav_snapshot",
        lambda *args, **kwargs: SimpleNamespace(
            risk_1y_effective_end_date="2026-10-20", risk_1y_latest_end_date="2026-10-20"
        ),
    )
    settings = SimpleNamespace(
        effective_nav_coverage_ratio=0.9,
        effective_nav_weekday_only=True,
        exclude_holding_days_gt=90,
    )
    universe, _, _ = maintenance._read_bond_universe(settings, database)
    return universe[0], database


@pytest.mark.parametrize(
    "metadata, blocked",
    [
        ({"holding_days": 999, "source": "generic_placeholder"}, False),
        ({"holding_days": 999, "keywords": "定开"}, False),
        ({"shortname": "测试180天持有期债券A"}, True),
        ({"fullname": "测试180天持有期债券型证券投资基金"}, True),
        ({"fullname": "测试180天持有期债券基金", "holding_days": 999, "source": "generic_placeholder"}, True),
        ({"shortname": "测试30天持有期债券A", "holding_days": 180}, True),
        ({"holding_days": 999, "source": "api_structured"}, True),
        ({}, False),
        ({"shortname": ""}, True),
        ({"shortname": "--", "fullname": "nan"}, True),
        ({"shortname": "nan", "fullname": "<NA>"}, True),
    ],
)
def test_maintenance_uses_shared_holding_policy(tmp_path, monkeypatch, metadata, blocked):
    row, database = _read_fund(tmp_path, monkeypatch, **metadata)

    assert row["holding_period_blocked"] is blocked
    assert row["screenable_universe"] is not blocked
    blocker = maintenance._classify_long_term_gap_blocker(row)
    action = maintenance._classify_long_term_gap_action(
        row, source_category="local_missing_history_gap", blocker_category=blocker
    )
    assert (action == "scope_filtered_no_retry") is blocked
    with sqlite3.connect(database) as conn:
        # Name fallback must not replace missing database facts with fake zero.
        assert conn.execute("SELECT holding_days FROM funds").fetchone()[0] == metadata.get("holding_days")


@pytest.mark.parametrize("shortname,blocked", [("测试普通债券A", False), ("测试180天持有期债券A", True)])
def test_maintenance_holding_policy_supports_legacy_schema(tmp_path, monkeypatch, shortname, blocked):
    row, _ = _read_fund(tmp_path, monkeypatch, shortname=shortname, legacy_schema=True)

    assert row["holding_period_blocked"] is blocked


@pytest.mark.parametrize(
    "scope, expected_blocker, expected_priority",
    [
        ({"runtime_excluded": True}, "runtime_excluded", "deferred_priority"),
        ({"latest_nav_date": "2026-09-01"}, "nav_or_risk_not_ready", "buyable_active_priority"),
        ({"risk_ready": False}, "nav_or_risk_not_ready", "buyable_active_priority"),
        ({"buyable": 0}, "not_buyable", "screenable_priority"),
    ],
)
def test_maintenance_holding_fallback_preserves_other_scope_conditions(
    tmp_path, monkeypatch, scope, expected_blocker, expected_priority
):
    row, _ = _read_fund(
        tmp_path, monkeypatch, holding_days=999, source="generic_placeholder", **scope
    )

    assert row["holding_period_blocked"] is False
    assert maintenance._classify_long_term_gap_blocker(row) == expected_blocker
    assert row["priority_label"] == expected_priority
