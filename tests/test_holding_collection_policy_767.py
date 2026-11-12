"""Synthetic, isolated metadata checks for holding-aware collection queues."""
from datetime import date, datetime, timedelta
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "core")]

from analyzers.risk_analyzer import RiskAnalyzer
from cli.aux_commands import _resolve_changed_nav_risk_candidates
from collectors.period_return_collector import PeriodReturnCollector
from database.connection import DatabaseManager
from database.models import Base, Fund, FundNavHistory, FundReturns


TODAY = date(2026, 10, 21)
ELIGIBLE_CODES = {"900001", "900002", "900005"}


@pytest.fixture
def holding_db(tmp_path):
    """All identifiers and observations here are synthetic validation inputs."""
    manager = DatabaseManager(str(tmp_path / "holding_collection.sqlite"), wal_mode=False)
    Base.metadata.create_all(manager.engine)
    metadata = [
        {"fcode": "900001", "shortname": "测试普通债券A", "holding_days": None},
        {"fcode": "900002", "shortname": "测试定开债券A", "holding_days": 999,
         "has_holding_period": 1, "holding_period_source": "name_generic", "holding_keywords": "定开"},
        {"fcode": "900003", "shortname": "测试普通债券B", "holding_days": 180,
         "has_holding_period": 1, "holding_period_source": "detail_struct"},
        {"fcode": "900004", "shortname": "测试30天持有债券A", "holding_days": None,
         "fullname": "测试180天持有期债券型证券投资基金"},
        {"fcode": "900005", "shortname": "测试30天持有期债券A", "holding_days": None},
        {"fcode": "900006", "shortname": "测试普通债券C", "holding_days": 999,
         "has_holding_period": 1, "holding_period_source": "detail_struct"},
    ]
    with manager.get_session() as session:
        for row in metadata:
            session.add(Fund(ftype="债券型-混合一级", **row))
            session.add(FundReturns(fcode=row["fcode"], latest_nav_date=TODAY))
            session.bulk_insert_mappings(FundNavHistory, [
                {"fcode": row["fcode"], "nav_date": TODAY - timedelta(days=offset), "unit_nav": 1.0}
                for offset in range(366)
            ])
    yield manager
    manager.engine.dispose()


def _period_collector(manager):
    # Target selection needs no HTTP client and must never perform a request.
    collector = PeriodReturnCollector.__new__(PeriodReturnCollector)
    collector.db_manager = manager
    collector.max_nav_age_days = 14
    collector.refresh_hours = 24
    collector.failure_hours = 12
    collector.max_targets = 200
    return collector


def test_risk_queue_uses_concrete_name_terms_and_preserves_raw_null(holding_db):
    analyzer = RiskAnalyzer.__new__(RiskAnalyzer)
    analyzer.holding_period_skip_days = 90
    with holding_db.get_session(readonly=True) as session:
        gates = analyzer._load_fund_risk_gate_map(session, [f"90000{index}" for index in range(1, 7)])
    eligible = {code for code, row in gates.items() if not analyzer._should_skip_risk_metrics_for_fund(row)}
    assert eligible == ELIGIBLE_CODES
    assert gates["900001"]["holding_days"] is None
    assert gates["900004"]["holding_days"] is None
    assert gates["900005"]["holding_days"] is None
    assert gates["900002"]["holding_days"] == 999


def test_scheduled_period_queue_accepts_generic_placeholder_but_blocks_concrete_long_terms(holding_db):
    rows, counts = _period_collector(holding_db)._select_targets(now=datetime.combine(TODAY, datetime.min.time()))
    assert {row["fcode"] for row in rows} == ELIGIBLE_CODES
    assert counts["long_holding_count"] == 3
    assert counts["eligible_count"] == 3


def test_explicit_period_targets_keep_original_long_holding_override(holding_db):
    rows, counts = _period_collector(holding_db)._select_targets(
        now=datetime.combine(TODAY, datetime.min.time()), fcodes=["900004"],
    )
    assert [row["fcode"] for row in rows] == ["900004"]
    assert counts["long_holding_count"] == 0


def test_changed_nav_queue_matches_policy_and_does_not_rewrite_holding_facts(holding_db):
    candidates = _resolve_changed_nav_risk_candidates(
        holding_db.db_path, config={"risk_metrics": {"periods": ["1y"]}},
    )
    assert set(candidates) == ELIGIBLE_CODES
    with holding_db.get_session(readonly=True) as session:
        rows = {row.fcode: row.holding_days for row in session.query(Fund).all()}
    assert rows == {"900001": None, "900002": 999, "900003": 180,
                    "900004": None, "900005": None, "900006": 999}
