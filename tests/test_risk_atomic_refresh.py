from datetime import date, datetime
from pathlib import Path
import sys

import pytest


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "core"))

from analyzers.risk_analyzer import RiskAnalyzer
from database.connection import DatabaseManager
from database.models import Base, FundRiskMetrics


def _metric(fcode: str, period: str = "1y") -> dict:
    return {
        "fcode": fcode,
        "period": period,
        "volatility": 0.02,
        "max_drawdown": 0.01,
        "max_drawdown_duration": 7,
        "annual_return": 3.5,
        "sharpe_ratio": 1.2,
        "sortino_ratio": 1.5,
        "raw_sharpe_ratio": 1.2,
        "raw_sortino_ratio": 1.5,
        "sharpe_ratio_cap_hit": False,
        "sortino_ratio_cap_hit": False,
        "calmar_ratio": 3.5,
        "var_95": -0.002,
        "var_99": -0.004,
        "positive_ratio": 61.0,
        "data_points": 250,
        "start_date": date(2025, 7, 10),
        "end_date": date(2026, 7, 10),
        "nav_snapshot_signature": f"signature-{fcode}-{period}",
        "calculated_at": datetime(2026, 7, 11, 12, 0, 0),
    }


def _analyzer(tmp_path: Path) -> tuple[RiskAnalyzer, DatabaseManager]:
    manager = DatabaseManager(db_path=str(tmp_path / "risk.db"), wal_mode=False)
    Base.metadata.create_all(manager.engine)
    analyzer = RiskAnalyzer(config={"risk_metrics": {"risk_free_rate": 0.015}}, db_manager=manager)
    analyzer._write_full_refresh_audit = lambda **_: None
    return analyzer, manager


def test_full_risk_refresh_atomically_replaces_formal_rows(tmp_path: Path) -> None:
    analyzer, manager = _analyzer(tmp_path)
    with manager.get_session(task_name="seed_old_risk") as session:
        session.bulk_insert_mappings(FundRiskMetrics, [_metric("old001")])

    analyzer._replace_all_metrics_atomically([_metric("new001"), _metric("new002")])

    with manager.get_session(readonly=True) as session:
        rows = session.query(FundRiskMetrics).order_by(FundRiskMetrics.fcode).all()
        assert [(row.fcode, row.period) for row in rows] == [
            ("new001", "1y"),
            ("new002", "1y"),
        ]
        assert all(row.max_drawdown_duration == 7 for row in rows)


def test_full_risk_refresh_validation_failure_preserves_previous_snapshot(tmp_path: Path) -> None:
    analyzer, manager = _analyzer(tmp_path)
    with manager.get_session(task_name="seed_old_risk") as session:
        session.bulk_insert_mappings(FundRiskMetrics, [_metric("old001")])

    with pytest.raises(RuntimeError, match="staging 校验失败"):
        analyzer._replace_all_metrics_atomically([_metric("dup001"), _metric("dup001")])

    with manager.get_session(readonly=True) as session:
        rows = session.query(FundRiskMetrics).all()
        assert [(row.fcode, row.period) for row in rows] == [("old001", "1y")]
