"""Synthetic boundary fixtures for holding-name fallback and scoring callers."""

from contextlib import contextmanager
from datetime import date
from types import SimpleNamespace

import pandas as pd
import pytest

from core.bond_fund_screening.core.enhanced_scoring_engine import EnhancedScoringEngine
from core.utils.holding_period_policy import holding_period_gate_reason, resolve_holding_period_days


def _score_row(**holding):
    return {
        "annual_return": 5.0,
        "excess_return": 3.0,
        "positive_ratio": 60.0,
        "sharpe_ratio": 1.2,
        "sortino_ratio": 1.8,
        "max_drawdown": 0.02,
        "var_95": -0.03,
        "volatility": 0.03,
        "calmar_ratio": 2.0,
        "multi_period_consistency": 0.8,
        "scale_billion": 5.0,
        "establish_days": 2000,
        **holding,
    }


@pytest.mark.parametrize("name", ["测试半年封闭债券A", "测试半年封闭式债券A"])
def test_half_year_closed_name_is_a_concrete_long_period(name):
    assert resolve_holding_period_days(holding_days=None, fund_name=name) == 180
    assert holding_period_gate_reason(
        has_holding_period=0, holding_days=None, fund_name=name
    ) == "holding_period_limit"


def test_positive_holding_flag_with_zero_days_remains_unscorable():
    frame = pd.DataFrame([_score_row(
        shortname="测试普通债券A", has_holding_period=1, holding_days=0
    )])

    result = EnhancedScoringEngine().calculate_scores(frame).iloc[0]

    assert holding_period_gate_reason(
        has_holding_period=1, holding_days=0, fund_name="测试普通债券A"
    ) == "holding_days_unknown"
    assert pd.isna(result["liquidity_score"])
    assert pd.isna(result["comprehensive_score"])
    assert result["score_status"] == "missing_input"
    assert frame.loc[0, "holding_days"] == result["holding_days"] == 0


@pytest.mark.parametrize("name", ["测试普通债券A", "测试180天持有期债券A"])
def test_fund_name_alias_scores_like_shortname_without_mutating_null(name):
    common = _score_row(has_holding_period=0, holding_days=None)
    frame = pd.DataFrame([{**common, "fund_name": name}, {**common, "shortname": name}])

    result = EnhancedScoringEngine().calculate_scores(frame)

    assert result.loc[0, "liquidity_score"] == result.loc[1, "liquidity_score"]
    assert result["score_status"].tolist() == ["scored", "scored"]
    assert result["holding_days"].isna().all()
    assert frame["holding_days"].isna().all()


def test_accumulation_scoring_passes_name_source_keywords_and_flag(tmp_path, monkeypatch):
    import database
    from bond_fund_screening.utils import accumulation_tracker as tracker_module

    funds = {
        "900001": SimpleNamespace(
            shortname="测试普通债券A", fullname=None, has_holding_period=0,
            holding_days=None, holding_period_source=None, holding_keywords=None,
        ),
        "900002": SimpleNamespace(
            shortname="测试普通债券B", fullname=None, has_holding_period=1,
            holding_days=999, holding_period_source="generic_placeholder", holding_keywords=None,
        ),
        "900003": SimpleNamespace(
            shortname="测试30天持有期债券A", fullname="测试180天持有期债券基金",
            has_holding_period=0, holding_days=None, holding_period_source=None, holding_keywords=None,
        ),
        "900004": SimpleNamespace(
            shortname="测试普通债券C", fullname=None, has_holding_period=1,
            holding_days=999, holding_period_source=None, holding_keywords="定开",
        ),
        "900005": SimpleNamespace(
            shortname="测试普通债券D", fullname=None, has_holding_period=1,
            holding_days=0, holding_period_source=None, holding_keywords=None,
        ),
    }
    for fund in funds.values():
        fund.estab_date = date(2010, 1, 1)
        fund.scale_billion = 5.0

    metrics = SimpleNamespace(**{
        key: value for key, value in _score_row().items()
        if key not in {"excess_return", "multi_period_consistency", "scale_billion", "establish_days"}
    })

    class Query:
        def __init__(self, model):
            self.model = model

        def filter_by(self, **filters):
            self.filters = filters
            return self

        def first(self):
            return funds[self.filters["fcode"]] if self.model.__name__ == "Fund" else metrics

    class Session:
        def query(self, model):
            return Query(model)

    class Manager:
        @contextmanager
        def get_session(self):
            yield Session()

    monkeypatch.setattr(database, "DatabaseManager", lambda **kwargs: Manager())
    monkeypatch.setattr(tracker_module, "_get_project_db_path", lambda: tmp_path / "unused.db")
    logs = []
    tracker = SimpleNamespace(df_tracker=pd.DataFrame({"fcode": list(funds)}), _log=logs.append)

    tracker_module.AccumulationTracker._calculate_enhanced_scores(tracker)

    assert "liquidity_score" in tracker.df_tracker, logs
    scored = tracker.df_tracker.set_index("fcode")
    ordinary = scored.loc["900001", "liquidity_score"]
    assert pd.notna(ordinary)
    assert scored.loc["900002", "liquidity_score"] == ordinary
    assert scored.loc["900004", "liquidity_score"] == ordinary
    assert scored.loc["900003", "liquidity_score"] < ordinary
    assert pd.isna(scored.loc["900005", "liquidity_score"])
    assert funds["900001"].holding_days is None
    assert funds["900003"].holding_days is None
    assert funds["900002"].holding_days == 999
