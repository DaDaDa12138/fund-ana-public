import math

import pandas as pd

from core.bond_fund_screening.core.enhanced_scoring_engine import EnhancedScoringEngine


def test_missing_holding_period_does_not_receive_zero_day_score() -> None:
    assert math.isnan(EnhancedScoringEngine._holding_period_score(float("nan")))
    assert EnhancedScoringEngine._holding_period_score(0) == 100.0


def test_missing_holding_period_keeps_comprehensive_score_unavailable() -> None:
    frame = pd.DataFrame(
        [
            {
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
                "holding_days": float("nan"),
                "scale_billion": 5.0,
                "establish_days": 2000,
            }
        ]
    )
    result = EnhancedScoringEngine().calculate_scores(frame).iloc[0]
    assert pd.isna(result["liquidity_score"])
    assert pd.isna(result["comprehensive_score"])
    assert result["score_status"] == "missing_input"


def test_reviewed_no_signal_scores_like_zero_days_without_mutating_null() -> None:
    base = {
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
        "has_holding_period": 0,
    }
    frame = pd.DataFrame(
        [
            {
                **base,
                "holding_days": float("nan"),
                "holding_period_source": "no_signal_reviewed",
            },
            {**base, "holding_days": 0, "holding_period_source": ""},
        ]
    )

    result = EnhancedScoringEngine().calculate_scores(frame)

    assert result.loc[0, "liquidity_score"] == result.loc[1, "liquidity_score"]
    assert pd.notna(result.loc[0, "liquidity_score"])
    assert pd.isna(result.loc[0, "holding_days"])
    assert result.loc[0, "score_status"] == result.loc[1, "score_status"] == "scored"


def test_unreviewed_null_holding_remains_unscorable() -> None:
    row = {
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
        "holding_days": float("nan"),
        "holding_period_source": "",
        "has_holding_period": 0,
        "scale_billion": 5.0,
        "establish_days": 2000,
    }

    result = EnhancedScoringEngine().calculate_scores(pd.DataFrame([row])).iloc[0]

    assert pd.isna(result["liquidity_score"])
    assert pd.isna(result["comprehensive_score"])
    assert result["score_status"] == "missing_input"


def test_name_without_holding_signal_scores_as_ordinary_without_filling_source_value() -> None:
    row = {
        "shortname": "普通债券A",
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
        "holding_days": None,
        "holding_period_source": None,
        "has_holding_period": 0,
        "scale_billion": 5.0,
        "establish_days": 2000,
    }

    result = EnhancedScoringEngine().calculate_scores(pd.DataFrame([row])).iloc[0]

    assert pd.notna(result["liquidity_score"])
    assert pd.notna(result["comprehensive_score"])
    assert result["score_status"] == "scored"
    assert pd.isna(result["holding_days"])


def test_name_derived_period_is_used_for_score_and_name_without_term_is_ordinary() -> None:
    base = {
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
        "holding_period_source": None,
        "has_holding_period": 0,
        "scale_billion": 5.0,
        "establish_days": 2000,
    }
    frame = pd.DataFrame([
        {**base, "shortname": "普通债券A", "holding_days": None},
        {**base, "shortname": "示例180天持有期债券A", "holding_days": None},
        {**base, "shortname": "滚动持有债券A", "holding_days": None},
    ])

    result = EnhancedScoringEngine().calculate_scores(frame)

    assert result.loc[0, "score_status"] == "scored"
    assert result.loc[1, "score_status"] == "scored"
    assert result.loc[2, "score_status"] == "missing_input"
    assert pd.isna(result.loc[2, "liquidity_score"])
    assert result.loc[0, "liquidity_score"] > result.loc[1, "liquidity_score"]
    assert result["holding_days"].isna().all()
