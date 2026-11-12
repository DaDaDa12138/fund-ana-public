from __future__ import annotations

from pathlib import Path
import sys

import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.risk_metric_formula_shadow import (
    adjusted_calmar_series,
    apply_metric_variant,
    membership_delta,
    standard_calmar_series,
)


def test_calmar_shadow_distinguishes_standard_and_layer6_adjusted_contracts() -> None:
    frame = pd.DataFrame(
        [
            {
                "annual_return": 3.0,
                "max_drawdown": 0.001,
                "债基风格标签": "中短债",
            }
        ]
    )

    standard = standard_calmar_series(frame)
    adjusted, effective_drawdown, floor_bound = adjusted_calmar_series(
        frame,
        risk_free_rate=0.015,
        min_drawdown_by_style={"中短债": 0.002, "default": 0.005},
        deduct_risk_free_rate=True,
        cap_abs=20.0,
    )

    assert standard.iloc[0] == pytest.approx(30.0)
    assert effective_drawdown.iloc[0] == pytest.approx(0.002)
    assert adjusted.iloc[0] == pytest.approx(7.5)
    assert bool(floor_bound.iloc[0]) is True


def test_sortino_zero_downside_variants_preserve_missing_or_bounded_semantics() -> None:
    frame = pd.DataFrame(
        [
            {
                "基金代码": "000001",
                "raw_sortino_ratio": 88.0,
                "sortino_ratio": 10.0,
                "1年-索提诺比率": 88.0,
                "1年-索提诺比率(展示封顶)": 10.0,
                "sortino_ratio_cap_hit": True,
                "1年-索提诺cap命中": 1,
            },
            {
                "基金代码": "000002",
                "raw_sortino_ratio": 2.0,
                "sortino_ratio": 2.0,
                "1年-索提诺比率": 2.0,
            },
        ]
    )
    kwargs = {
        "risk_free_rate": 0.015,
        "min_drawdown_by_style": {"default": 0.005},
        "zero_downside_codes": {"000001"},
        "positive_zero_downside_codes": {"000001"},
    }

    missing = apply_metric_variant(frame, variant="sortino_zero_downside_missing", **kwargs)
    bounded = apply_metric_variant(frame, variant="sortino_zero_downside_bounded", **kwargs)

    assert pd.isna(missing.loc[0, "1年-索提诺比率"])
    assert pd.isna(missing.loc[0, "1年-索提诺比率(展示封顶)"])
    assert pd.isna(missing.loc[0, "sortino_ratio_cap_hit"])
    assert pd.isna(bounded.loc[0, "sortino_ratio_cap_hit"])
    assert bounded.loc[0, "1年-索提诺比率(展示封顶)"] == 10.
    assert bounded.loc[0, "1年-索提诺比率"] == pytest.approx(10.0)
    assert bounded.loc[1, "1年-索提诺比率"] == pytest.approx(2.0)


def test_bounded_sortino_requires_evidence_for_every_affected_code():
    with pytest.raises(ValueError, match="require_zero_downside_evidence"):
        apply_metric_variant(pd.DataFrame([{"基金代码": "000002", "sortino_ratio": 2.}]),
            variant="sortino_zero_downside_bounded", risk_free_rate=.015, min_drawdown_by_style={},
            zero_downside_codes={"000001"}, positive_zero_downside_codes={"000002"})


def test_membership_delta_reports_exact_substitution() -> None:
    delta = membership_delta({"000001", "000002"}, {"000002", "000003"})

    assert delta["added_codes"] == ["000003"]
    assert delta["removed_codes"] == ["000001"]
    assert delta["changed_count"] == 2
    assert delta["jaccard"] == pytest.approx(1 / 3, abs=1e-6)


@pytest.mark.parametrize("drawdown", [None, float("nan"), float("inf"), -float("inf")])
def test_missing_or_nonfinite_drawdown_cannot_be_replaced_by_a_style_floor(drawdown) -> None:
    frame = pd.DataFrame([{"annual_return": 3., "max_drawdown": drawdown, "债基风格标签": "中短债"}])
    standard = standard_calmar_series(frame)
    adjusted, effective, bounded = adjusted_calmar_series(
        frame, risk_free_rate=.015, min_drawdown_by_style={"中短债": .002, "default": .005},
        deduct_risk_free_rate=True,
    )
    assert pd.isna(standard.iloc[0])
    assert pd.isna(adjusted.iloc[0]) and pd.isna(effective.iloc[0])
    assert not bool(bounded.iloc[0])


def test_observed_zero_drawdown_remains_distinct_from_missing_drawdown() -> None:
    frame = pd.DataFrame([{"annual_return": 3., "max_drawdown": 0., "债基风格标签": "中短债"}])
    adjusted, effective, bounded = adjusted_calmar_series(
        frame, risk_free_rate=.015, min_drawdown_by_style={"中短债": .002, "default": .005},
        deduct_risk_free_rate=True,
    )
    assert pd.isna(standard_calmar_series(frame).iloc[0])
    assert adjusted.iloc[0] == pytest.approx(7.5)
    assert effective.iloc[0] == .002 and bool(bounded.iloc[0])
