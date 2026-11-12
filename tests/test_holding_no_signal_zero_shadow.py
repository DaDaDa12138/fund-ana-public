from __future__ import annotations

import pandas as pd

from scripts.audit_holding_no_signal_zero_shadow import _scenario_universe


def test_no_signal_zero_shadow_changes_only_null_no_signal_rows() -> None:
    source = pd.DataFrame(
        [
            {"fcode": "000001", "holding_period_source": "no_signal_reviewed", "holding_days": None, "has_holding_period": 0},
            {"fcode": "000002", "holding_period_source": "", "holding_days": None, "has_holding_period": 0},
            {"fcode": "000003", "holding_period_source": "no_signal_reviewed", "holding_days": 60, "has_holding_period": 1},
            {"fcode": "000004", "holding_period_source": "detail_struct", "holding_days": 180, "has_holding_period": 1},
        ]
    )

    shadow, mask = _scenario_universe(source)

    assert mask.tolist() == [True, False, False, False]
    assert shadow.loc[0, "holding_days"] == 0
    assert shadow.loc[0, "has_holding_period"] == 0
    assert pd.isna(source.loc[0, "holding_days"])
    assert pd.isna(shadow.loc[1, "holding_days"])
    assert shadow.loc[2, "holding_days"] == 60
    assert shadow.loc[3, "holding_days"] == 180


def test_no_signal_shadow_requires_null_holding_days() -> None:
    source = pd.DataFrame(
        [{"holding_period_source": "no_signal_reviewed", "holding_days": 0, "has_holding_period": 0}]
    )
    shadow, mask = _scenario_universe(source)
    assert mask.tolist() == [False]
    assert shadow.equals(source)
