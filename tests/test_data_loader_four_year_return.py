from __future__ import annotations

import sqlite3
import gzip
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from bond_fund_screening.core.data_loader import DataLoader


def _loader_with_history(tmp_path, frame: pd.DataFrame) -> DataLoader:
    database = tmp_path / "fixture.db"
    with sqlite3.connect(database):
        pass
    loader = DataLoader(database)
    loader.get_batch_nav_history = lambda codes, days, end_date=None: {
        str(code): frame.copy() for code in codes
    }
    return loader


def _four_year_history(*, daily_growth: bool = False, reliable_accum: bool = True) -> pd.DataFrame:
    dates = pd.bdate_range("2022-10-28", "2026-10-28")
    n = len(dates)
    if reliable_accum:
        accum_nav = np.linspace(1.0, 1.05, n)
        unit_nav = accum_nav.copy()
        # A distribution lowers unit NAV; accumulated NAV stays smooth but
        # is not itself a dividend-reinvestment wealth index.
        unit_nav[n // 2:] -= 0.05
    else:
        unit_nav = np.ones(n)
        accum_nav = np.linspace(1.0, 1.1, n)
        accum_nav[n // 2:] -= 0.08
    values = {
        "nav_date": dates,
        "unit_nav": unit_nav,
        "accum_nav": accum_nav,
    }
    if daily_growth:
        values["daily_growth"] = np.zeros(n, dtype=float)
    else:
        values["daily_growth"] = np.full(n, np.nan)
    return pd.DataFrame(values)


def test_four_year_return_stays_missing_without_verified_total_returns(tmp_path):
    loader = _loader_with_history(tmp_path, _four_year_history())

    result = loader._batch_four_year_returns(pd.Series(["000001"]), end_date="2026-10-28")

    assert pd.isna(result.iloc[0])


def test_four_year_return_does_not_fall_back_to_unit_nav_when_accumulated_nav_is_unreliable(tmp_path):
    loader = _loader_with_history(tmp_path, _four_year_history(reliable_accum=False))

    result = loader._batch_four_year_returns(pd.Series(["000001"]), end_date="2026-10-28")

    assert pd.isna(result.iloc[0])


def test_four_year_return_prefers_verified_daily_growth_over_nav_ratios(tmp_path):
    loader = _loader_with_history(tmp_path, _four_year_history(daily_growth=True))

    result = loader._batch_four_year_returns(pd.Series(["000001"]), end_date="2026-10-28")

    assert result.iloc[0] == pytest.approx(0.0)


@pytest.mark.parametrize("damage", [None, "missing_open_day", "missing_growth"])
def test_real_four_year_history_accepts_proven_holidays_and_rejects_missing_inputs(tmp_path, damage):
    with gzip.open(Path(__file__).parent / "fixtures/nav_100066_four_year_20261028.json.gz", "rt") as f:
        payload = json.load(f)
    assert hashlib.sha256(json.dumps(payload["rows"], ensure_ascii=False, sort_keys=True).encode()).hexdigest() == payload["rows_sha256"]
    frame = pd.DataFrame(payload["rows"])
    frame["nav_date"] = pd.to_datetime(frame["nav_date"])
    if damage == "missing_open_day":
        frame = frame.loc[frame.nav_date.ne(pd.Timestamp("2024-02-08"))]
    elif damage == "missing_growth":
        frame.loc[frame.nav_date.eq(pd.Timestamp("2024-02-08")), "daily_growth"] = np.nan
    result = _loader_with_history(tmp_path, frame)._batch_four_year_returns(
        pd.Series(["100066"]), end_date="2026-10-28",
    )
    if damage:
        assert pd.isna(result.iloc[0])
    else:
        start = frame.index[frame.nav_date.le(pd.Timestamp("2022-10-28"))][-1]
        expected = np.expm1(np.log1p(frame.daily_growth.iloc[start + 1:] / 100).sum()) * 100
        assert result.iloc[0] == pytest.approx(expected)
