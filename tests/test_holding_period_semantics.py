from __future__ import annotations

from backend.project_publish_decisions import resolve_holding_period_status
from core.utils.holding_period_parser import parse_holding_match, parse_holding_with_priority
from core.utils.holding_period_policy import holding_period_gate_reason, resolve_holding_period_days
import pandas as pd
import pytest
from types import SimpleNamespace


@pytest.mark.parametrize("name,days,reason", [
    ("普通债券A", None, None),
    ("30天持有期债券A", None, None),
    ("三个月持有期债券A", None, None),
    ("180天持有期债券A", None, "holding_period_limit"),
    ("一年持有期债券A", None, "holding_period_limit"),
    ("30天持有期债券A", 180, "holding_period_limit"),
    ("", None, "holding_days_unknown"),
])
def test_name_fallback_keeps_concrete_limit(name, days, reason):
    assert holding_period_gate_reason(
        has_holding_period=0, holding_days=days, fund_name=name,
    ) == reason


@pytest.mark.parametrize("name", [None, float("nan"), pd.NA, "nan", "<NA>", "--"])
def test_missing_name_is_not_an_ordinary_fund_signal(name):
    assert holding_period_gate_reason(
        has_holding_period=0, holding_days=None, fund_name=name,
    ) == "holding_days_unknown"


def test_conflicting_name_terms_keep_longest_period():
    match = parse_holding_match(shortname="示例30天持有债券A", fullname="示例180天持有期债券基金")
    assert match.days == 180
    assert resolve_holding_period_days(
        holding_days=None, fund_name="示例30天持有及一年持有债券A",
    ) == 365


@pytest.mark.parametrize("days", [180, 365])
def test_generic_name_cannot_replace_concrete_api_term(days):
    match, source = parse_holding_with_priority(shortname="示例定开债券A", cycle=days)
    assert match.days == days
    assert source == "detail_struct"


def test_core_filter_uses_fullname_and_placeholder_keyword_without_mutating_days():
    from core.bond_fund_screening.core.filtering_engine import FilteringEngine

    frame = pd.DataFrame([
        {"fcode": "a", "shortname": "普通债券A", "holding_days": None},
        {"fcode": "b", "shortname": "30天持有债券A", "fullname": "180天持有期债券基金", "holding_days": None},
        {"fcode": "c", "shortname": "定开债券A", "holding_days": 999, "holding_keywords": "定开"},
        {"fcode": "d", "shortname": "普通债券B", "holding_days": 180},
        {"fcode": "e", "shortname": "", "holding_days": None},
    ])
    original = frame.copy(deep=True)
    engine = SimpleNamespace(max_holding_days=90, tracker=None, _record_layer_stats=lambda *args: None)

    result = FilteringEngine._layer2_holding_period_filter(engine, frame)

    assert result["fcode"].tolist() == ["a"]
    pd.testing.assert_frame_equal(frame, original)


def test_final_name_filter_preserves_real_999_days():
    from core.bond_fund_screening.tasks.final_screening_common_helpers import has_fixed_holding_period
    assert has_fixed_holding_period("示例999天持有期债券A", holding_keywords=())
    assert not has_fixed_holding_period("示例定开债券A", holding_keywords=())


def test_explicit_999_day_name_keeps_limit_status_with_legacy_placeholder_source():
    assert resolve_holding_period_status(
        has_holding_period=1,
        holding_days=999,
        holding_period_source="name_generic",
        holding_keywords="定开",
        fund_name="示例999天持有期债券A",
    ) == "已确认>90天"


def test_holding_period_status_keeps_uncollected_null_distinct_from_no_signal() -> None:
    assert (
        resolve_holding_period_status(has_holding_period=0, holding_days=None)
        == "结构化持有期未知"
    )
    assert (
        resolve_holding_period_status(
            has_holding_period=0,
            holding_days=None,
            holding_period_source="no_signal_reviewed",
        )
        == "未见持有期限制信号"
    )
    assert (
        resolve_holding_period_status(has_holding_period=1, holding_days=0)
        == "结构化持有期未知"
    )


def test_holding_period_status_keeps_confirmed_values_and_limit_boundary() -> None:
    assert (
        resolve_holding_period_status(has_holding_period=1, holding_days=90)
        == "已确认<=90天"
    )
    assert (
        resolve_holding_period_status(has_holding_period=1, holding_days=91)
        == "已确认>90天"
    )

