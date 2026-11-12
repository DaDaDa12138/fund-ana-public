from core.utils.danjuan_holding import parse_danjuan_holding_period


def test_parses_explicit_rolling_lock_period() -> None:
    match = parse_danjuan_holding_period(
        {
            "data": {
                "lock_fund_type": "1",
                "hold_lock_period": "60天",
                "close_period": "锁定期60天",
            }
        }
    )
    assert match is not None
    assert match.days == 60
    assert match.source == "danjuan_struct"


def test_parses_descriptive_year_lock_period() -> None:
    match = parse_danjuan_holding_period(
        {"data": {"close_period_describe": "买入确认后最短持有1年，开放后可随时卖出。"}}
    )
    assert match is not None
    assert match.days == 365


def test_empty_or_zero_lock_marker_does_not_prove_unrestricted() -> None:
    assert parse_danjuan_holding_period({"data": {"lock_fund_type": "0"}}) is None
    assert parse_danjuan_holding_period({"data": {"hold_lock_period": "--"}}) is None


def test_invalid_payload_is_unknown() -> None:
    assert parse_danjuan_holding_period({"data": None}) is None
