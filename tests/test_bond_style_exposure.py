from __future__ import annotations

import pytest

from backend.bond_style_classifier import classify_bond_style


def classify(*, ftype="债券型-长债", benchmark="中债综合全价指数收益率", target="", strategy="", sub_type=None):
    return classify_bond_style(
        ftype=ftype,
        sub_type=sub_type,
        benchmark=benchmark,
        investment_target=target,
        investment_strategy=strategy,
    )


def test_real_boshi_003564_and_003565_exclusions_do_not_create_exposure():
    # Published frozen run 20261017_134604_sync, both share classes. Text is
    # shortened to the original investment-range and prohibition clauses.
    result = classify(
        benchmark="中债-1-5年债券综合财富(总值)指数收益率*95%+活期存款基准利率*5%",
        strategy=(
            "投资范围为:国家债券、金融债券、可分离交易可转债的纯债部分、资产支持证券。"
            "本基金不投资于股票、权证等资产,也不投资于可转换债券"
            "(可分离交易可转债的纯债部分除外)、可交换债券。"
        ),
    )
    assert result["style_tag"] == "现金增强"
    assert result["has_equity_convertible_exposure"] is False
    assert result["exposure_labels"] == []
    assert result["is_pure_bond_style"] is True


@pytest.mark.parametrize("strategy", [
    "本基金不投资于股票、权证等资产，也不投资于可转换债券、可交换债券。",
    "本基金不投资于股票等权益类资产，也不投资于可转换债券。",
    "本基金不得投资于可转债、股票、权证。",
    "禁止投资股票，可交换债券。",
    "本基金不投资可转换债券（可分离交易可转债的纯债部分除外）、股票。",
])
def test_explicit_prohibited_instrument_lists_do_not_create_exposure(strategy):
    result = classify(strategy=strategy)
    assert result["has_equity_convertible_exposure"] is False
    assert result["is_enhanced_style"] is False


@pytest.mark.parametrize("strategy", [
    "本基金不投资于可转换债券以外的其他资产。",
    "本基金不投资于股票指数期货。",
    "本基金不投资于股票型基金。",
    "本基金不投资于股票、可转债以外的其他资产。",
    "本基金不投资于可转债,股票指数期货。",
])
def test_unknown_suffixes_and_except_lists_keep_conservative_exposure(strategy):
    result = classify(strategy=strategy)
    assert result["has_equity_convertible_exposure"] is True


def test_complete_prohibition_list_can_span_lines():
    result = classify(strategy="本基金不投资于\n股票、\n权证，\n可转换债券、\n可交换债券。")
    assert result["has_equity_convertible_exposure"] is False


def test_line_break_alone_does_not_end_an_unknown_instrument_name():
    result = classify(strategy="本基金不投资于股票\n指数期货。")
    assert result["exposure_labels"] == ["权益暴露"]


@pytest.mark.parametrize("prefix", ["并非", "不是", "并不是", "并非 "])
def test_double_negated_prohibition_keeps_conservative_exposure(prefix):
    result = classify(strategy=f"本基金{prefix}不投资于可转换债券、股票。")
    assert result["style_tag"] == "转债增强"
    assert result["exposure_labels"] == ["权益暴露", "可转债暴露"]


@pytest.mark.parametrize("pure_component", [
    "可分离交易可转债的纯债部分",
    "分离交易可转换债券的纯债部分",
    "可分离交易的可转债纯债部分",
])
def test_only_stripped_pure_bond_component_does_not_create_exposure(pure_component):
    result = classify(strategy=f"本基金投资于国家债券、{pure_component}、资产支持证券。")
    assert result["has_equity_convertible_exposure"] is False
    assert result["style_tag"] == "综合纯债"


@pytest.mark.parametrize("strategy", [
    "本基金投资于可分离交易可转债的纯债部分以外的资产。",
    "本基金投资于分离交易可转换债券的纯债部分之外的资产。",
    "本基金投资于可分离交易的可转债纯债部分以外的其他部分。",
])
def test_risk_references_outside_the_pure_bond_component_are_preserved(strategy):
    result = classify(strategy=strategy)
    assert result["style_tag"] == "转债增强"
    assert result["exposure_labels"] == ["可转债暴露"]


@pytest.mark.parametrize("strategy", [
    "本基金不投资于股票，但可投资于可转换债券。",
    "本基金不投资于可转换债券。履行适当程序调整后，本基金将投资于可转换债券。",
    "本基金投资于可转换债券，另一类资产不投资于股票、权证。",
    "本基金投资于可分离交易可转债的纯债部分，也投资于可交换债券。",
    "本基金可转换债券投资比例不超过基金资产的20%。",
])
def test_affirmative_convertible_mentions_survive_local_exclusion(strategy):
    result = classify(strategy=strategy)
    assert result["style_tag"] == "转债增强"
    assert "可转债暴露" in result["exposure_labels"]


def test_affirmative_equity_after_negated_list_survives():
    result = classify(strategy="本基金不投资于可转换债券，但可投资于股票。")
    assert result["has_equity_convertible_exposure"] is True
    assert result["exposure_labels"] == ["权益暴露"]
    assert result["is_pure_bond_style"] is False


def test_positive_benchmark_is_not_overridden_by_strategy_prohibition():
    result = classify(
        benchmark="中证可转债指数收益率*90%+存款利率*10%",
        strategy="本基金不投资于可转债、股票。",
    )
    assert result["style_tag"] == "转债增强"
    assert result["has_equity_convertible_exposure"] is True


def test_positive_target_is_not_overridden_by_strategy_prohibition():
    result = classify(
        target="通过可转换债券投资力争获取稳健收益。",
        strategy="本基金不投资于可转换债券、股票。",
    )
    assert "可转债暴露" in result["exposure_labels"]


@pytest.mark.parametrize(("ftype", "sub_type", "expected"), [
    ("债券型-混合一级", None, "一级债增强"),
    ("债券型-混合二级", None, "二级债增强"),
    ("债券型", "混合一级", "一级债增强"),
    ("债券型", "混合二级", "二级债增强"),
])
def test_explicit_mixed_type_keeps_priority(ftype, sub_type, expected):
    result = classify(ftype=ftype, sub_type=sub_type, strategy="本基金不投资于可转债、股票。")
    assert result["style_tag"] == expected
    assert result["is_enhanced_style"] is True


def test_rating_exclusion_alone_is_not_treated_as_global_investment_prohibition():
    # 013522/013523 frozen wording. A rating exception alone cannot establish
    # a global prohibition; this narrow fix deliberately leaves it unchanged.
    result = classify(
        ftype="债券型-中短债",
        strategy="本基金买入信用债(不含可交换债券、可转换债券)的债项评级不得低于AA+。",
    )
    assert result["style_tag"] == "转债增强"


@pytest.mark.parametrize("missing", [None, "", 0])
def test_empty_fields_do_not_invent_exposure(missing):
    result = classify(ftype=missing, sub_type=missing, benchmark=missing, target=missing, strategy=missing)
    assert result["has_equity_convertible_exposure"] is False
    assert result["exposure_labels"] == []
    assert result["style_tag"] == "综合纯债"
