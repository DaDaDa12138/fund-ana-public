"""Isolated fixtures: missing holding inputs must never reach formal selection."""
from datetime import datetime
import json
import sqlite3

import pandas as pd
import pytest

from backend.project_publish_decisions import append_project_decision_columns, resolve_holding_period_status
from backend.report_builder import _read_metadata
from backend.truthfulness_gate import publication_truthfulness_checks
from core.bond_fund_screening.core.metric_formula_contract import VERSION


def ready_row(holding_days):
    return {"基金代码": "000001", "基金名称": "isolated holding fixture A", "ftype": "债券型",
            "has_holding_period": 0, "holding_days": holding_days, "is_buyable": 1,
            "latest_nav_date": "2026-10-12", "period_return_status": "dated",
            "mgr_fee_rate": .2, "trust_fee_rate": .05, "sales_fee_rate": 0,
            "综合加权得分": 90, "近期得分": 90, "长期得分": 90,
            "1年-年化收益(%)": 5, "1年-夏普比率": 2, "1年-最大回撤(%)": .01}


def no_signal_reviewed_row(holding_days=None):
    """A fund whose name/detail scan found no holding-period restriction.

    ``holding_days`` intentionally remains NULL: the scan result is an
    explicit semantic state, not an imputed factual value of zero days.
    """
    row = ready_row(holding_days)
    row["holding_period_source"] = "no_signal_reviewed"
    return row


@pytest.mark.parametrize("missing", [None, "", "bad", -1, 1.5, float("inf"), float("nan"), True])
def test_unknown_holding_enters_diagnostics_before_selection(missing):
    row = ready_row(missing)
    row["基金名称"] = ""
    result = append_project_decision_columns(pd.DataFrame([row]), decision_map={},
        soft_warning_map={}, evaluated_at=datetime(2026, 10, 13)).iloc[0]
    assert result["项目推荐结论"] == "自动诊断"
    assert not result["基础门禁通过"]
    assert result["基础推荐原因代码"] == "holding_days_unknown"
    assert result["持有期状态"] == "结构化持有期未知"
    assert result["可投资性状态"] == "待确认"
    assert result["数据就绪状态"] == "待补齐"
    assert result["质量结论状态"] == "待评估"


def test_no_signal_reviewed_null_is_treated_as_normal_fund() -> None:
    """An explicit no-signal scan must not be mistaken for missing evidence."""
    result = append_project_decision_columns(
        pd.DataFrame([no_signal_reviewed_row()]), decision_map={},
        soft_warning_map={}, evaluated_at=datetime(2026, 10, 13),
    ).iloc[0]

    assert result["持有期状态"] == "未见持有期限制信号"
    assert result["基础门禁通过"]
    assert result["基础推荐原因代码"] == ""
    assert result["可投资性状态"] == "可投资"
    assert result["数据就绪状态"] == "已就绪"
    assert result["质量结论状态"] == "已通过基础门禁"
    # Preserve the database NULL; this is not a fabricated 0-day contract.
    assert pd.isna(result["holding_days"])


@pytest.mark.parametrize("days", [0, 7, 30, 90])
def test_verified_holding_days_can_pass_basic_gate(days):
    result = append_project_decision_columns(pd.DataFrame([ready_row(days)]), decision_map={},
        soft_warning_map={}, evaluated_at=datetime(2026, 10, 13)).iloc[0]
    assert result["基础门禁通过"]
    assert result["基础推荐原因代码"] == ""
    assert result["holding_days"] == days


def test_name_derived_holding_days_is_an_explicit_verified_value() -> None:
    row = ready_row(30)
    row["has_holding_period"] = 1
    row["holding_period_source"] = "name"
    result = append_project_decision_columns(
        pd.DataFrame([row]), decision_map={}, soft_warning_map={},
        evaluated_at=datetime(2026, 10, 13),
    ).iloc[0]

    assert result["基础门禁通过"]
    assert result["基础推荐原因代码"] == ""
    assert result["持有期状态"] == "已确认<=90天"
    assert result["holding_days"] == 30


def test_nonempty_name_without_concrete_term_treats_null_as_ordinary() -> None:
    row = ready_row(None)
    row["holding_period_source"] = None
    result = append_project_decision_columns(
        pd.DataFrame([row]), decision_map={}, soft_warning_map={},
        evaluated_at=datetime(2026, 10, 13),
    ).iloc[0]
    assert result["基础门禁通过"]
    assert result["持有期状态"] == "未见持有期限制信号"


def test_structured_longer_term_wins_over_shorter_name_term() -> None:
    row = ready_row(180)
    row["基金名称"] = "示例30天持有债券"
    row["has_holding_period"] = 1
    result = append_project_decision_columns(
        pd.DataFrame([row]), decision_map={}, soft_warning_map={},
        evaluated_at=datetime(2026, 10, 13),
    ).iloc[0]
    assert not result["基础门禁通过"]
    assert result["基础推荐原因代码"] == "holding_period_limit"


def test_fullname_longer_term_blocks_basic_gate():
    row = ready_row(None)
    row.update({"基金名称": "示例30天持有债券A", "fullname": "示例180天持有期债券基金"})
    result = append_project_decision_columns(
        pd.DataFrame([row]), decision_map={}, soft_warning_map={},
        evaluated_at=datetime(2026, 10, 13),
    ).iloc[0]
    assert not result["基础门禁通过"]
    assert result["基础推荐原因代码"] == "holding_period_limit"
    assert pd.isna(result["holding_days"])


def test_over_90_days_block_even_without_name_flag():
    result = append_project_decision_columns(pd.DataFrame([ready_row(91)]), decision_map={},
        soft_warning_map={}, evaluated_at=datetime(2026, 10, 13)).iloc[0]
    assert not result["基础门禁通过"]
    assert result["基础推荐原因代码"] == "holding_period_limit"
    assert result["可投资性状态"] == "不可投资"
    assert resolve_holding_period_status(has_holding_period=0, holding_days=91) == "已确认>90天"


def test_database_metadata_loader_preserves_null_and_real_zero(tmp_path):
    db = tmp_path / "isolated_metadata.db"
    columns = _read_metadata(tmp_path / "absent.db").columns.tolist()
    with sqlite3.connect(db) as conn:
        conn.execute("CREATE TABLE funds (" + ",".join(f'"{name}"' for name in columns) + ")")
        conn.executemany("INSERT INTO funds (fcode, holding_days, has_holding_period) VALUES (?, ?, ?)",
                         [("000001", None, 0), ("000002", 0, 0), ("000003", 90, 1)])
    actual = _read_metadata(db).set_index("fcode")
    assert pd.isna(actual.loc["000001", "holding_days"])
    assert actual.loc["000002", "holding_days"] == 0
    assert actual.loc["000003", "holding_days"] == 90


@pytest.mark.parametrize("value,reason", [(None, "holding_days_unknown"), (True, "holding_days_unknown"),
                                          (91, "holding_days_over_limit"), (999, "holding_days_over_limit")])
def test_final_gate_still_rejects_invalid_holding_after_basic_selection(value, reason):
    summary = {"generated_at": "2026-10-13T12:00:00", "database_freshness": {
        "effective_nav_snapshot": {"candidate_date": "2026-10-12"}}}
    inputs = {"official_rows": [{"基金代码": "000001", "最新单位净值": 1, "最新累计净值": 1,
              "1年-年化收益(%)": 0, "1年-最大回撤(%)": 0, "1年-波动率": 0}],
              "fund_metadata_rows": [{"fcode": "000001", "holding_days": value}],
              "risk_snapshot_rows": [{"fcode": "000001", "period": "1y", "end_date": "2026-10-12",
                  "formula_contract_json": json.dumps({"version": VERSION, "engine": "database",
                    "return_source": "daily_growth", "return_observations_verified": True})}]}
    result = publication_truthfulness_checks(summary, inputs)
    check = next(item for item in result if item["name"] == "publication_selected_inputs_verified")
    assert not check["aligned"]
    assert check["samples"] == [{"fcode": "000001", "reason": reason}]


def test_final_gate_accepts_explicit_no_signal_reviewed_holding() -> None:
    summary = {"generated_at": "2026-10-13T12:00:00", "database_freshness": {
        "effective_nav_snapshot": {"candidate_date": "2026-10-12"}}}
    inputs = {"official_rows": [{"基金代码": "000001", "最新单位净值": 1, "最新累计净值": 1,
              "1年-年化收益(%)": 0, "1年-最大回撤(%)": 0, "1年-波动率": 0}],
              "fund_metadata_rows": [{"fcode": "000001", "holding_days": None,
                  "has_holding_period": 0, "holding_period_source": "no_signal_reviewed"}],
              "risk_snapshot_rows": [{"fcode": "000001", "period": "1y", "end_date": "2026-10-12",
                  "formula_contract_json": json.dumps({"version": VERSION, "engine": "database",
                    "return_source": "daily_growth", "return_observations_verified": True})}]}
    result = publication_truthfulness_checks(summary, inputs)
    check = next(item for item in result if item["name"] == "publication_selected_inputs_verified")
    assert check["aligned"]
    assert check["samples"] == []


def test_final_gate_uses_fullname_term_even_with_ordinary_shortname():
    summary = {"generated_at": "2026-10-13T12:00:00", "database_freshness": {
        "effective_nav_snapshot": {"candidate_date": "2026-10-12"}}}
    inputs = {"official_rows": [{"基金代码": "000001", "最新单位净值": 1, "最新累计净值": 1,
              "1年-年化收益(%)": 0, "1年-最大回撤(%)": 0, "1年-波动率": 0}],
              "fund_metadata_rows": [{"fcode": "000001", "holding_days": None,
                  "shortname": "普通债券A", "fullname": "示例180天持有期债券基金"}],
              "risk_snapshot_rows": [{"fcode": "000001", "period": "1y", "end_date": "2026-10-12",
                  "formula_contract_json": json.dumps({"version": VERSION, "engine": "database",
                    "return_source": "daily_growth", "return_observations_verified": True})}]}

    check = next(item for item in publication_truthfulness_checks(summary, inputs)
                 if item["name"] == "publication_selected_inputs_verified")

    assert not check["aligned"]
    assert check["samples"] == [{"fcode": "000001", "reason": "holding_days_over_limit"}]
