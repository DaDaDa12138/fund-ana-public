"""Synthetic API I/O fixtures verify coverage accounting, never business writes."""
from __future__ import annotations

from copy import deepcopy
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "core"))

from collectors.fund_collector import FundCollector
from config.api_config import FundType
from utils.fund_list_pagination_audit import observed_total
from scripts.audit_data_truthfulness import _validate_collect_funds_api_source_evidence


def collector(pages):
    instance = FundCollector.__new__(FundCollector)
    calls = []
    def get(**kwargs):
        calls.append(kwargs)
        return deepcopy(pages[kwargs["page_index"] - 1])
    instance.api = SimpleNamespace(get_bond_fund_rank=get)
    instance._parse_fund_list = lambda rows, fund_type: deepcopy(rows)
    return instance, calls


def rows(count, offset=0):
    return [{"FCODE": f"io-fixture-{offset+i}", "SHORTNAME": "isolated test payload",
             "FSRQ": None, "DWJZ": None, "SYL_1N": 0} for i in range(count)]


def test_repeated_pages_do_not_prove_unique_coverage_by_matching_raw_count():
    records = rows(30)
    pages = [{"Datas": records, "TotalCount": 60}, {"Datas": list(reversed(records)), "TotalCount": 60}]
    instance, calls = collector(pages)
    result = instance._collect_by_type(FundType.BOND, False)
    evidence = instance._last_collect_by_type_evidence
    assert result == records + list(reversed(records))
    assert len(calls) == 2
    assert evidence["raw_record_count"] == evidence["declared_total_count"] == 60
    assert evidence["declared_collected_shortfall"] == 0  # Legacy raw-row meaning is unchanged.
    assert evidence["unique_raw_code_count"] == 30
    assert evidence["declared_unique_code_shortfall"] == 30
    assert evidence["unique_coverage_status"] == "coverage_not_proven"
    first, second = evidence["page_observations"]
    assert first["ordered_codes_sha256"] != second["ordered_codes_sha256"]
    assert first["code_set_sha256"] == second["code_set_sha256"]
    assert second["same_membership_pages"] == [1]
    assert second["previous_pages_overlap_codes"] == sorted(row["FCODE"] for row in records)
    assert first["first_sort_value"] == first["last_sort_value"] == 0
    assert "repeated_page_membership" in evidence["pagination_warning_codes"]


def test_complete_pages_keep_order_null_zero_and_no_spurious_warning():
    first, second = rows(30), rows(1, 30)
    instance, calls = collector([{"Datas": first, "TotalCount": "31"}, {"Datas": second, "TotalCount": 31}])
    assert instance._collect_by_type(FundType.BOND, False) == first + second
    evidence = instance._last_collect_by_type_evidence
    assert len(calls) == 2
    assert evidence["unique_raw_code_count"] == 31
    assert evidence["unique_coverage_status"] == "matched_declared_total"
    assert evidence["pagination_warning_codes"] == []


def test_changed_total_and_early_empty_page_preserve_actual_coverage():
    instance, _ = collector([{"Datas": rows(30), "TotalCount": 60}, {"Datas": [], "TotalCount": 61}])
    instance._collect_by_type(FundType.BOND, False)
    evidence = instance._last_collect_by_type_evidence
    assert evidence["observed_declared_totals"] == [60, 61]
    assert evidence["early_stop_page"] == 2
    assert evidence["declared_unique_code_shortfall"] == 30
    assert evidence["page_observations"][1]["unique_code_count"] == 0
    assert {"declared_total_changed", "pagination_ended_early"} <= set(evidence["pagination_warning_codes"])


@pytest.mark.parametrize("total", [None, 0, -1, "invalid", "1.5", True])
def test_nonempty_page_with_invalid_total_fails_instead_of_silent_empty_result(total):
    instance, _ = collector([{"Datas": rows(1), "TotalCount": total}])
    with pytest.raises(RuntimeError, match="TotalCount"):
        instance._collect_by_type(FundType.BOND, False)


@pytest.mark.parametrize("total,status", [(0, "matched_declared_total"), (None, "unknown_declared_total"), (3, "coverage_not_proven")])
def test_empty_page_distinguishes_confirmed_empty_unknown_and_shortfall(total, status):
    instance, calls = collector([{"Datas": [], "TotalCount": total}])
    assert instance._collect_by_type(FundType.BOND, False) == []
    evidence = instance._last_collect_by_type_evidence
    assert len(calls) == 1
    assert evidence["unique_coverage_status"] == status
    assert evidence["declared_unique_code_shortfall"] == (None if total is None else total)


def test_count_normalization_preserves_unknown_and_rejects_boolean():
    assert observed_total("30") == 30
    assert observed_total(0) == 0
    assert all(observed_total(value) is None for value in [None, "", False, -1, "--", 1.2, "²"])


def test_truthfulness_warns_for_intra_type_duplicates_but_not_cross_type_overlap():
    evidence = {"task": "collect-funds", "source": "fundMNRank", "type_count": 0,
                "total_intra_type_duplicate_code_count": 2, "duplicate_record_count": 2}
    _, warnings = _validate_collect_funds_api_source_evidence([evidence], bond_fund_count=0)
    assert "api_source_evidence_intra_type_duplicate_coverage_unproven:1" in warnings
    evidence["total_intra_type_duplicate_code_count"] = 0
    _, warnings = _validate_collect_funds_api_source_evidence([evidence], bond_fund_count=0)
    assert not any("duplicate_coverage" in value for value in warnings)
    evidence["types"] = [{"unique_coverage_status": "unknown_declared_total"}]
    _, warnings = _validate_collect_funds_api_source_evidence([evidence], bond_fund_count=0)
    assert "api_source_evidence_type_unique_coverage_unproven:1:1" in warnings
