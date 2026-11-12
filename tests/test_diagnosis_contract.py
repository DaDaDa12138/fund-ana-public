"""Real frozen diagnosis rows plus explicit synthetic schema boundaries."""
import json
from pathlib import Path

import pandas as pd
import pytest

from backend.diagnosis_contract import (
    diagnosis_categories, diagnosis_category, summarize_diagnoses,
)
from backend.project_publish_decisions import (
    AUTO_DIAGNOSIS_CATEGORY_COLUMN, FINAL_SELECTION_NOT_SELECTED_DIAGNOSIS,
    MANAGER_STABILITY_DIAGNOSIS,
)
from scripts.audit_annual_return_gate_shadow import _resolve_local_shadow_scenario_outcome


SAMPLES = json.loads((Path(__file__).parent / "fixtures/diagnosis_rows_20260908.json").read_text())["samples"]


@pytest.mark.parametrize("sample", SAMPLES, ids=lambda sample: sample["label"])
def test_real_frozen_rows_retain_all_diagnosis_counts_and_originals(sample):
    frame = pd.DataFrame(sample["rows"])
    before = frame.copy(deep=True)
    assert summarize_diagnoses(frame) == sample["expected"]
    pd.testing.assert_frame_equal(frame, before)


@pytest.mark.parametrize("category", ["经理稳定性观察", "经理稳定性诊断", MANAGER_STABILITY_DIAGNOSIS])
@pytest.mark.parametrize("column", ["观察层分类", AUTO_DIAGNOSIS_CATEGORY_COLUMN])
def test_current_and_legacy_manager_categories_have_the_same_meaning(column, category):
    assert summarize_diagnoses(pd.DataFrame([{column: category}])) == {
        "manager_stability_watch_count": 1, "publish_post_review_watch_count": 0,
        "final_selection_removed_count": 0,
    }


def test_legacy_post_review_and_final_selection_counts_are_not_lost():
    frame = pd.DataFrame({"观察层分类": ["发布后复核观察", "最终精选未入围", "不可申购观察"]})
    assert summarize_diagnoses(frame) == {
        "manager_stability_watch_count": 0, "publish_post_review_watch_count": 1,
        "final_selection_removed_count": 1,
    }


@pytest.mark.parametrize("frame", [pd.DataFrame(), pd.DataFrame(columns=[AUTO_DIAGNOSIS_CATEGORY_COLUMN])])
def test_known_empty_pool_keeps_real_zero(frame):
    assert set(summarize_diagnoses(frame).values()) == {0}


@pytest.mark.parametrize("value", [None, "", " ", float("nan"), pd.NA, "NaN", "null"])
def test_nonempty_pool_with_missing_category_cannot_be_counted_as_zero(value):
    with pytest.raises(ValueError, match="diagnosis_category_missing"):
        summarize_diagnoses(pd.DataFrame({AUTO_DIAGNOSIS_CATEGORY_COLUMN: [value]}))


def test_missing_column_and_partial_missing_rows_are_rejected():
    with pytest.raises(ValueError, match="diagnosis_category_missing"):
        summarize_diagnoses(pd.DataFrame({"基金代码": ["synthetic_schema_case"]}))
    with pytest.raises(ValueError, match="diagnosis_category_missing"):
        summarize_diagnoses(pd.DataFrame({AUTO_DIAGNOSIS_CATEGORY_COLUMN: [MANAGER_STABILITY_DIAGNOSIS, None]}))


@pytest.mark.parametrize("value", ["未识别的未来分类", 0, False])
def test_unknown_and_falsey_nonmissing_categories_are_not_silent_zero(value):
    with pytest.raises(ValueError, match="diagnosis_category_unknown"):
        summarize_diagnoses(pd.DataFrame({AUTO_DIAGNOSIS_CATEGORY_COLUMN: [value]}))


def test_mixed_legacy_and_current_rows_preserve_index_and_require_consistency():
    frame = pd.DataFrame([
        {AUTO_DIAGNOSIS_CATEGORY_COLUMN: MANAGER_STABILITY_DIAGNOSIS, "观察层分类": "经理稳定性观察"},
        {AUTO_DIAGNOSIS_CATEGORY_COLUMN: None, "观察层分类": "最终精选未入围"},
    ], index=[4, 9])
    assert diagnosis_categories(frame).to_dict() == {4: MANAGER_STABILITY_DIAGNOSIS, 9: FINAL_SELECTION_NOT_SELECTED_DIAGNOSIS}
    frame.loc[4, "观察层分类"] = "发布后复核观察"
    with pytest.raises(ValueError, match="diagnosis_category_conflict"):
        summarize_diagnoses(frame)


def test_duplicate_columns_are_rejected_instead_of_losing_one():
    frame = pd.DataFrame([[MANAGER_STABILITY_DIAGNOSIS, None]], columns=[AUTO_DIAGNOSIS_CATEGORY_COLUMN] * 2)
    with pytest.raises(ValueError, match="diagnosis_columns_duplicated"):
        summarize_diagnoses(frame)


def test_formal_row_may_have_no_diagnosis_but_unknown_category_is_not_hidden():
    assert diagnosis_category({"项目推荐结论": "正式推荐"}) == ""
    with pytest.raises(ValueError, match="diagnosis_category_unknown"):
        diagnosis_category({AUTO_DIAGNOSIS_CATEGORY_COLUMN: "未知诊断"})


@pytest.mark.parametrize("sample", SAMPLES, ids=lambda sample: sample["label"])
def test_local_shadow_outcome_uses_real_diagnosis_instead_of_generic_fallback(sample):
    row = sample["rows"][0]
    code = row["基金代码"]
    assert _resolve_local_shadow_scenario_outcome(
        code, direct_relieved_codes=set(), already_in_actual_codes=set(), watch_reason_map={code: row},
    ) == MANAGER_STABILITY_DIAGNOSIS
