"""Presentation must retain audit facts, source order, NULL and true zero."""
from copy import deepcopy
import re

import pandas as pd

from backend.report_builder import _markdown_table
from backend.report_markdown import markdown_cell, reader_report


def summary():
    return {"run_id": "synthetic_run", "generated_at": "2026-01-02T01:00:00Z",
            "counts": {"final_count": 0, "stable_bond_final_count": 0,
                       "enhanced_bond_final_count": None, "project_tracker_total_periods": 4},
            "database_freshness": {"effective_nav_snapshot": {"candidate_date": "2026-01-01"}},
            "data_sources": {"provider": "synthetic"}}


def test_reader_prioritizes_results_and_keeps_audit_facts_collapsed():
    value = summary()
    before = deepcopy(value)
    lines = ["# Legacy", "## 1. 本轮执行范围", "process_fact",
             "## 5.1 标准筛选审计", "audit_fact",
             "## 8. 正式推荐结构摘要", "structure_fact",
             "## 纯债稳健榜预览", "stable_fact", "## 增强债弹性榜预览", "enhanced_fact",
             "## 3.2 前瞻验证闭环", "history_fact", "## 12. 交付产物", "artifact_fact",
             "## 新增审计章节", "future_fact"]
    text = reader_report(value, lines)
    assert value == before
    assert text.index("stable_fact") < text.index("audit_fact")
    for fact in ["process_fact", "audit_fact", "history_fact", "artifact_fact", "future_fact"]:
        assert text.count(fact) == 1
        assert text.rfind("<details>", 0, text.index(fact)) > text.rfind("</details>", 0, text.index(fact))
    assert "## 5.1" not in text and "## 8." not in text
    assert "| 2026-01-01 | 0 | 0 | — |" in text
    assert "2026-01-02 09:00:00 +08:00" in text
    assert "正式状态以发布清单及当前指针为准" in text
    assert text.count("<details>") == text.count("</details>")
    for anchor in re.findall(r"\]\(#([^)]+)\)", text):
        assert "## " + anchor in text


def test_diagnostic_report_is_never_labeled_formally_published():
    value = summary()
    value["publication_control"] = {"mode": "diagnostic_only"}
    text = reader_report(value, [])
    assert "诊断/验证报告" in text
    assert "已正式发布" not in text


def test_wide_table_preserves_every_field_and_row_order():
    frame = pd.DataFrame([{key: ("000001" if key == "code" else f"{key}_first")
                           for key in ["code", "name", *[f"metric{i}" for i in range(12)]]},
                          {key: ("000002" if key == "code" else f"{key}_second")
                           for key in ["code", "name", *[f"metric{i}" for i in range(12)]]}])
    text = _markdown_table(frame, list(frame.columns))
    assert "字段分组 1/3" in text and "字段分组 3/3" in text
    for line in text.splitlines():
        if line.startswith("|"):
            assert line.count("|") - 1 <= 7
    for i in range(12):
        first, second = markdown_cell(f"metric{i}_first"), markdown_cell(f"metric{i}_second")
        assert text.count(first) == text.count(second) == 1
        assert text.index(first) < text.index(second)
    assert frame.iloc[0]["code"] == "000001"


def test_table_escapes_gfm_and_preserves_null_and_zero():
    frame = pd.DataFrame([{"code": "000001", "value": 0, "missing": None,
                           "name": "A|[link](x)<script>\r\nB"}])
    text = _markdown_table(frame, list(frame.columns))
    assert "| 000001 | 0 | — |" in text
    assert "A&#124;&#91;link&#93;(x)&lt;script&gt;<br>B" in text
    assert "<script>" not in text
    assert len(text.splitlines()) == 3


def test_portfolio_groups_and_details_have_markdown_block_boundaries():
    text = reader_report(summary(), ["## 9. 组合建议（仅基于当前可获取字段）",
                                    "### Synthetic group", "description", "", "| a | b |",
                                    "| --- | --- |", "| 0 | — |"])
    assert "<summary>Synthetic group</summary>\n\ndescription" in text
    assert "| 0 | — |\n\n</details>" in text
