"""Synthetic tracker I/O and GitHub presentation contracts; no production data."""
from __future__ import annotations

from copy import deepcopy
import json
import posixpath
import re

from openpyxl import load_workbook
import pytest

from backend.github_reports import ReportExportError, sha256, write_json
from backend.github_tracker_view import tracker_view
from scripts.sync_github_reports import merge_export
from tests.test_github_reports import export


def render(output, bundle):
    return tracker_view(bundle, output / "tracker.xlsx", (output / "formal.csv").read_text(encoding="utf-8-sig"),
                        "reports/2026-01-02/synthetic_run/revision_one")


def edit_workbook(output, edit):
    book = load_workbook(output / "tracker.xlsx")
    edit(book)
    book.save(output / "tracker.xlsx")
    book.close()


def test_main_list_preserves_source_precision_null_zero_and_leading_code(tmp_path):
    output, bundle, _, _ = export(tmp_path)
    text = render(output, bundle)
    assert "| 000001 | synthetic fixture | 0 | 1 | 100.00 | 2026-01-01 | 2026-01-01 |" in text
    assert "| 000001 | synthetic fixture | 0 | — | — | synthetic status |" in text
    assert "synthetic exited fixture" not in text
    assert "| 首次新增 | 连续在榜 | 缺席后回归 | 本期掉出 |\n" in text
    assert "| — | — | — | — |" in text
    assert sha256(output / "tracker.xlsx") in text
    assert "Asia/Shanghai" in text and "2026-01-02 09:01:00" in text
    assert max(line.count(" | ") - 1 for line in text.splitlines() if line.startswith("| ")) <= 7


@pytest.mark.parametrize("change,error", [
    (("A4", "000002"), "members_mismatch"),
    (("A4", 1), "members_mismatch"),
    (("C4", "增强债弹性榜"), "formal_field_mismatch"),
    (("D4", "1"), "formal_field_mismatch"),
    (("L4", "0"), "formal_field_mismatch"),  # missing annual return cannot become zero
    (("M4", "unknown"), "formal_field_mismatch"),
    (("I4", "否"), "inactive_member"),
    (("F4", "=1+1"), "formula_unsupported"),
    (("B1", 2), "summary_mismatch"),
    (("D1", "2026-01-02"), "nav_date_mismatch"),
    (("E3", "unrecognized tracking field"), "columns_invalid"),
])
def test_tracker_semantic_mismatch_fails_closed(tmp_path, change, error):
    output, bundle, _, _ = export(tmp_path)
    edit_workbook(output, lambda book: setattr(book["当前在榜"][change[0]], "value", change[1]))
    with pytest.raises(ReportExportError, match=error):
        render(output, bundle)


def test_duplicate_current_rows_and_corrupt_workbook_rejected(tmp_path):
    output, bundle, _, _ = export(tmp_path)
    edit_workbook(output, lambda book: book["当前在榜"].append([c.value for c in book["当前在榜"]['4']]))
    with pytest.raises(ReportExportError, match="members_mismatch"):
        render(output, bundle)
    (output / "tracker.xlsx").write_bytes(b"not a workbook")
    with pytest.raises(ReportExportError, match="workbook_unreadable"):
        render(output, bundle)


def test_order_chunking_and_markdown_escaping(tmp_path):
    output, bundle, _, _ = export(tmp_path)
    special_name = "synthetic | [name]* <x>\nnext"
    def extend(book):
        sheet = book["当前在榜"]
        sheet["B1"] = 31
        sheet["B4"] = special_name
        sheet["M4"] = "[status]* | <tag>"
        for number in range(2, 32):
            row = [c.value for c in sheet['4']]
            row[0] = f"{number:06d}"
            row[1] = f"synthetic {number}"
            sheet.append(row)
    edit_workbook(output, extend)
    import csv
    with (output / "formal.csv").open("w", newline="", encoding="utf-8-sig") as stream:
        writer = csv.writer(stream)
        writer.writerow(["基金代码", "基金名称", "正式榜分组", "正式投资评分", "近1年(%)", "持有期状态"])
        for number in range(31, 0, -1):
            writer.writerow([f"{number:06d}", special_name if number == 1 else f"synthetic {number}",
                             "纯债稳健榜", 0, None, "[status]* | <tag>"])
    bundle["publication"]["counts"].update(final_count=31, stable_bond_final_count=31)
    text = render(output, bundle)
    assert text.count("<details open>") == 1
    assert "第 1–30 项" in text and "第 31–31 项" in text
    assert text.index("| 000001 |") < text.index("| 000002 |") < text.index("| 000031 |")
    assert "synthetic &#124; &#91;name&#93;&#42; &lt;x&gt;<br>next" in text
    assert "&#91;status&#93;&#42; &#124; &lt;tag&gt;" in text
    assert text.count("<details") == text.count("</details>")


def test_sync_writes_linked_tracker_view_without_changing_originals(tmp_path):
    output, bundle, check, _ = export(tmp_path)
    before = {p.name: p.read_bytes() for p in output.iterdir() if p.is_file()}
    repository = tmp_path / "repository"
    repository.mkdir()
    merge_export(repository, check, output)
    latest = json.loads((repository / "latest.json").read_text())
    path = latest["tracker_reading_view"]
    assert path == "views/synthetic_run/revision_one/tracker.md"
    text = (repository / path).read_text()
    assert path in (repository / "README.md").read_text()
    assert "revision_one/tracker.md" in (repository / latest["reading_view"]).read_text()
    assert {p.name: p.read_bytes() for p in (repository / latest["directory"]).iterdir()} == before
    for target in re.findall(r"\]\(([^)]+)\)", text):
        if not target.startswith("#"):
            assert (repository / posixpath.normpath(posixpath.join(posixpath.dirname(path), target))).is_file()
    all_files = {p.relative_to(repository): p.read_bytes() for p in repository.rglob("*") if p.is_file()}
    merge_export(repository, check, output)
    assert all_files == {p.relative_to(repository): p.read_bytes() for p in repository.rglob("*") if p.is_file()}


def test_failed_tracker_render_cannot_advance_latest_or_rewrite_views(tmp_path):
    output, bundle, check, _ = export(tmp_path)
    repository = tmp_path / "repository"
    repository.mkdir()
    merge_export(repository, check, output)
    previous = {p: p.read_bytes() for p in (repository / "views").rglob("*.md")}
    latest = (repository / "latest.json").read_bytes()
    edit_workbook(output, lambda book: setattr(book["当前在榜"]["D4"], "value", "1"))
    updated = deepcopy(bundle)
    updated["publication"]["revision"] = "revision_two"
    for item in updated["files"]:
        if item["path"] == "tracker.xlsx":
            item["sha256"] = sha256(output / item["path"])
            item["bytes"] = (output / item["path"]).stat().st_size
    write_json(output / "bundle.json", updated)
    with pytest.raises(ReportExportError, match="tracker_view_formal_field_mismatch"):
        merge_export(repository, {**check, "publication": updated["publication"]}, output)
    assert (repository / "latest.json").read_bytes() == latest
    assert previous == {p: p.read_bytes() for p in (repository / "views").rglob("*.md")}
