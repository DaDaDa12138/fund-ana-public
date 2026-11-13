"""Read-only GitHub main-list view of a verified cumulative tracker workbook."""
from __future__ import annotations

import csv
import io
from decimal import Decimal, InvalidOperation
from pathlib import Path
import posixpath

from openpyxl import load_workbook

from backend.github_reports import ReportExportError
from backend.report_markdown import markdown_cell as cell
from backend.shanghai_time import display_shanghai


GROUPS = (("增强债弹性榜", "enhanced_bond_final_count"), ("纯债稳健榜", "stable_bond_final_count"))
TRACKING_COLUMNS = ("基金代码", "基金名称", "正式投资评分", "出现次数", "出现频率(%)", "首次出现日期", "最后出现日期")
METRIC_COLUMNS = ("基金代码", "基金名称", "近1月(%)", "近3月(%)", "近1年(%)", "持有期状态")
REQUIRED_COLUMNS = {*TRACKING_COLUMNS, *METRIC_COLUMNS, "正式榜分组", "当前是否在榜"}


def table(headers, rows) -> list[str]:
    return ["| " + " | ".join(map(cell, headers)) + " |",
            "| " + " | ".join("---" for _ in headers) + " |",
            *("| " + " | ".join(map(cell, row)) + " |" for row in rows)]


def _same_value(left, right) -> bool:
    # Excel can store a number as text; compare without rounding or making NULL zero.
    if left is None or str(left).strip() == "":
        return right is None or str(right).strip() == ""
    if right is None or str(right).strip() == "":
        return False
    if str(left) == str(right):
        return True
    try:
        return Decimal(str(left)) == Decimal(str(right))
    except InvalidOperation:
        return False


def _read_current(workbook: Path) -> tuple[dict, list[dict]]:
    try:
        with workbook.open("rb") as stream:
            book = load_workbook(stream, read_only=True, data_only=False)
            try:
                sheet = book["当前在榜"]
                values = iter(sheet.values)
                summary_row = next(values)
                next(values)  # Section labels; row 3 is the existing tracker column contract.
                headers = next(values)
                names = [h for h in headers if h is not None]
                if len(set(names)) != len(names) or not REQUIRED_COLUMNS.issubset(names):
                    raise ReportExportError("tracker_view_columns_invalid")
                summary = {summary_row[i]: summary_row[i + 1]
                           for i in range(0, len(summary_row) - 1, 2) if summary_row[i] is not None}
                rows = []
                for values_row in values:
                    if all(v is None or v == "" for v in values_row):
                        continue
                    row = dict(zip(headers, values_row))
                    if any(isinstance(row.get(key), str) and row[key].startswith("=") for key in REQUIRED_COLUMNS):
                        raise ReportExportError("tracker_view_formula_unsupported")
                    rows.append(row)
                return summary, rows
            finally:
                book.close()
    except ReportExportError:
        raise
    except Exception as error:
        raise ReportExportError("tracker_view_workbook_unreadable") from error


def tracker_view(bundle: dict, workbook: Path, csv_text: str, directory: str) -> str:
    """Render source values only; reject a tracker that differs from the formal list."""
    publication = bundle["publication"]
    counts = publication["counts"]
    summary, rows = _read_current(workbook)
    formal = list(csv.DictReader(io.StringIO(csv_text.lstrip("\ufeff"))))
    codes = [r.get("基金代码") for r in rows]
    formal_codes = [r.get("基金代码") for r in formal]
    if (len(rows) != counts["final_count"] or len(formal) != len(rows)
            or len(set(codes)) != len(codes) or len(set(formal_codes)) != len(formal_codes)
            or any(not isinstance(c, str) or not c.isascii() or not c.isdigit() or len(c) != 6 for c in codes)
            or set(codes) != set(formal_codes)):
        raise ReportExportError("tracker_view_members_mismatch")
    by_code = {r["基金代码"]: r for r in formal}
    for row in rows:
        if row.get("当前是否在榜") != "是":
            raise ReportExportError("tracker_view_inactive_member")
        original = by_code[row["基金代码"]]
        for key in ("基金名称", "正式榜分组", "正式投资评分", *METRIC_COLUMNS[2:]):
            if key in original and not _same_value(row.get(key), original[key]):
                raise ReportExportError("tracker_view_formal_field_mismatch")
    if set(r["正式榜分组"] for r in rows) - {label for label, _ in GROUPS}:
        raise ReportExportError("tracker_view_group_unknown")
    for label, key in GROUPS:
        if sum(r["正式榜分组"] == label for r in rows) != counts.get(key):
            raise ReportExportError("tracker_view_group_count_mismatch")
    for label, key in (("当前在榜数", "final_count"), ("总跟踪基金数", "project_tracker_fund_count"),
                       ("累计跟踪期数", "project_tracker_total_periods"),
                       ("重复出现基金数", "project_tracker_repeated_fund_count")):
        if (label == "当前在榜数" or key in counts) and not _same_value(summary.get(label), counts.get(key)):
            raise ReportExportError("tracker_view_summary_mismatch")
    if summary.get("最新净值日期") != publication.get("nav_date"):
        raise ReportExportError("tracker_view_nav_date_mismatch")
    root = posixpath.relpath(directory, f'views/{publication["run_id"]}/{publication["revision"]}')
    lines = [f'# 正式主榜累计跟踪 · {cell(publication.get("nav_date"))}', "",
             "> 本页对应 tracker.xlsx 的“当前在榜”工作表；仅展示本期正式主榜。时间统一使用 Asia/Shanghai（UTC+08:00）。", "",
             "[增强榜](#增强债弹性榜) · [稳健榜](#纯债稳健榜) · [跟踪口径](#跟踪口径) · [来源校验](#来源校验)", "",
             f'[返回本期报告](../{publication["revision"]}.md) · [完整累计跟踪 Excel]({root}/tracker.xlsx)', "",
             "## 累计概览", "",
             *table(["当前在榜", "历史跟踪基金", "累计期数", "重复出现基金"], [[summary.get(k) for k in
                     ("当前在榜数", "总跟踪基金数", "累计跟踪期数", "重复出现基金数")]]), "",
             *table(["首次新增", "连续在榜", "缺席后回归", "本期掉出"], [[summary.get(k) for k in
                     ("新增入榜", "连续在榜", "缺席后回归", "本期掉出")]]), "",
             f'- 最新业务日期：{cell(summary.get("最新业务日期"))}；净值日期：**{cell(summary.get("最新净值日期"))}**。',
             f'- 正式提交：{cell(display_shanghai(publication.get("committed_at")))}；generation **{publication["generation"]}**。', "",
             "组内保持工作簿原始顺序，不重新排名。评分、次数、频率与收益直接读取原单元格，不重新计算或四舍五入；— 表示缺失，真实零值保留为 0。", ""]
    for label, _ in GROUPS:
        members = [r for r in rows if r["正式榜分组"] == label]
        lines += [f"## {label}", "", f"本期 **{len(members)}** 只。", ""]
        if not members:
            lines += ["本期没有满足正式规则的基金。", ""]
        for start in range(0, len(members), 30):
            chunk = members[start:start + 30]
            if len(members) > 30:
                lines += ["<details" + (" open" if start == 0 else "") + ">",
                          f"<summary>第 {start + 1}–{start + len(chunk)} 项：累计跟踪与当期指标</summary>", ""]
            lines += ["**累计跟踪**", "",
                      *table(["代码", "基金名称", "评分", "出现次数", "出现频率 %", "首次在榜", "最后在榜"],
                             [[r.get(c) for c in TRACKING_COLUMNS] for r in chunk]), "",
                      "**当期指标**", "",
                      *table(["代码", "基金名称", "近1月 %", "近3月 %", "近1年 %", "持有期说明"],
                             [[r.get(c) for c in METRIC_COLUMNS] for r in chunk]), ""]
            if len(members) > 30:
                lines += ["</details>", ""]
    lines += ["## 跟踪口径", "",
              "- 出现次数、出现频率、首末日期及名单变化沿用原累计工作簿口径；不将缺席后回归计作首次新增。",
              "- 累计期数按既有唯一净值日期口径统计；同日修订不额外增加一期。首次和最后在榜日期沿用该历史口径。",
              "- 历史跟踪总数包含已退出基金，不代表当前推荐数量；全部历史记录与完整字段见 Excel。",
              "- 收益列单位为百分比。持有期说明保留原始状态；名称推断不等同合同核实。历史在榜频率不代表未来收益。", "",
              "## 来源校验", "", "<details>", "<summary>展开原文件与正式发布身份</summary>", "",
              f'- 运行：`{publication["run_id"]}`；修订：`{publication["revision"]}`。',
              f'- [原始导出 SHA 清单]({root}/bundle.json) · [原正式 manifest]({root}/source-manifest.json)。']
    for name in ("tracker.xlsx", "formal.csv"):
        digest = next(item["sha256"] for item in bundle["files"] if item["path"] == name)
        lines.append(f'- {name} SHA256：`{digest}`。')
    lines += ["", "当前成员、分组、评分及共同展示指标已与正式 CSV 核对。本页为派生阅读文件，不替代正式发布清单。", "", "</details>", ""]
    return "\n".join(lines)
