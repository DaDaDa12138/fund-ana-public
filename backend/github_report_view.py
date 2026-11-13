"""GitHub reading views derived only from verified presentation files."""
from __future__ import annotations

import csv
import html
import io
import posixpath

from backend.github_reports import ReportExportError
from backend.shanghai_time import display_shanghai
from backend.github_storage import LEGACY_REPOSITORY, repository_name


def cell(value) -> str:
    if value is None or str(value).strip() == "":
        return "—"
    return html.escape(str(value)).replace("|", "&#124;").replace("\r", "").replace("\n", "<br>")


def table(headers, rows) -> list[str]:
    return ["| " + " | ".join(map(cell, headers)) + " |",
            "| " + " | ".join("---" for _ in headers) + " |",
            *("| " + " | ".join(map(cell, row)) + " |" for row in rows)]


def report_view(bundle: dict, csv_text: str, directory: str) -> str:
    publication = bundle["publication"]
    counts = publication["counts"]
    rows = list(csv.DictReader(io.StringIO(csv_text.lstrip("\ufeff"))))
    if len(rows) != counts["final_count"]:
        raise ReportExportError("reading_view_formal_count_mismatch")
    codes = [r.get("基金代码", "") for r in rows]
    if len(set(codes)) != len(codes) or any(not c.isdigit() or len(c) != 6 for c in codes):
        raise ReportExportError("reading_view_formal_codes_invalid")
    groups = [("增强债弹性榜", "enhanced_bond_final_count"), ("纯债稳健榜", "stable_bond_final_count")]
    if set(r.get("正式榜分组") for r in rows) - {label for label, _ in groups}:
        raise ReportExportError("reading_view_formal_group_unknown")
    root = posixpath.relpath(directory, "views/" + publication["run_id"])
    lines = [f'# 每日正式基金报告 · {cell(publication.get("nav_date"))}', "",
             "> 时间统一使用 Asia/Shanghai（UTC+08:00）。本页为核验后生成的阅读版，原始报告和清单保持原样。", "",
             "[增强榜](#增强债弹性榜) · [稳健榜](#纯债稳健榜) · [报告下载](#报告下载) · [数据与校验](#数据与校验)", "",
             "## 本期概览", "",
             *table(["正式基金", "纯债稳健榜", "增强债弹性榜", "累计期数", "累计基金"], [[
                 counts.get("final_count"), counts.get("stable_bond_final_count"), counts.get("enhanced_bond_final_count"),
                 counts.get("project_tracker_total_periods"), counts.get("project_tracker_fund_count")]]), "",
             f'- 净值日期：**{cell(publication.get("nav_date"))}**',
             f'- 正式提交：{cell(display_shanghai(publication.get("committed_at")))}',
             f'- 运行：`{publication["run_id"]}` · generation **{publication["generation"]}**', "",
             f'[正式 Excel]({root}/formal.xlsx) · [CSV]({root}/formal.csv) · [累计跟踪]({root}/tracker.xlsx) · [完整分析报告]({root}/report.md)', "",
             "收益、回撤单位为百分比；评分保留原文件精度。— 表示缺失，真实零值仍显示为 0。表内保持原正式文件相对顺序，无新增排名或投资承诺。", ""]
    columns = ["基金代码", "基金名称", "正式投资评分", "近1年(%)", "1年-最大回撤(%)", "持有期状态"]
    headers = ["代码", "基金名称", "评分", "近1年 %", "1年回撤 %", "持有期说明"]
    for label, count_key in groups:
        members = [r for r in rows if r.get("正式榜分组") == label]
        if len(members) != counts.get(count_key):
            raise ReportExportError("reading_view_group_count_mismatch")
        lines += [f"## {label}", "", f"本期 **{len(members)}** 只。", ""]
        if not members:
            lines += ["本期没有满足正式规则的基金。", ""]
        for start in range(0, len(members), 30):
            chunk = members[start:start + 30]
            collapse = len(members) > 30
            if collapse:
                lines += ["<details" + (" open" if start == 0 else "") + ">",
                          f"<summary>查看第 {start + 1}–{start + len(chunk)} 项</summary>", ""]
            lines += table(headers, [[r.get(c) for c in columns] for r in chunk]) + [""]
            if collapse:
                lines += ["</details>", ""]
    lines += ["## 报告下载", "",
              "| 内容 | 文件 |", "|---|---|",
              f"| 正式名单与当期指标 | [Excel]({root}/formal.xlsx) · [CSV]({root}/formal.csv) |",
              f"| 累计跟踪 | [Excel]({root}/tracker.xlsx) |",
              f"| 完整分析与筛选说明 | [report.md]({root}/report.md) |", "",
              "### 前瞻验证", "",
              "尚未成熟的观察窗口不代表已实现收益；收益和风险口径以原报告所载契约为准。", ""]
    for item in bundle["files"]:
        if item["path"].startswith("forward-validation."):
            lines.append(f'- [{item["path"]}]({root}/{item["path"]})')
    lines += ["", "## 数据与校验", "",
              "天天基金与 Eastmoney 属于同一数据体系；跨接口一致性不等同独立供应商真实性证明。", "",
              "<details>", "<summary>展开运行身份与文件校验信息</summary>", "",
              f'- 修订：`{publication["revision"]}`',
              f'- 原正式清单 SHA256：`{publication["source_manifest_sha256"]}`',
              f'- 原报告 JSON SHA256：`{publication["source_report_sha256"]}`',
              f'- [导出文件 SHA 清单]({root}/bundle.json) · [原正式 manifest]({root}/source-manifest.json)',
              "", "</details>", ""]
    return "\n".join(lines) + "\n"


def report_index(latest: dict | None, check: dict) -> str:
    source = repository_name(check.get("github_repository") or LEGACY_REPOSITORY)
    labels = {"published": "已发布", "unchanged": "净值未推进，保留原正式报告",
              "published_with_warnings": "已发布，存在已隔离的数据警告",
              "unchanged_with_warnings": "原正式报告已核验，存在已隔离的数据警告",
              "verified_existing_publication": "仅核验已有报告，未发起采集", "failed": "执行失败，查看原因"}
    lines = ["# Fund-ANA 每日正式报告", "",
             "> 时间统一使用 **Asia/Shanghai（UTC+08:00）**。本私有分支保存核验后的正式报告和执行收据，源码与执行在独立仓库。", "",
             "## 最新执行", "",
             *table(["项目", "结果"], [
                 ["自动化结果", labels[check["status"]]],
                 ["检查时间", display_shanghai(check.get("finished_at") or check.get("checked_at"))],
                 ["任务原始状态", check.get("task_status") or "未执行采集"],
                 ["数据警告", check.get("task_warning_reason")],
                 ["失败原因", check.get("error_code")],
             ]), "",
             f'[本次 Actions](https://github.com/{source}/actions/runs/{check["github_run_id"]}) · '
             f'[执行收据](checks/{check["check_date"]}/{check["github_run_id"]}-{check["attempt"]}.json)', "",
             "数据警告不会被删除。只有隔离证据、正式排除、发布门禁、API、状态持久化和报告同步均通过，自动化才可带警告完成；其他失败仍为 failed。", ""]
    if latest:
        pub = latest["publication"]; counts = pub["counts"]
        view = latest.get("reading_view")
        lines += ["## 当前正式报告", "",
                  *table(["净值日期", "正式基金", "稳健", "增强", "累计期数", "累计基金"], [[
                      pub.get("nav_date"), counts.get("final_count"), counts.get("stable_bond_final_count"),
                      counts.get("enhanced_bond_final_count"), counts.get("project_tracker_total_periods"), counts.get("project_tracker_fund_count")]]), "",
                  f'- 正式提交：{cell(display_shanghai(pub.get("committed_at")))}',
                  f'- 运行：`{pub["run_id"]}` · generation **{pub["generation"]}**', ""]
        if view:
            lines += [f'### [打开本期阅读版报告]({view})', ""]
        labels = {"formal.xlsx": "正式名单 Excel", "formal.csv": "正式名单 CSV",
                  "tracker.xlsx": "累计跟踪", "report.md": "完整分析报告"}
        lines += ["| 报告内容 | 打开或下载 |", "|---|---|"]
        lines += [f'| {labels[name]} | [{name}]({latest["directory"]}/{name}) |'
                  for name in latest["files"] if name in labels]
        additional = [name for name in latest["files"] if name not in labels]
        if additional:
            lines += ["", "<details>", "<summary>前瞻验证与原始校验清单</summary>", ""]
            lines += [f'- [{name}]({latest["directory"]}/{name})' for name in additional]
            lines += ["", "</details>"]
        lines += ["", "## 历史与证据", "",
                  "[历史报告](reports/) · [执行收据](checks/)", "",
                  "无新净值时保留原报告日期；同步或核验旧报告不表示完成新采集。原报告、清单及 SHA 不改写；阅读版由已核验 CSV 生成。", "",
                  "数据库、收藏、凭据、原始响应和冻结输入不在此分支。报告文件不是完整运行备份。", ""]
    return "\n".join(lines)
