"""Reader layout for generated reports; business facts remain in their source payload."""
from __future__ import annotations

import html
import math
import re
from typing import Any

from backend.shanghai_time import display_shanghai


def markdown_cell(value: Any) -> str:
    if value is None or str(value).strip() in {"", "nan", "NaN", "<NA>", "NaT"}:
        return "—"
    if isinstance(value, float) and not math.isfinite(value):
        return "—"
    text = html.escape(str(value), quote=False)
    for char, encoded in (("|", "&#124;"), ("*", "&#42;"), ("_", "&#95;"),
                          ("[", "&#91;"), ("]", "&#93;"), ("`", "&#96;"), ("\\", "&#92;")):
        text = text.replace(char, encoded)
    return text.replace("\r\n", "\n").replace("\r", "\n").replace("\n", "<br>")


def _table(headers: list[str], rows: list[list[Any]]) -> list[str]:
    return ["| " + " | ".join(map(markdown_cell, headers)) + " |",
            "| " + " | ".join("---" for _ in headers) + " |",
            *("| " + " | ".join(map(markdown_cell, row)) + " |" for row in rows)]


def _sections(lines: list[str]) -> list[tuple[str, str]]:
    sections: list[tuple[str, str]] = []
    title, body = "", []
    for line in "\n".join(lines).splitlines():
        if line.startswith("## "):
            sections.append((title, "\n".join(body).strip()))
            title = re.sub(r"^\d+(?:\.\d+)*\.?(?:\s+|$)", "", line[3:])
            body = []
        else:
            body.append(line)
    sections.append((title, "\n".join(body).strip()))
    return sections


def _details(title: str, body: str) -> list[str]:
    return ["<details>", f"<summary>{html.escape(title)}</summary>", "", body, "", "</details>", ""]


def reader_report(summary: dict[str, Any], source_lines: list[str]) -> str:
    counts = summary["counts"]
    snapshot = (summary.get("database_freshness") or {}).get("effective_nav_snapshot") or {}
    nav_date = snapshot.get("candidate_date")
    generated = display_shanghai(summary.get("generated_at"))
    control = summary.get("publication_control") or {}
    mode = control.get("mode")
    status = ("诊断/验证报告" if mode in {"diagnostic_only", "validation_only"} else
              "报告已生成；正式状态以发布清单及当前指针为准")
    lines = ["# 债券基金筛选报告", "",
             "> 时间：Asia/Shanghai（UTC+08:00）。本页展示当期筛选结果；预览保留来源顺序。", "",
             "[本期概览](#本期概览) · [基金预览](#基金预览) · [组合建议](#组合建议) · "
             "[跟踪与验证](#跟踪与验证) · [筛选与审计](#筛选与审计) · [数据与产物](#数据与产物)", "",
             "## 本期概览", "",
             *_table(["净值日期", "正式榜", "纯债稳健", "增强债弹性"], [[
                 nav_date, counts.get("final_count"), counts.get("stable_bond_final_count"),
                 counts.get("enhanced_bond_final_count")]]), "",
             f"- 生成时间：{markdown_cell(generated)}",
             f"- 运行批次：`{summary['run_id']}`",
             f"- 发布说明：{status}。", "",
             "## 基金预览", "",
             "以下为当期预览；完整名单及指标见正式Excel/CSV。—表示缺失，真实零值显示为0。", ""]
    sections = _sections(source_lines)
    for title, body in sections:
        if title in {"纯债稳健榜预览", "增强债弹性榜预览"}:
            lines += [f"### {title}", "", body, ""]
    lines += ["## 组合建议", "",
              "按当前可获取字段生成的研究参考；详细指标、入选原因与风险提示可展开查看。", ""]
    for title, body in sections:
        if title.startswith("组合建议"):
            parts = re.split(r"(?m)^### ", body)
            if parts[0].strip():
                lines += [parts[0].strip(), ""]
            for part in parts[1:]:
                name, _, content = part.partition("\n")
                lines += _details(name.strip(), content.strip())
    lines += ["## 跟踪与验证", "",
              *_table(["累计期数", "累计基金", "当期在榜", "重复出现"], [[
                  counts.get("project_tracker_total_periods"), counts.get("project_tracker_fund_count"),
                  counts.get("project_tracker_current_active_count"), counts.get("project_tracker_repeated_fund_count")]]), ""]
    forward = ((summary.get("project_publish_summary") or {}).get("forward_validation_summary") or {})
    lines += [f"前瞻验证：{markdown_cell(forward.get('status_label'))}。未成熟窗口不代表已实现收益。", ""]
    for title, body in sections:
        if title in {"长期稳定性数据就绪情况", "前瞻验证闭环", "正式推荐结构摘要"}:
            lines += _details(title, body)
    lines += ["## 筛选与审计", "",
              "候选池、诊断池与正式榜使用各自口径；完整执行记录与门禁明细如下。", ""]
    shown = {"", "纯债稳健榜预览", "增强债弹性榜预览", "正式推荐结构摘要", "累积跟踪摘要",
             "长期稳定性数据就绪情况", "前瞻验证闭环", "交付产物", "优化项"}
    for title, body in sections:
        if title not in shown and not title.startswith("组合建议"):
            if title == "结论":
                continue
            lines += _details(title, body)
    lines += ["## 数据与产物", "",
              "天天基金/Eastmoney属于同一数据体系，跨接口一致不等于独立来源真实性验证。", ""]
    artifacts = next((body for title, body in sections if title == "交付产物"), "")
    lines += _details("产物路径索引", artifacts or "—")
    provenance = [f"- {label}：`{value}`" for label, value in (summary.get("data_sources") or {}).items()]
    lines += _details("来源与运行环境", "\n".join(provenance) or "—")
    optimizations = next((body for title, body in sections if title == "优化项"), "")
    if optimizations:
        lines += _details("实现与治理说明", optimizations)
    return "\n".join(lines).rstrip() + "\n"
